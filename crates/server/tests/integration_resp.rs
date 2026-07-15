use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use futures::StreamExt;
use redis::AsyncCommands;

struct TestServer {
    child: Child,
    port: u16,
}

impl TestServer {
    async fn spawn() -> (Self, tempfile::TempDir) {
        let data_dir = tempfile::tempdir().expect("create temp data dir");
        let server = Self::spawn_in(data_dir.path(), &[]).await;
        (server, data_dir)
    }

    async fn spawn_in(data_dir: &Path, extra_args: &[&str]) -> Self {
        let mut args: Vec<String> = vec![
            "--bind".into(),
            "127.0.0.1".into(),
            "--port".into(),
            "0".into(), // let the OS assign a free port; we read it back below
            "--data-dir".into(),
            data_dir.to_string_lossy().into_owned(),
            "--snapshot-interval-secs".into(),
            "0".into(), // disable the periodic timer; tests trigger BGSAVE explicitly
        ];
        args.extend(extra_args.iter().map(|s| s.to_string()));
        Self::spawn_with_args(&args).await
    }

    /// Lower-level spawn for tests that need full control over the CLI
    /// args (e.g. a fixed `--port` and `--cluster-nodes`, which must be
    /// known before the process starts).
    async fn spawn_with_args(args: &[String]) -> Self {
        let bin = env!("CARGO_BIN_EXE_imcache-server");
        let mut cmd = Command::new(bin);
        cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::null());
        let mut child = cmd.spawn().expect("spawn imcache-server");
        let port = read_ready_port(child.stdout.take().expect("piped stdout"));
        Self { child, port }
    }

    async fn client(&self) -> redis::aio::MultiplexedConnection {
        let url = format!("redis://127.0.0.1:{}/", self.port);
        let client = redis::Client::open(url).expect("build redis client");
        client
            .get_multiplexed_async_connection()
            .await
            .expect("connect to imcache-server")
    }

    /// Kills the process, simulating a restart (used to test AOF/snapshot
    /// recovery against the same, still-on-disk data directory).
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Reads the server's `IMCACHE_READY port=NNNN` line off its stdout to
/// learn which OS-assigned port it bound (started with `--port 0`), then
/// keeps draining stdout in the background so the child never blocks on a
/// full pipe buffer.
fn read_ready_port(stdout: std::process::ChildStdout) -> u16 {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut sent = false;
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    if !sent {
                        if let Some(rest) = line.trim().strip_prefix("IMCACHE_READY port=") {
                            if let Ok(port) = rest.parse::<u16>() {
                                let _ = tx.send(port);
                                sent = true;
                            }
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(5))
        .expect("server never printed its ready line")
}

#[tokio::test]
async fn get_set_del_roundtrip() {
    let (server, _dir) = TestServer::spawn().await;
    let mut conn = server.client().await;

    let _: () = conn.set("greeting", "hello").await.unwrap();
    let value: String = conn.get("greeting").await.unwrap();
    assert_eq!(value, "hello");

    let existed: i64 = conn.del("greeting").await.unwrap();
    assert_eq!(existed, 1);

    let missing: Option<String> = conn.get("greeting").await.unwrap();
    assert_eq!(missing, None);
}

#[tokio::test]
async fn expire_removes_key_after_ttl() {
    let (server, _dir) = TestServer::spawn().await;
    let mut conn = server.client().await;

    let _: () = conn.set("temp", "gone-soon").await.unwrap();
    let _: bool = conn.pexpire("temp", 150).await.unwrap();

    let still_there: Option<String> = conn.get("temp").await.unwrap();
    assert_eq!(still_there.as_deref(), Some("gone-soon"));

    tokio::time::sleep(Duration::from_millis(800)).await;

    let gone: Option<String> = conn.get("temp").await.unwrap();
    assert_eq!(gone, None);
}

#[tokio::test]
async fn incr_and_append_work() {
    let (server, _dir) = TestServer::spawn().await;
    let mut conn = server.client().await;

    let n: i64 = conn.incr("counter", 1).await.unwrap();
    assert_eq!(n, 1);
    let n: i64 = conn.incr("counter", 1).await.unwrap();
    assert_eq!(n, 2);

    let _: () = conn.set("s", "foo").await.unwrap();
    let len: i64 = conn.append("s", "bar").await.unwrap();
    assert_eq!(len, 6);
    let value: String = conn.get("s").await.unwrap();
    assert_eq!(value, "foobar");
}

#[tokio::test]
async fn restart_recovers_from_aof() {
    let dir = tempfile::tempdir().expect("create temp data dir");
    let mut server = TestServer::spawn_in(dir.path(), &[]).await;

    {
        let mut conn = server.client().await;
        let _: () = conn.set("durable-key", "durable-value").await.unwrap();
        let _: () = conn.set("another-key", "another-value").await.unwrap();
    }
    server.kill();

    let restarted = TestServer::spawn_in(dir.path(), &[]).await;
    let mut conn = restarted.client().await;
    let value: String = conn.get("durable-key").await.unwrap();
    assert_eq!(value, "durable-value");
    let value2: String = conn.get("another-key").await.unwrap();
    assert_eq!(value2, "another-value");
}

#[tokio::test]
async fn bgsave_snapshot_survives_restart() {
    let dir = tempfile::tempdir().expect("create temp data dir");
    let mut server = TestServer::spawn_in(dir.path(), &[]).await;

    {
        let mut conn = server.client().await;
        let _: () = conn.set("snap-key", "snap-value").await.unwrap();
        let _: String = redis::cmd("BGSAVE").query_async(&mut conn).await.unwrap();
        // Give the background save task a moment to finish writing.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    server.kill();

    assert!(
        dir.path().join("dump.rdb").exists(),
        "expected a snapshot file to be written"
    );

    let restarted = TestServer::spawn_in(dir.path(), &[]).await;
    let mut conn = restarted.client().await;
    let value: String = conn.get("snap-key").await.unwrap();
    assert_eq!(value, "snap-value");
}

#[tokio::test]
async fn lru_eviction_bounds_shard_size() {
    let (server, _dir) = {
        let dir = tempfile::tempdir().expect("create temp data dir");
        let server = TestServer::spawn_in(dir.path(), &["--max-keys-per-shard", "1"]).await;
        (server, dir)
    };
    let mut conn = server.client().await;

    for i in 0..200 {
        let _: () = conn.set(format!("key-{i}"), "v").await.unwrap();
    }

    let mut present = 0;
    for i in 0..200 {
        let exists: i64 = conn.exists(format!("key-{i}")).await.unwrap();
        present += exists;
    }
    // 16 shards * 1 key max => at most 16 keys should remain resident.
    assert!(
        present <= 16,
        "expected eviction to bound resident keys, found {present}"
    );
    assert!(present > 0, "expected at least some keys to remain");
}

#[tokio::test]
async fn list_operations() {
    let (server, _dir) = TestServer::spawn().await;
    let mut conn = server.client().await;

    let len: i64 = conn.rpush("mylist", &["a", "b", "c"]).await.unwrap();
    assert_eq!(len, 3);
    let len: i64 = conn.lpush("mylist", "z").await.unwrap();
    assert_eq!(len, 4);

    let range: Vec<String> = conn.lrange("mylist", 0, -1).await.unwrap();
    assert_eq!(range, vec!["z", "a", "b", "c"]);

    let llen: i64 = conn.llen("mylist").await.unwrap();
    assert_eq!(llen, 4);

    let popped: String = conn.lpop("mylist", None).await.unwrap();
    assert_eq!(popped, "z");
    let popped: String = redis::cmd("RPOP")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(popped, "c");

    // Pop the rest down to empty; the key should then disappear entirely.
    let _: String = conn.lpop("mylist", None).await.unwrap();
    let _: String = conn.lpop("mylist", None).await.unwrap();
    let exists: i64 = conn.exists("mylist").await.unwrap();
    assert_eq!(exists, 0);

    let missing: Option<String> = conn.lpop("mylist", None).await.unwrap();
    assert_eq!(missing, None);
}

#[tokio::test]
async fn hash_operations() {
    let (server, _dir) = TestServer::spawn().await;
    let mut conn = server.client().await;

    let added: i64 = conn.hset("myhash", "f1", "v1").await.unwrap();
    assert_eq!(added, 1);
    let added: i64 = conn.hset("myhash", "f1", "v1-updated").await.unwrap();
    assert_eq!(added, 0, "overwriting an existing field adds nothing new");

    let v: String = conn.hget("myhash", "f1").await.unwrap();
    assert_eq!(v, "v1-updated");

    let _: i64 = conn.hset("myhash", "f2", "v2").await.unwrap();
    let exists: bool = conn.hexists("myhash", "f2").await.unwrap();
    assert!(exists);

    let all: HashMap<String, String> = conn.hgetall("myhash").await.unwrap();
    assert_eq!(all.get("f1").map(String::as_str), Some("v1-updated"));
    assert_eq!(all.get("f2").map(String::as_str), Some("v2"));

    let removed: i64 = conn.hdel("myhash", &["f1", "f2"]).await.unwrap();
    assert_eq!(removed, 2);
    let exists: i64 = conn.exists("myhash").await.unwrap();
    assert_eq!(exists, 0, "removing every field should delete the key");
}

#[tokio::test]
async fn set_operations() {
    let (server, _dir) = TestServer::spawn().await;
    let mut conn = server.client().await;

    let added: i64 = conn.sadd("myset", &["a", "b", "c"]).await.unwrap();
    assert_eq!(added, 3);
    let added: i64 = conn.sadd("myset", "a").await.unwrap();
    assert_eq!(added, 0, "adding a duplicate member adds nothing new");

    let is_member: bool = conn.sismember("myset", "b").await.unwrap();
    assert!(is_member);
    let is_member: bool = conn.sismember("myset", "missing").await.unwrap();
    assert!(!is_member);

    let mut members: Vec<String> = conn.smembers("myset").await.unwrap();
    members.sort();
    assert_eq!(members, vec!["a", "b", "c"]);

    let removed: i64 = conn.srem("myset", &["a", "b", "c"]).await.unwrap();
    assert_eq!(removed, 3);
    let exists: i64 = conn.exists("myset").await.unwrap();
    assert_eq!(exists, 0, "removing every member should delete the key");
}

#[tokio::test]
async fn zset_operations() {
    let (server, _dir) = TestServer::spawn().await;
    let mut conn = server.client().await;

    let added: i64 = conn
        .zadd_multiple("myzset", &[(1.0, "one"), (3.0, "three"), (2.0, "two")])
        .await
        .unwrap();
    assert_eq!(added, 3);

    let range: Vec<String> = conn.zrange("myzset", 0, -1).await.unwrap();
    assert_eq!(
        range,
        vec!["one", "two", "three"],
        "expected ascending-by-score order"
    );

    let with_scores: Vec<(String, f64)> = conn.zrange_withscores("myzset", 0, -1).await.unwrap();
    assert_eq!(
        with_scores,
        vec![
            ("one".to_string(), 1.0),
            ("two".to_string(), 2.0),
            ("three".to_string(), 3.0),
        ]
    );

    let score: f64 = conn.zscore("myzset", "two").await.unwrap();
    assert_eq!(score, 2.0);

    let removed: i64 = conn.zrem("myzset", &["one", "two", "three"]).await.unwrap();
    assert_eq!(removed, 3);
    let exists: i64 = conn.exists("myzset").await.unwrap();
    assert_eq!(exists, 0, "removing every member should delete the key");
}

#[tokio::test]
async fn wrongtype_errors() {
    let (server, _dir) = TestServer::spawn().await;
    let mut conn = server.client().await;

    let _: () = conn.set("strkey", "hello").await.unwrap();

    let err = conn
        .lpush::<_, _, ()>("strkey", "x")
        .await
        .expect_err("LPUSH on a string key should fail");
    assert!(err.to_string().contains("WRONGTYPE"), "got: {err}");

    let err = conn
        .hset::<_, _, _, ()>("strkey", "f", "v")
        .await
        .expect_err("HSET on a string key should fail");
    assert!(err.to_string().contains("WRONGTYPE"), "got: {err}");

    let err = conn
        .sadd::<_, _, ()>("strkey", "m")
        .await
        .expect_err("SADD on a string key should fail");
    assert!(err.to_string().contains("WRONGTYPE"), "got: {err}");
}

#[tokio::test]
async fn pubsub_basic() {
    let (server, _dir) = TestServer::spawn().await;

    let url = format!("redis://127.0.0.1:{}/", server.port);
    let client = redis::Client::open(url).unwrap();
    let mut pubsub_conn = client.get_async_pubsub().await.unwrap();
    pubsub_conn.subscribe("news").await.unwrap();
    let mut stream = pubsub_conn.on_message();

    let mut publisher = server.client().await;
    // Give the subscription a moment to register before publishing.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let receivers: i64 = redis::cmd("PUBLISH")
        .arg("news")
        .arg("hello subscribers")
        .query_async(&mut publisher)
        .await
        .unwrap();
    assert_eq!(receivers, 1);

    let msg = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("timed out waiting for pubsub message")
        .expect("stream ended unexpectedly");
    assert_eq!(msg.get_channel_name(), "news");
    let payload: String = msg.get_payload().unwrap();
    assert_eq!(payload, "hello subscribers");
}

#[tokio::test]
async fn restart_recovers_rich_types() {
    let dir = tempfile::tempdir().expect("create temp data dir");
    let mut server = TestServer::spawn_in(dir.path(), &[]).await;

    {
        let mut conn = server.client().await;
        let _: i64 = conn.rpush("list", &["a", "b"]).await.unwrap();
        let _: i64 = conn.hset("hash", "f", "v").await.unwrap();
        let _: i64 = conn.sadd("set", &["x", "y"]).await.unwrap();
        let _: i64 = conn
            .zadd_multiple("zset", &[(1.0, "m1"), (2.0, "m2")])
            .await
            .unwrap();
    }
    server.kill();

    let restarted = TestServer::spawn_in(dir.path(), &[]).await;
    let mut conn = restarted.client().await;

    let list: Vec<String> = conn.lrange("list", 0, -1).await.unwrap();
    assert_eq!(list, vec!["a", "b"]);

    let hash_val: String = conn.hget("hash", "f").await.unwrap();
    assert_eq!(hash_val, "v");

    let mut set_members: Vec<String> = conn.smembers("set").await.unwrap();
    set_members.sort();
    assert_eq!(set_members, vec!["x", "y"]);

    let zset: Vec<String> = conn.zrange("zset", 0, -1).await.unwrap();
    assert_eq!(zset, vec!["m1", "m2"]);
}

/// Reserves `n` distinct free ports by holding all of them open at once
/// (so they can't collide with each other) and then releasing them right
/// before the caller re-binds them — needed because a static cluster's
/// `--cluster-nodes` list must name every node's port before any node has
/// started.
fn reserve_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

#[tokio::test]
async fn replication_basic() {
    let (primary, _primary_dir) = TestServer::spawn().await;
    let primary_addr = format!("127.0.0.1:{}", primary.port);

    let replica_dir = tempfile::tempdir().expect("create temp data dir");
    let replica = TestServer::spawn_in(replica_dir.path(), &["--replicaof", &primary_addr]).await;

    let mut primary_conn = primary.client().await;
    let mut replica_conn = replica.client().await;

    // Wait for the replica's initial full sync to complete before writing,
    // so we're specifically exercising the streamed-write path.
    for _ in 0..30 {
        let pong: String = redis::cmd("PING")
            .query_async(&mut replica_conn)
            .await
            .unwrap();
        if pong == "PONG" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _: () = primary_conn.set("repl-key", "repl-value").await.unwrap();

    let mut got = None;
    for _ in 0..40 {
        let v: Option<String> = replica_conn.get("repl-key").await.unwrap();
        if v.is_some() {
            got = v;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        got.as_deref(),
        Some("repl-value"),
        "write on primary did not propagate to replica"
    );

    let err = replica_conn
        .set::<_, _, ()>("x", "y")
        .await
        .expect_err("a direct write to a replica should be rejected");
    assert_eq!(err.kind(), redis::ErrorKind::ReadOnly, "got: {err}");
}

#[tokio::test]
async fn replica_full_sync_carries_existing_data() {
    let (primary, _primary_dir) = TestServer::spawn().await;
    let mut primary_conn = primary.client().await;
    let _: () = primary_conn.set("pre-existing", "value").await.unwrap();

    let primary_addr = format!("127.0.0.1:{}", primary.port);
    let replica_dir = tempfile::tempdir().expect("create temp data dir");
    let replica = TestServer::spawn_in(replica_dir.path(), &["--replicaof", &primary_addr]).await;
    let mut replica_conn = replica.client().await;

    let mut got = None;
    for _ in 0..40 {
        let v: Option<String> = replica_conn.get("pre-existing").await.unwrap();
        if v.is_some() {
            got = v;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        got.as_deref(),
        Some("value"),
        "initial full sync did not carry pre-existing data"
    );
}

/// Mirrors `cluster::crc16` in the server crate (not reachable from an
/// integration test, since this is a bin crate with no lib target) so the
/// test can deterministically pick a key owned by node B rather than
/// probing for one.
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

fn find_key_owned_by_node(node_index: usize, num_nodes: u32) -> String {
    for i in 0.. {
        let key = format!("probe-{i}");
        let slot = crc16(key.as_bytes()) % 16384;
        let owner = (slot as u32 * num_nodes / 16384) as usize;
        if owner == node_index {
            return key;
        }
        assert!(
            i < 100_000,
            "couldn't find a key owned by node {node_index}"
        );
    }
    unreachable!()
}

#[tokio::test]
async fn cluster_moved_redirect_and_correct_routing() {
    let ports = reserve_ports(2);
    let nodes_spec = format!("127.0.0.1:{},127.0.0.1:{}", ports[0], ports[1]);
    let dir_a = tempfile::tempdir().expect("create temp data dir");
    let dir_b = tempfile::tempdir().expect("create temp data dir");

    let node_a = TestServer::spawn_with_args(&[
        "--bind".into(),
        "127.0.0.1".into(),
        "--port".into(),
        ports[0].to_string(),
        "--data-dir".into(),
        dir_a.path().to_string_lossy().into_owned(),
        "--snapshot-interval-secs".into(),
        "0".into(),
        "--cluster-nodes".into(),
        nodes_spec.clone(),
        "--cluster-self-index".into(),
        "0".into(),
    ])
    .await;
    let node_b = TestServer::spawn_with_args(&[
        "--bind".into(),
        "127.0.0.1".into(),
        "--port".into(),
        ports[1].to_string(),
        "--data-dir".into(),
        dir_b.path().to_string_lossy().into_owned(),
        "--snapshot-interval-secs".into(),
        "0".into(),
        "--cluster-nodes".into(),
        nodes_spec.clone(),
        "--cluster-self-index".into(),
        "1".into(),
    ])
    .await;

    let key_for_b = find_key_owned_by_node(1, 2);

    // Writing a key owned by node B, through node A, must fail with MOVED.
    let mut conn_a = node_a.client().await;
    let err = conn_a
        .set::<_, _, ()>(&key_for_b, "v")
        .await
        .expect_err("writing a foreign-slot key should be redirected");
    assert_eq!(err.kind(), redis::ErrorKind::Moved, "got: {err}");
    let msg = err.to_string();
    assert!(
        msg.contains(&ports[1].to_string()),
        "MOVED should point at node B's port, got: {msg}"
    );

    // Writing it directly on node B (its actual owner) must succeed.
    let mut conn_b = node_b.client().await;
    let _: () = conn_b.set(&key_for_b, "owned-by-b").await.unwrap();
    let v: String = conn_b.get(&key_for_b).await.unwrap();
    assert_eq!(v, "owned-by-b");
}

#[tokio::test]
async fn cluster_slots_command_reports_full_coverage() {
    let ports = reserve_ports(2);
    let nodes_spec = format!("127.0.0.1:{},127.0.0.1:{}", ports[0], ports[1]);
    let dir_a = tempfile::tempdir().expect("create temp data dir");
    let dir_b = tempfile::tempdir().expect("create temp data dir");

    let node_a = TestServer::spawn_with_args(&[
        "--bind".into(),
        "127.0.0.1".into(),
        "--port".into(),
        ports[0].to_string(),
        "--data-dir".into(),
        dir_a.path().to_string_lossy().into_owned(),
        "--snapshot-interval-secs".into(),
        "0".into(),
        "--cluster-nodes".into(),
        nodes_spec.clone(),
        "--cluster-self-index".into(),
        "0".into(),
    ])
    .await;
    let _node_b = TestServer::spawn_with_args(&[
        "--bind".into(),
        "127.0.0.1".into(),
        "--port".into(),
        ports[1].to_string(),
        "--data-dir".into(),
        dir_b.path().to_string_lossy().into_owned(),
        "--snapshot-interval-secs".into(),
        "0".into(),
        "--cluster-nodes".into(),
        nodes_spec,
        "--cluster-self-index".into(),
        "1".into(),
    ])
    .await;

    let mut conn = node_a.client().await;
    let slots: Vec<(i64, i64, (String, i64))> = redis::cmd("CLUSTER")
        .arg("SLOTS")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(slots.len(), 2, "expected one contiguous range per node");
    let total: i64 = slots.iter().map(|(start, end, _)| end - start + 1).sum();
    assert_eq!(total, 16384);
}
