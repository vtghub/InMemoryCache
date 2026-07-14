use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

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
        let bin = env!("CARGO_BIN_EXE_imcache-server");
        let mut cmd = Command::new(bin);
        cmd.arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg("0") // let the OS assign a free port; we read it back below
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--snapshot-interval-secs")
            .arg("0") // disable the periodic timer; tests trigger BGSAVE explicitly
            .args(extra_args)
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
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
