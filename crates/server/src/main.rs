mod cluster;
mod commands;
mod expiry;
mod persistence;
mod pubsub;
mod replication;
mod resp;
mod shard;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use clap::Parser;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamMap;
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

use cluster::ClusterConfig;
use commands::Command;
use persistence::aof::{Aof, FsyncPolicy};
use pubsub::PubSub;
use replication::ReplicationHub;
use resp::{Reply, RespCodec};
use shard::{Entry, Store};

#[derive(Parser, Debug)]
#[command(
    name = "imcache-server",
    about = "Fast local in-memory cache, RESP-compatible"
)]
struct Args {
    /// Address to bind the TCP listener to.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,

    /// Port to listen on.
    #[arg(long, default_value_t = 6380)]
    port: u16,

    /// Directory for the AOF log and snapshot file.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// AOF fsync policy: always | everysec | no.
    #[arg(long, default_value = "everysec")]
    fsync: String,

    /// Maximum keys per shard before LRU eviction kicks in (unbounded if unset).
    #[arg(long)]
    max_keys_per_shard: Option<usize>,

    /// How often to write a full snapshot and rotate the AOF, in seconds.
    #[arg(long, default_value_t = 300)]
    snapshot_interval_secs: u64,

    /// Address of a primary to replicate from (host:port). When set, this
    /// node becomes a read-only replica: it full-syncs from the primary on
    /// startup and rejects writes from ordinary clients.
    #[arg(long)]
    replicaof: Option<String>,

    /// Comma-separated `host:port` list of every node in the cluster, in a
    /// fixed order shared by all nodes. Must be set together with
    /// `--cluster-self-index`.
    #[arg(long)]
    cluster_nodes: Option<String>,

    /// This node's position (0-based) in `--cluster-nodes`.
    #[arg(long)]
    cluster_self_index: Option<usize>,
}

struct Paths {
    snapshot: PathBuf,
    aof: PathBuf,
}

fn parse_cluster_nodes(spec: &str) -> anyhow::Result<Vec<(String, u16)>> {
    spec.split(',')
        .map(|entry| {
            let (host, port) = entry.rsplit_once(':').ok_or_else(|| {
                anyhow::anyhow!("invalid cluster node '{entry}', expected host:port")
            })?;
            let port: u16 = port
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid port in cluster node '{entry}'"))?;
            Ok((host.to_string(), port))
        })
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs go to stderr so stdout is reserved for the single machine-readable
    // "ready" line below, which tests rely on to discover the bound port.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    let fsync_policy = FsyncPolicy::parse(&args.fsync)
        .ok_or_else(|| anyhow::anyhow!("invalid --fsync value: {}", args.fsync))?;

    std::fs::create_dir_all(&args.data_dir)?;
    let paths = Paths {
        snapshot: args.data_dir.join("dump.rdb"),
        aof: args.data_dir.join("appendonly.aof"),
    };

    let store = Arc::new(Store::new(args.max_keys_per_shard));

    let loaded_snapshot = persistence::snapshot::load(&store, &paths.snapshot)?;
    if loaded_snapshot {
        info!(path = %paths.snapshot.display(), "loaded snapshot");
    }
    persistence::aof::replay(&paths.aof, &store)?;
    info!(path = %paths.aof.display(), "replayed AOF");

    let aof = Arc::new(Aof::open(paths.aof.clone(), fsync_policy)?);
    let pubsub = Arc::new(PubSub::new());
    let replication = Arc::new(ReplicationHub::new());

    let cluster: Option<Arc<ClusterConfig>> = match (&args.cluster_nodes, args.cluster_self_index) {
        (Some(spec), Some(self_index)) => {
            let nodes = parse_cluster_nodes(spec)?;
            if self_index >= nodes.len() {
                anyhow::bail!(
                    "--cluster-self-index {self_index} out of range for {} cluster nodes",
                    nodes.len()
                );
            }
            info!(nodes = nodes.len(), self_index, "cluster mode enabled");
            Some(Arc::new(ClusterConfig::new(nodes, self_index)))
        }
        (None, None) => None,
        _ => anyhow::bail!("--cluster-nodes and --cluster-self-index must be set together"),
    };

    // Fixed for the process lifetime: v1 has no runtime REPLICAOF/promotion.
    let read_only = args.replicaof.is_some();

    expiry::spawn_active_expire_cycle(store.clone());
    spawn_periodic_snapshot(
        store.clone(),
        aof.clone(),
        paths.snapshot.clone(),
        args.snapshot_interval_secs,
    );
    if fsync_policy == FsyncPolicy::EverySec {
        spawn_everysec_fsync(aof.clone());
    }
    if let Some(primary_addr) = args.replicaof.clone() {
        info!(%primary_addr, "starting as replica");
        spawn_replica_client(
            store.clone(),
            aof.clone(),
            pubsub.clone(),
            replication.clone(),
            primary_addr,
        );
    }

    let listener = TcpListener::bind((args.bind.as_str(), args.port)).await?;
    let bound_port = listener.local_addr()?.port();
    info!(bind = %args.bind, port = bound_port, "imcache-server listening");
    // Machine-readable line for callers (e.g. tests) that started us with
    // `--port 0` and need to discover the OS-assigned port.
    println!("IMCACHE_READY port={bound_port}");
    use std::io::Write;
    std::io::stdout().flush().ok();

    loop {
        let (socket, peer) = listener.accept().await?;
        let store = store.clone();
        let aof = aof.clone();
        let pubsub = pubsub.clone();
        let replication = replication.clone();
        let cluster = cluster.clone();
        let snapshot_path = paths.snapshot.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(
                socket,
                store,
                aof,
                pubsub,
                replication,
                cluster,
                read_only,
                snapshot_path,
            )
            .await
            {
                warn!(%peer, error = %e, "connection closed with error");
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    socket: TcpStream,
    store: Arc<Store>,
    aof: Arc<Aof>,
    pubsub: Arc<PubSub>,
    replication: Arc<ReplicationHub>,
    cluster: Option<Arc<ClusterConfig>>,
    read_only: bool,
    snapshot_path: PathBuf,
) -> anyhow::Result<()> {
    let mut framed = Framed::new(socket, RespCodec);

    while let Some(frame) = framed.next().await {
        let args = match frame {
            Ok(args) => args,
            Err(e) => {
                let _ = framed
                    .send(Reply::Error(format!("ERR protocol error: {}", e)))
                    .await;
                break;
            }
        };
        if args.is_empty() {
            continue;
        }

        let cmd = match commands::parse(&args) {
            Ok(cmd) => cmd,
            Err(e) => {
                framed.send(Reply::Error(e)).await?;
                continue;
            }
        };

        if let Command::Subscribe(channels) = cmd {
            run_subscriber_loop(&mut framed, &pubsub, channels).await?;
            continue;
        }

        if matches!(cmd, Command::BgSave) {
            let store = store.clone();
            let aof = aof.clone();
            let snapshot_path = snapshot_path.clone();
            tokio::spawn(async move {
                if let Err(e) = do_snapshot(&store, &aof, &snapshot_path) {
                    error!(error = %e, "background save failed");
                }
            });
            framed
                .send(Reply::Simple("Background saving started".to_string()))
                .await?;
            continue;
        }

        if matches!(cmd, Command::Sync) {
            run_replica_feed(&mut framed, &store, &replication).await?;
            continue;
        }

        if matches!(cmd, Command::ClusterInfo) {
            framed.send(cluster_info_reply(&cluster)).await?;
            continue;
        }
        if matches!(cmd, Command::ClusterSlots) {
            framed.send(cluster_slots_reply(&cluster)).await?;
            continue;
        }

        if let Some(cluster) = &cluster {
            let keys = cmd.keys();
            if !keys.is_empty() {
                let first_slot = cluster::slot_for(keys[0]);
                let same_slot = keys.iter().all(|k| cluster::slot_for(k) == first_slot);
                if !same_slot {
                    framed
                        .send(Reply::Error(
                            "CROSSSLOT Keys in request don't hash to the same slot".to_string(),
                        ))
                        .await?;
                    continue;
                }
                if !cluster.owns(first_slot) {
                    let (host, port) = cluster.owner_addr(first_slot);
                    framed
                        .send(Reply::Error(format!("MOVED {first_slot} {host}:{port}")))
                        .await?;
                    continue;
                }
            }
        }

        if read_only && cmd.is_write() {
            framed
                .send(Reply::Error(
                    "READONLY You can't write against a read only replica.".to_string(),
                ))
                .await?;
            continue;
        }

        let reply = commands::execute(&store, &pubsub, &cmd);
        if cmd.is_write() {
            if let Err(e) = aof.append(&args) {
                error!(error = %e, "failed to append to AOF");
            }
            replication.publish(&args);
        }

        let is_quit = matches!(cmd, Command::Quit);
        framed.send(reply).await?;
        if is_quit {
            break;
        }
    }
    Ok(())
}

fn cluster_info_reply(cluster: &Option<Arc<ClusterConfig>>) -> Reply {
    let enabled = if cluster.is_some() { 1 } else { 0 };
    let known_nodes = cluster.as_ref().map(|c| c.nodes().len()).unwrap_or(1);
    Reply::Bulk(Bytes::from(format!(
        "cluster_enabled:{enabled}\r\ncluster_state:ok\r\ncluster_slots_assigned:{}\r\ncluster_known_nodes:{known_nodes}\r\n",
        if cluster.is_some() { cluster::SLOT_COUNT } else { 0 }
    )))
}

fn cluster_slots_reply(cluster: &Option<Arc<ClusterConfig>>) -> Reply {
    let Some(cluster) = cluster else {
        return Reply::Array(Vec::new());
    };
    let mut out = Vec::new();
    for (idx, (host, port)) in cluster.nodes().iter().enumerate() {
        for (start, end) in cluster.slot_ranges_for_index(idx) {
            out.push(Reply::Array(vec![
                Reply::Integer(start as i64),
                Reply::Integer(end as i64),
                Reply::Array(vec![
                    Reply::Bulk(Bytes::from(host.clone())),
                    Reply::Integer(*port as i64),
                ]),
            ]));
        }
    }
    Reply::Array(out)
}

/// Takes over the connection once a replica issues `SYNC`: sends a single
/// atomic full-dataset snapshot, then streams every subsequent write
/// command as it happens. Subscribing to the replication hub before taking
/// the snapshot (which itself blocks all writes while it copies) means no
/// write can land in the gap between "state captured" and "streaming
/// starts" — see `Store::snapshot_all_locked`.
async fn run_replica_feed(
    framed: &mut Framed<TcpStream, RespCodec>,
    store: &Store,
    replication: &ReplicationHub,
) -> anyhow::Result<()> {
    let mut rx = replication.subscribe();
    let entries = store.snapshot_all_locked();
    let key_count = entries.len();
    let blob = bincode::serialize(&entries)?;
    framed.send(Reply::Bulk(Bytes::from(blob))).await?;
    info!(keys = key_count, "sent full sync to replica");

    loop {
        match rx.recv().await {
            Ok(args) => {
                framed
                    .send(Reply::Array(args.into_iter().map(Reply::Bulk).collect()))
                    .await?;
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
        }
    }
    Ok(())
}

fn spawn_replica_client(
    store: Arc<Store>,
    aof: Arc<Aof>,
    pubsub: Arc<PubSub>,
    replication: Arc<ReplicationHub>,
    primary_addr: String,
) {
    tokio::spawn(async move {
        loop {
            match sync_from_primary(&store, &aof, &pubsub, &replication, &primary_addr).await {
                Ok(()) => warn!(%primary_addr, "replication stream ended, reconnecting"),
                Err(e) => {
                    error!(%primary_addr, error = %e, "replication connection failed, retrying")
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

/// Connects to a primary, performs a full sync, then applies streamed
/// writes as they arrive until the connection drops (at which point the
/// caller reconnects and full-syncs again — there is no partial resync in
/// v1). Also republishes applied writes to this node's own
/// `ReplicationHub`, so a replica can itself be replicated from
/// (chaining), and appends them to its own AOF so its durability keeps
/// working independently of the primary.
async fn sync_from_primary(
    store: &Store,
    aof: &Aof,
    pubsub: &PubSub,
    replication: &ReplicationHub,
    primary_addr: &str,
) -> anyhow::Result<()> {
    let mut stream = TcpStream::connect(primary_addr).await?;
    stream
        .write_all(&resp::encode_command(&[Bytes::from_static(b"SYNC")]))
        .await?;

    let mut buf = BytesMut::with_capacity(16 * 1024);
    let mut chunk = [0u8; 8192];
    let snapshot_bytes = loop {
        if let Some(b) = resp::parse_bulk_reply(&mut buf)? {
            break b;
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            anyhow::bail!("primary closed connection during initial sync");
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let entries: Vec<(Bytes, Entry)> = bincode::deserialize(&snapshot_bytes)?;
    store.flush_all();
    let key_count = entries.len();
    for (k, e) in entries {
        store.load_entry(k, e);
    }
    info!(%primary_addr, keys = key_count, "replica initial sync complete");

    loop {
        if let Some(args) = resp::parse_command(&mut buf)? {
            if !args.is_empty() {
                if let Ok(cmd) = commands::parse(&args) {
                    let _ = commands::execute(store, pubsub, &cmd);
                    if cmd.is_write() {
                        if let Err(e) = aof.append(&args) {
                            error!(error = %e, "replica failed to append to local AOF");
                        }
                        replication.publish(&args);
                    }
                }
            }
            continue;
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            anyhow::bail!("primary connection closed");
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn subscribe_reply(kind: &'static str, channel: Bytes, count: usize) -> Reply {
    Reply::Array(vec![
        Reply::Bulk(Bytes::from_static(kind.as_bytes())),
        Reply::Bulk(channel),
        Reply::Integer(count as i64),
    ])
}

/// Takes over the connection once a client issues `SUBSCRIBE`, multiplexing
/// pushed pub/sub messages against further client frames until the client
/// unsubscribes from every channel (or disconnects). Only `(UN)SUBSCRIBE`
/// and `PING` are accepted while in this mode, matching Redis.
async fn run_subscriber_loop(
    framed: &mut Framed<TcpStream, RespCodec>,
    pubsub: &PubSub,
    initial_channels: Vec<Bytes>,
) -> anyhow::Result<()> {
    let mut streams: StreamMap<Bytes, BroadcastStream<Bytes>> = StreamMap::new();

    for channel in initial_channels {
        let rx = pubsub.subscribe(channel.clone());
        streams.insert(channel.clone(), BroadcastStream::new(rx));
        let count = streams.len();
        framed
            .send(subscribe_reply("subscribe", channel, count))
            .await?;
    }

    loop {
        tokio::select! {
            Some((channel, msg)) = streams.next() => {
                if let Ok(payload) = msg {
                    framed
                        .send(Reply::Array(vec![
                            Reply::Bulk(Bytes::from_static(b"message")),
                            Reply::Bulk(channel),
                            Reply::Bulk(payload),
                        ]))
                        .await?;
                }
                // Err(Lagged) is silently skipped: the subscriber fell behind
                // the broadcast buffer, so we just resume from where we are.
            }
            frame = framed.next() => {
                match frame {
                    Some(Ok(args)) if !args.is_empty() => {
                        let cmd = match commands::parse(&args) {
                            Ok(cmd) => cmd,
                            Err(e) => {
                                framed.send(Reply::Error(e)).await?;
                                continue;
                            }
                        };
                        match cmd {
                            Command::Subscribe(channels) => {
                                for channel in channels {
                                    let rx = pubsub.subscribe(channel.clone());
                                    streams.insert(channel.clone(), BroadcastStream::new(rx));
                                    let count = streams.len();
                                    framed.send(subscribe_reply("subscribe", channel, count)).await?;
                                }
                            }
                            Command::Unsubscribe(channels) => {
                                let to_remove: Vec<Bytes> = if channels.is_empty() {
                                    streams.keys().cloned().collect()
                                } else {
                                    channels
                                };
                                for channel in to_remove {
                                    streams.remove(&channel);
                                    let count = streams.len();
                                    framed.send(subscribe_reply("unsubscribe", channel, count)).await?;
                                }
                                if streams.is_empty() {
                                    return Ok(());
                                }
                            }
                            Command::Ping(msg) => {
                                let reply = match msg {
                                    Some(m) => Reply::Bulk(m),
                                    None => Reply::Simple("PONG".to_string()),
                                };
                                framed.send(reply).await?;
                            }
                            _ => {
                                framed
                                    .send(Reply::Error(
                                        "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING are allowed in this context"
                                            .to_string(),
                                    ))
                                    .await?;
                            }
                        }
                    }
                    Some(Ok(_)) => continue, // empty inline command, ignore
                    Some(Err(e)) => {
                        let _ = framed.send(Reply::Error(format!("ERR protocol error: {}", e))).await;
                        return Ok(());
                    }
                    None => return Ok(()), // client disconnected
                }
            }
        }
    }
}

fn do_snapshot(store: &Store, aof: &Aof, snapshot_path: &std::path::Path) -> std::io::Result<()> {
    persistence::snapshot::save(store, snapshot_path)?;
    aof.truncate()?;
    Ok(())
}

fn spawn_periodic_snapshot(
    store: Arc<Store>,
    aof: Arc<Aof>,
    snapshot_path: PathBuf,
    interval_secs: u64,
) {
    if interval_secs == 0 {
        return;
    }
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.tick().await; // skip the immediate first tick
        loop {
            interval.tick().await;
            if let Err(e) = do_snapshot(&store, &aof, &snapshot_path) {
                error!(error = %e, "periodic snapshot failed");
            } else {
                info!(path = %snapshot_path.display(), "periodic snapshot complete");
            }
        }
    });
}

fn spawn_everysec_fsync(aof: Arc<Aof>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            if let Err(e) = aof.sync() {
                error!(error = %e, "AOF fsync failed");
            }
        }
    });
}
