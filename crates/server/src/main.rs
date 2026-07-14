mod commands;
mod expiry;
mod persistence;
mod pubsub;
mod resp;
mod shard;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamMap;
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

use commands::Command;
use persistence::aof::{Aof, FsyncPolicy};
use pubsub::PubSub;
use resp::{Reply, RespCodec};
use shard::Store;

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
}

struct Paths {
    snapshot: PathBuf,
    aof: PathBuf,
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
        let snapshot_path = paths.snapshot.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, store, aof, pubsub, snapshot_path).await {
                warn!(%peer, error = %e, "connection closed with error");
            }
        });
    }
}

async fn handle_connection(
    socket: TcpStream,
    store: Arc<Store>,
    aof: Arc<Aof>,
    pubsub: Arc<PubSub>,
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

        let reply = commands::execute(&store, &pubsub, &cmd);
        if cmd.is_write() {
            if let Err(e) = aof.append(&args) {
                error!(error = %e, "failed to append to AOF");
            }
        }

        let is_quit = matches!(cmd, Command::Quit);
        framed.send(reply).await?;
        if is_quit {
            break;
        }
    }
    Ok(())
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
