use bytes::Bytes;
use tokio::sync::broadcast;

/// How many pending replicated commands a lagging replica can be behind
/// before it starts missing them. Bounded so a stalled replica can't grow
/// primary memory without limit; a replica that falls behind this far
/// will resync from scratch on its next reconnect (see `run_replica_client`
/// in `main.rs`).
const CHANNEL_CAPACITY: usize = 4096;

/// Fans out every write command executed on the primary to any number of
/// connected replicas. A single shared broadcast channel is enough here —
/// unlike `PubSub`, there is only one "topic" (the write stream), and
/// replicas that fall behind are expected to reconnect and full-resync
/// rather than seek a specific offset.
pub struct ReplicationHub {
    sender: broadcast::Sender<Vec<Bytes>>,
}

impl ReplicationHub {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self { sender }
    }

    /// Publishes a write command's raw arguments to every subscribed
    /// replica feed. A no-op if nobody is currently subscribed.
    pub fn publish(&self, args: &[Bytes]) {
        let _ = self.sender.send(args.to_vec());
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Vec<Bytes>> {
        self.sender.subscribe()
    }
}

impl Default for ReplicationHub {
    fn default() -> Self {
        Self::new()
    }
}
