use std::collections::HashMap;

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::broadcast;

/// How many pending messages a subscriber can lag behind before it starts
/// missing them (reported as a `Lagged` error on next read). Bounded so a
/// slow subscriber can't grow memory without limit.
const CHANNEL_CAPACITY: usize = 128;

#[derive(Default)]
pub struct PubSub {
    channels: RwLock<HashMap<Bytes, broadcast::Sender<Bytes>>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribes to `channel`, creating its broadcast channel on first
    /// subscriber.
    pub fn subscribe(&self, channel: Bytes) -> broadcast::Receiver<Bytes> {
        let mut channels = self.channels.write();
        channels
            .entry(channel)
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .subscribe()
    }

    /// Publishes `message` to `channel`, returning the number of current
    /// subscribers (0 if nobody is subscribed, matching Redis's `PUBLISH`
    /// return value).
    pub fn publish(&self, channel: &[u8], message: Bytes) -> usize {
        let channels = self.channels.read();
        match channels.get(channel) {
            Some(sender) => sender.send(message).unwrap_or(0),
            None => 0,
        }
    }
}
