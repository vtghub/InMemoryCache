use std::sync::Arc;
use std::time::Duration;

use crate::shard::Store;

/// Spawns the background task that periodically samples a few keys per
/// shard and purges any that have expired, so keys nobody ever reads
/// again don't linger in memory forever.
pub fn spawn_active_expire_cycle(store: Arc<Store>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            store.active_expire_cycle();
        }
    });
}
