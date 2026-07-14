use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};

pub const NUM_SHARDS: usize = 16;
/// How many random keys to sample when approximating LRU eviction,
/// same trick Redis uses instead of maintaining a full LRU list.
const EVICTION_SAMPLE_SIZE: usize = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Str(Bytes),
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Str(_) => "string",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub value: Value,
    pub expires_at_ms: Option<u64>,
    last_access: u64,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

fn is_expired(entry: &Entry, now: u64) -> bool {
    matches!(entry.expires_at_ms, Some(exp) if exp <= now)
}

struct Shard {
    map: HashMap<Bytes, Entry>,
    clock: u64,
}

impl Shard {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            clock: 0,
        }
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// Removes `key` if it is present but expired. Returns true if the key
    /// is absent after this call (either it never existed or was purged).
    fn purge_if_expired(&mut self, key: &[u8], now: u64) -> bool {
        match self.map.get(key) {
            Some(entry) if is_expired(entry, now) => {
                self.map.remove(key);
                true
            }
            Some(_) => false,
            None => true,
        }
    }

    fn evict_one(&mut self) {
        if self.map.is_empty() {
            return;
        }
        let mut rng = rand::thread_rng();
        let mut oldest_key: Option<Bytes> = None;
        let mut oldest_access = u64::MAX;
        let len = self.map.len();
        let sample = EVICTION_SAMPLE_SIZE.min(len);
        // Reservoir-style random sample over the map's iteration order.
        let skip = if len > sample {
            rng.gen_range(0..len - sample + 1)
        } else {
            0
        };
        for (k, e) in self.map.iter().skip(skip).take(sample) {
            if e.last_access < oldest_access {
                oldest_access = e.last_access;
                oldest_key = Some(k.clone());
            }
        }
        if let Some(k) = oldest_key {
            self.map.remove(&k);
        }
    }
}

pub struct Store {
    shards: Vec<RwLock<Shard>>,
    max_keys_per_shard: Option<usize>,
}

impl Store {
    pub fn new(max_keys_per_shard: Option<usize>) -> Self {
        let shards = (0..NUM_SHARDS).map(|_| RwLock::new(Shard::new())).collect();
        Self {
            shards,
            max_keys_per_shard,
        }
    }

    fn shard_index(key: &[u8]) -> usize {
        // FNV-1a: fast, good-enough distribution for shard routing.
        let mut hash: u64 = 0xcbf29ce484222325;
        for &b in key {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        (hash as usize) % NUM_SHARDS
    }

    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    pub fn get(&self, key: &[u8]) -> Option<Value> {
        let idx = Self::shard_index(key);
        let mut shard = self.shards[idx].write();
        let now = now_ms();
        if shard.purge_if_expired(key, now) {
            return None;
        }
        let clock = shard.tick();
        let entry = shard.map.get_mut(key).expect("checked present above");
        entry.last_access = clock;
        Some(entry.value.clone())
    }

    pub fn set(
        &self,
        key: Bytes,
        value: Value,
        expires_at_ms: Option<u64>,
        nx: bool,
        xx: bool,
    ) -> bool {
        let idx = Self::shard_index(&key);
        let mut shard = self.shards[idx].write();
        let now = now_ms();
        let exists = !shard.purge_if_expired(&key, now);
        if (nx && exists) || (xx && !exists) {
            return false;
        }
        let clock = shard.tick();
        shard.map.insert(
            key,
            Entry {
                value,
                expires_at_ms,
                last_access: clock,
            },
        );
        if let Some(max) = self.max_keys_per_shard {
            while shard.map.len() > max {
                shard.evict_one();
            }
        }
        true
    }

    pub fn del(&self, keys: &[Bytes]) -> i64 {
        let now = now_ms();
        let mut removed = 0i64;
        for key in keys {
            let idx = Self::shard_index(key);
            let mut shard = self.shards[idx].write();
            if !shard.purge_if_expired(key, now) {
                shard.map.remove(key.as_ref());
                removed += 1;
            }
        }
        removed
    }

    pub fn exists(&self, keys: &[Bytes]) -> i64 {
        let now = now_ms();
        let mut count = 0i64;
        for key in keys {
            let idx = Self::shard_index(key);
            let mut shard = self.shards[idx].write();
            if !shard.purge_if_expired(key, now) {
                count += 1;
            }
        }
        count
    }

    pub fn expire(&self, key: &[u8], at_ms: i64) -> bool {
        let idx = Self::shard_index(key);
        let mut shard = self.shards[idx].write();
        let now = now_ms();
        if shard.purge_if_expired(key, now) {
            return false;
        }
        if at_ms <= now as i64 {
            shard.map.remove(key);
            return true;
        }
        if let Some(entry) = shard.map.get_mut(key) {
            entry.expires_at_ms = Some(at_ms as u64);
            true
        } else {
            false
        }
    }

    pub fn persist(&self, key: &[u8]) -> bool {
        let idx = Self::shard_index(key);
        let mut shard = self.shards[idx].write();
        let now = now_ms();
        if shard.purge_if_expired(key, now) {
            return false;
        }
        if let Some(entry) = shard.map.get_mut(key) {
            let had = entry.expires_at_ms.is_some();
            entry.expires_at_ms = None;
            had
        } else {
            false
        }
    }

    /// Returns remaining TTL in milliseconds: Some(ms) if a TTL is set,
    /// None if the key exists with no TTL, and -1 encoded by the caller
    /// when the key doesn't exist at all (mirrors Redis's TTL semantics).
    pub fn ttl_ms(&self, key: &[u8]) -> Option<Option<i64>> {
        let idx = Self::shard_index(key);
        let mut shard = self.shards[idx].write();
        let now = now_ms();
        if shard.purge_if_expired(key, now) {
            return None;
        }
        let entry = shard.map.get(key)?;
        Some(
            entry
                .expires_at_ms
                .map(|exp| (exp as i64 - now as i64).max(0)),
        )
    }

    pub fn key_type(&self, key: &[u8]) -> Option<&'static str> {
        let idx = Self::shard_index(key);
        let mut shard = self.shards[idx].write();
        let now = now_ms();
        if shard.purge_if_expired(key, now) {
            return None;
        }
        shard.map.get(key).map(|e| e.value.type_name())
    }

    pub fn flush_all(&self) {
        for shard in &self.shards {
            shard.write().map.clear();
        }
    }

    /// Applies a mutating closure to the raw entry (creating a default via
    /// `default` if absent/expired), used by INCR/DECR/APPEND to avoid a
    /// separate get+set round trip. Returns the closure's output.
    pub fn with_entry_mut<T>(
        &self,
        key: &Bytes,
        default: impl FnOnce() -> Value,
        f: impl FnOnce(&mut Value) -> T,
    ) -> T {
        let idx = Self::shard_index(key);
        let mut shard = self.shards[idx].write();
        let now = now_ms();
        let existed = !shard.purge_if_expired(key, now);
        if !existed {
            let clock = shard.tick();
            shard.map.insert(
                key.clone(),
                Entry {
                    value: default(),
                    expires_at_ms: None,
                    last_access: clock,
                },
            );
        }
        let clock = shard.tick();
        let entry = shard
            .map
            .get_mut(key.as_ref())
            .expect("just inserted or present");
        entry.last_access = clock;
        f(&mut entry.value)
    }

    /// Iterates every live (non-expired) key/entry across all shards.
    /// Used for snapshotting; takes a read lock per shard.
    pub fn for_each_live_entry(&self, mut f: impl FnMut(&Bytes, &Entry)) {
        let now = now_ms();
        for shard in &self.shards {
            let shard = shard.read();
            for (k, e) in shard.map.iter() {
                if !is_expired(e, now) {
                    f(k, e);
                }
            }
        }
    }

    pub fn load_entry(&self, key: Bytes, entry: Entry) {
        let idx = Self::shard_index(&key);
        let mut shard = self.shards[idx].write();
        shard.map.insert(key, entry);
    }

    /// Background sweep: samples a handful of keys per shard and purges
    /// any that have expired, so idle expired keys don't linger forever.
    pub fn active_expire_cycle(&self) {
        let now = now_ms();
        const SAMPLE: usize = 20;
        for shard in &self.shards {
            let mut shard = shard.write();
            let expired_keys: Vec<Bytes> = shard
                .map
                .iter()
                .take(SAMPLE)
                .filter(|(_, e)| is_expired(e, now))
                .map(|(k, _)| k.clone())
                .collect();
            for k in expired_keys {
                shard.map.remove(&k);
            }
        }
    }
}
