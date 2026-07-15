pub const SLOT_COUNT: u16 = 16384;

/// CRC16/XMODEM (poly 0x1021, init 0, no reflect, no final xor) — the same
/// algorithm Redis Cluster uses to map keys to slots, so real cluster-aware
/// clients (e.g. `redis-cli -c`) compute the same slot we do. Hash tags
/// (`{...}` substrings) are not supported in this v1: the whole key is
/// always hashed.
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

pub fn slot_for(key: &[u8]) -> u16 {
    crc16(key) % SLOT_COUNT
}

/// Static cluster topology: a fixed, operator-configured list of nodes and
/// which one this process is. Slot ownership is derived deterministically
/// (an even split of the 16384 slots across nodes, in list order) — there
/// is no gossip, no runtime resharding, and no `CLUSTER SETSLOT`. This
/// matches the plan's explicit "configured, not auto-discovered" scope.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    nodes: Vec<(String, u16)>,
    self_index: usize,
}

impl ClusterConfig {
    pub fn new(nodes: Vec<(String, u16)>, self_index: usize) -> Self {
        Self { nodes, self_index }
    }

    pub fn nodes(&self) -> &[(String, u16)] {
        &self.nodes
    }

    /// Index of the node that owns `slot`, splitting the slot space as
    /// evenly as possible across nodes in list order.
    pub fn owner_index(&self, slot: u16) -> usize {
        let n = self.nodes.len() as u32;
        (slot as u32 * n / SLOT_COUNT as u32) as usize
    }

    pub fn owns(&self, slot: u16) -> bool {
        self.owner_index(slot) == self.self_index
    }

    pub fn owner_addr(&self, slot: u16) -> &(String, u16) {
        &self.nodes[self.owner_index(slot)]
    }

    /// Contiguous slot ranges owned by `index`. `owner_index` is
    /// non-decreasing in `slot`, so a node's ownership is always a single
    /// contiguous range — but this returns a `Vec` defensively rather than
    /// assuming that invariant holds forever.
    pub fn slot_ranges_for_index(&self, index: usize) -> Vec<(u16, u16)> {
        let mut ranges = Vec::new();
        let mut start: Option<u16> = None;
        let mut end = 0u16;
        for slot in 0..SLOT_COUNT {
            if self.owner_index(slot) == index {
                if start.is_none() {
                    start = Some(slot);
                }
                end = slot;
            } else if let Some(s) = start.take() {
                ranges.push((s, end));
            }
        }
        if let Some(s) = start {
            ranges.push((s, end));
        }
        ranges
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc16_matches_known_redis_cluster_vectors() {
        // These are well-known reference values published alongside Redis
        // Cluster's crc16.c test vectors.
        assert_eq!(crc16(b""), 0x0000);
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }

    #[test]
    fn slots_split_evenly_and_cover_the_whole_space() {
        let nodes = vec![
            ("a".to_string(), 1),
            ("b".to_string(), 2),
            ("c".to_string(), 3),
        ];
        let cfg = ClusterConfig::new(nodes, 0);
        let mut total = 0u32;
        for i in 0..3 {
            let ranges = cfg.slot_ranges_for_index(i);
            for (s, e) in &ranges {
                total += (*e as u32) - (*s as u32) + 1;
            }
        }
        assert_eq!(total, SLOT_COUNT as u32);
    }

    #[test]
    fn every_slot_has_exactly_one_owner() {
        let nodes = vec![("a".to_string(), 1), ("b".to_string(), 2)];
        let cfg = ClusterConfig::new(nodes, 0);
        for slot in 0..SLOT_COUNT {
            let owner = cfg.owner_index(slot);
            assert!(owner < 2);
        }
    }
}
