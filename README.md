# InMemoryCache

A fast, local, Redis-compatible in-memory cache server, written in Rust.

Speaks the Redis RESP protocol, so `redis-cli` and any existing Redis
client library can talk to it without modification.

## Status: Phase 3 — replication + clustering

- Sharded in-memory keyspace (16 shards, each independently locked) for
  low-contention concurrent access
- RESP2 protocol support
- String/counter commands: `PING`, `ECHO`, `SET` (`EX`/`PX`/`NX`/`XX`),
  `GET`, `DEL`, `EXISTS`, `EXPIRE`/`PEXPIRE`, `PERSIST`, `TTL`/`PTTL`,
  `INCR`/`DECR`/`INCRBY`/`DECRBY`, `APPEND`, `MGET`/`MSET`, `TYPE`,
  `FLUSHALL`, `INFO`, `CONFIG GET/SET` (stub), `BGSAVE`
- List: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`
- Hash: `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`
- Set: `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`
- Sorted set: `ZADD`, `ZRANGE` (`WITHSCORES`), `ZSCORE`, `ZREM`
- Pub/Sub: `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` (node-local; not
  replicated across primary/replica or cluster nodes)
- `WRONGTYPE` errors when a command targets a key of the wrong kind;
  removing the last element of a List/Hash/Set/SortedSet deletes the key,
  matching Redis semantics
- Lazy + active-cycle key expiration
- Approximated-LRU eviction when a shard exceeds `--max-keys-per-shard`
- Durability: append-only file (AOF) log plus periodic full snapshots,
  replayed on startup (snapshot + AOF-since-snapshot, like Redis's
  RDB+AOF hybrid) — covers every data type above, not just strings
- **Replication:** `--replicaof host:port` makes a node a read-only
  asynchronous replica of a primary. On connect it receives one atomic
  full-dataset snapshot (`SYNC`), then every subsequent write the primary
  executes streams to it live. Replicas reject writes from ordinary
  clients (`READONLY` error) and can themselves be replicated from
  (chaining) — but there's no partial resync: any dropped connection
  triggers a full resync from scratch, and no offset/ack tracking exists
  yet.
- **Clustering:** `--cluster-nodes host:port,...` + `--cluster-self-index N`
  statically shards the 16384-slot keyspace evenly across a fixed,
  operator-configured node list (CRC16, same algorithm as Redis Cluster,
  so `redis-cli -c` and other cluster-aware clients route correctly).
  A node returns a `MOVED` redirect for keys it doesn't own, or
  `CROSSSLOT` if a multi-key command's keys don't share a slot.
  `CLUSTER INFO`/`CLUSTER SLOTS` are implemented for client discovery.
  Topology is fixed at startup — no gossip, no live resharding, no
  per-node replicas within the cluster (that would combine with
  `--replicaof`, but the combination isn't specifically tested).

See the project's plan history for the full phased roadmap and the
known limitations above in more detail.

## Running

```sh
cargo run --release -- --port 6380 --data-dir ./data
```

Then connect with any Redis client:

```sh
redis-cli -p 6380 SET foo bar
redis-cli -p 6380 GET foo
```

### CLI options

| Flag | Default | Description |
|---|---|---|
| `--bind` | `127.0.0.1` | Address to listen on |
| `--port` | `6380` | Port to listen on (`0` picks a free port) |
| `--data-dir` | `./data` | Directory for the AOF log and snapshot |
| `--fsync` | `everysec` | AOF fsync policy: `always` \| `everysec` \| `no` |
| `--max-keys-per-shard` | unbounded | Per-shard key budget before LRU eviction |
| `--snapshot-interval-secs` | `300` | How often to snapshot + rotate the AOF (`0` disables) |
| `--replicaof` | unset | `host:port` of a primary to replicate from (read-only mode) |
| `--cluster-nodes` | unset | Comma-separated `host:port` list of every cluster node |
| `--cluster-self-index` | unset | This node's 0-based position in `--cluster-nodes` |

### Replication example

```sh
cargo run --release -- --port 6380 --data-dir ./data-primary
cargo run --release -- --port 6381 --data-dir ./data-replica --replicaof 127.0.0.1:6380
```

### Cluster example (2 nodes)

```sh
cargo run --release -- --port 6380 --data-dir ./data-a \
  --cluster-nodes 127.0.0.1:6380,127.0.0.1:6381 --cluster-self-index 0
cargo run --release -- --port 6381 --data-dir ./data-b \
  --cluster-nodes 127.0.0.1:6380,127.0.0.1:6381 --cluster-self-index 1

redis-cli -c -p 6380 SET foo bar   # -c makes redis-cli follow MOVED automatically
```

## Testing

```sh
cargo test
```

Unit tests cover the RESP codec, CRC16 slot hashing, and cluster slot-range
math; integration tests (`crates/server/tests`) spawn the real server
binary (sometimes several at once) and drive them with the `redis` crate,
covering get/set/expire, INCR/APPEND, AOF-restart recovery,
snapshot-restart recovery, LRU eviction bounds, List/Hash/Set/SortedSet
operations (including the delete-on-empty prune behavior), `WRONGTYPE`
errors, Pub/Sub delivery, restart recovery of the rich data types,
primary→replica write propagation and read-only enforcement, and cluster
`MOVED` redirects / `CLUSTER SLOTS` coverage.
