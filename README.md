# InMemoryCache

A fast, local, Redis-compatible in-memory cache server, written in Rust.

Speaks the Redis RESP protocol, so `redis-cli` and any existing Redis
client library can talk to it without modification.

## Status: Phase 1 — durable core KV engine

- Sharded in-memory keyspace (16 shards, each independently locked) for
  low-contention concurrent access
- RESP2 protocol support
- Commands: `PING`, `ECHO`, `SET` (`EX`/`PX`/`NX`/`XX`), `GET`, `DEL`,
  `EXISTS`, `EXPIRE`/`PEXPIRE`, `PERSIST`, `TTL`/`PTTL`, `INCR`/`DECR`/
  `INCRBY`/`DECRBY`, `APPEND`, `MGET`/`MSET`, `TYPE`, `FLUSHALL`, `INFO`,
  `CONFIG GET/SET` (stub), `BGSAVE`
- Lazy + active-cycle key expiration
- Approximated-LRU eviction when a shard exceeds `--max-keys-per-shard`
- Durability: append-only file (AOF) log plus periodic full snapshots,
  replayed on startup (snapshot + AOF-since-snapshot, like Redis's
  RDB+AOF hybrid)

Planned next: rich data structures (List/Hash/Set/SortedSet) + Pub/Sub,
then replication/clustering. See the project's plan history for the full
phased roadmap.

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

## Testing

```sh
cargo test
```

Unit tests cover the RESP codec; integration tests (`crates/server/tests`)
spawn the real server binary and drive it with the `redis` crate, covering
get/set/expire, INCR/APPEND, AOF-restart recovery, snapshot-restart
recovery, and LRU eviction bounds.
