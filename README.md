# InMemoryCache

A fast, local, Redis-compatible in-memory cache server, written in Rust.

Speaks the Redis RESP protocol, so `redis-cli` and any existing Redis
client library can talk to it without modification.

## Status: Phase 2 — rich data structures + Pub/Sub

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
- Pub/Sub: `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
- `WRONGTYPE` errors when a command targets a key of the wrong kind;
  removing the last element of a List/Hash/Set/SortedSet deletes the key,
  matching Redis semantics
- Lazy + active-cycle key expiration
- Approximated-LRU eviction when a shard exceeds `--max-keys-per-shard`
- Durability: append-only file (AOF) log plus periodic full snapshots,
  replayed on startup (snapshot + AOF-since-snapshot, like Redis's
  RDB+AOF hybrid) — covers every data type above, not just strings

Planned next: replication/clustering. See the project's plan history for
the full phased roadmap.

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
recovery, LRU eviction bounds, List/Hash/Set/SortedSet operations
(including the delete-on-empty prune behavior), `WRONGTYPE` errors,
Pub/Sub delivery, and restart recovery of the rich data types.
