use std::collections::{HashMap, HashSet, VecDeque};

use bytes::Bytes;

use crate::pubsub::PubSub;
use crate::resp::Reply;
use crate::shard::{now_ms, Store, Value};

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

#[derive(Debug, Clone)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        expires_at_ms: Option<u64>,
        nx: bool,
        xx: bool,
    },
    Get(Bytes),
    Del(Vec<Bytes>),
    Exists(Vec<Bytes>),
    Expire {
        key: Bytes,
        at_ms: i64,
    },
    Persist(Bytes),
    Ttl {
        key: Bytes,
        as_ms: bool,
    },
    Incr(Bytes),
    Decr(Bytes),
    IncrBy(Bytes, i64),
    DecrBy(Bytes, i64),
    Append(Bytes, Bytes),
    MGet(Vec<Bytes>),
    MSet(Vec<(Bytes, Bytes)>),
    Type(Bytes),
    FlushAll,
    Info,
    ConfigGet(Bytes),
    ConfigSet(Bytes, Bytes),
    BgSave,
    Quit,

    // Lists
    LPush(Bytes, Vec<Bytes>),
    RPush(Bytes, Vec<Bytes>),
    LPop(Bytes),
    RPop(Bytes),
    LRange {
        key: Bytes,
        start: i64,
        stop: i64,
    },
    LLen(Bytes),

    // Hashes
    HSet(Bytes, Vec<(Bytes, Bytes)>),
    HGet(Bytes, Bytes),
    HDel(Bytes, Vec<Bytes>),
    HGetAll(Bytes),
    HExists(Bytes, Bytes),

    // Sets
    SAdd(Bytes, Vec<Bytes>),
    SRem(Bytes, Vec<Bytes>),
    SMembers(Bytes),
    SIsMember(Bytes, Bytes),

    // Sorted sets
    ZAdd(Bytes, Vec<(f64, Bytes)>),
    ZRange {
        key: Bytes,
        start: i64,
        stop: i64,
        with_scores: bool,
    },
    ZScore(Bytes, Bytes),
    ZRem(Bytes, Vec<Bytes>),

    // Pub/Sub
    Subscribe(Vec<Bytes>),
    Unsubscribe(Vec<Bytes>),
    Publish(Bytes, Bytes),
}

impl Command {
    /// Whether executing this command mutates the keyspace and therefore
    /// needs to be appended to the AOF. Pub/Sub commands are transient and
    /// deliberately excluded — there is nothing to replay.
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            Command::Set { .. }
                | Command::Del(_)
                | Command::Expire { .. }
                | Command::Persist(_)
                | Command::Incr(_)
                | Command::Decr(_)
                | Command::IncrBy(_, _)
                | Command::DecrBy(_, _)
                | Command::Append(_, _)
                | Command::MSet(_)
                | Command::FlushAll
                | Command::LPush(_, _)
                | Command::RPush(_, _)
                | Command::LPop(_)
                | Command::RPop(_)
                | Command::HSet(_, _)
                | Command::HDel(_, _)
                | Command::SAdd(_, _)
                | Command::SRem(_, _)
                | Command::ZAdd(_, _)
                | Command::ZRem(_, _)
        )
    }
}

fn arity_err(name: &str) -> String {
    format!(
        "ERR wrong number of arguments for '{}' command",
        name.to_lowercase()
    )
}

fn parse_i64(b: &Bytes, what: &str) -> Result<i64, String> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| format!("ERR {} is not an integer or out of range", what))
}

fn parse_f64(b: &Bytes) -> Result<f64, String> {
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|f| !f.is_nan())
        .ok_or_else(|| "ERR value is not a valid float".to_string())
}

/// Parses a decoded RESP request (command name + args) into a `Command`.
pub fn parse(args: &[Bytes]) -> Result<Command, String> {
    if args.is_empty() {
        return Err("ERR empty command".to_string());
    }
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    let rest = &args[1..];

    match name.as_str() {
        "PING" => match rest.len() {
            0 => Ok(Command::Ping(None)),
            1 => Ok(Command::Ping(Some(rest[0].clone()))),
            _ => Err(arity_err("ping")),
        },
        "ECHO" => {
            if rest.len() != 1 {
                return Err(arity_err("echo"));
            }
            Ok(Command::Echo(rest[0].clone()))
        }
        "SET" => {
            if rest.len() < 2 {
                return Err(arity_err("set"));
            }
            let key = rest[0].clone();
            let value = rest[1].clone();
            let mut expires_at_ms = None;
            let mut nx = false;
            let mut xx = false;
            let mut i = 2;
            while i < rest.len() {
                let opt = String::from_utf8_lossy(&rest[i]).to_ascii_uppercase();
                match opt.as_str() {
                    "EX" | "PX" => {
                        if i + 1 >= rest.len() {
                            return Err("ERR syntax error".to_string());
                        }
                        let n = parse_i64(&rest[i + 1], "value")?;
                        let ms = if opt == "EX" { n * 1000 } else { n };
                        expires_at_ms = Some((now_ms() as i64 + ms).max(0) as u64);
                        i += 2;
                    }
                    "NX" => {
                        nx = true;
                        i += 1;
                    }
                    "XX" => {
                        xx = true;
                        i += 1;
                    }
                    _ => return Err("ERR syntax error".to_string()),
                }
            }
            if nx && xx {
                return Err("ERR syntax error".to_string());
            }
            Ok(Command::Set {
                key,
                value,
                expires_at_ms,
                nx,
                xx,
            })
        }
        "GET" => {
            if rest.len() != 1 {
                return Err(arity_err("get"));
            }
            Ok(Command::Get(rest[0].clone()))
        }
        "DEL" => {
            if rest.is_empty() {
                return Err(arity_err("del"));
            }
            Ok(Command::Del(rest.to_vec()))
        }
        "EXISTS" => {
            if rest.is_empty() {
                return Err(arity_err("exists"));
            }
            Ok(Command::Exists(rest.to_vec()))
        }
        "EXPIRE" | "PEXPIRE" => {
            if rest.len() != 2 {
                return Err(arity_err(&name));
            }
            let n = parse_i64(&rest[1], "value")?;
            let ms = if name == "EXPIRE" { n * 1000 } else { n };
            Ok(Command::Expire {
                key: rest[0].clone(),
                at_ms: now_ms() as i64 + ms,
            })
        }
        "PERSIST" => {
            if rest.len() != 1 {
                return Err(arity_err("persist"));
            }
            Ok(Command::Persist(rest[0].clone()))
        }
        "TTL" | "PTTL" => {
            if rest.len() != 1 {
                return Err(arity_err(&name));
            }
            Ok(Command::Ttl {
                key: rest[0].clone(),
                as_ms: name == "PTTL",
            })
        }
        "INCR" => {
            if rest.len() != 1 {
                return Err(arity_err("incr"));
            }
            Ok(Command::Incr(rest[0].clone()))
        }
        "DECR" => {
            if rest.len() != 1 {
                return Err(arity_err("decr"));
            }
            Ok(Command::Decr(rest[0].clone()))
        }
        "INCRBY" => {
            if rest.len() != 2 {
                return Err(arity_err("incrby"));
            }
            Ok(Command::IncrBy(
                rest[0].clone(),
                parse_i64(&rest[1], "value")?,
            ))
        }
        "DECRBY" => {
            if rest.len() != 2 {
                return Err(arity_err("decrby"));
            }
            Ok(Command::DecrBy(
                rest[0].clone(),
                parse_i64(&rest[1], "value")?,
            ))
        }
        "APPEND" => {
            if rest.len() != 2 {
                return Err(arity_err("append"));
            }
            Ok(Command::Append(rest[0].clone(), rest[1].clone()))
        }
        "MGET" => {
            if rest.is_empty() {
                return Err(arity_err("mget"));
            }
            Ok(Command::MGet(rest.to_vec()))
        }
        "MSET" => {
            if rest.is_empty() || !rest.len().is_multiple_of(2) {
                return Err(arity_err("mset"));
            }
            let pairs = rest
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();
            Ok(Command::MSet(pairs))
        }
        "TYPE" => {
            if rest.len() != 1 {
                return Err(arity_err("type"));
            }
            Ok(Command::Type(rest[0].clone()))
        }
        "FLUSHALL" => Ok(Command::FlushAll),
        "INFO" => Ok(Command::Info),
        "CONFIG" => {
            if rest.len() < 2 {
                return Err(arity_err("config"));
            }
            let sub = String::from_utf8_lossy(&rest[0]).to_ascii_uppercase();
            match sub.as_str() {
                "GET" => Ok(Command::ConfigGet(rest[1].clone())),
                "SET" if rest.len() >= 3 => {
                    Ok(Command::ConfigSet(rest[1].clone(), rest[2].clone()))
                }
                _ => Err("ERR syntax error".to_string()),
            }
        }
        "BGSAVE" => Ok(Command::BgSave),
        "QUIT" => Ok(Command::Quit),

        "LPUSH" | "RPUSH" => {
            if rest.len() < 2 {
                return Err(arity_err(&name));
            }
            let key = rest[0].clone();
            let values = rest[1..].to_vec();
            Ok(if name == "LPUSH" {
                Command::LPush(key, values)
            } else {
                Command::RPush(key, values)
            })
        }
        "LPOP" => {
            if rest.len() != 1 {
                return Err(arity_err("lpop"));
            }
            Ok(Command::LPop(rest[0].clone()))
        }
        "RPOP" => {
            if rest.len() != 1 {
                return Err(arity_err("rpop"));
            }
            Ok(Command::RPop(rest[0].clone()))
        }
        "LRANGE" => {
            if rest.len() != 3 {
                return Err(arity_err("lrange"));
            }
            Ok(Command::LRange {
                key: rest[0].clone(),
                start: parse_i64(&rest[1], "start")?,
                stop: parse_i64(&rest[2], "stop")?,
            })
        }
        "LLEN" => {
            if rest.len() != 1 {
                return Err(arity_err("llen"));
            }
            Ok(Command::LLen(rest[0].clone()))
        }

        "HSET" => {
            if rest.len() < 3 || !(rest.len() - 1).is_multiple_of(2) {
                return Err(arity_err("hset"));
            }
            let key = rest[0].clone();
            let pairs = rest[1..]
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect();
            Ok(Command::HSet(key, pairs))
        }
        "HGET" => {
            if rest.len() != 2 {
                return Err(arity_err("hget"));
            }
            Ok(Command::HGet(rest[0].clone(), rest[1].clone()))
        }
        "HDEL" => {
            if rest.len() < 2 {
                return Err(arity_err("hdel"));
            }
            Ok(Command::HDel(rest[0].clone(), rest[1..].to_vec()))
        }
        "HGETALL" => {
            if rest.len() != 1 {
                return Err(arity_err("hgetall"));
            }
            Ok(Command::HGetAll(rest[0].clone()))
        }
        "HEXISTS" => {
            if rest.len() != 2 {
                return Err(arity_err("hexists"));
            }
            Ok(Command::HExists(rest[0].clone(), rest[1].clone()))
        }

        "SADD" => {
            if rest.len() < 2 {
                return Err(arity_err("sadd"));
            }
            Ok(Command::SAdd(rest[0].clone(), rest[1..].to_vec()))
        }
        "SREM" => {
            if rest.len() < 2 {
                return Err(arity_err("srem"));
            }
            Ok(Command::SRem(rest[0].clone(), rest[1..].to_vec()))
        }
        "SMEMBERS" => {
            if rest.len() != 1 {
                return Err(arity_err("smembers"));
            }
            Ok(Command::SMembers(rest[0].clone()))
        }
        "SISMEMBER" => {
            if rest.len() != 2 {
                return Err(arity_err("sismember"));
            }
            Ok(Command::SIsMember(rest[0].clone(), rest[1].clone()))
        }

        "ZADD" => {
            if rest.len() < 3 || !(rest.len() - 1).is_multiple_of(2) {
                return Err(arity_err("zadd"));
            }
            let key = rest[0].clone();
            let mut pairs = Vec::with_capacity((rest.len() - 1) / 2);
            for c in rest[1..].chunks(2) {
                pairs.push((parse_f64(&c[0])?, c[1].clone()));
            }
            Ok(Command::ZAdd(key, pairs))
        }
        "ZRANGE" => {
            if rest.len() < 3 || rest.len() > 4 {
                return Err(arity_err("zrange"));
            }
            let with_scores = if rest.len() == 4 {
                if String::from_utf8_lossy(&rest[3]).to_ascii_uppercase() != "WITHSCORES" {
                    return Err("ERR syntax error".to_string());
                }
                true
            } else {
                false
            };
            Ok(Command::ZRange {
                key: rest[0].clone(),
                start: parse_i64(&rest[1], "start")?,
                stop: parse_i64(&rest[2], "stop")?,
                with_scores,
            })
        }
        "ZSCORE" => {
            if rest.len() != 2 {
                return Err(arity_err("zscore"));
            }
            Ok(Command::ZScore(rest[0].clone(), rest[1].clone()))
        }
        "ZREM" => {
            if rest.len() < 2 {
                return Err(arity_err("zrem"));
            }
            Ok(Command::ZRem(rest[0].clone(), rest[1..].to_vec()))
        }

        "SUBSCRIBE" => {
            if rest.is_empty() {
                return Err(arity_err("subscribe"));
            }
            Ok(Command::Subscribe(rest.to_vec()))
        }
        "UNSUBSCRIBE" => Ok(Command::Unsubscribe(rest.to_vec())),
        "PUBLISH" => {
            if rest.len() != 2 {
                return Err(arity_err("publish"));
            }
            Ok(Command::Publish(rest[0].clone(), rest[1].clone()))
        }

        _ => Err(format!("ERR unknown command '{}'", name)),
    }
}

fn as_int(value: &Value) -> Result<i64, String> {
    match value {
        Value::Str(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| "ERR value is not an integer or out of range".to_string()),
        _ => Err(WRONGTYPE.to_string()),
    }
}

/// Clamps a `[start, stop]` index range (Redis's negative-index and
/// out-of-bounds rules) against a collection of length `len`. Returns
/// `None` if the resulting range is empty. Shared by `LRANGE`/`ZRANGE`.
fn normalize_range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len_i = len as i64;
    let norm = |i: i64| if i < 0 { i + len_i } else { i };
    let mut s = norm(start);
    let mut e = norm(stop);
    if s < 0 {
        s = 0;
    }
    if e >= len_i {
        e = len_i - 1;
    }
    if s > e || s >= len_i || e < 0 {
        return None;
    }
    Some((s as usize, e as usize))
}

/// Executes a parsed command against the store, producing the RESP reply.
pub fn execute(store: &Store, pubsub: &PubSub, cmd: &Command) -> Reply {
    match cmd {
        Command::Ping(msg) => match msg {
            Some(m) => Reply::Bulk(m.clone()),
            None => Reply::Simple("PONG".to_string()),
        },
        Command::Echo(msg) => Reply::Bulk(msg.clone()),
        Command::Set {
            key,
            value,
            expires_at_ms,
            nx,
            xx,
        } => {
            let ok = store.set(
                key.clone(),
                Value::Str(value.clone()),
                *expires_at_ms,
                *nx,
                *xx,
            );
            if ok {
                Reply::ok()
            } else {
                Reply::Nil
            }
        }
        Command::Get(key) => match store.get(key) {
            Some(Value::Str(b)) => Reply::Bulk(b),
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
            None => Reply::Nil,
        },
        Command::Del(keys) => Reply::Integer(store.del(keys)),
        Command::Exists(keys) => Reply::Integer(store.exists(keys)),
        Command::Expire { key, at_ms } => {
            Reply::Integer(if store.expire(key, *at_ms) { 1 } else { 0 })
        }
        Command::Persist(key) => Reply::Integer(if store.persist(key) { 1 } else { 0 }),
        Command::Ttl { key, as_ms } => match store.ttl_ms(key) {
            None => Reply::Integer(-2),       // key doesn't exist
            Some(None) => Reply::Integer(-1), // no TTL set
            Some(Some(ms)) => Reply::Integer(if *as_ms { ms } else { ms / 1000 }),
        },
        Command::Incr(key) => incr_by(store, key, 1),
        Command::Decr(key) => incr_by(store, key, -1),
        Command::IncrBy(key, n) => incr_by(store, key, *n),
        Command::DecrBy(key, n) => incr_by(store, key, -*n),
        Command::Append(key, suffix) => {
            let result = store.with_entry_mut(
                key,
                || Value::Str(Bytes::new()),
                |v| match v {
                    Value::Str(b) => {
                        let mut buf = b.to_vec();
                        buf.extend_from_slice(suffix);
                        *b = Bytes::from(buf);
                        Ok(b.len())
                    }
                    _ => Err(WRONGTYPE.to_string()),
                },
            );
            match result {
                Ok(len) => Reply::Integer(len as i64),
                Err(e) => Reply::Error(e),
            }
        }
        Command::MGet(keys) => Reply::Array(
            keys.iter()
                .map(|k| match store.get(k) {
                    Some(Value::Str(b)) => Reply::Bulk(b),
                    Some(_) => Reply::Nil,
                    None => Reply::Nil,
                })
                .collect(),
        ),
        Command::MSet(pairs) => {
            for (k, v) in pairs {
                store.set(k.clone(), Value::Str(v.clone()), None, false, false);
            }
            Reply::ok()
        }
        Command::Type(key) => match store.key_type(key) {
            Some(t) => Reply::Simple(t.to_string()),
            None => Reply::Simple("none".to_string()),
        },
        Command::FlushAll => {
            store.flush_all();
            Reply::ok()
        }
        Command::Info => Reply::Bulk(Bytes::from(format!(
            "# Server\r\nimcache_version:0.1.0\r\nmode:standalone\r\nshards:{}\r\n",
            store.num_shards()
        ))),
        Command::ConfigGet(param) => {
            Reply::Array(vec![Reply::Bulk(param.clone()), Reply::Bulk(Bytes::new())])
        }
        Command::ConfigSet(param, value) => {
            tracing::debug!(param = %String::from_utf8_lossy(param), value = %String::from_utf8_lossy(value), "CONFIG SET (no-op stub)");
            Reply::ok()
        }
        Command::BgSave => Reply::Simple("Background saving started".to_string()),
        Command::Quit => Reply::ok(),

        Command::LPush(key, values) | Command::RPush(key, values) => {
            let push_front = matches!(cmd, Command::LPush(_, _));
            let result = store.with_entry_mut_prune(
                key,
                || Value::List(VecDeque::new()),
                |v| match v {
                    Value::List(list) => {
                        for val in values {
                            if push_front {
                                list.push_front(val.clone());
                            } else {
                                list.push_back(val.clone());
                            }
                        }
                        (Ok(list.len() as i64), false)
                    }
                    _ => (Err(WRONGTYPE.to_string()), false),
                },
            );
            match result {
                Ok(n) => Reply::Integer(n),
                Err(e) => Reply::Error(e),
            }
        }
        Command::LPop(key) | Command::RPop(key) => {
            let pop_front = matches!(cmd, Command::LPop(_));
            let result = store.with_existing_entry_mut(key, |v| match v {
                Value::List(list) => {
                    let popped = if pop_front {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    };
                    let empty = list.is_empty();
                    (Ok(popped), empty)
                }
                _ => (Err(WRONGTYPE.to_string()), false),
            });
            match result {
                None | Some(Ok(None)) => Reply::Nil,
                Some(Ok(Some(val))) => Reply::Bulk(val),
                Some(Err(e)) => Reply::Error(e),
            }
        }
        Command::LRange { key, start, stop } => match store.get(key) {
            Some(Value::List(list)) => {
                let items = match normalize_range(list.len(), *start, *stop) {
                    Some((s, e)) => list
                        .iter()
                        .skip(s)
                        .take(e - s + 1)
                        .map(|b| Reply::Bulk(b.clone()))
                        .collect(),
                    None => Vec::new(),
                };
                Reply::Array(items)
            }
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
            None => Reply::Array(Vec::new()),
        },
        Command::LLen(key) => match store.get(key) {
            Some(Value::List(list)) => Reply::Integer(list.len() as i64),
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
            None => Reply::Integer(0),
        },

        Command::HSet(key, pairs) => {
            let result = store.with_entry_mut_prune(
                key,
                || Value::Hash(HashMap::new()),
                |v| match v {
                    Value::Hash(h) => {
                        let mut added = 0i64;
                        for (f, val) in pairs {
                            if h.insert(f.clone(), val.clone()).is_none() {
                                added += 1;
                            }
                        }
                        (Ok(added), false)
                    }
                    _ => (Err(WRONGTYPE.to_string()), false),
                },
            );
            match result {
                Ok(n) => Reply::Integer(n),
                Err(e) => Reply::Error(e),
            }
        }
        Command::HGet(key, field) => match store.get(key) {
            Some(Value::Hash(h)) => match h.get(field) {
                Some(v) => Reply::Bulk(v.clone()),
                None => Reply::Nil,
            },
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
            None => Reply::Nil,
        },
        Command::HDel(key, fields) => {
            let result = store.with_existing_entry_mut(key, |v| match v {
                Value::Hash(h) => {
                    let mut removed = 0i64;
                    for f in fields {
                        if h.remove(f).is_some() {
                            removed += 1;
                        }
                    }
                    let empty = h.is_empty();
                    (Ok(removed), empty)
                }
                _ => (Err(WRONGTYPE.to_string()), false),
            });
            match result {
                None => Reply::Integer(0),
                Some(Ok(n)) => Reply::Integer(n),
                Some(Err(e)) => Reply::Error(e),
            }
        }
        Command::HGetAll(key) => match store.get(key) {
            Some(Value::Hash(h)) => {
                let mut items = Vec::with_capacity(h.len() * 2);
                for (f, v) in h.into_iter() {
                    items.push(Reply::Bulk(f));
                    items.push(Reply::Bulk(v));
                }
                Reply::Array(items)
            }
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
            None => Reply::Array(Vec::new()),
        },
        Command::HExists(key, field) => match store.get(key) {
            Some(Value::Hash(h)) => Reply::Integer(if h.contains_key(field) { 1 } else { 0 }),
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
            None => Reply::Integer(0),
        },

        Command::SAdd(key, members) => {
            let result = store.with_entry_mut_prune(
                key,
                || Value::Set(HashSet::new()),
                |v| match v {
                    Value::Set(s) => {
                        let mut added = 0i64;
                        for m in members {
                            if s.insert(m.clone()) {
                                added += 1;
                            }
                        }
                        (Ok(added), false)
                    }
                    _ => (Err(WRONGTYPE.to_string()), false),
                },
            );
            match result {
                Ok(n) => Reply::Integer(n),
                Err(e) => Reply::Error(e),
            }
        }
        Command::SRem(key, members) => {
            let result = store.with_existing_entry_mut(key, |v| match v {
                Value::Set(s) => {
                    let mut removed = 0i64;
                    for m in members {
                        if s.remove(m) {
                            removed += 1;
                        }
                    }
                    let empty = s.is_empty();
                    (Ok(removed), empty)
                }
                _ => (Err(WRONGTYPE.to_string()), false),
            });
            match result {
                None => Reply::Integer(0),
                Some(Ok(n)) => Reply::Integer(n),
                Some(Err(e)) => Reply::Error(e),
            }
        }
        Command::SMembers(key) => match store.get(key) {
            Some(Value::Set(s)) => Reply::Array(s.into_iter().map(Reply::Bulk).collect()),
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
            None => Reply::Array(Vec::new()),
        },
        Command::SIsMember(key, member) => match store.get(key) {
            Some(Value::Set(s)) => Reply::Integer(if s.contains(member) { 1 } else { 0 }),
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
            None => Reply::Integer(0),
        },

        Command::ZAdd(key, pairs) => {
            let result = store.with_entry_mut_prune(
                key,
                || Value::ZSet(HashMap::new()),
                |v| match v {
                    Value::ZSet(z) => {
                        let mut added = 0i64;
                        for (score, member) in pairs {
                            if z.insert(member.clone(), *score).is_none() {
                                added += 1;
                            }
                        }
                        (Ok(added), false)
                    }
                    _ => (Err(WRONGTYPE.to_string()), false),
                },
            );
            match result {
                Ok(n) => Reply::Integer(n),
                Err(e) => Reply::Error(e),
            }
        }
        Command::ZRange {
            key,
            start,
            stop,
            with_scores,
        } => match store.get(key) {
            Some(Value::ZSet(z)) => {
                let mut members: Vec<(Bytes, f64)> = z.into_iter().collect();
                members.sort_by(|a, b| {
                    a.1.partial_cmp(&b.1)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| a.0.cmp(&b.0))
                });
                let items = match normalize_range(members.len(), *start, *stop) {
                    Some((s, e)) => {
                        let mut out = Vec::new();
                        for (member, score) in &members[s..=e] {
                            out.push(Reply::Bulk(member.clone()));
                            if *with_scores {
                                out.push(Reply::Bulk(Bytes::from(score.to_string())));
                            }
                        }
                        out
                    }
                    None => Vec::new(),
                };
                Reply::Array(items)
            }
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
            None => Reply::Array(Vec::new()),
        },
        Command::ZScore(key, member) => match store.get(key) {
            Some(Value::ZSet(z)) => match z.get(member) {
                Some(score) => Reply::Bulk(Bytes::from(score.to_string())),
                None => Reply::Nil,
            },
            Some(_) => Reply::Error(WRONGTYPE.to_string()),
            None => Reply::Nil,
        },
        Command::ZRem(key, members) => {
            let result = store.with_existing_entry_mut(key, |v| match v {
                Value::ZSet(z) => {
                    let mut removed = 0i64;
                    for m in members {
                        if z.remove(m).is_some() {
                            removed += 1;
                        }
                    }
                    let empty = z.is_empty();
                    (Ok(removed), empty)
                }
                _ => (Err(WRONGTYPE.to_string()), false),
            });
            match result {
                None => Reply::Integer(0),
                Some(Ok(n)) => Reply::Integer(n),
                Some(Err(e)) => Reply::Error(e),
            }
        }

        Command::Publish(channel, message) => {
            let n = pubsub.publish(channel, message.clone());
            Reply::Integer(n as i64)
        }
        // SUBSCRIBE/UNSUBSCRIBE are intercepted in main.rs's connection loop
        // before reaching execute(); these arms are an unreachable-in-practice
        // safety net so `execute` stays a total function over `Command`.
        Command::Subscribe(_) | Command::Unsubscribe(_) => {
            Reply::Error("ERR SUBSCRIBE/UNSUBSCRIBE is not allowed in this context".to_string())
        }
    }
}

fn incr_by(store: &Store, key: &Bytes, delta: i64) -> Reply {
    let result = store.with_entry_mut(
        key,
        || Value::Str(Bytes::from_static(b"0")),
        |v| -> Result<i64, String> {
            let current = as_int(v)?;
            let next = current
                .checked_add(delta)
                .ok_or_else(|| "ERR increment or decrement would overflow".to_string())?;
            if let Value::Str(b) = v {
                *b = Bytes::from(next.to_string());
            }
            Ok(next)
        },
    );
    match result {
        Ok(n) => Reply::Integer(n),
        Err(e) => Reply::Error(e),
    }
}
