use bytes::Bytes;

use crate::resp::Reply;
use crate::shard::{now_ms, Store, Value};

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
}

impl Command {
    /// Whether executing this command mutates the keyspace and therefore
    /// needs to be appended to the AOF.
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
        _ => Err(format!("ERR unknown command '{}'", name)),
    }
}

fn as_int(value: &Value) -> Result<i64, String> {
    match value {
        Value::Str(b) => std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| "ERR value is not an integer or out of range".to_string()),
    }
}

/// Executes a parsed command against the store, producing the RESP reply.
pub fn execute(store: &Store, cmd: &Command) -> Reply {
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
            let new_len = store.with_entry_mut(
                key,
                || Value::Str(Bytes::new()),
                |v| {
                    let Value::Str(b) = v;
                    let mut buf = b.to_vec();
                    buf.extend_from_slice(suffix);
                    *b = Bytes::from(buf);
                    b.len()
                },
            );
            Reply::Integer(new_len as i64)
        }
        Command::MGet(keys) => Reply::Array(
            keys.iter()
                .map(|k| match store.get(k) {
                    Some(Value::Str(b)) => Reply::Bulk(b),
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
            let Value::Str(b) = v;
            *b = Bytes::from(next.to_string());
            Ok(next)
        },
    );
    match result {
        Ok(n) => Reply::Integer(n),
        Err(e) => Reply::Error(e),
    }
}
