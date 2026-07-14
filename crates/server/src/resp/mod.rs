use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Error)]
pub enum RespError {
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A reply value the server sends back to a client, mirroring the RESP2
/// reply types.
#[derive(Debug, Clone)]
pub enum Reply {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Nil,
    Array(Vec<Reply>),
}

impl Reply {
    pub fn ok() -> Reply {
        Reply::Simple("OK".to_string())
    }
}

fn encode_reply(reply: &Reply, out: &mut BytesMut) {
    match reply {
        Reply::Simple(s) => {
            out.put_u8(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Error(s) => {
            out.put_u8(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Integer(n) => {
            out.put_u8(b':');
            out.extend_from_slice(n.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Reply::Bulk(b) => {
            out.put_u8(b'$');
            out.extend_from_slice(b.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
        }
        Reply::Nil => {
            out.extend_from_slice(b"$-1\r\n");
        }
        Reply::Array(items) => {
            out.put_u8(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_reply(item, out);
            }
        }
    }
}

/// Encodes a command (as sent by a client, or as recorded to the AOF) as a
/// RESP array of bulk strings, e.g. `*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n`.
pub fn encode_command(args: &[Bytes]) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(b'*');
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for arg in args {
        out.put_u8(b'$');
        out.extend_from_slice(arg.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    out.freeze()
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Parses one RESP request (an array of bulk strings) from `buf`, advancing
/// it past the consumed bytes. Returns `Ok(None)` if more data is needed.
///
/// Also accepts the inline-command form (a bare line, no `*`/`$` framing)
/// since it doubles as the AOF's own on-disk format and real `redis-cli`
/// falls back to it for simple commands.
pub fn parse_command(buf: &mut BytesMut) -> Result<Option<Vec<Bytes>>, RespError> {
    if buf.is_empty() {
        return Ok(None);
    }

    if buf[0] != b'*' {
        // Inline command: a line of whitespace-separated tokens.
        return match find_crlf(buf) {
            None => Ok(None),
            Some(pos) => {
                let line = buf.split_to(pos);
                buf.advance(2);
                let tokens = line
                    .as_ref()
                    .split(|&b| b == b' ')
                    .filter(|t| !t.is_empty())
                    .map(Bytes::copy_from_slice)
                    .collect();
                Ok(Some(tokens))
            }
        };
    }

    let mut cursor = 0usize;
    let header_end = match find_crlf(&buf[cursor..]) {
        None => return Ok(None),
        Some(p) => cursor + p,
    };
    let count: i64 = std::str::from_utf8(&buf[cursor + 1..header_end])
        .map_err(|_| RespError::Protocol("invalid array length".into()))?
        .parse()
        .map_err(|_| RespError::Protocol("invalid array length".into()))?;
    if count < 0 {
        buf.advance(header_end + 2);
        return Ok(Some(Vec::new()));
    }
    cursor = header_end + 2;

    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        if cursor >= buf.len() {
            return Ok(None);
        }
        if buf[cursor] != b'$' {
            return Err(RespError::Protocol(format!(
                "expected '$', got '{}'",
                buf[cursor] as char
            )));
        }
        let len_end = match find_crlf(&buf[cursor..]) {
            None => return Ok(None),
            Some(p) => cursor + p,
        };
        let len: i64 = std::str::from_utf8(&buf[cursor + 1..len_end])
            .map_err(|_| RespError::Protocol("invalid bulk length".into()))?
            .parse()
            .map_err(|_| RespError::Protocol("invalid bulk length".into()))?;
        let data_start = len_end + 2;
        if len < 0 {
            cursor = data_start;
            args.push(Bytes::new());
            continue;
        }
        let data_end = data_start + len as usize;
        if buf.len() < data_end + 2 {
            return Ok(None);
        }
        args.push(Bytes::copy_from_slice(&buf[data_start..data_end]));
        cursor = data_end + 2;
    }

    let consumed = buf.split_to(cursor);
    drop(consumed);
    Ok(Some(args))
}

/// Tokio codec used on client connections: decodes RESP requests, encodes
/// RESP replies.
#[derive(Default)]
pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = Vec<Bytes>;
    type Error = RespError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        parse_command(src)
    }
}

impl Encoder<Reply> for RespCodec {
    type Error = RespError;

    fn encode(&mut self, item: Reply, dst: &mut BytesMut) -> Result<(), Self::Error> {
        encode_reply(&item, dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_array() {
        let mut buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"[..]);
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            vec![Bytes::from_static(b"GET"), Bytes::from_static(b"foo")]
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn returns_none_on_partial_input() {
        let mut buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nfo"[..]);
        assert!(parse_command(&mut buf).unwrap().is_none());
    }

    #[test]
    fn parses_inline_command() {
        let mut buf = BytesMut::from(&b"PING\r\n"[..]);
        let cmd = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, vec![Bytes::from_static(b"PING")]);
    }

    #[test]
    fn round_trips_encode_command() {
        let args = vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
        ];
        let encoded = encode_command(&args);
        let mut buf = BytesMut::from(&encoded[..]);
        let parsed = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(parsed, args);
    }
}
