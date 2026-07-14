use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use bytes::{Bytes, BytesMut};

use crate::commands;
use crate::resp::{encode_command, parse_command};
use crate::shard::Store;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every write - safest, slowest.
    Always,
    /// fsync roughly once a second via a background task - the Redis default.
    EverySec,
    /// let the OS decide when to flush to disk - fastest, least durable.
    Never,
}

impl FsyncPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "always" => Some(FsyncPolicy::Always),
            "everysec" => Some(FsyncPolicy::EverySec),
            "no" | "never" => Some(FsyncPolicy::Never),
            _ => None,
        }
    }
}

pub struct Aof {
    path: PathBuf,
    file: Mutex<BufWriter<File>>,
    policy: FsyncPolicy,
}

impl Aof {
    pub fn open(path: PathBuf, policy: FsyncPolicy) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(BufWriter::new(file)),
            policy,
        })
    }

    pub fn append(&self, args: &[Bytes]) -> std::io::Result<()> {
        let encoded = encode_command(args);
        let mut f = self.file.lock().expect("aof lock poisoned");
        f.write_all(&encoded)?;
        f.flush()?;
        if self.policy == FsyncPolicy::Always {
            f.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Flushes and fsyncs the log; called on a timer for `EverySec` policy.
    pub fn sync(&self) -> std::io::Result<()> {
        let mut f = self.file.lock().expect("aof lock poisoned");
        f.flush()?;
        f.get_ref().sync_all()
    }

    /// Truncates the log to empty. Used right after a successful snapshot,
    /// since the snapshot now captures everything the log had recorded.
    pub fn truncate(&self) -> std::io::Result<()> {
        let mut f = self.file.lock().expect("aof lock poisoned");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        *f = BufWriter::new(file);
        Ok(())
    }
}

/// Replays a previously written AOF file against `store`, reconstructing
/// its state. Malformed trailing bytes (e.g. a partial write from a crash
/// mid-append) are ignored, matching Redis's tolerant AOF loading.
pub fn replay(path: &Path, store: &Store) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let mut file = File::open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;
    let mut buf = BytesMut::from(&data[..]);
    loop {
        match parse_command(&mut buf) {
            Ok(Some(args)) if !args.is_empty() => {
                if let Ok(cmd) = commands::parse(&args) {
                    let _ = commands::execute(store, &cmd);
                }
            }
            Ok(Some(_)) => continue,
            Ok(None) | Err(_) => break,
        }
    }
    Ok(())
}
