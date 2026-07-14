use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::shard::{Entry, Store};

#[derive(Serialize, Deserialize)]
struct SnapshotEntry {
    key: Vec<u8>,
    entry: Entry,
}

fn io_err(e: impl std::error::Error) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

/// Serializes every live key into `path`, writing to a temp file first and
/// renaming into place so a crash mid-write never leaves a corrupt snapshot.
pub fn save(store: &Store, path: &Path) -> std::io::Result<()> {
    let mut entries = Vec::new();
    store.for_each_live_entry(|k, e| {
        entries.push(SnapshotEntry {
            key: k.to_vec(),
            entry: e.clone(),
        });
    });
    let tmp_path = path.with_extension("tmp");
    {
        let file = File::create(&tmp_path)?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, &entries).map_err(io_err)?;
    }
    std::fs::rename(tmp_path, path)?;
    Ok(())
}

/// Loads a snapshot into `store`, if one exists. Returns whether a
/// snapshot file was found and loaded.
pub fn load(store: &Store, path: &Path) -> std::io::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let entries: Vec<SnapshotEntry> = bincode::deserialize_from(reader).map_err(io_err)?;
    for se in entries {
        store.load_entry(Bytes::from(se.key), se.entry);
    }
    Ok(true)
}
