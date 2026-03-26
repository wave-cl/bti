use std::path::Path;

use redb::{Database, ReadTransaction, ReadableTable, ReadableTableMetadata, TableDefinition, WriteTransaction};

use crate::model::{decode_u48, encode_u48, InfoHash, TorrentEntry};
use crate::Error;

/// Primary table: infohash -> compact binary (discovered_at + size + name).
const TORRENTS: TableDefinition<&[u8; 20], &[u8]> = TableDefinition::new("torrents");

/// Time index: (discovered_at_be ++ infohash) -> () for ordered sync.
const TIME_INDEX: TableDefinition<&[u8; 24], &[u8]> = TableDefinition::new("time_index");

/// Category overlay: infohash -> category u8 (written by web service).
pub const CATEGORIES: TableDefinition<&[u8; 20], u8> = TableDefinition::new("categories");

/// Search index: (token ++ \0 ++ infohash) -> () (written by web service).
pub const SEARCH_INDEX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("search_index");

/// Per-category counts (written by web service).
pub const CATEGORY_STATS: TableDefinition<u8, u64> = TableDefinition::new("category_stats");

pub fn open_db(path: &Path) -> Result<Database, Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let db = Database::create(path)?;

    let wtx = db.begin_write()?;
    wtx.open_table(TORRENTS)?;
    wtx.open_table(TIME_INDEX)?;
    wtx.commit()?;

    Ok(db)
}

/// Encode a TorrentEntry into compact binary.
pub fn encode_entry(entry: &TorrentEntry) -> Vec<u8> {
    let name_bytes = entry.name.as_bytes();
    let mut buf = Vec::with_capacity(10 + name_bytes.len());
    buf.extend_from_slice(&entry.discovered_at.to_be_bytes());
    buf.extend_from_slice(&encode_u48(entry.size));
    buf.extend_from_slice(name_bytes);
    buf
}

/// Decode compact binary into a TorrentEntry.
pub fn decode_entry(data: &[u8]) -> Result<TorrentEntry, Error> {
    if data.len() < 10 {
        return Err(Error::InvalidData("entry too short".into()));
    }
    let discovered_at = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let size = decode_u48(&[data[4], data[5], data[6], data[7], data[8], data[9]]);
    let name = String::from_utf8_lossy(&data[10..]).into_owned();
    Ok(TorrentEntry {
        name,
        size,
        discovered_at,
    })
}

/// Build the 24-byte composite key for TIME_INDEX.
pub fn time_index_key(discovered_at: u32, infohash: &InfoHash) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[..4].copy_from_slice(&discovered_at.to_be_bytes());
    key[4..24].copy_from_slice(infohash);
    key
}

/// Insert a torrent entry. Returns true if it was new.
pub fn put_entry(
    wtx: &WriteTransaction,
    infohash: &InfoHash,
    entry: &TorrentEntry,
) -> Result<bool, Error> {
    let mut table = wtx.open_table(TORRENTS)?;
    if table.get(infohash)?.is_some() {
        return Ok(false);
    }
    let encoded = encode_entry(entry);
    table.insert(infohash, encoded.as_slice())?;

    let ti_key = time_index_key(entry.discovered_at, infohash);
    let mut ti = wtx.open_table(TIME_INDEX)?;
    ti.insert(&ti_key, &[] as &[u8])?;

    Ok(true)
}

/// Check if an infohash exists.
pub fn has_entry(rtx: &ReadTransaction, infohash: &InfoHash) -> Result<bool, Error> {
    let table = rtx.open_table(TORRENTS)?;
    Ok(table.get(infohash)?.is_some())
}

/// Get a single entry by infohash.
pub fn get_entry(
    rtx: &ReadTransaction,
    infohash: &InfoHash,
) -> Result<Option<TorrentEntry>, Error> {
    let table = rtx.open_table(TORRENTS)?;
    match table.get(infohash)? {
        Some(data) => Ok(Some(decode_entry(data.value())?)),
        None => Ok(None),
    }
}

/// Iterate entries since a given timestamp (epoch-offset u32), yielding (infohash, entry).
/// Calls the provided closure for each entry. Stops if the closure returns false.
pub fn entries_since<F>(
    rtx: &ReadTransaction,
    since: u32,
    mut f: F,
) -> Result<(), Error>
where
    F: FnMut(InfoHash, TorrentEntry) -> bool,
{
    let ti = rtx.open_table(TIME_INDEX)?;
    let torrents = rtx.open_table(TORRENTS)?;

    let start_key: &[u8; 24] = &time_index_key(since, &[0u8; 20]);
    let range = ti.range::<&[u8; 24]>(start_key..)?;

    for item in range {
        let (key, _) = item?;
        let key_bytes = key.value();
        let mut infohash = [0u8; 20];
        infohash.copy_from_slice(&key_bytes[4..24]);

        if let Some(data) = torrents.get(&infohash)? {
            let entry = decode_entry(data.value())?;
            if !f(infohash, entry) {
                break;
            }
        }
    }

    Ok(())
}

/// Get the latest timestamp in the TIME_INDEX, or 0 if empty.
pub fn latest_timestamp(rtx: &ReadTransaction) -> Result<u32, Error> {
    let ti = rtx.open_table(TIME_INDEX)?;
    let result = match ti.last()? {
        Some((key, _)) => {
            let key_bytes = key.value();
            u32::from_be_bytes([
                key_bytes[0],
                key_bytes[1],
                key_bytes[2],
                key_bytes[3],
            ])
        }
        None => 0,
    };
    Ok(result)
}

/// Get total count of torrents.
pub fn count(rtx: &ReadTransaction) -> Result<u64, Error> {
    let table = rtx.open_table(TORRENTS)?;
    Ok(table.len()?)
}
