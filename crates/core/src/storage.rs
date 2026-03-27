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

/// Metadata key-value store (watermarks, state).
pub const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

pub fn open_db(path: &Path) -> Result<Database, Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut db = Database::builder()
        .set_cache_size(64 * 1024 * 1024)
        .create(path)?;

    // Ensure tables exist
    let wtx = db.begin_write()?;
    wtx.open_table(TORRENTS)?;
    wtx.open_table(TIME_INDEX)?;
    wtx.commit()?;

    Ok(db)
}

const ZSTD_MAGIC: u8 = 0x01;

/// Encode a TorrentEntry into compact binary, zstd-compressed.
/// Format: [0x01][zstd(inner)] where inner = [4B discovered_at][6B size][name_bytes][0x00 + files_bytes if non-empty]
/// Files: [2B count][per file: 8B size][2B path_len][path_bytes]
pub fn encode_entry(entry: &TorrentEntry) -> Vec<u8> {
    let raw = encode_entry_raw(entry);
    let compressed = zstd::encode_all(raw.as_slice(), 3).unwrap_or(raw);
    let mut buf = Vec::with_capacity(1 + compressed.len());
    buf.push(ZSTD_MAGIC);
    buf.extend_from_slice(&compressed);
    buf
}

fn encode_entry_raw(entry: &TorrentEntry) -> Vec<u8> {
    let name_bytes = entry.name.as_bytes();
    let mut buf = Vec::with_capacity(10 + name_bytes.len());
    buf.extend_from_slice(&entry.discovered_at.to_be_bytes());
    buf.extend_from_slice(&encode_u48(entry.size));
    buf.extend_from_slice(name_bytes);
    if !entry.files.is_empty() {
        buf.push(0x00);
        let count = entry.files.len().min(u16::MAX as usize) as u16;
        buf.extend_from_slice(&count.to_be_bytes());
        for f in entry.files.iter().take(count as usize) {
            buf.extend_from_slice(&f.size.to_be_bytes());
            let path_bytes = f.path.as_bytes();
            let path_len = path_bytes.len().min(u16::MAX as usize) as u16;
            buf.extend_from_slice(&path_len.to_be_bytes());
            buf.extend_from_slice(&path_bytes[..path_len as usize]);
        }
    }
    buf
}

/// Decode compact binary into a TorrentEntry. Handles both zstd-compressed (0x01 prefix) and legacy raw entries.
pub fn decode_entry(data: &[u8]) -> Result<TorrentEntry, Error> {
    if data.is_empty() {
        return Err(Error::InvalidData("empty entry".into()));
    }
    if data[0] == ZSTD_MAGIC {
        let decompressed = zstd::decode_all(&data[1..])
            .map_err(|e| Error::InvalidData(format!("zstd decompress: {e}").into()))?;
        decode_entry_raw(&decompressed)
    } else {
        decode_entry_raw(data)
    }
}

fn decode_entry_raw(data: &[u8]) -> Result<TorrentEntry, Error> {
    if data.len() < 10 {
        return Err(Error::InvalidData("entry too short".into()));
    }
    let discovered_at = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let size = decode_u48(&[data[4], data[5], data[6], data[7], data[8], data[9]]);

    let rest = &data[10..];
    let (name_bytes, files) = if let Some(null_pos) = rest.iter().position(|&b| b == 0) {
        let files = decode_files(&rest[null_pos + 1..]);
        (&rest[..null_pos], files)
    } else {
        (rest, Vec::new())
    };

    let name = String::from_utf8_lossy(name_bytes).into_owned();
    Ok(TorrentEntry { name, size, discovered_at, files })
}

fn decode_files(data: &[u8]) -> Vec<crate::model::FileInfo> {
    if data.len() < 2 { return Vec::new(); }
    let count = u16::from_be_bytes([data[0], data[1]]) as usize;
    let mut files = Vec::with_capacity(count);
    let mut pos = 2;
    for _ in 0..count {
        if pos + 10 > data.len() { break; }
        let file_size = u64::from_be_bytes([
            data[pos], data[pos+1], data[pos+2], data[pos+3],
            data[pos+4], data[pos+5], data[pos+6], data[pos+7],
        ]);
        let path_len = u16::from_be_bytes([data[pos+8], data[pos+9]]) as usize;
        pos += 10;
        if pos + path_len > data.len() { break; }
        let path = String::from_utf8_lossy(&data[pos..pos + path_len]).into_owned();
        pos += path_len;
        files.push(crate::model::FileInfo { path, size: file_size });
    }
    files
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

/// Insert a batch of entries in a single transaction. Returns the count of newly inserted entries.
/// Opens TORRENTS and TIME_INDEX once for the entire batch.
pub fn put_entry_batch(
    wtx: &WriteTransaction,
    entries: &[(InfoHash, TorrentEntry)],
) -> Result<u64, Error> {
    let mut table = wtx.open_table(TORRENTS)?;
    let mut ti = wtx.open_table(TIME_INDEX)?;
    let mut inserted = 0u64;
    for (infohash, entry) in entries {
        if table.get(infohash)?.is_some() {
            continue;
        }
        let encoded = encode_entry(entry);
        table.insert(infohash, encoded.as_slice())?;
        let ti_key = time_index_key(entry.discovered_at, infohash);
        ti.insert(&ti_key, &[] as &[u8])?;
        inserted += 1;
    }
    Ok(inserted)
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

/// Iterate entries from a given timestamp (inclusive). For sync protocol use.
pub fn entries_since<F>(
    rtx: &ReadTransaction,
    since: u32,
    mut f: F,
) -> Result<(), Error>
where
    F: FnMut(InfoHash, TorrentEntry) -> bool,
{
    let cursor = time_index_key(since, &[0u8; 20]);
    entries_after(rtx, &cursor, |ih, entry, _key| f(ih, entry))
}

/// Iterate entries after a given cursor (timestamp + infohash), yielding (infohash, entry).
/// Calls the provided closure for each entry. Stops if the closure returns false.
/// Use `[0u8; 24]` as cursor to start from the beginning.
pub fn entries_after<F>(
    rtx: &ReadTransaction,
    cursor: &[u8; 24],
    mut f: F,
) -> Result<(), Error>
where
    F: FnMut(InfoHash, TorrentEntry, [u8; 24]) -> bool,
{
    let ti = rtx.open_table(TIME_INDEX)?;
    let torrents = rtx.open_table(TORRENTS)?;

    // Use exclusive start: skip the cursor key itself
    let range = if *cursor == [0u8; 24] {
        ti.range::<&[u8; 24]>(..)?
    } else {
        // Start after the cursor by using an exclusive range
        // We need the next key after cursor, so we iterate from cursor and skip the first if it matches
        ti.range::<&[u8; 24]>(cursor..)?
    };

    let skip_first = *cursor != [0u8; 24];
    let mut skipped = false;

    for item in range {
        let (key, _) = item?;
        let key_bytes = key.value();

        // Skip the cursor key itself (exclusive start)
        if skip_first && !skipped {
            let mut cursor_match = true;
            for i in 0..24 {
                if key_bytes[i] != cursor[i] {
                    cursor_match = false;
                    break;
                }
            }
            skipped = true;
            if cursor_match {
                continue;
            }
        }

        let mut ti_key = [0u8; 24];
        ti_key.copy_from_slice(key_bytes);
        let mut infohash = [0u8; 20];
        infohash.copy_from_slice(&key_bytes[4..24]);

        if let Some(data) = torrents.get(&infohash)? {
            let entry = decode_entry(data.value())?;
            if !f(infohash, entry, ti_key) {
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

/// Get the N most recently discovered entries (newest first), optionally filtered by category.
/// `offset` skips the first N matching entries (for pagination).
/// When cat_filter is Some, scans the full TIME_INDEX until N matching entries are found.
pub fn recent_entries(
    rtx: &ReadTransaction,
    n: usize,
    offset: usize,
    cat_filter: Option<u8>,
) -> Result<Vec<(InfoHash, TorrentEntry)>, Error> {
    let ti = rtx.open_table(TIME_INDEX)?;
    let torrents = rtx.open_table(TORRENTS)?;
    let cats = cat_filter.map(|_| rtx.open_table(CATEGORIES)).transpose()?;
    let mut results = Vec::with_capacity(n);
    let mut skipped = 0usize;

    for item in ti.range::<&[u8; 24]>(..)?.rev() {
        if results.len() >= n {
            break;
        }
        let (key, _) = item?;
        let key_bytes = key.value();
        let mut infohash = [0u8; 20];
        infohash.copy_from_slice(&key_bytes[4..24]);

        if let Some(filter) = cat_filter {
            let entry_cat = cats.as_ref()
                .and_then(|t| t.get(&infohash).ok().flatten())
                .map(|v| v.value())
                .unwrap_or(crate::model::Category::Other as u8);
            if entry_cat != filter {
                continue;
            }
        }

        if skipped < offset {
            skipped += 1;
            continue;
        }

        if let Some(data) = torrents.get(&infohash)? {
            results.push((infohash, decode_entry(data.value())?));
        }
    }

    Ok(results)
}

/// Get total count of torrents.
pub fn count(rtx: &ReadTransaction) -> Result<u64, Error> {
    let table = rtx.open_table(TORRENTS)?;
    Ok(table.len()?)
}

/// Get a u32 value from the META table.
pub fn get_meta_u32(rtx: &ReadTransaction, key: &str) -> Result<u32, Error> {
    let table = match rtx.open_table(META) {
        Ok(t) => t,
        Err(_) => return Ok(0),
    };
    match table.get(key)? {
        Some(v) => {
            let bytes = v.value();
            if bytes.len() >= 4 {
                Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            } else {
                Ok(0)
            }
        }
        None => Ok(0),
    }
}

/// Set a u32 value in the META table.
pub fn set_meta_u32(wtx: &WriteTransaction, key: &str, val: u32) -> Result<(), Error> {
    let mut table = wtx.open_table(META)?;
    table.insert(key, val.to_be_bytes().as_slice())?;
    Ok(())
}

/// Drop all classification tables and reset the classifier watermark.
pub fn drop_classification(db: &Database) -> Result<(), Error> {
    let wtx = db.begin_write()?;
    let _ = wtx.delete_table(CATEGORIES);
    let _ = wtx.delete_table(SEARCH_INDEX);
    let _ = wtx.delete_table(CATEGORY_STATS);
    // Reset classifier watermark
    if let Ok(mut meta) = wtx.open_table(META) {
        let _ = meta.remove("classifier_ts");
    }
    wtx.commit()?;
    Ok(())
}
