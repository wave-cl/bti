use std::io::Read;

use bti_core::model::{FileInfo, TorrentEntry};
use redb::Database;
use tracing::info;

/// Import entries from a binary dump file (or stdin with "-").
/// Format per entry:
///   [20 bytes] infohash
///   [8 bytes]  u64 BE discovered_at (unix seconds)
///   [8 bytes]  u64 BE size
///   [2 bytes]  u16 BE name_length
///   [name_length bytes] name UTF-8
///   [2 bytes]  u16 BE file_count
///   per file:
///     [8 bytes]  u64 BE file_size
///     [2 bytes]  u16 BE path_length
///     [path_length bytes] path UTF-8
pub fn run_migrate(path: &str, db: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader: Box<dyn Read> = if path == "-" {
        Box::new(std::io::stdin())
    } else {
        Box::new(std::fs::File::open(path)?)
    };

    let mut count = 0u64;
    let mut batch_count = 0u32;

    let wtx = db.begin_write()?;
    loop {
        // Read infohash
        let mut infohash = [0u8; 20];
        match reader.read_exact(&mut infohash) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        // Read discovered_at
        let mut ts_buf = [0u8; 8];
        reader.read_exact(&mut ts_buf)?;
        let unix_ts = u64::from_be_bytes(ts_buf);

        // Read size
        let mut size_buf = [0u8; 8];
        reader.read_exact(&mut size_buf)?;
        let size = u64::from_be_bytes(size_buf);

        // Read name
        let mut name_len_buf = [0u8; 2];
        reader.read_exact(&mut name_len_buf)?;
        let name_len = u16::from_be_bytes(name_len_buf) as usize;
        let mut name_buf = vec![0u8; name_len];
        reader.read_exact(&mut name_buf)?;
        let name = String::from_utf8_lossy(&name_buf).into_owned();

        // Read files
        let mut file_count_buf = [0u8; 2];
        reader.read_exact(&mut file_count_buf)?;
        let file_count = u16::from_be_bytes(file_count_buf) as usize;
        let mut files = Vec::with_capacity(file_count);
        for _ in 0..file_count {
            let mut fsize_buf = [0u8; 8];
            reader.read_exact(&mut fsize_buf)?;
            let fsize = u64::from_be_bytes(fsize_buf);
            let mut path_len_buf = [0u8; 2];
            reader.read_exact(&mut path_len_buf)?;
            let path_len = u16::from_be_bytes(path_len_buf) as usize;
            let mut path_buf = vec![0u8; path_len];
            reader.read_exact(&mut path_buf)?;
            files.push(FileInfo {
                path: String::from_utf8_lossy(&path_buf).into_owned(),
                size: fsize,
            });
        }

        let entry = TorrentEntry::from_unix(name, size, unix_ts, files);
        match bti_core::storage::put_entry(&wtx, &infohash, &entry) {
            Ok(_) => count += 1,
            Err(e) => {
                tracing::warn!("migrate put error: {}", e);
            }
        }

        batch_count += 1;
        if batch_count % 10000 == 0 {
            info!("migrated {} entries...", batch_count);
        }
    }

    wtx.commit()?;
    info!("migration complete: {} entries imported", count);

    Ok(())
}
