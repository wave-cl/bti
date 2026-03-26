use std::path::PathBuf;
use std::time::Instant;

use bti_core::storage;
use redb::ReadableTable;
use tracing::info;

pub fn run(db_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let mut db = bti_core::storage::open_db(&db_path)?;

    let total = {
        let rtx = db.begin_read()?;
        storage::count(&rtx)?
    };
    info!("{} torrents in database", total);

    // Drop existing classification
    info!("dropping classification tables...");
    storage::drop_classification(&db)?;
    info!("classification tables dropped");

    // Classify all entries using cursor-based iteration
    let mut cursor = [0u8; 24];
    let mut classified: u64 = 0;
    let start = Instant::now();

    loop {
        let rtx = db.begin_read()?;
        let mut entries = Vec::with_capacity(10_000);
        let mut last_cursor = cursor;
        storage::entries_after(&rtx, &cursor, |infohash, entry, key| {
            last_cursor = key;
            entries.push((infohash, entry));
            entries.len() < 10_000
        })?;
        drop(rtx);

        if entries.is_empty() {
            break;
        }

        cursor = last_cursor;

        let wtx = db.begin_write()?;
        let mut cat_table = wtx.open_table(storage::CATEGORIES)?;
        let mut search_table = wtx.open_table(storage::SEARCH_INDEX)?;
        let mut stats_table = wtx.open_table(storage::CATEGORY_STATS)?;

        for (infohash, entry) in &entries {
            let category = bti_classifier::classify(&entry.name);
            cat_table.insert(infohash, category as u8)?;

            let current = stats_table
                .get(category as u8)?
                .map(|v| v.value())
                .unwrap_or(0);
            stats_table.insert(category as u8, current + 1)?;

            for token in tokenize(&entry.name) {
                let mut key = Vec::with_capacity(token.len() + 1 + 20);
                key.extend_from_slice(token.as_bytes());
                key.push(0);
                key.extend_from_slice(infohash);
                search_table.insert(key.as_slice(), &[] as &[u8])?;
            }
        }

        // Save watermark using the timestamp from the cursor
        let last_ts = u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
        storage::set_meta_u32(&wtx, "classifier_ts", last_ts)?;

        drop(cat_table);
        drop(search_table);
        drop(stats_table);
        wtx.commit()?;

        classified += entries.len() as u64;
        let elapsed = start.elapsed().as_secs_f64();
        let rate = classified as f64 / elapsed;
        info!(
            "classified {}/{} ({:.0}/s, {:.0}%)",
            classified,
            total,
            rate,
            (classified as f64 / total as f64) * 100.0
        );
    }

    let elapsed = start.elapsed();
    info!(
        "classification complete: {} entries in {:.1}s ({:.0}/s)",
        classified,
        elapsed.as_secs_f64(),
        classified as f64 / elapsed.as_secs_f64()
    );

    // Compact
    info!("compacting database...");
    let size_before = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
    if let Err(e) = db.compact() {
        tracing::warn!("compaction failed: {}", e);
    }
    let size_after = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
    info!(
        "compaction done: {} -> {} ({:.1}MB saved)",
        format_size(size_before),
        format_size(size_after),
        (size_before as f64 - size_after as f64) / (1024.0 * 1024.0)
    );

    Ok(())
}

fn tokenize(name: &str) -> Vec<String> {
    name.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| s.len() >= 2)
        .filter(|s| !is_stopword(s))
        .map(|s| s.to_string())
        .collect()
}

fn is_stopword(word: &str) -> bool {
    matches!(
        word,
        "the" | "and" | "for" | "are" | "but" | "not" | "you" | "all" | "can" | "had"
            | "her" | "was" | "one" | "our" | "out" | "has" | "have" | "from" | "with"
            | "this" | "that" | "what" | "www" | "com" | "org" | "net"
    )
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1}GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1}MB", bytes as f64 / 1_048_576.0)
    } else {
        format!("{}KB", bytes / 1024)
    }
}
