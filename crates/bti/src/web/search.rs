use std::sync::Arc;
use std::time::Duration;

use bti_core::storage;
use redb::{Database, ReadableTable};
use tracing::{error, info};

const CLASSIFIER_TS_KEY: &str = "classifier_ts";
const BATCH_SIZE: usize = 10_000;

/// Continuously classifies new entries from TORRENTS that aren't yet in CATEGORIES.
pub async fn run_classifier_loop(db: Arc<Database>) {
    // Load watermark and build cursor from persisted timestamp
    let mut cursor: [u8; 24] = {
        match db.begin_read() {
            Ok(rtx) => {
                let ts = storage::get_meta_u32(&rtx, CLASSIFIER_TS_KEY).unwrap_or(0);
                if ts > 0 {
                    info!("classifier resuming from timestamp {}", ts);
                    // Start from this timestamp with zeroed infohash — entries_after will skip exact match
                    storage::time_index_key(ts, &[0u8; 20])
                } else {
                    [0u8; 24]
                }
            }
            Err(_) => [0u8; 24],
        }
    };

    loop {
        match classify_batch(&db, &mut cursor) {
            Ok(0) => {}
            Ok(n) => {
                info!("classified {} entries", n);
            }
            Err(e) => {
                error!("classifier error: {}", e);
            }
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn classify_batch(
    db: &Database,
    cursor: &mut [u8; 24],
) -> Result<u64, Box<dyn std::error::Error>> {
    let rtx = db.begin_read()?;
    let mut entries = Vec::with_capacity(BATCH_SIZE);
    let mut last_cursor = *cursor;

    storage::entries_after(&rtx, cursor, |infohash, entry, key| {
        last_cursor = key;
        entries.push((infohash, entry));
        entries.len() < BATCH_SIZE
    })?;
    drop(rtx);

    if entries.is_empty() {
        return Ok(0);
    }

    *cursor = last_cursor;

    let wtx = db.begin_write()?;

    let mut cat_table = wtx.open_table(storage::CATEGORIES)?;
    let mut search_table = wtx.open_table(storage::SEARCH_INDEX)?;
    let mut stats_table = wtx.open_table(storage::CATEGORY_STATS)?;

    let mut count = 0u64;
    for (infohash, entry) in &entries {
        // Skip if already classified
        if cat_table.get(infohash)?.is_some() {
            continue;
        }

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

        count += 1;
    }

    // Persist watermark as timestamp
    let last_ts = u32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
    storage::set_meta_u32(&wtx, CLASSIFIER_TS_KEY, last_ts)?;

    drop(cat_table);
    drop(search_table);
    drop(stats_table);
    wtx.commit()?;
    Ok(count)
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
