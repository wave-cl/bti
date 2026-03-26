use std::sync::Arc;
use std::time::Duration;

use bti_core::storage;
use redb::{Database, ReadableTable};
use tracing::{error, info};

const CLASSIFIER_TS_KEY: &str = "classifier_ts";
const BATCH_SIZE: usize = 10_000;

/// Continuously classifies new entries from TORRENTS that aren't yet in CATEGORIES.
pub async fn run_classifier_loop(db: Arc<Database>) {
    // Load watermark from persistent storage
    let mut last_ts: u32 = {
        match db.begin_read() {
            Ok(rtx) => storage::get_meta_u32(&rtx, CLASSIFIER_TS_KEY).unwrap_or(0),
            Err(_) => 0,
        }
    };

    if last_ts > 0 {
        info!("classifier resuming from timestamp {}", last_ts);
    }

    loop {
        match classify_batch(&db, &mut last_ts) {
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
    last_ts: &mut u32,
) -> Result<u64, Box<dyn std::error::Error>> {
    let rtx = db.begin_read()?;
    let mut entries = Vec::with_capacity(BATCH_SIZE);

    storage::entries_since(&rtx, *last_ts, |infohash, entry| {
        entries.push((infohash, entry));
        entries.len() < BATCH_SIZE
    })?;
    drop(rtx);

    if entries.is_empty() {
        return Ok(0);
    }

    let wtx = db.begin_write()?;

    let mut cat_table = wtx.open_table(storage::CATEGORIES)?;
    let mut search_table = wtx.open_table(storage::SEARCH_INDEX)?;
    let mut stats_table = wtx.open_table(storage::CATEGORY_STATS)?;

    let mut count = 0u64;
    for (infohash, entry) in &entries {
        // Skip if already classified
        if cat_table.get(infohash)?.is_some() {
            *last_ts = entry.discovered_at;
            continue;
        }

        let category = bti_classifier::classify(&entry.name);
        cat_table.insert(infohash, category as u8)?;

        // Update category stats
        let current = stats_table
            .get(category as u8)?
            .map(|v| v.value())
            .unwrap_or(0);
        stats_table.insert(category as u8, current + 1)?;

        // Build search index
        for token in tokenize(&entry.name) {
            let mut key = Vec::with_capacity(token.len() + 1 + 20);
            key.extend_from_slice(token.as_bytes());
            key.push(0);
            key.extend_from_slice(infohash);
            search_table.insert(key.as_slice(), &[] as &[u8])?;
        }

        *last_ts = entry.discovered_at;
        count += 1;
    }

    // Persist watermark
    storage::set_meta_u32(&wtx, CLASSIFIER_TS_KEY, *last_ts)?;

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
