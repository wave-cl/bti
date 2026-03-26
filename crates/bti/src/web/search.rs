use std::sync::Arc;
use std::time::Duration;

use bti_core::storage;
use redb::{Database, ReadableTable};
use tracing::{error, info};

/// Continuously classifies new entries from TORRENTS that aren't yet in CATEGORIES.
pub async fn run_classifier_loop(db: Arc<Database>) {
    let mut last_ts: u32 = 0;

    loop {
        match classify_batch(&db, &mut last_ts) {
            Ok(0) => {} // nothing new
            Ok(n) => {
                info!("classified {} new entries", n);
            }
            Err(e) => {
                error!("classifier error: {}", e);
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

fn classify_batch(
    db: &Database,
    last_ts: &mut u32,
) -> Result<u64, Box<dyn std::error::Error>> {
    let rtx = db.begin_read()?;
    let mut entries = Vec::new();

    storage::entries_since(&rtx, *last_ts, |infohash, entry| {
        entries.push((infohash, entry));
        entries.len() < 1000
    })?;
    drop(rtx);

    if entries.is_empty() {
        return Ok(0);
    }

    let wtx = db.begin_write()?;

    // Open/create overlay tables
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
        let current = stats_table.get(category as u8)?
            .map(|v| v.value())
            .unwrap_or(0);
        stats_table.insert(category as u8, current + 1)?;

        // Build search index
        let tokens = tokenize(&entry.name);
        for token in tokens {
            let mut key = Vec::with_capacity(token.len() + 1 + 20);
            key.extend_from_slice(token.as_bytes());
            key.push(0);
            key.extend_from_slice(infohash);
            search_table.insert(key.as_slice(), &[] as &[u8])?;
        }

        *last_ts = entry.discovered_at;
        count += 1;
    }

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
