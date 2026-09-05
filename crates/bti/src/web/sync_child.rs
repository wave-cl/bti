use std::net::SocketAddr;

use crate::web::CrawlerStats;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use redb::Database;
use tracing::{error, info};

pub const STATUS_CONNECTING: u8 = 1;
pub const STATUS_CONNECTED: u8 = 2;
pub const STATUS_DISCONNECTED: u8 = 3;

pub async fn run_sync_loop(
    crawler_addr: SocketAddr,
    crawler_key: &[u8; 32],
    db: Arc<Database>,
    status: Arc<AtomicU8>,
    stats: CrawlerStats,
) {
    loop {
        status.store(STATUS_CONNECTING, Ordering::Relaxed);
        match sync_once(crawler_addr, crawler_key, &db, &status, &stats).await {
            Ok(count) => {
                info!("web sync: connection closed ({} entries total)", count);
            }
            Err(e) => {
                error!("web sync error: {}", e);
                status.store(STATUS_DISCONNECTED, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn sync_once(
    crawler_addr: SocketAddr,
    crawler_key: &[u8; 32],
    db: &Database,
    status: &AtomicU8,
    stats: &CrawlerStats,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let since = {
        let rtx = db.begin_read()?;
        bti_core::storage::latest_timestamp(&rtx)?
    };

    let conn = squic::dial(crawler_addr, crawler_key, squic::Config {
        keep_alive: Some(Duration::from_secs(10)),
        ..Default::default()
    }).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    status.store(STATUS_CONNECTED, Ordering::Relaxed);

    bti_core::sync_proto::write_sync_request(&mut send, since).await?;
    send.finish()?;

    let h = bti_core::sync_proto::read_sync_header(&mut recv).await?;
    stats.db_size.store(h.db_size, Ordering::Relaxed);
    stats.total.store(h.total, Ordering::Relaxed);
    stats.mem_rss.store(h.mem_rss, Ordering::Relaxed);
    stats.disk_used.store(h.disk_used, Ordering::Relaxed);
    stats.disk_total.store(h.disk_total, Ordering::Relaxed);

    const BATCH_SIZE: usize = 1000;
    let mut batch: Vec<(bti_core::model::InfoHash, bti_core::model::TorrentEntry)> =
        Vec::with_capacity(BATCH_SIZE);
    let mut count = 0u64;

    while let Some(e) = bti_core::sync_proto::read_sync_entry(&mut recv).await? {
        batch.push(e);
        if batch.len() >= BATCH_SIZE {
            let wtx = db.begin_write()?;
            count += bti_core::storage::put_entry_batch(&wtx, &batch)?;
            wtx.commit()?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        let wtx = db.begin_write()?;
        count += bti_core::storage::put_entry_batch(&wtx, &batch)?;
        wtx.commit()?;
    }

    Ok(count)
}
