use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
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
    crawler_db_size: Arc<AtomicU64>,
    crawler_total: Arc<AtomicU64>,
    crawler_mem_rss: Arc<AtomicU64>,
    crawler_disk_used: Arc<AtomicU64>,
    crawler_disk_total: Arc<AtomicU64>,
) {
    loop {
        status.store(STATUS_CONNECTING, Ordering::Relaxed);
        match sync_once(crawler_addr, crawler_key, &db, &status, &crawler_db_size, &crawler_total, &crawler_mem_rss, &crawler_disk_used, &crawler_disk_total).await {
            Ok(count) => {
                if count > 0 {
                    info!("web sync: {} new entries", count);
                }
            }
            Err(e) => {
                error!("web sync error: {}", e);
                status.store(STATUS_DISCONNECTED, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
        status.store(STATUS_CONNECTING, Ordering::Relaxed);
    }
}

async fn sync_once(
    crawler_addr: SocketAddr,
    crawler_key: &[u8; 32],
    db: &Database,
    status: &AtomicU8,
    crawler_db_size: &AtomicU64,
    crawler_total: &AtomicU64,
    crawler_mem_rss: &AtomicU64,
    crawler_disk_used: &AtomicU64,
    crawler_disk_total: &AtomicU64,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let since = {
        let rtx = db.begin_read()?;
        bti_core::storage::latest_timestamp(&rtx)?
    };

    let conn = squic::dial(crawler_addr, crawler_key, squic::Config::default()).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    status.store(STATUS_CONNECTED, Ordering::Relaxed);

    bti_core::sync_proto::write_sync_request(&mut send, since).await?;
    send.finish()?;

    let h = bti_core::sync_proto::read_sync_header(&mut recv).await?;
    crawler_db_size.store(h.db_size, Ordering::Relaxed);
    crawler_total.store(h.total, Ordering::Relaxed);
    crawler_mem_rss.store(h.mem_rss, Ordering::Relaxed);
    crawler_disk_used.store(h.disk_used, Ordering::Relaxed);
    crawler_disk_total.store(h.disk_total, Ordering::Relaxed);

    let mut count = 0u64;
    loop {
        match bti_core::sync_proto::read_sync_entry(&mut recv).await? {
            Some((infohash, entry)) => {
                let wtx = db.begin_write()?;
                if bti_core::storage::put_entry(&wtx, &infohash, &entry)? {
                    count += 1;
                }
                wtx.commit()?;
            }
            None => break,
        }
    }

    Ok(count)
}
