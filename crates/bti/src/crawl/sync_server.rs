use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use ed25519_dalek::SigningKey;
use redb::Database;
use tracing::{info, warn};

pub async fn run_sync_server(
    addr: SocketAddr,
    key_file: &Path,
    db: Arc<Database>,
) -> Result<(), Box<dyn std::error::Error>> {
    let signing_key = load_or_generate_key(key_file)?;
    let pub_key = signing_key.verifying_key();

    info!(
        "sync server listening on {} (pubkey: {})",
        addr,
        hex::encode(pub_key.to_bytes())
    );

    let listener = squic::listen(addr, &signing_key, squic::Config::default()).await?;

    loop {
        let incoming = match listener.accept().await {
            Some(inc) => inc,
            None => {
                info!("sync server stopped");
                return Ok(());
            }
        };

        let db = db.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    if let Err(e) = handle_sync_connection(conn, db).await {
                        warn!("sync connection error: {}", e);
                    }
                }
                Err(e) => {
                    warn!("sync accept error: {}", e);
                }
            }
        });
    }
}

async fn handle_sync_connection(
    conn: quinn::Connection,
    db: Arc<Database>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut send, mut recv) = conn.accept_bi().await?;

    let since = bti_core::sync_proto::read_sync_request(&mut recv).await?;
    info!("sync request from {}: since={}", conn.remote_address(), since);

    let rtx = db.begin_read()?;
    let mut count = 0u64;

    bti_core::storage::entries_since(&rtx, since, |_infohash, _entry| {
        // We need to block on async write — use a channel approach
        // For simplicity, collect entries and write after
        // Actually, we can't call async from a closure easily.
        // We'll collect into a vec (limited batches).
        count += 1;
        true
    })?;

    // Re-iterate and write entries via async stream
    // Since entries_since takes a sync closure, we collect first then stream
    let rtx = db.begin_read()?;
    let mut entries = Vec::new();
    bti_core::storage::entries_since(&rtx, since, |infohash, entry| {
        entries.push((infohash, entry));
        entries.len() < 100_000 // cap at 100k per sync
    })?;
    drop(rtx);

    for (infohash, entry) in &entries {
        bti_core::sync_proto::write_sync_entry(&mut send, infohash, entry).await?;
    }

    bti_core::sync_proto::write_sync_eof(&mut send).await?;
    send.finish()?;

    info!("sync complete: sent {} entries to {}", entries.len(), conn.remote_address());
    conn.closed().await;
    Ok(())
}

fn load_or_generate_key(path: &Path) -> Result<SigningKey, Box<dyn std::error::Error>> {
    if path.exists() {
        let hex_str = std::fs::read_to_string(path)?.trim().to_string();
        let bytes = hex::decode(&hex_str)?;
        if bytes.len() != 32 {
            return Err("invalid key file: expected 32 bytes".into());
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&bytes);
        Ok(SigningKey::from_bytes(&key_bytes))
    } else {
        let (signing_key, pub_key_bytes) = squic::generate_keypair();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, hex::encode(signing_key.to_bytes()))?;
        info!(
            "generated new sync key: {} (saved to {})",
            hex::encode(pub_key_bytes),
            path.display()
        );
        Ok(signing_key)
    }
}
