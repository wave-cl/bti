use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, Json};
use axum::routing::get;
use axum::Router;
use bti_core::model::Category;
use bti_core::storage;
use redb::Database;
use tracing::info;

#[derive(Clone)]
struct AppState {
    db: Arc<Database>,
    db_path: PathBuf,
    sync_status: Option<Arc<AtomicU8>>,
    sync_info: Option<String>,
    crawler_db_size: Option<Arc<AtomicU64>>,
    crawler_total: Option<Arc<AtomicU64>>,
    crawler_mem_rss: Option<Arc<AtomicU64>>,
    crawler_disk_used: Option<Arc<AtomicU64>>,
    crawler_disk_total: Option<Arc<AtomicU64>>,
}

pub async fn run_server(
    listen: SocketAddr,
    db: Arc<Database>,
    db_path: PathBuf,
    sync_status: Option<Arc<AtomicU8>>,
    sync_info: Option<String>,
    crawler_db_size: Option<Arc<AtomicU64>>,
    crawler_total: Option<Arc<AtomicU64>>,
    crawler_mem_rss: Option<Arc<AtomicU64>>,
    crawler_disk_used: Option<Arc<AtomicU64>>,
    crawler_disk_total: Option<Arc<AtomicU64>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState { db, db_path, sync_status, sync_info, crawler_db_size, crawler_total, crawler_mem_rss, crawler_disk_used, crawler_disk_total };

    let app = Router::new()
        .route("/", get(index))
        .route("/logo.svg", get(logo))
        .route("/favicon.svg", get(favicon))
        .route("/api/stats", get(api_stats))
        .route("/api/recent", get(api_recent))
        .route("/api/search", get(api_search))
        .route("/api/torrent/{infohash}", get(api_torrent))
        .with_state(state);

    info!("web server listening on {}", listen);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn logo() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "image/svg+xml")
        .body(axum::body::Body::from(include_str!("logo.svg")))
        .unwrap()
}

async fn favicon() -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", "image/svg+xml")
        .body(axum::body::Body::from(include_str!("favicon.svg")))
        .unwrap()
}

#[derive(serde::Serialize)]
struct SyncInfo {
    label: String,
    state: String,
    crawler_db_size: u64,
    crawler_total: u64,
    crawler_mem_rss: u64,
    crawler_disk_used: u64,
    crawler_disk_total: u64,
}

#[derive(serde::Serialize)]
struct StatsResponse {
    total: u64,
    db_size: u64,
    categories: std::collections::HashMap<String, u64>,
    sync: Option<SyncInfo>,
}

async fn api_stats(State(state): State<AppState>) -> Result<Json<StatsResponse>, StatusCode> {
    let rtx = state.db.begin_read().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let total = storage::count(&rtx).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let db_size = std::fs::metadata(&state.db_path)
        .map(|m| m.len())
        .unwrap_or(0);

    let mut categories = std::collections::HashMap::new();
    if let Ok(table) = rtx.open_table(storage::CATEGORY_STATS) {
        for cat in Category::ALL {
            if let Ok(Some(v)) = table.get(cat as u8) {
                categories.insert(cat.as_str().to_string(), v.value());
            }
        }
    }

    let sync = state.sync_info.as_ref().map(|label| {
        let state_str = match state.sync_status.as_ref().map(|s| s.load(Ordering::Relaxed)) {
            Some(2) => "connected",
            Some(3) => "disconnected",
            _ => "connecting",
        };
        let crawler_db_size = state.crawler_db_size.as_ref().map(|a| a.load(Ordering::Relaxed)).unwrap_or(0);
        let crawler_total = state.crawler_total.as_ref().map(|a| a.load(Ordering::Relaxed)).unwrap_or(0);
        let crawler_mem_rss = state.crawler_mem_rss.as_ref().map(|a| a.load(Ordering::Relaxed)).unwrap_or(0);
        let crawler_disk_used = state.crawler_disk_used.as_ref().map(|a| a.load(Ordering::Relaxed)).unwrap_or(0);
        let crawler_disk_total = state.crawler_disk_total.as_ref().map(|a| a.load(Ordering::Relaxed)).unwrap_or(0);
        SyncInfo { label: label.clone(), state: state_str.to_string(), crawler_db_size, crawler_total, crawler_mem_rss, crawler_disk_used, crawler_disk_total }
    });

    Ok(Json(StatsResponse { total, db_size, categories, sync }))
}

#[derive(serde::Deserialize)]
struct SearchQuery {
    q: String,
    category: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(serde::Serialize)]
struct FileResult {
    path: String,
    size: u64,
}

#[derive(serde::Serialize)]
struct SearchResult {
    infohash: String,
    name: String,
    size: u64,
    category: String,
    discovered_at: u64,
    files: Vec<FileResult>,
}

#[derive(serde::Deserialize)]
struct RecentQuery {
    category: Option<String>,
    offset: Option<usize>,
}

async fn api_recent(
    State(state): State<AppState>,
    Query(params): Query<RecentQuery>,
) -> Result<Json<Vec<SearchResult>>, StatusCode> {
    let rtx = state.db.begin_read().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let cats = rtx.open_table(storage::CATEGORIES).ok();

    let cat_filter = params.category.as_ref().and_then(|c| {
        Category::ALL.iter().find(|cat| cat.as_str() == c.as_str()).copied()
    });

    let cat_filter_u8 = cat_filter.map(|c| c as u8);
    let offset = params.offset.unwrap_or(0);
    let entries = storage::recent_entries(&rtx, 50, offset, cat_filter_u8)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let results = entries
        .into_iter()
        .map(|(ih, entry)| {
            let cat = cats
                .as_ref()
                .and_then(|t| t.get(&ih).ok().flatten())
                .map(|v| Category::from_u8(v.value()))
                .unwrap_or(Category::Other);
            let ts = entry.unix_timestamp();
            let files = entry.files.into_iter().map(|f| FileResult { path: f.path, size: f.size }).collect();
            SearchResult {
                infohash: hex::encode(ih),
                name: entry.name,
                size: entry.size,
                category: cat.as_str().to_string(),
                discovered_at: ts,
                files,
            }
        })
        .collect();

    Ok(Json(results))
}

async fn api_search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<Vec<SearchResult>>, StatusCode> {
    let limit = params.limit.unwrap_or(20).min(100);
    let offset = params.offset.unwrap_or(0);

    let rtx = state.db.begin_read().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let tokens = params
        .q
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| s.len() >= 2)
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    if tokens.is_empty() {
        return Ok(Json(Vec::new()));
    }

    // Find infohashes matching all tokens (intersection)
    let search_table = rtx
        .open_table(storage::SEARCH_INDEX)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut result_sets: Vec<std::collections::HashSet<[u8; 20]>> = Vec::new();

    for token in &tokens {
        let mut prefix = token.as_bytes().to_vec();
        prefix.push(0);
        let prefix_end = {
            let mut end = prefix.clone();
            *end.last_mut().unwrap() = 1; // null + 1
            end
        };

        let mut hashes = std::collections::HashSet::new();
        const MAX_PER_TOKEN: usize = 10_000;
        if let Ok(range) = search_table.range::<&[u8]>(prefix.as_slice()..prefix_end.as_slice()) {
            for item in range {
                if hashes.len() >= MAX_PER_TOKEN {
                    break;
                }
                if let Ok((key, _)) = item {
                    let key_bytes = key.value();
                    if key_bytes.len() >= prefix.len() + 20 {
                        let start = key_bytes.len() - 20;
                        let mut ih = [0u8; 20];
                        ih.copy_from_slice(&key_bytes[start..]);
                        hashes.insert(ih);
                    }
                }
            }
        }
        result_sets.push(hashes);
    }

    let matches: Vec<[u8; 20]> = if result_sets.is_empty() {
        Vec::new()
    } else {
        let mut iter = result_sets.into_iter();
        let mut intersection = iter.next().unwrap();
        for set in iter {
            intersection = intersection.intersection(&set).copied().collect();
        }
        intersection.into_iter().collect()
    };

    // Filter by category if specified
    let cat_filter = params.category.as_ref().and_then(|c| {
        Category::ALL.iter().find(|cat| cat.as_str() == c.as_str()).copied()
    });

    let torrents = rtx
        .open_table(storage::CATEGORIES)
        .ok();

    let mut results = Vec::new();
    let mut skipped = 0;

    for ih in &matches {
        if results.len() >= limit {
            break;
        }

        // Category filter
        if let Some(filter_cat) = cat_filter {
            if let Some(ref cat_table) = torrents {
                match cat_table.get(ih) {
                    Ok(Some(v)) if Category::from_u8(v.value()) != filter_cat => continue,
                    _ => {}
                }
            }
        }

        if skipped < offset {
            skipped += 1;
            continue;
        }

        if let Ok(Some(entry)) = storage::get_entry(&rtx, ih) {
            let cat = torrents
                .as_ref()
                .and_then(|t| t.get(ih).ok().flatten())
                .map(|v| Category::from_u8(v.value()))
                .unwrap_or(Category::Other);

            let ts = entry.unix_timestamp();
            let files = entry.files.into_iter().map(|f| FileResult { path: f.path, size: f.size }).collect();
            results.push(SearchResult {
                infohash: hex::encode(ih),
                name: entry.name,
                size: entry.size,
                category: cat.as_str().to_string(),
                discovered_at: ts,
                files,
            });
        }
    }

    Ok(Json(results))
}

async fn api_torrent(
    State(state): State<AppState>,
    Path(infohash_hex): Path<String>,
) -> Result<Json<SearchResult>, StatusCode> {
    let bytes = hex::decode(&infohash_hex).map_err(|_| StatusCode::BAD_REQUEST)?;
    if bytes.len() != 20 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut ih = [0u8; 20];
    ih.copy_from_slice(&bytes);

    let rtx = state.db.begin_read().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let entry = storage::get_entry(&rtx, &ih)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let cat = rtx
        .open_table(storage::CATEGORIES)
        .ok()
        .and_then(|t| t.get(&ih).ok().flatten())
        .map(|v| Category::from_u8(v.value()))
        .unwrap_or(Category::Other);

    let ts = entry.unix_timestamp();
    let files = entry.files.into_iter().map(|f| FileResult { path: f.path, size: f.size }).collect();
    Ok(Json(SearchResult {
        infohash: infohash_hex,
        name: entry.name,
        size: entry.size,
        category: cat.as_str().to_string(),
        discovered_at: ts,
        files,
    }))
}
