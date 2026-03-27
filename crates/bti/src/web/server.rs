use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
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
}

pub async fn run_server(
    listen: SocketAddr,
    db: Arc<Database>,
    db_path: PathBuf,
    sync_status: Option<Arc<AtomicU8>>,
    sync_info: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = AppState { db, db_path, sync_status, sync_info };

    let app = Router::new()
        .route("/", get(index))
        .route("/api/stats", get(api_stats))
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

#[derive(serde::Serialize)]
struct SyncInfo {
    label: String,
    state: String,
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
            _ => "connecting",
        };
        SyncInfo { label: label.clone(), state: state_str.to_string() }
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
struct SearchResult {
    infohash: String,
    name: String,
    size: u64,
    category: String,
    discovered_at: u64,
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
            results.push(SearchResult {
                infohash: hex::encode(ih),
                name: entry.name,
                size: entry.size,
                category: cat.as_str().to_string(),
                discovered_at: ts,
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
    Ok(Json(SearchResult {
        infohash: infohash_hex,
        name: entry.name,
        size: entry.size,
        category: cat.as_str().to_string(),
        discovered_at: ts,
    }))
}
