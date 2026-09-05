use std::net::SocketAddr;
use std::path::PathBuf;

/// What the crawler reads from the environment.
///
/// **`LOG_LEVEL` is not here, and it does work.** It used to be read into a
/// field nothing consulted, while `main` read the same variable directly for
/// the tracing filter — one setting with two owners, only one of which had any
/// effect. The field is gone; the variable is unchanged.
pub struct CrawlConfig {
    pub dht_port: u16,
    pub scaling_factor: usize,
    pub db_path: PathBuf,
    pub sync_addr: SocketAddr,
    pub sync_key_file: PathBuf,
    pub bootstrap_nodes: Vec<String>,
}

impl CrawlConfig {
    pub fn from_env() -> Self {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let bti_dir = home.join(".bti");

        Self {
            dht_port: env_or("DHT_PORT", 6881),
            scaling_factor: env_or("SCALING_FACTOR", 10),
            db_path: env_path_or("BTI_DB_PATH", bti_dir.join("db")),
            sync_addr: env_or("SYNC_ADDR", "0.0.0.0:6880".parse().unwrap()),
            sync_key_file: env_path_or("SYNC_KEY_FILE", bti_dir.join("sync.key")),
            bootstrap_nodes: vec![
                "router.bittorrent.com:6881".into(),
                "router.utorrent.com:6881".into(),
                "dht.transmissionbt.com:6881".into(),
                "dht.libtorrent.org:25401".into(),
            ],
        }
    }
}

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_path_or(key: &str, default: PathBuf) -> PathBuf {
    std::env::var(key)
        .ok()
        .map(PathBuf::from)
        .unwrap_or(default)
}
