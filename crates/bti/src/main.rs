mod crawl;
mod migrate;
mod sync;
mod web;

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bti", about = "BitTorrent indexing service suite")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the DHT crawler and sQUIC sync server
    Crawl {
        /// Import entries from a binary dump file (use "-" for stdin)
        #[arg(long)]
        migrate: Option<String>,
    },

    /// Sync entries from a crawl agent via sQUIC
    Sync {
        /// Crawl agent sQUIC address
        #[arg(long)]
        server_addr: SocketAddr,

        /// Crawl agent Ed25519 public key (hex)
        #[arg(long)]
        server_key: String,

        /// Local database path
        #[arg(long, default_value = "~/.bti/db")]
        db_path: String,

        /// Sync once and exit (default: continuous loop)
        #[arg(long)]
        once: bool,
    },

    /// Start the web dashboard with background sync and classification
    Web {
        /// Crawl agent sQUIC address
        #[arg(long)]
        crawler_addr: SocketAddr,

        /// Crawl agent Ed25519 public key (hex)
        #[arg(long)]
        crawler_key: String,

        /// Database path (shared with crawl/sync)
        #[arg(long, default_value = "~/.bti/db")]
        db_path: String,

        /// HTTP listen address
        #[arg(long, default_value = "0.0.0.0:8080")]
        listen: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("LOG_LEVEL")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Crawl { migrate } => {
            crawl::run(migrate).await?;
        }
        Commands::Sync {
            server_addr,
            server_key,
            db_path,
            once,
        } => {
            let key_bytes = parse_key(&server_key)?;
            let db_path = expand_path(&db_path);
            sync::run(sync::SyncConfig {
                server_addr,
                server_key: key_bytes,
                db_path,
                once,
            })
            .await?;
        }
        Commands::Web {
            crawler_addr,
            crawler_key,
            db_path,
            listen,
        } => {
            let key_bytes = parse_key(&crawler_key)?;
            let db_path = expand_path(&db_path);
            web::run(web::WebConfig {
                crawler_addr,
                crawler_key: key_bytes,
                db_path,
                listen,
            })
            .await?;
        }
    }

    Ok(())
}

fn parse_key(hex_str: &str) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let bytes = hex::decode(hex_str)?;
    if bytes.len() != 32 {
        return Err("key must be 32 bytes (64 hex chars)".into());
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

fn expand_path(path: &str) -> PathBuf {
    if path.starts_with("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(&path[2..]);
        }
    }
    PathBuf::from(path)
}
