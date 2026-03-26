mod crawl;
mod migrate;
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

    /// Start the web dashboard with classification
    Web {
        /// Run the DHT crawler in-process (same-machine mode)
        #[arg(long)]
        crawl: bool,

        /// Remote crawl agent sQUIC address (mutually exclusive with --crawl)
        #[arg(long, requires = "crawler_key")]
        crawler_addr: Option<SocketAddr>,

        /// Remote crawl agent Ed25519 public key (hex)
        #[arg(long, requires = "crawler_addr")]
        crawler_key: Option<String>,

        /// Database path
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
        Commands::Web {
            crawl,
            crawler_addr,
            crawler_key,
            db_path,
            listen,
        } => {
            if crawl && crawler_addr.is_some() {
                return Err("--crawl and --crawler-addr are mutually exclusive".into());
            }

            let db_path = expand_path(&db_path);
            let sync_target = match (crawler_addr, crawler_key) {
                (Some(addr), Some(key)) => Some((addr, parse_key(&key)?)),
                _ => None,
            };

            web::run(web::WebConfig {
                crawl,
                sync_target,
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
