mod adysec_engine;
mod bencode;
mod config;
mod metadata;
mod model;
mod search;
mod storage;

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::AppConfig;
use metadata::parser::parse_torrent_metainfo;
use model::{Source, TorrentRecord, now_ts};
use storage::LibsqlStore;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
#[command(
    name = "dht-lens",
    version,
    about = "High-performance DHT magnet metadata crawler"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Migrate,
    Crawl {
        #[arg(long, default_value_t = true)]
        print: bool,
    },
    Search {
        query: String,
        #[arg(long, default_value_t = 20)]
        limit: u32,
    },
    ParseTorrent {
        path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("dht_lens=info".parse()?))
        .init();

    let cli = Cli::parse();
    let mut config = AppConfig::from_env()?;

    match cli.command {
        Command::Migrate => {
            let store = connect_store(&config).await?;
            store.migrate().await?;
            println!("migration complete");
        }
        Command::Crawl { print } => {
            config.pipeline.print_jsonl = print;
            let store = if config.storage.enabled {
                let store = Arc::new(connect_store(&config).await?);
                store.migrate().await?;
                Some(store)
            } else {
                None
            };
            adysec_engine::run_crawl(config, store).await?;
        }
        Command::Search { query, limit } => {
            let store = connect_store(&config).await?;
            let rows = store
                .search(&query, limit, config.search.max_name_ngram_len)
                .await?;
            for row in rows {
                println!("{}", serde_json::to_string(&row)?);
            }
        }
        Command::ParseTorrent { path } => {
            let bytes = tokio::fs::read(path).await?;
            let (info_hash, metadata) = parse_torrent_metainfo(&bytes)?;
            let now = now_ts();
            let file_count = metadata.files.len();
            let record = TorrentRecord {
                info_hash: hex::encode(info_hash),
                info_hash_bytes: info_hash,
                name: metadata.name,
                total_size: metadata.total_size,
                file_count,
                files_stored_count: file_count,
                files: metadata.files,
                peer_count: 0,
                source: Source::ManualMagnet,
                hot_score: 1,
                first_seen_at: now,
                last_seen_at: now,
                metadata_fetched_at: now,
            };
            println!("{}", serde_json::to_string_pretty(&record)?);
        }
    }
    Ok(())
}

async fn connect_store(config: &AppConfig) -> Result<LibsqlStore> {
    let url = config
        .storage
        .database_url
        .as_deref()
        .context("LIBSQL_DATABASE_URL is required")?;
    LibsqlStore::connect(url, config.storage.auth_token.as_deref()).await
}
