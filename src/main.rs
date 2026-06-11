mod ady_dht;
mod config;
mod crawler;
mod model;
mod search;
mod storage;

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use config::AppConfig;
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
        #[arg(long, default_value_t = false)]
        print: bool,
    },
    Search {
        query: String,
        #[arg(long, default_value_t = 20)]
        limit: u32,
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
                println!("migration complete");
                Some(store)
            } else {
                None
            };
            crawler::run(config, store).await?;
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
