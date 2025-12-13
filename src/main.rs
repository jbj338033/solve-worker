mod job;
mod language;
mod queue;
mod sandbox;
mod service;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into());
    let max_boxes: usize = std::env::var("MAX_BOXES")
        .unwrap_or_else(|_| "16".into())
        .parse()
        .unwrap_or(16);

    info!(redis_url = %redis_url, max_boxes = %max_boxes, "Starting solve-worker");

    let worker = queue::Worker::new(&redis_url, max_boxes).await?;
    worker.run().await?;

    Ok(())
}
