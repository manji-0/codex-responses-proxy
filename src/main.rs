mod app;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    app::run().await
}
