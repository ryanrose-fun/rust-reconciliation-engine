#[tokio::main]
async fn main() -> reconcile_cli::streaming::Result<()> {
    reconcile_cli::streaming::run(std::env::args().collect(), None).await?;
    Ok(())
}
