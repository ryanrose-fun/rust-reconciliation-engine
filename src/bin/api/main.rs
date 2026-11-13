use axum::serve::ListenerExt;
mod config;
mod request_log;
mod routes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() == Some("--openapi") {
        println!("{}", serde_json::to_string_pretty(&routes::api_doc())?);
        return Ok(());
    }
    let config = config::Config::from_env()?;
    let database_existed = config.event_db.exists();
    let store = std::sync::Arc::new(reconcile_cli::events::EventStore::open(
        &config.event_db,
        config.event_wait,
    )?);
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    println!("API listening on http://{}", config.bind);
    println!(
        "Event database: {} ({})",
        std::fs::canonicalize(&config.event_db)?.display(),
        if database_existed { "existing" } else { "new" }
    );
    if config.api_key.is_none() {
        println!("Loopback development mode: authentication disabled");
    }
    println!(
        "GET /health | GET /openapi.json | POST /v1/reconcile | POST /v1/events | GET /v1/payments/{{payment_id}} | Ctrl+C to stop"
    );
    let queue = std::sync::Arc::new(routes::EventQueue::new(config.event_queue_capacity));
    axum::serve(
        listener.tap_io(|stream| {
            if let Err(error) = stream.set_nodelay(true) {
                eprintln!("Could not enable TCP_NODELAY: {error}");
            }
        }),
        routes::router_with_queue(config, store, queue.clone()),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    queue.wait_idle().await;
    Ok(())
}
