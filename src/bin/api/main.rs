use axum::serve::ListenerExt;
mod config;
mod metrics;
mod partition_api;
mod permissions;
mod request_log;
mod routes;
mod tenancy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() == Some("--openapi") {
        println!("{}", serde_json::to_string_pretty(&routes::api_doc())?);
        return Ok(());
    }
    let config = config::Config::from_env()?;
    let (router, queues) = if let Some(path) = &config.tenant_file {
        if config.api_key.is_some() {
            return Err("Use tenant credentials or RECONCILE_API_KEY, not both".into());
        }
        let entries = serde_json::from_slice(&std::fs::read(path)?)?;
        tenancy::build(&config, entries)?
    } else {
        let store = std::sync::Arc::new(reconcile_cli::events::EventStore::open(
            &config.event_db,
            config.event_wait,
        )?);
        let queue = std::sync::Arc::new(routes::EventQueue::new(config.event_queue_capacity));
        (
            routes::router_with_queue(config.clone(), store, queue.clone()),
            vec![queue],
        )
    };
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    println!("API listening on http://{}", listener.local_addr()?);
    // Launchers discover the OS-assigned port from redirected stdout.
    std::io::Write::flush(&mut std::io::stdout())?;
    println!("Ctrl+C to stop; active storage operations drain before exit");
    axum::serve(
        listener.tap_io(|stream| {
            if let Err(error) = stream.set_nodelay(true) {
                eprintln!("Could not enable TCP_NODELAY: {error}");
            }
        }),
        router,
    )
    .with_graceful_shutdown(async {
        shutdown_signal().await;
    })
    .await?;
    for queue in queues {
        queue.wait_idle().await;
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tenancy_tests;
