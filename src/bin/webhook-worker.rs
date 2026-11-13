use reconcile_cli::{events::EventStore, webhooks::WebhookWorker};
use std::{sync::Arc, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        return Err(
            "Usage: webhook-worker <run|status|list|redrive> <database> [delivery-id]".into(),
        );
    }
    let store = Arc::new(EventStore::open(
        std::path::Path::new(&args[1]),
        Duration::from_secs(30),
    )?);
    match args[0].as_str() {
        "list" if args.len() <= 3 => println!(
            "{}",
            serde_json::to_string_pretty(
                &store.notification_deliveries(args.get(2).map(String::as_str).unwrap_or(""))?
            )?
        ),

        "status" if args.len() == 2 => println!(
            "{}",
            serde_json::to_string_pretty(&store.notification_summary()?)?
        ),
        "redrive" if args.len() == 3 => store.redrive_notification(&args[2])?,
        "run" if args.len() == 2 => {
            let worker = WebhookWorker::new(
                store,
                &std::env::var("RECONCILE_WEBHOOK_URL")?,
                std::env::var("RECONCILE_WEBHOOK_SECRET")?.into_bytes(),
            )?;
            loop {
                tokio::select! {_=tokio::signal::ctrl_c()=>break,result=worker.deliver_one()=>{match result {Ok(true)=>{},Ok(false)=>tokio::time::sleep(Duration::from_millis(500)).await,Err(error)=>{eprintln!("Webhook storage operation failed: {error}");tokio::time::sleep(Duration::from_secs(1)).await;}}}}
            }
        }
        _ => return Err("Invalid webhook-worker arguments".into()),
    }
    Ok(())
}
