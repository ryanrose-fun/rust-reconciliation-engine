use reconcile_cli::{
    client::{EventClient, RetryPolicy},
    outbox::Outbox,
};
use std::{path::Path, sync::Arc, time::Duration};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        return Err(
            "Usage: outbox <enqueue|status|list|redrive|rebind|run> <database> [event-file|event-id]".into(),
        );
    }
    let store = Arc::new(Outbox::open(Path::new(&args[1]))?);
    match args[0].as_str() {
        "list" if args.len() <= 3 => println!(
            "{}",
            serde_json::to_string_pretty(
                &store.list(args.get(2).map(String::as_str).unwrap_or(""))?
            )?
        ),
        "rebind" if args.len() == 2 => {
            let client = EventClient::new(
                &std::env::var("RECONCILE_URL")?,
                std::env::var("RECONCILE_API_KEY")?,
                RetryPolicy::default(),
            )?;
            store.rebind(&client)?;
        }

        "enqueue" if args.len() == 3 => {
            let e = serde_json::from_slice(&std::fs::read(&args[2])?)?;
            println!("inserted={}", store.enqueue(&e)?);
        }
        "status" if args.len() == 2 => {
            println!("{}", serde_json::to_string_pretty(&store.summary()?)?)
        }
        "redrive" if args.len() == 3 => store.redrive(&args[2])?,
        "run" if args.len() == 2 => {
            let client = EventClient::new(
                &std::env::var("RECONCILE_URL")?,
                std::env::var("RECONCILE_API_KEY")?,
                RetryPolicy::default(),
            )?;
            loop {
                tokio::select! {_ = tokio::signal::ctrl_c()=>break,result=store.deliver_one(&client)=>{if !result? {tokio::time::sleep(Duration::from_millis(500)).await;}}}
            }
        }
        _ => return Err("Invalid outbox arguments".into()),
    }
    Ok(())
}
