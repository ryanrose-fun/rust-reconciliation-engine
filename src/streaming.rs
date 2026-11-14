//! Explicit-partition Kafka tools for the GPU API. No CPU reconciliation fallback.
mod store;
use chrono::Utc;
use rskafka::{
    client::{
        ClientBuilder,
        partition::{Compression, OffsetAt, UnknownTopicHandling},
    },
    record::Record,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    error::Error,
    time::{Duration, Instant},
};
pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    schema: u32,
    created_at_ms: i64,
    events: Vec<Value>,
}
fn request_id(topic: &str, offset: i64) -> String {
    format!("kafka-{:x}", Sha256::digest(format!("{topic}:0:{offset}")))
}
fn event(run: &str, i: u64, source: &str, final_state: bool) -> Value {
    let provider = source == "Provider";
    let version = if provider && final_state { 2 } else { 1 };
    json!({"event_id":format!("{run}-{source}-{i}-{version}"),"source":source,"version":version,
        "payment":{"id":format!("{run}-{i}"),"currency":"GBP","amount_minor":10000+i-u64::from(provider && final_state && i.is_multiple_of(10)),
        "status":if provider && (!final_state || i.is_multiple_of(20)) {"Pending"} else {"Completed"}}})
}
fn arg(args: &[String], i: usize) -> Result<&str> {
    args.get(i)
        .map(String::as_str)
        .ok_or_else(|| "Missing argument; run payment-stream help".into())
}
pub async fn run(
    args: Vec<String>,
    barrier: Option<std::sync::Arc<tokio::sync::Barrier>>,
) -> Result<Option<Value>> {
    let command = args.get(1).map(String::as_str).unwrap_or("help");
    if command == "help" {
        println!(
            "payment-stream init <broker> <run>\npayment-stream generate <broker> <run> <Internal|Provider> <seconds> <payments/s> <burst-start-s> <burst-duration-s> <burst-payments/s>\npayment-stream consume <broker> <run> <api-url> <state.sqlite3>\nUse a NEW run ID for each dataset. Set RECONCILE_API_KEY for consume. Kafka is local plaintext only."
        );
        return Ok(None);
    }
    let broker = arg(&args, 2)?;
    let run = arg(&args, 3)?;
    if run.is_empty()
        || run.len() > 64
        || !run.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-')
    {
        return Err("Run ID must contain 1..64 alphanumeric/hyphen characters".into());
    }
    let client = tokio::time::timeout(
        Duration::from_secs(30),
        ClientBuilder::new(vec![broker.to_owned()])
            .backoff_config(rskafka::BackoffConfig {
                deadline: Some(Duration::from_secs(30)),
                ..Default::default()
            })
            .build(),
    )
    .await??;
    if command == "init" {
        for source in ["internal", "provider"] {
            let topic = format!("{run}-{source}");
            if let Err(error) = client
                .controller_client()?
                .create_topic(topic.clone(), 1, 1, 10000)
                .await
            {
                // Topic creation can succeed before its response is lost. Accept only an empty log.
                let partition = client
                    .partition_client(topic, 0, UnknownTopicHandling::Error)
                    .await?;
                if partition.get_offset(OffsetAt::Latest).await? != 0 {
                    return Err(error.into());
                }
            }
        }
        println!("Created independent internal/provider topics for {run}");
        return Ok(None);
    }
    if command == "generate" {
        let source = arg(&args, 4)?;
        if !["Internal", "Provider"].contains(&source) {
            return Err("Source must be Internal or Provider".into());
        }
        let seconds: u64 = arg(&args, 5)?.parse()?;
        let rate: u64 = arg(&args, 6)?.parse()?;
        let burst_start: u64 = arg(&args, 7)?.parse()?;
        let burst_duration: u64 = arg(&args, 8)?.parse()?;
        let burst_rate: u64 = arg(&args, 9)?.parse()?;
        if seconds == 0
            || seconds > 3600
            || rate == 0
            || rate > 100000
            || burst_rate == 0
            || burst_rate > 100000
        {
            return Err("Seconds must be 1..3600 and rates 1..100000".into());
        }
        let topic = format!("{run}-{}", source.to_lowercase());
        let partition = client
            .partition_client(topic, 0, UnknownTopicHandling::Error)
            .await?;
        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
        let began = Instant::now();
        let mut count = 0u64;
        let mut records = 0;
        let mut events = 0;
        for tick in 0..seconds * 10 {
            let due = began + Duration::from_millis(tick * 100);
            tokio::time::sleep_until(due.into()).await;
            let active_rate = if tick / 10 >= burst_start
                && tick / 10 < burst_start.saturating_add(burst_duration)
            {
                burst_rate
            } else {
                rate
            };
            // Integer remainder preserves exact rates even below ten payments/s.
            let n = ((tick % 10 + 1) * active_rate / 10) - (tick % 10 * active_rate / 10);
            for start in (count..count + n).step_by(512) {
                let end = (start + 512).min(count + n);
                let mut batch = Vec::new();
                for final_state in if source == "Provider" {
                    vec![false, true]
                } else {
                    vec![true]
                } {
                    let env = Envelope {
                        schema: 1,
                        created_at_ms: Utc::now().timestamp_millis(),
                        events: (start..end)
                            .map(|i| event(run, i, source, final_state))
                            .collect(),
                    };
                    events += env.events.len();
                    batch.push(Record {
                        key: None,
                        value: Some(serde_json::to_vec(&env)?),
                        headers: BTreeMap::new(),
                        timestamp: Utc::now(),
                    });
                }
                // Duplicate an old provider version after the completion, exercising stale/retry handling.
                if source == "Provider" && tick % 10 == 0 && !batch.is_empty() {
                    batch.push(batch[0].clone());
                    events += (end - start) as usize;
                }
                records += batch.len();
                tokio::time::timeout(
                    Duration::from_secs(30),
                    partition.produce(batch, Compression::NoCompression),
                )
                .await??;
            }
            count += n;
            if tick % 10 == 9 {
                println!(
                    "{}",
                    json!({"kind":"producer","source":source,"payments":count,"events":events,"records":records,"elapsed_s":began.elapsed().as_secs_f64(),"schedule_lag_ms":began.elapsed().as_millis().saturating_sub((tick*100) as u128)})
                );
            }
        }
        let summary = json!({"kind":"producer_done","source":source,"payments":count,"events":events,"records":records,"elapsed_s":began.elapsed().as_secs_f64()});
        println!("{summary}");
        return Ok(Some(summary));
    }
    if command != "consume" {
        return Err("Unknown command".into());
    }
    let api = arg(&args, 4)?.trim_end_matches('/');
    let key = std::env::var("RECONCILE_API_KEY")?;
    if !local_api_url(api) {
        return Err("This local integration requires a loopback GPU API".into());
    }
    let mut store = store::Store::open(arg(&args, 5)?, &format!("{broker}|{run}|{api}"))?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let mut partitions = Vec::new();
    for source in ["internal", "provider"] {
        let topic = format!("{run}-{source}");
        partitions.push((
            topic.clone(),
            client
                .partition_client(topic, 0, UnknownTopicHandling::Error)
                .await?,
        ));
    }
    loop {
        let mut progressed = false;
        for (topic, partition) in &partitions {
            if let Some(pending) = store.pending()? {
                deliver(&http, api, &key, &mut store, pending).await?;
                progressed = true;
            }
            let offset = store.offset(topic)?;
            let (records, high) = tokio::time::timeout(
                Duration::from_secs(30),
                partition.fetch_records(offset, 1..1_048_576, 0),
            )
            .await??;
            if high < offset {
                return Err("Kafka log was truncated/recreated below durable cursor".into());
            }
            progressed |= !records.is_empty();
            for record in records.into_iter().take(4) {
                if record.offset != store.offset(topic)? {
                    return Err(
                        "Kafka retention gap or non-contiguous offset: refusing to skip evidence"
                            .into(),
                    );
                }
                let data = record
                    .record
                    .value
                    .ok_or("Tombstones are not valid payment records")?;
                if data.len() > 1_048_576 {
                    return Err("Record exceeds 1 MiB limit".into());
                }
                let env: Envelope = serde_json::from_slice(&data)?;
                if env.schema != 1 || env.events.is_empty() || env.events.len() > 512 {
                    return Err("Unsupported envelope schema or event count".into());
                }
                let expected_source = if topic.ends_with("-internal") {
                    "Internal"
                } else {
                    "Provider"
                };
                if env.events.iter().any(|e| e["source"] != expected_source) {
                    return Err("Event source does not match its topic".into());
                }
                let body = serde_json::to_string(
                    &json!({"request_id":request_id(topic,record.offset),"events":env.events}),
                )?;
                let pending = store::Pending {
                    topic: topic.clone(),
                    offset: record.offset,
                    body,
                    created_ms: env.created_at_ms,
                    events: env.events.len(),
                    high_watermark: high,
                };
                store.prepare(&pending)?;
                deliver(&http, api, &key, &mut store, pending).await?;
            }
        }
        // Never long-poll an empty source while its counterpart has backlog.
        if !progressed {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
fn local_api_url(value: &str) -> bool {
    reqwest::Url::parse(value).is_ok_and(|u| {
        u.scheme() == "http"
            && u.host_str() == Some("127.0.0.1")
            && u.username().is_empty()
            && u.password().is_none()
            && u.path() == "/"
            && u.query().is_none()
            && u.fragment().is_none()
    })
}
fn validate_receipt(expected: &Value, receipt: &Value) -> Result<()> {
    if receipt["request_id"] != expected["request_id"]
        || receipt["commit_sequence"].as_i64().is_none_or(|s| s <= 0)
        || !receipt["duplicate"].is_boolean()
    {
        return Err("Invalid durable GPU receipt; cursor unchanged".into());
    }
    let rows = receipt["gpu"]["receipts"]
        .as_array()
        .ok_or("Missing GPU receipts")?;
    let events = expected["events"].as_array().ok_or("Missing events")?;
    if rows.len() != events.len() {
        return Err("GPU receipt count mismatch".into());
    }
    let by_id: std::collections::HashMap<&str, &Value> = rows
        .iter()
        .filter_map(|r| r["id"].as_str().map(|id| (id, r)))
        .collect();
    if by_id.len() != events.len() {
        return Err("Duplicate/missing GPU receipt IDs".into());
    }
    for event in events {
        let row = by_id
            .get(
                event["payment"]["id"]
                    .as_str()
                    .ok_or("Missing payment ID")?,
            )
            .ok_or("Wrong GPU receipt ID")?;
        let source = event["source"]
            .as_str()
            .ok_or("Missing source")?
            .to_lowercase();
        if !matches!(row[&source].as_u64(), Some(1 | 2 | 4)) {
            return Err("GPU rejected/conflicted source update; cursor unchanged".into());
        }
    }
    Ok(())
}
async fn deliver(
    http: &reqwest::Client,
    api: &str,
    key: &str,
    store: &mut store::Store,
    p: store::Pending,
) -> Result<()> {
    let started = Instant::now();
    for attempt in 0..12u32 {
        let response = http
            .post(format!("{api}/v1/gpu/events"))
            .bearer_auth(key)
            .header("content-type", "application/json")
            .body(p.body.clone())
            .send()
            .await;
        let mut delay = Duration::from_millis((100u64 << attempt.min(5)).min(3200));
        match response {
            Ok(r) if r.status().is_success() => {
                let v: Value = serde_json::from_slice(&r.bytes().await?)?;
                let expected: Value = serde_json::from_str(&p.body)?;
                validate_receipt(&expected, &v)?;
                // Explicit fault injection for acceptance: durable API ACK but no local cursor commit.
                if let Ok(ms) = std::env::var("PAYMENT_STREAM_TEST_PAUSE_AFTER_ACK_MS")
                    && std::env::var("PAYMENT_STREAM_TEST_PAUSE_FILE")
                        .is_ok_and(|p| std::path::Path::new(&p).exists())
                {
                    let ms: u64 = ms.parse()?;
                    if ms > 10000 {
                        return Err("Fault-injection pause exceeds 10 seconds".into());
                    }
                    println!(
                        "{}",
                        json!({"kind":"ack_before_cursor","topic":p.topic,"offset":p.offset})
                    );
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
                let metrics = json!({"kind":"committed","topic":p.topic,"offset":p.offset,"events":p.events,"remaining_records_at_fetch":(p.high_watermark-p.offset-1).max(0),"created_at_ms":p.created_ms,"committed_at_ms":Utc::now().timestamp_millis(),"end_to_end_ms":(Utc::now().timestamp_millis()-p.created_ms).max(0),"http_ms":started.elapsed().as_secs_f64()*1000.0,"attempts":attempt+1,"duplicate":v["duplicate"]});
                store.complete(&p, &metrics)?;
                println!("{metrics}");
                return Ok(());
            }
            Ok(r) => {
                let status = r.status();
                if status.as_u16() != 429 && !status.is_server_error() {
                    return Err(format!(
                        "Permanent API rejection {status}; pending record retained: {}",
                        r.text().await?
                    )
                    .into());
                }
                if let Some(seconds) = r
                    .headers()
                    .get("retry-after")
                    .and_then(|h| h.to_str().ok())
                    .and_then(|h| h.parse::<u64>().ok())
                {
                    if seconds > 60 {
                        return Err("Retry-After exceeds local wait budget; pending retained for operator retry".into());
                    }
                    delay = delay.max(Duration::from_secs(seconds));
                }
            }
            Err(e) => eprintln!(
                "Retrying retained Kafka record {}:{}: {e}",
                p.topic, p.offset
            ),
        }
        tokio::time::sleep(
            delay + Duration::from_millis((p.offset as u64 * 17 + u64::from(attempt) * 31) % 97),
        )
        .await;
    }
    Err("Retry budget exhausted; pending record retained. Restart consumer to resume.".into())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stable_ids_bind_topic_and_offset() {
        assert_eq!(request_id("a", 1), request_id("a", 1));
        assert_ne!(request_id("a", 1), request_id("a", 2));
        assert_ne!(request_id("a", 1), request_id("b", 1));
    }
    #[test]
    fn paired_generator_has_known_evidence() {
        let a = event("run", 20, "Internal", true);
        let b = event("run", 20, "Provider", true);
        assert_eq!(a["payment"]["amount_minor"], 10020);
        assert_eq!(b["payment"]["amount_minor"], 10019);
        assert_eq!(b["payment"]["status"], "Pending");
        assert_eq!(b["version"], 2);
        assert_ne!(a["event_id"], b["event_id"]);
    }
    #[test]
    fn receipt_rejects_wrong_identity_or_conflict() {
        let request = json!({"request_id":"x","events":[event("run",1,"Internal",true)]});
        let mut receipt = json!({"request_id":"x","commit_sequence":1,"duplicate":false,"gpu":{"receipts":[{"id":"run-1","internal":1,"provider":0,"result":16}]}});
        assert!(validate_receipt(&request, &receipt).is_ok());
        receipt["gpu"]["receipts"][0]["internal"] = json!(3);
        assert!(validate_receipt(&request, &receipt).is_err());
        receipt["gpu"]["receipts"][0]["internal"] = json!(1);
        receipt["gpu"]["receipts"][0]["id"] = json!("wrong");
        assert!(validate_receipt(&request, &receipt).is_err());
        receipt["gpu"]["receipts"][0]["id"] = json!("run-1");
        receipt["request_id"] = json!("wrong");
        assert!(validate_receipt(&request, &receipt).is_err());
    }
    #[test]
    fn credentials_are_only_sent_to_loopback_without_url_userinfo() {
        assert!(local_api_url("http://127.0.0.1:3000"));
        for url in [
            "http://127.0.0.1:3000@evil.example",
            "http://127.0.0.1.evil.example:3000",
            "http://127.0.0.1:3000/path",
            "https://example.com",
        ] {
            assert!(!local_api_url(url));
        }
    }
}
