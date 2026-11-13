//! Durable at-least-once delivery. Receivers must deduplicate X-Reconcile-Delivery.
use crate::{
    delivery,
    events::{EventError, EventStore},
};
use hmac::{Hmac, Mac};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use sha2::Sha256;
use std::{sync::Arc, time::Duration};
type HmacSha256 = Hmac<Sha256>;
fn mac(secret: &[u8], timestamp: &str, id: &str, body: &[u8]) -> HmacSha256 {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts arbitrary key lengths");
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(id.as_bytes());
    mac.update(b".");
    mac.update(body);
    mac
}
pub fn verify_signature(
    secret: &[u8],
    timestamp: &str,
    id: &str,
    body: &[u8],
    signature: &str,
    now_ms: i64,
) -> bool {
    let Ok(time) = timestamp.parse::<i64>() else {
        return false;
    };
    if time.abs_diff(now_ms) > 300_000 || signature.len() != 64 {
        return false;
    }
    let mut bytes = [0u8; 32];
    for (i, byte) in bytes.iter_mut().enumerate() {
        let Some(part) = signature.get(i * 2..i * 2 + 2) else {
            return false;
        };
        let Ok(v) = u8::from_str_radix(part, 16) else {
            return false;
        };
        *byte = v;
    }
    mac(secret, timestamp, id, body)
        .verify_slice(&bytes)
        .is_ok()
}
pub struct WebhookWorker {
    store: Arc<EventStore>,
    endpoint: String,
    secret: Vec<u8>,
    http: reqwest::Client,
}
impl WebhookWorker {
    /// Enable future transitions. Destination changes are refused while deliveries remain.
    pub fn new(
        store: Arc<EventStore>,
        endpoint: &str,
        secret: Vec<u8>,
    ) -> Result<Self, EventError> {
        let url = reqwest::Url::parse(endpoint)
            .map_err(|_| EventError::InvalidInput("Invalid webhook URL"))?;
        let local = url.host_str().is_some_and(|s| {
            s == "localhost"
                || s.trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(url.scheme() == "https" || url.scheme() == "http" && local)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.query().is_some()
            || secret.len() < 32
        {
            return Err(EventError::InvalidInput(
                "Webhook requires HTTPS (or loopback HTTP), no URL credentials/query, and a 32-byte secret",
            ));
        }
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|_| EventError::InvalidInput("HTTP client initialization failed"))?;
        store.configure_webhook(endpoint)?;
        Ok(Self {
            store,
            endpoint: endpoint.into(),
            secret,
            http,
        })
    }
    pub async fn deliver_one(&self) -> Result<bool, EventError> {
        let store = self.store.clone();
        let endpoint = self.endpoint.clone();
        let job = tokio::task::spawn_blocking(move || {
            let mut c = store.lock()?;
            let current: String =
                c.query_row("SELECT endpoint FROM notification_settings", [], |r| {
                    r.get(0)
                })?;
            if current != endpoint {
                return Err(EventError::InvalidInput(
                    "Worker endpoint changed; restart the worker",
                ));
            }
            delivery::claim(
                &mut c,
                chrono::Utc::now().timestamp_millis(),
                ("webhook", &endpoint),
            )
        })
        .await
        .map_err(|_| EventError::CorruptData)??;
        let Some(job) = job else { return Ok(false) };
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let signature = mac(&self.secret, &timestamp, &job.id, job.payload.as_bytes())
            .finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let response = self
            .http
            .post(&self.endpoint)
            .header("content-type", "application/json")
            .header("x-reconcile-delivery", &job.id)
            .header("x-reconcile-timestamp", &timestamp)
            .header("x-reconcile-signature", signature)
            .body(job.payload.clone())
            .send()
            .await;
        let (success, permanent, error, retry) = match response {
            Ok(response) => {
                let code = response.status().as_u16();
                let retry = response
                    .headers()
                    .get("retry-after")
                    .and_then(|s| s.to_str().ok())
                    .and_then(|s| {
                        s.parse::<u64>()
                            .ok()
                            .map(|v| v.saturating_mul(1000).min(i64::MAX as u64) as i64)
                            .or_else(|| {
                                chrono::DateTime::parse_from_rfc2822(s).ok().map(|t| {
                                    t.timestamp_millis()
                                        .saturating_sub(chrono::Utc::now().timestamp_millis())
                                        .max(0)
                                })
                            })
                    });
                (
                    response.status().is_success(),
                    (!matches!(code, 429 | 500 | 502 | 503 | 504)
                        && !response.status().is_success())
                        || retry.is_some_and(|v| v > 86_400_000),
                    format!("HTTP {code}"),
                    retry,
                )
            }
            Err(_) => (
                false,
                false,
                "Transport failure; outcome unknown".into(),
                None,
            ),
        };
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            delivery::finish(
                &*store.lock()?,
                &job,
                chrono::Utc::now().timestamp_millis(),
                success,
                permanent,
                &error,
                retry,
            )
        })
        .await
        .map_err(|_| EventError::CorruptData)??;
        Ok(true)
    }
}
impl EventStore {
    pub fn notification_deliveries(
        &self,
        after: &str,
    ) -> Result<Vec<crate::DeliveryRecord>, EventError> {
        delivery::list(&*self.lock()?, after)
    }
    fn configure_webhook(&self, endpoint: &str) -> Result<(), EventError> {
        let mut c = self.lock()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous: Option<String> = tx
            .query_row("SELECT endpoint FROM notification_settings", [], |r| {
                r.get(0)
            })
            .optional()?;
        if previous.as_deref().is_some_and(|old| old != endpoint) {
            let pending: i64 = tx.query_row(
                "SELECT count(*) FROM delivery_jobs WHERE state!='delivered'",
                [],
                |r| r.get(0),
            )?;
            if pending != 0 {
                return Err(EventError::InvalidInput(
                    "Drain or resolve deliveries before changing destination",
                ));
            }
        }
        tx.execute("INSERT INTO notification_settings(singleton,endpoint) VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET endpoint=excluded.endpoint",params![endpoint])?;
        tx.commit()?;
        Ok(())
    }
    pub fn notification_summary(&self) -> Result<crate::DeliverySummary, EventError> {
        delivery::summary(&*self.lock()?)
    }
    pub fn redrive_notification(&self, id: &str) -> Result<(), EventError> {
        delivery::redrive(&*self.lock()?, id)
    }
}
