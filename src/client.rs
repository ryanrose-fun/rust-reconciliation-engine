//! Opt-in HTTP client. Persist your PaymentEvent before sending; retries always
//! use its original identity and serialized payload. This is not an outbox daemon.
use crate::events::PaymentEvent;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

#[derive(Clone)]
pub struct RetryPolicy {
    pub max_attempts: usize,
    pub attempt_timeout: Duration,
    pub total_timeout: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            attempt_timeout: Duration::from_secs(5),
            total_timeout: Duration::from_secs(20),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(2),
        }
    }
}
#[derive(Debug)]
pub enum Failure {
    Deadline,
    Transport,
    Rejected(u16),
    Protocol,
    ResponseTooLarge,
    InvalidEvent,
}
#[derive(Debug)]
pub struct DeliveryError {
    pub event_id: String,
    pub attempts: usize,
    pub outcome_unknown: bool,
    pub reason: Failure,
}
impl std::fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Event {}: {:?} after {} attempt(s); outcome unknown: {}",
            self.event_id, self.reason, self.attempts, self.outcome_unknown
        )
    }
}
impl std::error::Error for DeliveryError {}
#[derive(Debug)]
pub struct Delivery {
    pub receipt: serde_json::Value,
    pub attempts: usize,
}
#[derive(Clone)]
pub struct EventClient {
    http: reqwest::Client,
    endpoint: reqwest::Url,
    token: String,
    policy: RetryPolicy,
    slots: Arc<tokio::sync::Semaphore>,
}
impl EventClient {
    pub(crate) fn binding(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        hash.update(self.endpoint.as_str());
        hash.update([0]);
        hash.update(&self.token);
        format!("{:x}", hash.finalize())
    }

    /// HTTPS is required except for localhost/loopback development. Redirects
    /// are disabled so credentials cannot be forwarded to another destination.
    pub fn new(base: &str, token: String, policy: RetryPolicy) -> Result<Self, &'static str> {
        let mut url = reqwest::Url::parse(base).map_err(|_| "Invalid API URL")?;
        let local = url.host_str().is_some_and(|h| {
            h == "localhost"
                || h.trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(url.scheme() == "https" || url.scheme() == "http" && local)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err("Use HTTPS (or loopback HTTP), without URL credentials/query/fragment");
        }
        if token.len() < 32 || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err("A bearer key of at least 32 printable ASCII characters is required");
        }
        if !(1..=8).contains(&policy.max_attempts)
            || policy.attempt_timeout.is_zero()
            || policy.total_timeout.is_zero()
            || policy.total_timeout > Duration::from_secs(120)
            || policy.attempt_timeout > policy.total_timeout
            || policy.initial_backoff.is_zero()
            || policy.initial_backoff > policy.max_backoff
            || policy.max_backoff > policy.total_timeout
        {
            return Err("Invalid bounded retry policy");
        }
        url.set_path(&format!("{}/v1/events", url.path().trim_end_matches('/')));
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(policy.attempt_timeout)
            .build()
            .map_err(|_| "Could not initialize HTTP client")?;
        Ok(Self {
            http,
            endpoint: url,
            token,
            policy,
            slots: Arc::new(tokio::sync::Semaphore::new(8)),
        })
    }
    pub async fn deliver(&self, event: &PaymentEvent) -> Result<Delivery, DeliveryError> {
        let mut attempts = 0;
        let mut unknown = false;
        let deadline = Instant::now() + self.policy.total_timeout;
        let fail = |attempts, unknown, reason| DeliveryError {
            event_id: event.event_id.clone(),
            attempts,
            outcome_unknown: unknown,
            reason,
        };
        let bytes = serde_json::to_vec(event).map_err(|_| fail(0, false, Failure::InvalidEvent))?;
        if bytes.len() > 16 * 1024 || event.event_id.trim().is_empty() {
            return Err(fail(0, false, Failure::InvalidEvent));
        }
        let _slot = tokio::time::timeout_at(deadline, self.slots.acquire())
            .await
            .map_err(|_| fail(0, false, Failure::Deadline))?
            .map_err(|_| fail(0, false, Failure::Deadline))?;
        while attempts < self.policy.max_attempts {
            attempts += 1;
            let attempt = async {
                let mut response = self
                    .http
                    .post(self.endpoint.clone())
                    .bearer_auth(&self.token)
                    .header("content-type", "application/json")
                    .body(bytes.clone())
                    .send()
                    .await
                    .map_err(|_| Failure::Transport)?;
                let status = response.status().as_u16();
                let retry = response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(retry_after);
                let mut body = Vec::new();
                while let Some(chunk) = response.chunk().await.map_err(|_| Failure::Transport)? {
                    if body.len() + chunk.len() > 1024 * 1024 {
                        return Err(Failure::ResponseTooLarge);
                    }
                    body.extend_from_slice(&chunk);
                }
                Ok((status, retry, body))
            };
            let expires = (Instant::now() + self.policy.attempt_timeout).min(deadline);
            let result = tokio::time::timeout_at(expires, attempt).await;
            let (reason, retry) = match result {
                Ok(Ok((200, _, body))) => {
                    let receipt: serde_json::Value = serde_json::from_slice(&body)
                        .map_err(|_| fail(attempts, true, Failure::Protocol))?;
                    if receipt["event_id"].as_str() != Some(&event.event_id)
                        || receipt["duplicate_event"].as_bool().is_none()
                    {
                        return Err(fail(attempts, true, Failure::Protocol));
                    }
                    return Ok(Delivery { receipt, attempts });
                }
                Ok(Ok((status, retry, _))) if matches!(status, 429 | 500 | 502 | 503 | 504) => {
                    unknown = true;
                    (Failure::Rejected(status), retry)
                }
                Ok(Ok((status, _, _))) => {
                    return Err(fail(attempts, unknown, Failure::Rejected(status)));
                }
                Ok(Err(Failure::ResponseTooLarge)) => {
                    return Err(fail(attempts, true, Failure::ResponseTooLarge));
                }
                _ => {
                    unknown = true;
                    (Failure::Transport, None)
                }
            };
            if attempts == self.policy.max_attempts {
                return Err(fail(attempts, unknown, reason));
            }
            let cap = (self.policy.initial_backoff * 2u32.pow((attempts - 1) as u32))
                .min(self.policy.max_backoff);
            let jitter = Duration::from_millis(
                (uuid::Uuid::new_v4().as_u128() % (cap.as_millis().max(1))) as u64,
            );
            let delay = retry.unwrap_or(jitter);
            if Instant::now() + delay >= deadline {
                return Err(fail(attempts, unknown, Failure::Deadline));
            }
            tokio::time::sleep(delay).await;
        }
        unreachable!()
    }
}
fn retry_after(value: &str) -> Option<Duration> {
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds.min(86400)));
    }
    let date = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    Some(Duration::from_millis(
        (date.timestamp_millis() - chrono::Utc::now().timestamp_millis()).clamp(0, 86400000) as u64,
    ))
}
