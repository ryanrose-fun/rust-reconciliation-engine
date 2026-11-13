use std::{net::SocketAddr, time::Duration};

#[derive(Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub scoped_auth: bool,
    pub deadline_worker: bool,
    pub api_key: Option<String>,
    pub tenant_file: Option<std::path::PathBuf>,
    pub max_body: usize,
    pub max_payments: usize,
    pub concurrency: usize,
    pub timeout: Duration,
    pub requests_per_second: usize,
    pub event_db: std::path::PathBuf,
    pub event_wait: Duration,
    pub event_queue_capacity: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:3000".parse().unwrap(),
            scoped_auth: false,
            deadline_worker: false,
            api_key: None,
            tenant_file: None,
            max_body: 1_048_576,
            max_payments: 5000,
            concurrency: 8,
            timeout: Duration::from_secs(10),
            requests_per_second: 100,
            event_db: "state/reconciliation.sqlite3".into(),
            event_wait: Duration::from_secs(30),
            event_queue_capacity: 32,
        }
    }
}
impl Config {
    pub fn from_env() -> Result<Self, String> {
        let mut c = Self {
            deadline_worker: true,
            ..Self::default()
        };
        if let Ok(v) = std::env::var("RECONCILE_BIND") {
            c.bind = v.parse().map_err(|_| "Invalid RECONCILE_BIND")?;
        }
        c.api_key = std::env::var("RECONCILE_API_KEY").ok();
        c.tenant_file = std::env::var_os("RECONCILE_TENANTS_FILE").map(Into::into);
        if c.tenant_file
            .as_ref()
            .is_some_and(|p| p.as_os_str().is_empty())
        {
            return Err("Tenant config path must not be empty".into());
        }
        c.max_body = number("RECONCILE_MAX_BODY_BYTES", c.max_body, 16_777_216)?;
        c.max_payments = number("RECONCILE_MAX_PAYMENTS", c.max_payments, 100_000)?;
        c.concurrency = number("RECONCILE_CONCURRENCY", c.concurrency, 64)?;
        c.timeout = Duration::from_millis(number("RECONCILE_TIMEOUT_MS", 10_000, 120_000)? as u64);
        c.requests_per_second = number(
            "RECONCILE_REQUESTS_PER_SECOND",
            c.requests_per_second,
            10_000,
        )?;
        if let Some(path) = std::env::var_os("RECONCILE_DB_PATH") {
            if path.is_empty() {
                return Err("RECONCILE_DB_PATH must not be empty".into());
            }
            c.event_db = path.into();
        }
        c.event_wait =
            Duration::from_millis(number("RECONCILE_EVENT_WAIT_MS", 30_000, 86_400_000)? as u64);
        c.event_queue_capacity = number("RECONCILE_EVENT_QUEUE_CAPACITY", 32, 1024)?;
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=1024).contains(&self.event_queue_capacity) {
            return Err("Event queue capacity must be between 1 and 1024".into());
        }
        if self
            .api_key
            .as_ref()
            .is_some_and(|k| k.len() < 32 || !k.bytes().all(|b| b.is_ascii_graphic()))
        {
            return Err(
                "RECONCILE_API_KEY must contain at least 32 printable non-space ASCII characters"
                    .into(),
            );
        }
        if !self.bind.ip().is_loopback() && self.api_key.is_none() && self.tenant_file.is_none() {
            return Err("Non-loopback binding requires RECONCILE_API_KEY; terminate HTTPS at a trusted reverse proxy".into());
        }
        Ok(())
    }
}
fn number(name: &str, default: usize, max: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(format!("Invalid {name}")),
        Ok(v) => v
            .parse::<usize>()
            .ok()
            .filter(|v| *v > 0 && *v <= max)
            .ok_or_else(|| format!("{name} must be between 1 and {max}")),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_binding_requires_key() {
        let mut c = Config {
            bind: "0.0.0.0:3000".parse().unwrap(),
            ..Config::default()
        };
        assert!(c.validate().is_err());
        c.api_key = Some("x".repeat(32));
        assert!(c.validate().is_ok());
        c.api_key = Some("short".into());
        assert!(c.validate().is_err());
    }
}
