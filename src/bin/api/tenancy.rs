//! Credentials select a complete router, including its database and capacity limits.
use super::permissions::{Principal, Role};
use super::{config::Config, routes};
use axum::{
    Router,
    extract::{Request, State},
    response::Response,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};
use subtle::ConstantTimeEq;
use tower::ServiceExt;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tenant {
    pub id: String,
    #[serde(default)]
    pub api_key: String,
    #[serde(default)]
    pub credentials: Vec<Credential>,
    pub database: PathBuf,
    /// Opt-in fixed partition count; zero preserves existing single-file storage.
    #[serde(default)]
    pub partitions: usize,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub id: String,
    pub api_key: String,
    pub role: Role,
}
impl Tenant {
    fn keys(&self) -> Result<Vec<Credential>, &'static str> {
        if !self.api_key.is_empty() {
            if !self.credentials.is_empty() {
                return Err("Use api_key or credentials, never both");
            }
            return Ok(vec![Credential {
                id: "legacy-administrator".into(),
                api_key: self.api_key.clone(),
                role: Role::Administrator,
            }]);
        }
        if self.credentials.is_empty() || self.credentials.len() > 16 {
            return Err("Each tenant needs 1..16 credentials");
        }
        Ok(self.credentials.clone())
    }
}
type BuiltTenants = (Router, Vec<Arc<routes::EventQueue>>);
struct TenantRoutes {
    routers: Vec<([u8; 32], Principal, Router)>,
    config: Config,
    drains: Arc<tokio::sync::Semaphore>,
}
type Tenants = Arc<TenantRoutes>;

pub fn build(
    config: &Config,
    entries: Vec<Tenant>,
) -> Result<BuiltTenants, Box<dyn std::error::Error>> {
    if entries.is_empty() || entries.len() > 64 {
        return Err("Configure between 1 and 64 tenants".into());
    }
    let mut ids = std::collections::HashSet::new();
    let mut keys = std::collections::HashSet::new();
    let mut routers = Vec::new();
    let mut queues = Vec::new();
    // Validate every configuration before opening any database. Each tenant must
    // have a distinct canonical parent directory, preventing file aliases.
    let mut directories = std::collections::HashSet::new();
    for tenant in &entries {
        if tenant.partitions > 64 {
            return Err("At most 64 partitions per tenant".into());
        }
        if tenant.partitions > 0 && tenant.database.exists() {
            return Err("Partition mode requires a fresh directory; migrate existing single-file data explicitly".into());
        }
        if tenant.id.is_empty()
            || tenant.id.len() > 64
            || !tenant
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || !ids.insert(&tenant.id)
        {
            return Err("Tenant IDs and keys must be valid and unique".into());
        }
        let mut credential_ids = std::collections::HashSet::new();
        for credential in tenant.keys()? {
            if credential.id.is_empty()
                || credential.id.len() > 64
                || !credential
                    .id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                || !credential_ids.insert(credential.id.clone())
                || !keys.insert(credential.api_key.clone())
            {
                return Err("Credential IDs must be valid and keys globally unique".into());
            }
            let mut c = config.clone();
            c.api_key = Some(credential.api_key);
            c.validate()?;
        }
        if !tenant.database.is_absolute() || tenant.database.file_name().is_none() {
            return Err("Tenant database paths must be absolute".into());
        }
        let parent = tenant
            .database
            .parent()
            .ok_or("Database directory required")?;
        // Pre-create dedicated directories explicitly; no automatic provisioning.
        let directory = std::fs::canonicalize(parent)?;
        if !directories.insert(directory.to_string_lossy().to_lowercase()) {
            return Err("Each tenant requires a distinct existing database directory".into());
        }
        if tenant.database.file_name() != Some(std::ffi::OsStr::new("events.sqlite3")) {
            return Err("Tenant database filename must be events.sqlite3".into());
        }
        if tenant
            .database
            .symlink_metadata()
            .is_ok_and(|m| m.file_type().is_symlink())
        {
            return Err("Tenant database cannot be a symlink".into());
        }
    }
    for tenant in entries {
        let mut c = config.clone();
        c.api_key = None;
        c.scoped_auth = true;
        c.event_db = tenant.database.clone();
        let router = if tenant.partitions > 0 {
            let partitioned = reconcile_cli::events::PartitionedStore::open(
                &tenant
                    .database
                    .parent()
                    .ok_or("Database directory required")?
                    .join("partitions"),
                &tenant.id,
                tenant.partitions,
                c.event_wait,
            )?;
            let mut partitions = Vec::new();
            for store in partitioned.into_stores() {
                let queue = Arc::new(routes::EventQueue::new(c.event_queue_capacity));
                partitions.push(routes::router_with_queue(
                    c.clone(),
                    Arc::new(store),
                    queue.clone(),
                ));
                queues.push(queue);
            }
            crate::partition_api::router(tenant.id.clone(), partitions, c.clone())
        } else {
            if tenant
                .database
                .parent()
                .ok_or("Database directory required")?
                .join("partitions/layout.sqlite3")
                .exists()
            {
                return Err("Existing partition layout requires partitions configuration".into());
            }
            let store = Arc::new(reconcile_cli::events::EventStore::open(
                &c.event_db,
                c.event_wait,
            )?);
            let queue = Arc::new(routes::EventQueue::new(c.event_queue_capacity));
            let router = routes::router_with_queue(c, store, queue.clone());
            queues.push(queue);
            router
        };
        for credential in tenant.keys()? {
            routers.push((
                Sha256::digest(credential.api_key.as_bytes()).into(),
                Principal {
                    id: credential.id,
                    role: credential.role,
                },
                router.clone(),
            ));
        }
    }
    let router = Router::new()
        .route(
            "/health",
            axum::routing::get(|| async { axum::Json(serde_json::json!({"status":"ok"})) }),
        )
        .route(
            "/openapi.json",
            axum::routing::get(|| async { axum::Json(routes::api_doc()) }),
        )
        .fallback(dispatch)
        .with_state(Arc::new(TenantRoutes {
            routers,
            config: config.clone(),
            drains: Arc::new(tokio::sync::Semaphore::new(config.concurrency * 2)),
        }));
    Ok((router, queues))
}
async fn dispatch(State(tenants): State<Tenants>, mut request: Request) -> Response {
    let token = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let mut selected = None;
    for (key, principal, router) in tenants.routers.iter() {
        if bool::from(key.ct_eq(&digest)) {
            selected = Some((principal.clone(), router.clone()));
        }
    }
    if let Some((principal, router)) = selected {
        request.extensions_mut().insert(principal);
        return router.oneshot(request).await.unwrap();
    }
    routes::unauthorized(&tenants.config, &tenants.drains, request).await
}
