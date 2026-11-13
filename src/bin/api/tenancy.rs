//! Credentials select a complete router, including its database and capacity limits.
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
    pub api_key: String,
    pub database: PathBuf,
}
type BuiltTenants = (Router, Vec<Arc<routes::EventQueue>>);
struct TenantRoutes {
    routers: Vec<([u8; 32], Router)>,
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
        if tenant.id.is_empty()
            || tenant.id.len() > 64
            || !tenant
                .id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || !ids.insert(&tenant.id)
            || !keys.insert(&tenant.api_key)
        {
            return Err("Tenant IDs and keys must be valid and unique".into());
        }
        let mut c = config.clone();
        c.api_key = Some(tenant.api_key.clone());
        c.validate()?;
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
        c.api_key = Some(tenant.api_key.clone());
        c.event_db = tenant.database;
        let store = Arc::new(reconcile_cli::events::EventStore::open(
            &c.event_db,
            c.event_wait,
        )?);
        let queue = Arc::new(routes::EventQueue::new(c.event_queue_capacity));
        routers.push((
            Sha256::digest(tenant.api_key.as_bytes()).into(),
            routes::router_with_queue(c, store, queue.clone()),
        ));
        queues.push(queue);
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
async fn dispatch(State(tenants): State<Tenants>, request: Request) -> Response {
    let token = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .unwrap_or("");
    let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let mut selected = None;
    for (key, router) in tenants.routers.iter() {
        if bool::from(key.ct_eq(&digest)) {
            selected = Some(router.clone());
        }
    }
    if let Some(router) = selected {
        return router.oneshot(request).await.unwrap();
    }
    routes::unauthorized(&tenants.config, &tenants.drains, request).await
}
