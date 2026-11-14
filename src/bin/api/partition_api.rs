//! Authenticated tenant dispatcher. Does not read uploads before admission.
use axum::{
    Json, Router,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tower::ServiceExt;
#[derive(Clone)]
pub(crate) struct RoutingPayment(pub String);
struct Partitions {
    rate: std::sync::Mutex<(std::time::Instant, usize)>,
    tenant: String,
    routers: Vec<Router>,
    admission: tokio::sync::Semaphore,
    config: crate::config::Config,
    drains: Arc<tokio::sync::Semaphore>,
}
pub fn router(tenant: String, routers: Vec<Router>, config: crate::config::Config) -> Router {
    Router::new()
        .fallback(dispatch)
        .with_state(Arc::new(Partitions {
            drains: Arc::new(tokio::sync::Semaphore::new(config.concurrency * 2)),
            config: config.clone(),
            rate: std::sync::Mutex::new((std::time::Instant::now(), 0)),
            tenant,
            routers,
            admission: tokio::sync::Semaphore::new(
                config.concurrency + config.event_queue_capacity,
            ),
        }))
}
pub(crate) fn valid_event_id(event: &reconcile_cli::events::PaymentEvent) -> bool {
    let prefix = format!("{:x}:", Sha256::digest(event.payment.id.as_bytes()));
    event.event_id.starts_with(&prefix) && event.event_id.len() > prefix.len()
}
async fn reject(
    state: &Partitions,
    request: Request,
    status: StatusCode,
    code: &'static str,
    message: &'static str,
) -> Response {
    crate::routes::reject_external(&state.config, &state.drains, request, status, code, message)
        .await
}
fn decode(segment: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(segment.len());
    let mut input = segment.as_bytes().iter().copied();
    while let Some(b) = input.next() {
        if b == b'%' {
            let a = (input.next()? as char).to_digit(16)?;
            let b = (input.next()? as char).to_digit(16)?;
            bytes.push((a * 16 + b) as u8);
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8(bytes).ok()
}
async fn dispatch(State(state): State<Arc<Partitions>>, mut request: Request) -> Response {
    let Ok(_permit) = state.admission.try_acquire() else {
        return reject(
            &state,
            request,
            StatusCode::SERVICE_UNAVAILABLE,
            "tenant_busy",
            "Tenant capacity exhausted; retry the same event",
        )
        .await;
    };
    let limited = {
        let mut rate = state.rate.lock().unwrap_or_else(|e| e.into_inner());
        if rate.0.elapsed() >= std::time::Duration::from_secs(1) {
            *rate = (std::time::Instant::now(), 0);
        }
        if rate.1 >= state.config.requests_per_second {
            true
        } else {
            rate.1 += 1;
            false
        }
    };
    if limited {
        let mut response = reject(
            &state,
            request,
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            "Tenant request rate exceeded; retry later",
        )
        .await;
        response
            .headers_mut()
            .insert("retry-after", axum::http::HeaderValue::from_static("1"));
        return response;
    }
    let path = request.uri().path();
    if path == "/v1/partitions" && request.method() == axum::http::Method::GET {
        return Json(serde_json::json!({"count":state.routers.len(),"partitions":(0..state.routers.len()).collect::<Vec<_>>(),"routing":"sha256-length-prefix-v1","event_identity":"sha256(payment_id):unique-suffix","list_scope":"Supply X-Reconcile-Partition for cases, discrepancies, metrics and storage-health"})).into_response();
    }
    let payment = if path == "/v1/events" {
        match request
            .headers()
            .get("x-reconcile-payment-id")
            .and_then(|v| v.to_str().ok())
            .and_then(decode)
        {
            Some(id)
                if !id.trim().is_empty()
                    && id.len() <= 256
                    && !id.chars().any(char::is_control) =>
            {
                Some(id)
            }
            _ => {
                return reject(
                    &state,
                    request,
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "routing_required",
                    "Supply URL-encoded payment ID in X-Reconcile-Payment-Id",
                )
                .await;
            }
        }
    } else if let Some(rest) = path.strip_prefix("/v1/payments/") {
        match decode(rest.split('/').next().unwrap_or_default()) {
            Some(id) => Some(id),
            None => {
                return reject(
                    &state,
                    request,
                    StatusCode::BAD_REQUEST,
                    "invalid_payment_id",
                    "Invalid URL encoding",
                )
                .await;
            }
        }
    } else {
        None
    };
    let index = if let Some(id) = payment {
        let index = reconcile_cli::events::partition_index(&state.tenant, &id, state.routers.len());
        request.extensions_mut().insert(RoutingPayment(id));
        index
    } else if matches!(path, "/v1/reconcile" | "/health" | "/openapi.json") {
        0
    } else {
        match request
            .headers()
            .get("x-reconcile-partition")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
        {
            Some(i) if i < state.routers.len() => i,
            _ => {
                return reject(
                    &state,
                    request,
                    StatusCode::BAD_REQUEST,
                    "partition_required",
                    "Supply X-Reconcile-Partition; discover partitions at GET /v1/partitions",
                )
                .await;
            }
        }
    };
    let mut response = state.routers[index]
        .clone()
        .oneshot(request)
        .await
        .expect("infallible router");
    response.headers_mut().insert(
        "x-reconcile-partition",
        index.to_string().parse().expect("numeric header"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn routing_header_preserves_utf8_and_literal_percent() {
        assert_eq!(decode("PAY%2F%C2%A3%25").as_deref(), Some("PAY/£%"));
        assert!(decode("%FF").is_none());
        assert!(decode("PAY%").is_none());
    }
}
