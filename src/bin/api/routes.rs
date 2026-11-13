use super::config::Config;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use http_body_util::BodyExt;
use reconcile_cli::{Payment, ReconciliationError, ReconciliationReport, reconcile_report};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use subtle::ConstantTimeEq;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use utoipa::OpenApi;

#[cfg(test)]
const MAX_BODY_BYTES: usize = 1_048_576;
#[cfg(test)]
const MAX_PAYMENTS: usize = 5000;

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct ReconcileRequest {
    internal_payments: Vec<Payment>,
    provider_payments: Vec<Payment>,
    /// Optional source labels (not credentials), at most 128 characters each.
    #[serde(default)]
    source_labels: Option<SourceLabels>,
}
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
struct SourceLabels {
    internal: String,
    provider: String,
}
#[derive(Serialize, utoipa::ToSchema)]
struct ErrorBody {
    code: String,
    message: String,
}
#[derive(Serialize, utoipa::ToSchema)]
struct ErrorEnvelope {
    error: ErrorBody,
}
#[derive(Clone)]
struct ErrorCode(&'static str);
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}
impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            axum::Extension(ErrorCode(self.code)),
            Json(ErrorEnvelope {
                error: ErrorBody {
                    code: self.code.into(),
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}
#[derive(Clone)]
struct AppState {
    metrics: Arc<super::metrics::Metrics>,
    event_store: Option<Arc<reconcile_cli::events::EventStore>>,
    event_queue: Arc<EventQueue>,
    config: Config,
    workers: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    event_admission: Arc<Semaphore>,
    rate: Arc<Mutex<(Instant, usize)>>,
    rejection_drains: Arc<Semaphore>,
}

#[cfg(test)]
pub(super) fn router() -> Router {
    configured_router(Config::default())
}

#[cfg(test)]
pub(super) fn configured_router(config: Config) -> Router {
    let store = Arc::new(reconcile_cli::events::EventStore::in_memory(config.event_wait).unwrap());
    router_with_store(config, store)
}

#[path = "event_queue.rs"]
pub(super) mod event_queue;
pub(super) use event_queue::EventQueue;

#[path = "event_routes.rs"]
mod event_routes;

#[cfg(test)]
pub(super) fn router_with_store(
    config: Config,
    store: Arc<reconcile_cli::events::EventStore>,
) -> Router {
    let queue = Arc::new(EventQueue::new(config.event_queue_capacity));
    router_with_queue(config, store, queue)
}
pub(super) fn router_with_queue(
    config: Config,
    store: Arc<reconcile_cli::events::EventStore>,
    queue: Arc<EventQueue>,
) -> Router {
    let metrics = Arc::new(super::metrics::Metrics::default());
    let weak = Arc::downgrade(&metrics);
    tokio::spawn(async move {
        loop {
            let start = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let Some(metrics) = weak.upgrade() else { break };
            metrics.runtime_gap((milliseconds(start) - 100.).max(0.));
        }
    });
    let state = AppState {
        metrics,
        event_queue: queue,
        event_store: Some(store),
        workers: Arc::new(Semaphore::new(config.concurrency)),
        requests: Arc::new(Semaphore::new(config.concurrency)),
        event_admission: Arc::new(Semaphore::new(
            config.concurrency + config.event_queue_capacity,
        )),
        rate: Arc::new(Mutex::new((Instant::now(), 0))),
        rejection_drains: Arc::new(Semaphore::new(config.concurrency * 2)),
        config: config.clone(),
    };
    Router::new()
        .route("/v1/reconcile", post(reconcile))
        .route("/v1/events", post(event_routes::ingest))
        .route(
            "/v1/payments/{payment_id}/history",
            get(event_routes::history),
        )
        .route(
            "/v1/payments/{payment_id}/investigation",
            post(event_routes::investigate),
        )
        .route("/v1/discrepancies", get(event_routes::discrepancies))
        .route("/v1/storage-health", get(event_routes::health))
        .route("/metrics", get(export_metrics))
        .route("/v1/payments/{payment_id}", get(event_routes::status))
        .route_layer(middleware::from_fn_with_state(state.clone(), guard))
        .route("/health", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/openapi.json", get(|| async { Json(api_doc()) }))
        .fallback(|| async { ApiError::new(StatusCode::NOT_FOUND, "not_found", "Route not found") })
        .method_not_allowed_fallback(|| async {
            ApiError::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "method_not_allowed",
                "Method not allowed",
            )
        })
        .layer(DefaultBodyLimit::max(config.max_body))
        .with_state(state)
}

#[derive(Clone, Debug, Default, Serialize)]
struct StageTimes {
    body_ms: f64,
    queue_ms: f64,
    decode_ms: f64,
    reconcile_ms: f64,
    encode_ms: f64,
}
fn milliseconds(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

async fn admit(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    event_route: bool,
) -> Result<(OwnedSemaphorePermit, Option<OwnedSemaphorePermit>), ApiError> {
    if let Some(key) = &state.config.api_key {
        let supplied = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .unwrap_or("");
        if !bool::from(Sha256::digest(key.as_bytes()).ct_eq(&Sha256::digest(supplied.as_bytes()))) {
            return Err(ApiError::new(
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "A valid bearer token is required",
            ));
        }
    }
    {
        let mut rate = state.rate.lock().unwrap_or_else(|e| e.into_inner());
        if rate.0.elapsed().as_secs() >= 1 {
            *rate = (Instant::now(), 0);
        }
        if rate.1 >= state.config.requests_per_second {
            return Err(ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "Request rate exceeded; retry later",
            ));
        }
        rate.1 += 1;
    }

    if event_route {
        // Bound active + waiting event admissions before awaiting FIFO capacity.
        // Fast clients must not repeatedly jump ahead of clients retrying 503s.
        let waiting = state
            .event_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                ApiError::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "busy",
                    "Event admission capacity exhausted; retry later",
                )
            })?;
        let active = state.requests.clone().acquire_owned().await.map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "busy",
                "Service is shutting down",
            )
        })?;
        return Ok((active, Some(waiting)));
    }
    state
        .requests
        .clone()
        .try_acquire_owned()
        .map(|active| (active, None))
        .map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "busy",
                "Request capacity exhausted; retry later",
            )
        })
}

// Discard frames without collecting or parsing rejected uploads. Bound time,
// bytes and simultaneous drains: an untrusted sender cannot demand infinite work.
async fn reject_upload(state: &AppState, request: Request, error: ApiError) -> Response {
    reject_body(&state.config, &state.rejection_drains, request, error).await
}
pub(super) async fn unauthorized(
    config: &Config,
    drains: &Arc<Semaphore>,
    request: Request,
) -> Response {
    reject_body(
        config,
        drains,
        request,
        ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "A valid bearer token is required",
        ),
    )
    .await
}
async fn reject_body(
    config: &Config,
    drains: &Arc<Semaphore>,
    request: Request,
    error: ApiError,
) -> Response {
    let http1 = matches!(
        request.version(),
        axum::http::Version::HTTP_10 | axum::http::Version::HTTP_11
    );
    let mut response = error.into_response();
    let complete = if let Ok(_permit) = drains.clone().try_acquire_owned() {
        let mut body = request.into_body();
        let drain = async {
            let mut remaining = config.max_body;
            while let Some(frame) = body.frame().await {
                let Ok(frame) = frame else { return false };
                if let Some(data) = frame.data_ref() {
                    let Some(next) = remaining.checked_sub(data.len()) else {
                        return false;
                    };
                    remaining = next;
                }
            }
            true
        };
        tokio::time::timeout(config.timeout.min(std::time::Duration::from_secs(2)), drain)
            .await
            .unwrap_or(false)
    } else {
        false
    };
    if !complete && http1 {
        response
            .headers_mut()
            .insert("connection", axum::http::HeaderValue::from_static("close"));
    }
    response
}

async fn guard(State(state): State<AppState>, mut request: Request, next: Next) -> Response {
    let started = Instant::now();
    let route = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "/unknown".into());
    let principal = request
        .extensions()
        .get::<super::permissions::Principal>()
        .cloned();
    if state.config.scoped_auth && principal.is_none() {
        return unauthorized(&state.config, &state.rejection_drains, request).await;
    }
    let principal = principal.unwrap_or_else(super::permissions::Principal::legacy);
    if !principal.allows(request.method(), &route) {
        return reject_upload(
            &state,
            request,
            ApiError::new(
                StatusCode::FORBIDDEN,
                "forbidden",
                "Credential does not permit this operation",
            ),
        )
        .await;
    }
    request.extensions_mut().insert(principal);
    let request_id = uuid::Uuid::new_v4().to_string();
    let event_route = matches!(
        route.as_str(),
        "/v1/events"
            | "/v1/payments/{payment_id}"
            | "/v1/discrepancies"
            | "/v1/storage-health"
            | "/v1/payments/{payment_id}/history"
            | "/v1/payments/{payment_id}/investigation"
    );
    let admission = tokio::time::timeout(
        state.config.timeout,
        admit(&state, request.headers(), event_route),
    )
    .await;
    let admission_ms = milliseconds(started);
    let mut response = match admission {
        Ok(Err(error)) => reject_upload(&state, request, error).await,
        Err(_) => {
            reject_upload(
                &state,
                request,
                ApiError::new(
                    StatusCode::GATEWAY_TIMEOUT,
                    "timeout",
                    "Request deadline exceeded while awaiting capacity",
                ),
            )
            .await
        }
        Ok(Ok(_permits)) => {
            let remaining = state.config.timeout.saturating_sub(started.elapsed());
            match tokio::time::timeout(remaining, next.run(request)).await {
                Ok(response) => response,
                Err(_) => ApiError::new(
                    StatusCode::GATEWAY_TIMEOUT,
                    "timeout",
                    "Request deadline exceeded",
                )
                .into_response(),
            }
        }
    };
    if matches!(
        response.status(),
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    ) {
        response
            .headers_mut()
            .insert("retry-after", axum::http::HeaderValue::from_static("1"));
    }
    state.metrics.observe(
        response.status().as_u16(),
        response.extensions().get::<ErrorCode>().map(|c| c.0),
        milliseconds(started),
        admission_ms,
        response.extensions().get::<event_queue::EventStages>(),
    );
    // Encode first: formatting serde_json::Value directly into stderr can issue
    // a write for each JSON fragment, blocking other requests on the stderr lock.
    let log_line = json!({"event":"request","request_id":request_id,"route":route,
        "status":response.status().as_u16(),"elapsed_ms":milliseconds(started),"admission_ms":admission_ms,
        "stages":response.extensions().get::<StageTimes>(), "event_stages":response.extensions().get::<event_queue::EventStages>()})
    .to_string();
    super::request_log::emit(log_line);
    response.headers_mut().insert(
        "cache-control",
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
        .headers_mut()
        .insert("x-request-id", request_id.parse().unwrap());
    response
}
#[utoipa::path(post,path="/v1/reconcile",request_body=ReconcileRequest,
    security(("bearerAuth"=[])),
    responses((status=200,description="Report; mismatches are successful results",body=ReconciliationReport),
    (status=400,description="Malformed JSON",body=ErrorEnvelope),
    (status=401,description="Missing or invalid token",body=ErrorEnvelope),
    (status=413,description="Body or payment count limit",body=ErrorEnvelope),
    (status=415,description="Unsupported content type",body=ErrorEnvelope),
    (status=422,description="Invalid data or duplicate IDs",body=ErrorEnvelope),
    (status=429,description="Global rate limit",body=ErrorEnvelope),
    (status=500,description="Worker failure",body=ErrorEnvelope),
    (status=503,description="Capacity exhausted",body=ErrorEnvelope),
    (status=504,description="Deadline exceeded",body=ErrorEnvelope)))]
async fn reconcile(State(state): State<AppState>, request: Request) -> Result<Response, ApiError> {
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<mime::Mime>().ok());
    if !content_type.is_some_and(|m| {
        m.type_() == mime::APPLICATION
            && (m.subtype() == mime::JSON || m.suffix() == Some(mime::JSON))
    }) {
        return Ok(reject_upload(
            &state,
            request,
            ApiError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Use Content-Type: application/json",
            ),
        )
        .await);
    }
    let body_started = Instant::now();
    let bytes = axum::body::to_bytes(request.into_body(), state.config.max_body)
        .await
        .map_err(|error| {
            use std::error::Error;
            if error
                .source()
                .is_some_and(|source| source.is::<http_body_util::LengthLimitError>())
            {
                ApiError::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request_too_large",
                    "Request body exceeds configured byte limit",
                )
            } else {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_json",
                    "Request body could not be read",
                )
            }
        })?;
    let mut timings = StageTimes {
        body_ms: milliseconds(body_started),
        ..StageTimes::default()
    };
    let permit = state.workers.clone().try_acquire_owned().map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "busy",
            "Worker capacity exhausted; retry later",
        )
    })?;
    let config = state.config.clone();
    let queued = Instant::now();
    let (encoded, timings) = tokio::task::spawn_blocking(move || {
        // Keep capacity reserved through decoding, matching, encoding and cleanup,
        // even when the HTTP deadline expires and the awaiting task is cancelled.
        let _permit = permit;
        timings.queue_ms = milliseconds(queued);
        let started = Instant::now();
        let mut request: ReconcileRequest = serde_json::from_slice(&bytes).map_err(json_error)?;
        timings.decode_ms = milliseconds(started);
        if request.internal_payments.len() > config.max_payments
            || request.provider_payments.len() > config.max_payments
        {
            return Err(ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "too_many_payments",
                format!(
                    "Each source may contain at most {} payments",
                    config.max_payments
                ),
            ));
        }
        if request.source_labels.as_ref().is_some_and(|s| {
            [s.internal.as_str(), s.provider.as_str()]
                .iter()
                .any(|s| s.trim().is_empty() || s.chars().count() > 128)
        }) {
            return Err(ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_source_labels",
                "Source labels must contain 1 to 128 characters and not be blank",
            ));
        }
        let started = Instant::now();
        let mut report = reconcile_report(&request.internal_payments, &request.provider_payments)
            .map_err(|e| {
            let code = match e {
                ReconciliationError::DuplicateInternalIds(_) => "duplicate_internal_ids",
                ReconciliationError::DuplicateProviderIds(_) => "duplicate_provider_ids",
                ReconciliationError::InvalidPaymentId { .. } => "invalid_payment_id",
            };
            ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                code,
                "Snapshot validation failed; correct the indicated source before retrying",
            )
        })?;
        if let Some(labels) = request.source_labels.take() {
            report.metadata.internal_source.label = Some(labels.internal);
            report.metadata.provider_source.label = Some(labels.provider);
        }
        timings.reconcile_ms = milliseconds(started);
        let started = Instant::now();
        let encoded = serde_json::to_vec(&report).map_err(|_| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Report could not be encoded",
            )
        })?;
        timings.encode_ms = milliseconds(started);
        Ok::<_, ApiError>((encoded, timings))
    })
    .await
    .map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Reconciliation could not complete",
        )
    })??;
    let mut response = (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        encoded,
    )
        .into_response();
    response.headers_mut().insert("server-timing", format!(
        "body;dur={:.3}, queue;dur={:.3}, json_decode;dur={:.3}, reconcile;dur={:.3}, json_encode;dur={:.3}",
        timings.body_ms, timings.queue_ms, timings.decode_ms, timings.reconcile_ms, timings.encode_ms
    ).parse().unwrap());
    response.extensions_mut().insert(timings);
    Ok(response)
}

fn json_error(error: serde_json::Error) -> ApiError {
    let status = if error.is_data() {
        StatusCode::UNPROCESSABLE_ENTITY
    } else {
        StatusCode::BAD_REQUEST
    };
    if status == StatusCode::UNPROCESSABLE_ENTITY {
        ApiError::new(
            status,
            "invalid_payment_data",
            "Invalid fields, currency, status, amount or ID; see /openapi.json",
        )
    } else {
        ApiError::new(status, "invalid_json", "Request must contain valid JSON")
    }
}
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        reconcile,
        event_routes::ingest,
        event_routes::status,
        event_routes::discrepancies,
        event_routes::health,
        event_routes::history,
        event_routes::investigate
    ),
    components(schemas(
        ReconcileRequest,
        SourceLabels,
        ErrorEnvelope,
        ErrorBody,
        ReconciliationReport
    )),
    info(
        title = "Payment reconciliation API",
        version = "0.2.0",
        description = "Authenticated snapshot reconciliation. Local loopback development may omit authentication. Configured limits override documented defaults."
    )
)]
struct ApiDoc;
pub(super) fn api_doc() -> serde_json::Value {
    let mut doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
    doc["components"]["securitySchemes"] = json!({"bearerAuth":{"type":"http","scheme":"bearer"}});
    // JSON integers can represent u64 exactly; do not advertise signed int64.
    for (schema, field) in [
        ("Payment", "amount_minor"),
        ("ReportEntry", "internal_amount_minor"),
        ("ReportEntry", "provider_amount_minor"),
    ] {
        let property = &mut doc["components"]["schemas"][schema]["properties"][field];
        property["maximum"] = json!(u64::MAX);
        if let Some(object) = property.as_object_mut() {
            object.remove("format");
        }
    }
    for field in ["internal", "provider"] {
        let property = &mut doc["components"]["schemas"]["SourceLabels"]["properties"][field];
        property["minLength"] = json!(1);
        property["maxLength"] = json!(128);
        property["pattern"] = json!("\\S");
    }
    doc["paths"]["/health"] = json!({"get":{"responses":{"200":{"description":"Service is alive","content":{"application/json":{"schema":{"type":"object","required":["status"],"properties":{"status":{"type":"string","enum":["ok"]}}}}}}}}});
    doc["paths"]["/openapi.json"] =
        json!({"get":{"responses":{"200":{"description":"Generated OpenAPI contract"}}}});
    doc
}
#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;

    async fn request(
        method: &str,
        path: &str,
        body: String,
        content_type: bool,
    ) -> (StatusCode, serde_json::Value) {
        let mut builder = Request::builder().method(method).uri(path);
        if content_type {
            builder = builder.header("content-type", "application/json");
        }
        let response = router()
            .oneshot(builder.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let status = response.status();
        assert_eq!(response.headers()["content-type"], "application/json");
        let bytes = to_bytes(response.into_body(), 2 * MAX_BODY_BYTES)
            .await
            .unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    fn payment(id: &str, amount: u64) -> serde_json::Value {
        json!({"id": id, "currency":"GBP","amount_minor": amount, "status": "Completed"})
    }

    #[tokio::test]
    async fn health_is_available() {
        let (status, json) = request("GET", "/health", String::new(), false).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json, json!({"status":"ok"}));
    }

    #[tokio::test]
    async fn discrepancies_are_successful_reports() {
        let body = json!({"internal_payments":[payment("P1",100)], "provider_payments":[payment("P1",90)]});
        let (status, json) = request("POST", "/v1/reconcile", body.to_string(), true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            json["summary"],
            json!({"total_entries":1,"matched":0,"discrepancies":1})
        );
        assert_eq!(
            json["entries"][0]["recommended_action"],
            "InvestigateAmount"
        );
    }

    #[tokio::test]
    async fn empty_snapshots_are_valid() {
        let (status, json) = request(
            "POST",
            "/v1/reconcile",
            json!({"internal_payments":[],"provider_payments":[]}).to_string(),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["summary"]["total_entries"], 0);
    }

    #[tokio::test]
    async fn rejects_invalid_json_and_media_type() {
        let (status, json) = request("POST", "/v1/reconcile", "{".into(), true).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(json["error"]["code"], "invalid_json");
        let (status, _) = request("POST", "/v1/reconcile", "{}".into(), false).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn rejects_invalid_payment_data() {
        for bad_payment in [
            payment("", 100),
            json!({"id":"P1","currency":"GBP","amount_minor":-1,"status":"Completed"}),
            json!({"id":"P1","currency":"GBP","amount_minor":100,"status":"Banana"}),
        ] {
            let body = json!({"internal_payments":[bad_payment], "provider_payments":[]});
            let (status, json) = request("POST", "/v1/reconcile", body.to_string(), true).await;
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
            assert_eq!(json["error"]["code"], "invalid_payment_data");
        }
        let (status, _) = request("POST", "/v1/reconcile", "{}".into(), true).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn rejects_duplicates_in_both_sources() {
        for source in ["internal_payments", "provider_payments"] {
            let mut body = json!({"internal_payments":[],"provider_payments":[]});
            body[source] = json!([payment("P1", 100), payment("P1", 100)]);
            let (status, json) = request("POST", "/v1/reconcile", body.to_string(), true).await;
            assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
            let expected = if source == "internal_payments" {
                "duplicate_internal_ids"
            } else {
                "duplicate_provider_ids"
            };
            assert_eq!(json["error"]["code"], expected);
        }
    }

    #[tokio::test]
    async fn enforces_body_and_payment_limits() {
        let (status, json) = request(
            "POST",
            "/v1/reconcile",
            " ".repeat(MAX_BODY_BYTES + 1),
            true,
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(json["error"]["code"], "request_too_large");
        let body = json!({"internal_payments":vec![payment("P1",1);MAX_PAYMENTS+1], "provider_payments":[]});
        let (status, json) = request("POST", "/v1/reconcile", body.to_string(), true).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(json["error"]["code"], "too_many_payments");
    }

    #[tokio::test]
    async fn routing_errors_are_json() {
        let (status, _) = request("GET", "/missing", String::new(), false).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = request("GET", "/v1/reconcile", String::new(), false).await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    }
    #[tokio::test]
    async fn api_returns_status_evidence() {
        let body = json!({
            "internal_payments": [payment("P1", 5000)],
            "provider_payments": [{"id":"P1", "currency":"GBP","amount_minor":5000, "status":"Pending"}]
        });
        let (status, json) = request("POST", "/v1/reconcile", body.to_string(), true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["entries"][0]["internal_status"], "Completed");
        assert_eq!(json["entries"][0]["provider_status"], "Pending");
        assert_eq!(json["entries"][0]["internal_amount_minor"], 5000);
        assert_eq!(json["entries"][0]["provider_amount_minor"], 5000);
    }
    #[tokio::test]
    async fn api_reports_both_mismatches_as_one_discrepancy() {
        let body = json!({
            "internal_payments": [payment("P1", 100), payment("P2", 5000)],
            "provider_payments": [payment("P1", 100), {"id":"P2", "currency":"GBP","amount_minor":4500, "status":"Pending"}]
        });
        let (status, json) = request("POST", "/v1/reconcile", body.to_string(), true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            json["summary"],
            json!({"total_entries":2,"matched":1,"discrepancies":1})
        );
        assert_eq!(json["entries"].as_array().unwrap().len(), 2);
        assert_eq!(json["entries"][1]["result"], "AmountAndStatusMismatch");
        assert_eq!(
            json["entries"][1]["recommended_action"],
            "InvestigateAmountAndStatus"
        );
        assert_eq!(json["entries"][1]["internal_amount_minor"], 5000);
        assert_eq!(json["entries"][1]["provider_amount_minor"], 4500);
        assert_eq!(json["entries"][1]["internal_status"], "Completed");
        assert_eq!(json["entries"][1]["provider_status"], "Pending");
    }
    #[tokio::test]
    async fn authentication_and_limits_apply_to_real_routes() {
        let config = Config {
            api_key: Some("a".repeat(32)),
            requests_per_second: 1,
            ..Config::default()
        };
        let app = configured_router(config);
        let body = r#"{"internal_payments":[],"provider_payments":[]}"#;
        for key in [None, Some("Bearer wrong")] {
            let mut req = Request::builder()
                .method("POST")
                .uri("/v1/reconcile")
                .header("content-type", "application/json");
            if let Some(key) = key {
                req = req.header("authorization", key);
            }
            let response = app
                .clone()
                .oneshot(req.body(Body::from(body)).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
        for expected in [StatusCode::OK, StatusCode::TOO_MANY_REQUESTS] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/reconcile")
                        .header("content-type", "application/json")
                        .header("authorization", format!("Bearer {}", "a".repeat(32)))
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            assert!(response.headers().contains_key("x-request-id"));
        }
    }

    #[tokio::test]
    async fn configurable_count_limit_and_source_labels() {
        let app = configured_router(Config {
            max_payments: 1,
            ..Config::default()
        });
        let body =
            json!({"internal_payments":[payment("P1",1),payment("P2",2)],"provider_payments":[]});
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/reconcile")
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = json!({"internal_payments":[],"provider_payments":[],"source_labels":{"internal":"checkout-export-1","provider":"provider-export-1"}});
        let (status, response) = request("POST", "/v1/reconcile", body.to_string(), true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            response["metadata"]["internal_source"]["label"],
            "checkout-export-1"
        );
    }

    fn test_state(timeout: std::time::Duration) -> AppState {
        AppState {
            metrics: Arc::new(crate::metrics::Metrics::default()),
            event_store: None,
            event_queue: Arc::new(EventQueue::new(32)),
            config: Config {
                timeout,
                ..Config::default()
            },
            rejection_drains: Arc::new(Semaphore::new(2)),
            workers: Arc::new(Semaphore::new(1)),
            requests: Arc::new(Semaphore::new(1)),
            event_admission: Arc::new(Semaphore::new(2)),
            rate: Arc::new(Mutex::new((Instant::now(), 0))),
        }
    }
    #[tokio::test]
    async fn guard_times_out_and_rejects_over_capacity() {
        let state = test_state(std::time::Duration::from_millis(10));
        let app = Router::new()
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    "done"
                }),
            )
            .layer(middleware::from_fn_with_state(state.clone(), guard));
        let permit = state.requests.clone().acquire_owned().await.unwrap();
        let r = app
            .clone()
            .oneshot(Request::builder().uri("/slow").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        drop(permit);
        let r = app
            .oneshot(Request::builder().uri("/slow").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(state.requests.available_permits(), 1);
    }

    #[tokio::test]
    async fn generated_schema_validates_live_request_and_response() {
        let doc = api_doc();
        let request_schema =
            json!({"$ref":"#/components/schemas/ReconcileRequest","components":doc["components"]});
        let response_schema = json!({"$ref":"#/components/schemas/ReconciliationReport","components":doc["components"]});
        let req = json!({"internal_payments":[payment("P1",100)],"provider_payments":[]});
        assert!(
            jsonschema::validator_for(&request_schema)
                .unwrap()
                .is_valid(&req)
        );
        let (status, report) = request("POST", "/v1/reconcile", req.to_string(), true).await;
        assert_eq!(status, StatusCode::OK);
        let validator = jsonschema::validator_for(&response_schema).unwrap();
        assert!(
            validator.is_valid(&report),
            "{:?}",
            validator.validate(&report)
        );
        assert!(doc["paths"]["/v1/reconcile"]["post"]["responses"]["401"].is_object());
        let mut invalid = req.clone();
        invalid["internal_payments"][0]["currency"] = json!("XYZ");
        assert!(
            !jsonschema::validator_for(&request_schema)
                .unwrap()
                .is_valid(&invalid)
        );
    }
    #[test]
    fn checked_in_openapi_matches_generated_contract() {
        let saved: serde_json::Value =
            serde_json::from_str(include_str!("../../../docs/openapi.json")).unwrap();
        assert_eq!(
            saved,
            api_doc(),
            "Regenerate docs/openapi.json after API contract changes"
        );
    }
}

#[cfg(test)]
#[path = "wire_tests.rs"]
mod wire_tests;

#[cfg(test)]
#[path = "admission_tests.rs"]
mod admission_tests;

#[utoipa::path(get,path="/metrics",security(("bearerAuth"=[])),responses((status=200,description="Tenant Prometheus metrics, reset on restart",body=String,content_type="text/plain"),(status=401,description="Invalid credential",body=ErrorEnvelope)))]
async fn export_metrics(State(state): State<AppState>) -> Response {
    let (queued, active) = state.event_queue.depth();
    (
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        state.metrics.render(queued, active),
    )
        .into_response()
}
