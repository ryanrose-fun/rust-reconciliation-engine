use super::*;
use axum::extract::{Path, rejection::PathRejection};
use reconcile_cli::events::{EventError, EventReceipt, LivePayment, PaymentEvent};

const MAX_EVENT_BYTES: usize = 16 * 1024;

#[utoipa::path(get,path="/v1/cases",security(("bearerAuth"=[])),
params(("after"=Option<String>,Query),("limit"=Option<usize>,Query),("owner"=Option<String>,Query),("workflow"=Option<String>,Query),("acknowledged"=Option<bool>,Query),("min_age_ms"=Option<u64>,Query)),
responses((status=200,description="Active investigation queue; deadline worker materializes overdue cases",body=reconcile_cli::events::CasePage),(status=422,description="Invalid query",body=ErrorEnvelope)))]
pub(super) async fn cases(
    State(state): State<AppState>,
    query: Result<
        axum::extract::Query<reconcile_cli::events::CaseQuery>,
        axum::extract::rejection::QueryRejection,
    >,
) -> Result<Response, ApiError> {
    let axum::extract::Query(query) = query.map_err(|_| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_filter",
            "Invalid case query",
        )
    })?;
    let store = event_store(&state)?;
    let (value, stages) = state
        .event_queue
        .run(state.workers.clone(), move || {
            Ok((
                store.cases(query).map_err(storage_error)?,
                Default::default(),
            ))
        })
        .await?;
    no_cache(value, stages)
}
#[utoipa::path(post,path="/v1/payments/{payment_id}/case",security(("bearerAuth"=[])),
params(("payment_id"=String,Path)),request_body=reconcile_cli::events::CaseUpdate,
responses((status=200,description="Assignment and workflow recorded without changing payment evidence",body=reconcile_cli::events::InvestigationReceipt),(status=403,description="Investigator role required",body=ErrorEnvelope),(status=409,description="Case or evidence changed",body=ErrorEnvelope)))]
pub(super) async fn update_case(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    request: Request,
) -> Result<Response, ApiError> {
    let Path(id) = path.map_err(|_| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_event",
            "Invalid payment ID",
        )
    })?;
    let actor = request
        .extensions()
        .get::<crate::permissions::Principal>()
        .cloned()
        .unwrap_or_else(crate::permissions::Principal::legacy)
        .id;
    let media = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<mime::Mime>().ok());
    if !media.is_some_and(|m| {
        m.type_() == mime::APPLICATION
            && (m.subtype() == mime::JSON || m.suffix() == Some(mime::JSON))
    }) {
        return Ok(reject_upload(
            &state,
            request,
            ApiError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Use application/json",
            ),
        )
        .await);
    }
    let bytes = axum::body::to_bytes(
        request.into_body(),
        MAX_EVENT_BYTES.min(state.config.max_body),
    )
    .await
    .map_err(|_| {
        ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "Case body unreadable or too large",
        )
    })?;
    let input = serde_json::from_slice(&bytes).map_err(json_error)?;
    let store = event_store(&state)?;
    let (value, stages) = state
        .event_queue
        .run(state.workers.clone(), move || {
            Ok((
                store
                    .update_case(&id, &actor, input)
                    .map_err(storage_error)?,
                Default::default(),
            ))
        })
        .await?;
    no_cache(value, stages)
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub(super) struct HistoryQuery {
    after: i64,
    limit: Option<usize>,
}
#[utoipa::path(get,path="/v1/payments/{payment_id}/history",security(("bearerAuth"=[])),
 params(("payment_id"=String,Path),("after"=Option<i64>,Query),("limit"=Option<usize>,Query)),
 responses((status=200,description="Append-only timeline and current revision; deadlines evaluated on observation",body=reconcile_cli::events::HistoryPage), (status=401,description="Invalid credentials",body=ErrorEnvelope),(status=404,description="Payment not found",body=ErrorEnvelope),(status=422,description="Invalid cursor",body=ErrorEnvelope)))]
pub(super) async fn history(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    query: Result<axum::extract::Query<HistoryQuery>, axum::extract::rejection::QueryRejection>,
) -> Result<Response, ApiError> {
    let Path(id) = path.map_err(|_| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_event",
            "Invalid payment ID in URL",
        )
    })?;
    let axum::extract::Query(query) = query.map_err(|_| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_filter",
            "Invalid history query",
        )
    })?;
    let store = event_store(&state)?;
    let (value, stages) = state
        .event_queue
        .run(state.workers.clone(), move || {
            Ok((
                store
                    .history(&id, query.after, query.limit.unwrap_or(100))
                    .map_err(storage_error)?,
                Default::default(),
            ))
        })
        .await?;
    no_cache(value, stages)
}
#[utoipa::path(post,path="/v1/payments/{payment_id}/investigation",security(("bearerAuth"=[])),
 params(("payment_id"=String,Path)),request_body=reconcile_cli::events::Investigation,
 responses((status=200,description="Committed immutable note or acknowledgement",body=reconcile_cli::events::InvestigationReceipt),(status=403,description="Investigator permission required",body=ErrorEnvelope),(status=409,description="Stale revision or reused investigation ID",body=ErrorEnvelope),(status=422,description="Invalid note",body=ErrorEnvelope)))]
pub(super) async fn investigate(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
    request: Request,
) -> Result<Response, ApiError> {
    let Path(id) = path.map_err(|_| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_event",
            "Invalid payment ID in URL",
        )
    })?;
    let actor = request
        .extensions()
        .get::<crate::permissions::Principal>()
        .cloned()
        .unwrap_or_else(crate::permissions::Principal::legacy)
        .id;
    let media = request
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<mime::Mime>().ok());
    if !media.is_some_and(|m| {
        m.type_() == mime::APPLICATION
            && (m.subtype() == mime::JSON || m.suffix() == Some(mime::JSON))
    }) {
        return Ok(reject_upload(
            &state,
            request,
            ApiError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "Use application/json",
            ),
        )
        .await);
    }
    let bytes = axum::body::to_bytes(
        request.into_body(),
        MAX_EVENT_BYTES.min(state.config.max_body),
    )
    .await
    .map_err(|error| {
        use std::error::Error;
        if error
            .source()
            .is_some_and(|e| e.is::<http_body_util::LengthLimitError>())
        {
            ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "Investigation body exceeds its limit",
            )
        } else {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "Investigation body could not be read",
            )
        }
    })?;
    let input = serde_json::from_slice(&bytes).map_err(json_error)?;
    let store = event_store(&state)?;
    let (value, stages) = state
        .event_queue
        .run(state.workers.clone(), move || {
            Ok((
                store
                    .investigate(&id, &actor, input)
                    .map_err(storage_error)?,
                Default::default(),
            ))
        })
        .await?;
    no_cache(value, stages)
}

pub(super) fn storage_error(error: EventError) -> ApiError {
    let (status, code, message) = match error {
        EventError::InvalidInput(message) => {
            (StatusCode::UNPROCESSABLE_ENTITY, "invalid_event", message)
        }
        EventError::RevisionConflict => (
            StatusCode::CONFLICT,
            "revision_conflict",
            "Reload history: the revision or investigation ID conflicts",
        ),
        EventError::EventIdConflict => (
            StatusCode::CONFLICT,
            "event_id_conflict",
            "Event ID was already used with different content",
        ),
        EventError::VersionConflict => (
            StatusCode::CONFLICT,
            "version_conflict",
            "This source version already has different payment data",
        ),
        EventError::NotFound => (
            StatusCode::NOT_FOUND,
            "payment_not_found",
            "Payment not found",
        ),
        EventError::Busy => (
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_busy",
            "Event storage is busy; retry using the same event ID",
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "storage_error",
            "Event storage operation failed",
        ),
    };
    ApiError::new(status, code, message)
}
fn event_store(state: &AppState) -> Result<Arc<reconcile_cli::events::EventStore>, ApiError> {
    state.event_store.clone().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_unavailable",
            "Event storage is unavailable",
        )
    })
}
fn no_cache(
    value: impl Serialize,
    mut stages: super::event_queue::EventStages,
) -> Result<Response, ApiError> {
    let encode = Instant::now();
    let bytes = serde_json::to_vec(&value).map_err(|_| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "Response encoding failed",
        )
    })?;
    stages.encode_ms = milliseconds(encode);
    let timing = format!(
        "event_group;desc=\"{} events\", event_body;dur={:.4}, event_resume;dur={:.4}, event_queue;dur={:.4}, event_dispatch;dur={:.4}, event_service;dur={:.4}, event_prepare;dur={:.4}, event_begin;dur={:.4}, event_work;dur={:.4}, event_commit;dur={:.4}, event_encode;dur={:.4}",
        stages.group_size,
        stages.body_ms,
        stages.resume_ms,
        stages.queue_ms,
        stages.dispatch_ms,
        stages.service_ms,
        stages.storage.prepare_ms,
        stages.storage.begin_ms,
        stages.storage.work_ms,
        stages.storage.commit_ms,
        stages.encode_ms
    );
    let mut response = (
        StatusCode::OK,
        [
            ("content-type", "application/json"),
            ("cache-control", "no-store"),
        ],
        bytes,
    )
        .into_response();
    response
        .headers_mut()
        .insert("server-timing", timing.parse().unwrap());
    response.extensions_mut().insert(stages);
    Ok(response)
}
#[utoipa::path(post,path="/v1/events",request_body=PaymentEvent,
    security(("bearerAuth"=[])),
    responses((status=200,description="Committed event or exact retry; includes current payment state",body=EventReceipt),
    (status=400,description="Malformed JSON",body=ErrorEnvelope),
    (status=401,description="Missing or invalid token",body=ErrorEnvelope),
    (status=403,description="Credential does not permit this source or operation",body=ErrorEnvelope),
    (status=409,description="Event identity or source-version conflict",body=ErrorEnvelope),
    (status=413,description="Event body exceeds 16 KiB or configured body limit",body=ErrorEnvelope),
    (status=415,description="Unsupported media type",body=ErrorEnvelope),
    (status=422,description="Invalid event data",body=ErrorEnvelope),
    (status=429,description="Rate limit; Retry-After supplied",body=ErrorEnvelope),
    (status=500,description="Storage failure",body=ErrorEnvelope),
    (status=503,description="Capacity or storage busy; retry identical event",body=ErrorEnvelope),
    (status=504,description="Deadline expired; commit may have completed, retry identical event",body=ErrorEnvelope)))]
pub(super) async fn ingest(
    State(state): State<AppState>,
    request: Request,
) -> Result<Response, ApiError> {
    let media = request
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<mime::Mime>().ok());
    if !media.is_some_and(|m| {
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
    let principal = request
        .extensions()
        .get::<crate::permissions::Principal>()
        .cloned()
        .unwrap_or_else(crate::permissions::Principal::legacy);
    let routing_payment = request
        .extensions()
        .get::<crate::partition_api::RoutingPayment>()
        .cloned();
    let body_started = Instant::now();
    let body = axum::body::to_bytes(
        request.into_body(),
        state.config.max_body.min(MAX_EVENT_BYTES),
    )
    .await
    .map_err(|error| {
        use std::error::Error;
        if error
            .source()
            .is_some_and(|e| e.is::<http_body_util::LengthLimitError>())
        {
            ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "Event body exceeds its byte limit",
            )
        } else {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "invalid_json",
                "Event body could not be read",
            )
        }
    })?;
    let store = event_store(&state)?;
    let body_ms = milliseconds(body_started);
    let event: PaymentEvent = serde_json::from_slice(&body).map_err(|error| {
        if error.is_data() {
            ApiError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_event",
                "Invalid event fields, payment data or source",
            )
        } else {
            json_error(error)
        }
    })?;
    if !principal.writes_source(event.source) {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "source_forbidden",
            "Credential cannot write this source",
        ));
    }
    if let Some(expected) = routing_payment
        && (event.payment.id != expected.0 || !crate::partition_api::valid_event_id(&event))
    {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_partition_identity",
            "Payment routing header must match the body and event ID must start with SHA256(payment ID) followed by ':'",
        ));
    }
    let (receipt, mut stages) = state
        .event_queue
        .ingest(store, state.workers.clone(), event)
        .await?;
    stages.body_ms = body_ms;
    no_cache(receipt, stages)
}

#[utoipa::path(get,path="/v1/payments/{payment_id}",
    params(("payment_id"=String,Path,description="Exact payment ID, URL-encoded; at most 256 UTF-8 bytes")),
    security(("bearerAuth"=[])),
    responses((status=200,description="Current state, with waiting deadline evaluated now; Cache-Control: no-store",body=LivePayment),
    (status=401,description="Missing or invalid token",body=ErrorEnvelope),
    (status=404,description="Payment not found",body=ErrorEnvelope),
    (status=422,description="Invalid payment ID",body=ErrorEnvelope),
    (status=429,description="Rate limit",body=ErrorEnvelope),
    (status=500,description="Storage failure",body=ErrorEnvelope),
    (status=503,description="Storage or worker capacity busy",body=ErrorEnvelope),
    (status=504,description="Deadline expired",body=ErrorEnvelope)))]
pub(super) async fn status(
    State(state): State<AppState>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Response, ApiError> {
    let Path(id) = path.map_err(|_| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_event",
            "Invalid payment ID in URL",
        )
    })?;
    let store = event_store(&state)?;
    let (value, stages) = state
        .event_queue
        .run(state.workers.clone(), move || {
            store.status_profiled(&id).map_err(storage_error)
        })
        .await?;
    no_cache(value, stages)
}
#[cfg(test)]
#[path = "event_routes/tests.rs"]
mod tests;

#[utoipa::path(get,path="/v1/discrepancies",security(("bearerAuth"=[])),
    params(("after"=Option<String>,Query),("limit"=Option<usize>,Query),("status"=Option<String>,Query),("currency"=Option<String>,Query),("source"=Option<String>,Query),("min_age_ms"=Option<u64>,Query)),
    responses((status=200,description="Live bounded page; follow next_after even for empty entries",body=reconcile_cli::events::DiscrepancyPage), (status=422,description="Invalid filter",body=ErrorEnvelope)))]
pub(super) async fn discrepancies(
    State(state): State<AppState>,
    query: Result<
        axum::extract::Query<reconcile_cli::events::DiscrepancyQuery>,
        axum::extract::rejection::QueryRejection,
    >,
) -> Result<Response, ApiError> {
    let axum::extract::Query(query) = query.map_err(|_| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_filter",
            "Invalid discrepancy query",
        )
    })?;
    let store = event_store(&state)?;
    let (page, stages) = state
        .event_queue
        .run(state.workers.clone(), move || {
            Ok((
                store.discrepancies(query).map_err(storage_error)?,
                Default::default(),
            ))
        })
        .await?;
    no_cache(page, stages)
}
#[utoipa::path(get,path="/v1/storage-health",security(("bearerAuth"=[])),
    responses((status=200,description="Tenant storage counts; a full indexed metadata scan",body=reconcile_cli::events::StorageHealth)))]
pub(super) async fn health(State(state): State<AppState>) -> Result<Response, ApiError> {
    let store = event_store(&state)?;
    let (health, stages) = state
        .event_queue
        .run(state.workers.clone(), move || {
            Ok((store.health().map_err(storage_error)?, Default::default()))
        })
        .await?;
    no_cache(health, stages)
}
