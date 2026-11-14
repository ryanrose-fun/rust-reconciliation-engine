use super::*;
use crate::gpu_service::{Client, Error, Submit};
use axum::extract::{Path, rejection::JsonRejection};

pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/gpu/events", post(ingest))
        .route("/v1/gpu/payments/{payment_id}", get(payment))
        .route("/v1/gpu/health", get(health))
        .route("/v1/gpu/checkpoint", post(checkpoint))
}
#[utoipa::path(post,path="/v1/gpu/checkpoint",security(("bearerAuth"=[])),responses((status=200,description="GPU checkpoint atomically committed",body=serde_json::Value),(status=503,description="Checkpoint failed; restart required")))]
async fn checkpoint(State(state): State<AppState>) -> Result<Response, ApiError> {
    Ok(response(
        client(&state)?.checkpoint().await.map_err(map_error)?,
    ))
}
fn client(state: &AppState) -> Result<&Arc<Client>, ApiError> {
    state.config.gpu.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "gpu_disabled",
            "GPU backend is not configured",
        )
    })
}
fn map_error(error: Error) -> ApiError {
    match error {
        Error::Invalid(message) => ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_gpu_event",
            message,
        ),
        Error::Conflict => ApiError::new(
            StatusCode::CONFLICT,
            "idempotency_conflict",
            "Request or event ID was reused with different content",
        ),
        Error::Busy => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "gpu_busy",
            "GPU queue is full; retry the same request ID",
        ),
        Error::Unavailable => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "gpu_unavailable",
            "GPU worker unavailable; retry the same request ID after recovery",
        ),
        Error::NotFound => ApiError::new(
            StatusCode::NOT_FOUND,
            "payment_not_found",
            "Payment not found in GPU state",
        ),
    }
}
fn status(code: u64) -> &'static str {
    match code {
        0 => "Absent",
        1 => "Matched",
        3 => "AmountMismatch",
        5 => "CurrencyMismatch",
        7 => "AmountAndCurrencyMismatch",
        9 => "StatusMismatch",
        11 => "AmountAndStatusMismatch",
        13 => "StatusAndCurrencyMismatch",
        15 => "AmountStatusAndCurrencyMismatch",
        16 => "MissingFromProvider",
        32 => "MissingFromInternal",
        _ => "Unknown",
    }
}
fn response(mut value: serde_json::Value) -> Response {
    if let Some(code) = value["result"].as_u64() {
        value["status"] = json!(status(code));
    }
    if let Some(rows) = value
        .get_mut("gpu")
        .and_then(|v| v.get_mut("receipts"))
        .and_then(serde_json::Value::as_array_mut)
    {
        for row in rows {
            row["status"] = json!(status(row["result"].as_u64().unwrap_or(0)));
        }
    }
    for source in ["internal", "provider"] {
        if let Some(state) = value.get_mut(source).filter(|s| s.is_object()) {
            if let Some(currency) = state["currency"].as_u64() {
                state["currency"] = json!(match currency {
                    0 => "GBP",
                    1 => "USD",
                    2 => "EUR",
                    3 => "JPY",
                    _ => "Unknown",
                });
            }
            if let Some(status) = state["status"].as_u64() {
                state["status"] = json!(match status {
                    0 => "Pending",
                    1 => "Completed",
                    2 => "Failed",
                    _ => "Unknown",
                });
            }
        }
    }
    ([("cache-control", "no-store")], Json(value)).into_response()
}
#[utoipa::path(post,path="/v1/gpu/events",security(("bearerAuth"=[])),request_body=Submit,
responses((status=200,description="GPU result durably committed; receipts use outcome codes 0 absent, 1 applied, 2 exact retry, 3 version conflict, 4 stale",body=serde_json::Value),(status=409,description="Idempotency conflict"),(status=422,description="Invalid batch"),(status=503,description="Unavailable or bounded queue full")))]
async fn ingest(
    State(state): State<AppState>,
    input: Result<Json<Submit>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(request) =
        input.map_err(|e| ApiError::new(e.status(), "invalid_gpu_json", e.body_text()))?;
    Ok(response(
        client(&state)?.submit(request).await.map_err(map_error)?,
    ))
}
#[utoipa::path(get,path="/v1/gpu/payments/{payment_id}",security(("bearerAuth"=[])),params(("payment_id"=String,Path)),responses((status=200,description="Current state read from GPU",body=serde_json::Value),(status=404,description="Unknown payment")))]
async fn payment(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    if id.len() > 256 {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_payment_id",
            "Payment ID too long",
        ));
    }
    Ok(response(client(&state)?.get(id).await.map_err(map_error)?))
}
#[utoipa::path(get,path="/v1/gpu/health",security(("bearerAuth"=[])),responses((status=200,description="GPU initialized and committed journal replay verified",body=serde_json::Value),(status=503,description="GPU unavailable")))]
async fn health(State(state): State<AppState>) -> Result<Response, ApiError> {
    Ok(response(client(&state)?.health().await.map_err(map_error)?))
}
#[derive(utoipa::OpenApi)]
#[openapi(
    paths(ingest, payment, health, checkpoint),
    components(schemas(Submit, crate::gpu_service::GpuEvent))
)]
pub(super) struct GpuDoc;
