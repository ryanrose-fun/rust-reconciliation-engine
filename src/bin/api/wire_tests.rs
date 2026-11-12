use super::*;
use axum::body::Body;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    task::JoinHandle,
};

struct Server {
    addr: SocketAddr,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(state: AppState) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/v1/reconcile", post(|| async { "accepted" }))
        .layer(middleware::from_fn_with_state(state, guard));
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server { addr, task }
}
fn state() -> AppState {
    AppState {
        event_store: None,
        event_queue: Arc::new(EventQueue::new(32)),
        config: Config {
            requests_per_second: 10000,
            ..Config::default()
        },
        requests: Arc::new(Semaphore::new(1)),
        event_admission: Arc::new(Semaphore::new(2)),
        workers: Arc::new(Semaphore::new(1)),
        rejection_drains: Arc::new(Semaphore::new(2)),
        rate: Arc::new(Mutex::new((Instant::now(), 0))),
    }
}
async fn upload(addr: SocketAddr, chunked: bool) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let body = vec![b' '; 900_000];
    let framing = if chunked {
        "Transfer-Encoding: chunked".into()
    } else {
        format!("Content-Length: {}", body.len())
    };
    stream
        .write_all(
            format!(
                "POST /v1/reconcile HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nConnection: close\r\n{framing}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    for chunk in body.chunks(32_768) {
        if chunked {
            stream
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await
                .unwrap();
        }
        stream
            .write_all(chunk)
            .await
            .expect("Rejection must not reset an in-limit upload");
        if chunked {
            stream.write_all(b"\r\n").await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    if chunked {
        stream.write_all(b"0\r\n\r\n").await.unwrap();
    }
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("Read complete JSON rejection without reset");
    String::from_utf8(response).unwrap()
}
fn check(response: &str, status: u16, code: &str) {
    assert!(
        response.starts_with(&format!("HTTP/1.1 {status}")),
        "{response}"
    );
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(headers.to_lowercase().contains("retry-after: 1"));
    assert!(headers.to_lowercase().contains("x-request-id:"));
    let json: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(json["error"]["code"], code);
}
#[tokio::test]
async fn busy_large_uploads_receive_complete_json_for_both_framings() {
    let state = state();
    let permit = state.requests.clone().acquire_owned().await.unwrap();
    let server = server(state.clone()).await;
    for chunked in [false, true] {
        let response = tokio::time::timeout(Duration::from_secs(5), upload(server.addr, chunked))
            .await
            .unwrap();
        check(&response, 503, "busy");
        assert_eq!(state.rejection_drains.available_permits(), 2);
    }
    drop(permit);
}
#[tokio::test]
async fn rate_limited_large_uploads_receive_complete_json() {
    for chunked in [false, true] {
        let mut state = state();
        state.config.requests_per_second = 1;
        *state.rate.lock().unwrap() = (Instant::now(), 1);
        let server = server(state).await;
        let response = tokio::time::timeout(Duration::from_secs(5), upload(server.addr, chunked))
            .await
            .unwrap();
        check(&response, 429, "rate_limited");
    }
}
#[tokio::test]
async fn rejected_upload_size_and_drain_capacity_are_bounded() {
    let mut state = state();
    state.config.max_body = 16;
    let request = Request::builder().body(Body::from(vec![0; 17])).unwrap();
    let response = reject_upload(
        &state,
        request,
        ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "busy", "busy"),
    )
    .await;
    assert_eq!(response.headers()["connection"], "close");
    assert_eq!(state.rejection_drains.available_permits(), 2);
    let _all = state
        .rejection_drains
        .clone()
        .acquire_many_owned(2)
        .await
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_millis(100),
        reject_upload(
            &state,
            Request::builder().body(Body::empty()).unwrap(),
            ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "busy", "busy"),
        ),
    )
    .await
    .unwrap();
    assert_eq!(response.headers()["connection"], "close");
}
#[tokio::test]
async fn stalled_rejected_upload_releases_drain_permit_at_deadline() {
    let mut state = state();
    state.config.timeout = Duration::from_millis(30);
    let _busy = state.requests.clone().acquire_owned().await.unwrap();
    let server = server(state.clone()).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    stream
        .write_all(
            b"POST /v1/reconcile HTTP/1.1\r\nHost: localhost\r\nContent-Length: 900000\r\n\r\nx",
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    // A sender exceeding the drain deadline is intentionally disconnected.
    let _ = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .unwrap();
    assert_eq!(state.rejection_drains.available_permits(), 2);
}

async fn read_response(stream: &mut TcpStream) -> String {
    let mut buffer = Vec::new();
    loop {
        let mut bytes = [0; 4096];
        let count = stream.read(&mut bytes).await.unwrap();
        assert_ne!(count, 0, "Connection ended before response completed");
        buffer.extend_from_slice(&bytes[..count]);
        if let Some(end) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            let header = std::str::from_utf8(&buffer[..end]).unwrap();
            let length: usize = header
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .expect("Bounded response needs Content-Length");
            if buffer.len() >= end + 4 + length {
                return String::from_utf8(buffer).unwrap();
            }
        }
        assert!(buffer.len() < 16384);
    }
}

#[tokio::test]
async fn rejection_drains_body_and_preserves_http_keep_alive() {
    let state = state();
    let permit = state.requests.clone().acquire_owned().await.unwrap();
    let server = server(state).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    stream
        .write_all(
            b"POST /v1/reconcile HTTP/1.1\r\nHost: localhost\r\nContent-Length: 6\r\n\r\nabcdef",
        )
        .await
        .unwrap();
    let rejected = tokio::time::timeout(Duration::from_secs(2), read_response(&mut stream))
        .await
        .unwrap();
    check(&rejected, 503, "busy");
    assert!(!rejected.to_lowercase().contains("connection: close"));
    drop(permit);
    stream
        .write_all(b"POST /v1/reconcile HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
    let accepted = tokio::time::timeout(Duration::from_secs(2), read_response(&mut stream))
        .await
        .unwrap();
    assert!(accepted.starts_with("HTTP/1.1 200"));
    assert!(accepted.ends_with("accepted"));
}

#[tokio::test]
async fn direct_decoder_preserves_error_categories_and_timing_headers() {
    use tower::ServiceExt;
    for (bytes, status, code) in [
        (
            br#"{"internal_payments":[],"provider_payments":[]} trailing"#.to_vec(),
            400,
            "invalid_json",
        ),
        (vec![0xff], 400, "invalid_json"),
        (
            br#"{"internal_payments":[],"provider_payments":[],"unknown":1}"#.to_vec(),
            422,
            "invalid_payment_data",
        ),
        (
            br#"{"internal_payments":[],"internal_payments":[],"provider_payments":[]}"#.to_vec(),
            422,
            "invalid_payment_data",
        ),
        (
            br#"{"internal_payments":[],"provider_payments":{}}"#.to_vec(),
            422,
            "invalid_payment_data",
        ),
    ] {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/reconcile")
            .header("content-type", "application/json")
            .body(Body::from(bytes))
            .unwrap();
        let response = configured_router(Config::default())
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), status);
        let body = axum::body::to_bytes(response.into_body(), 16384)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], code);
    }
    let request = Request::builder()
        .method("POST")
        .uri("/v1/reconcile")
        .header("content-type", "application/problem+json; charset=utf-8")
        .body(Body::from(
            r#"{"internal_payments":[],"provider_payments":[]}"#,
        ))
        .unwrap();
    let response = configured_router(Config::default())
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let timings = response.headers()["server-timing"].to_str().unwrap();
    for name in ["body", "queue", "json_decode", "reconcile", "json_encode"] {
        assert!(timings.contains(&format!("{name};dur=")));
    }
}
