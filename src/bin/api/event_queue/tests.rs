use super::*;
use std::{future::Future, task::Poll};
async fn poll_waiting<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
fn success() -> Result<((), TransactionTimings), ApiError> {
    Ok(((), TransactionTimings::default()))
}

#[tokio::test]
async fn full_queue_rejects_and_cancelled_waiter_releases_capacity() {
    let q = EventQueue::new(1);
    let workers = Arc::new(Semaphore::new(2));
    let active = q.writer.clone().acquire_owned().await.unwrap();
    let slot = q.slots.clone().acquire_owned().await.unwrap();
    {
        let waiting = q.run(workers.clone(), success);
        tokio::pin!(waiting);
        poll_waiting(waiting.as_mut()).await;
        let error = q.run(workers.clone(), success).await.err().unwrap();
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.code, "event_queue_full");
    }
    assert_eq!(q.slots.available_permits(), 1);
    drop(active);
    drop(slot);
    assert!(q.run(workers, success).await.is_ok());
}

#[tokio::test]
async fn waiting_operations_execute_fifo() {
    let q = EventQueue::new(3);
    let workers = Arc::new(Semaphore::new(3));
    let active = q.writer.clone().acquire_owned().await.unwrap();
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let make = |n| {
        let order = order.clone();
        move || {
            order.lock().unwrap().push(n);
            success()
        }
    };
    let a = q.run(workers.clone(), make(1));
    let b = q.run(workers.clone(), make(2));
    let c = q.run(workers.clone(), make(3));
    tokio::pin!(a, b, c);
    poll_waiting(a.as_mut()).await;
    poll_waiting(b.as_mut()).await;
    poll_waiting(c.as_mut()).await;
    drop(active);
    let (a, b, c) = tokio::join!(a, b, c);
    assert!(a.is_ok() && b.is_ok() && c.is_ok());
    assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
}

struct Release(std::sync::mpsc::Sender<()>);
impl Drop for Release {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
#[tokio::test]
async fn cancelled_active_request_keeps_permits_until_transaction_finishes() {
    let q = Arc::new(EventQueue::new(1));
    let workers = Arc::new(Semaphore::new(2));
    let (release, wait) = std::sync::mpsc::channel();
    let release = Release(release);
    let (started, ready) = tokio::sync::oneshot::channel();
    let task = {
        let q = q.clone();
        let workers = workers.clone();
        tokio::spawn(async move {
            q.run(workers, move || {
                let _ = started.send(());
                wait.recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                success()
            })
            .await
        })
    };
    ready.await.unwrap();
    task.abort();
    let _ = task.await;
    assert_eq!(q.writer.available_permits(), 0);
    assert_eq!(q.slots.available_permits(), 1);
    assert_eq!(workers.available_permits(), 1);
    let second = q.run(workers.clone(), success);
    tokio::pin!(second);
    poll_waiting(second.as_mut()).await;
    assert_eq!(
        q.run(workers.clone(), success).await.err().unwrap().code,
        "event_queue_full"
    );
    drop(release);
    assert!(second.await.is_ok());
    q.wait_idle().await;
    assert_eq!(workers.available_permits(), 2);
    assert_eq!(q.slots.available_permits(), 2);
}

#[tokio::test]
async fn worker_panic_does_not_leak_queue_capacity() {
    let q = EventQueue::new(1);
    let workers = Arc::new(Semaphore::new(1));
    let result = q
        .run(
            workers.clone(),
            || -> Result<((), TransactionTimings), ApiError> { panic!("injected worker failure") },
        )
        .await;
    assert_eq!(result.err().unwrap().code, "internal_error");
    assert!(q.run(workers, success).await.is_ok());
}

#[test]
fn cancellation_before_blocking_dispatch_skips_storage_work() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (release, wait) = std::sync::mpsc::channel();
        let release = Release(release);
        let (started, ready) = tokio::sync::oneshot::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            wait.recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
        });
        ready.await.unwrap();
        let q = EventQueue::new(1);
        let workers = Arc::new(Semaphore::new(1));
        let called = Arc::new(AtomicBool::new(false));
        {
            let flag = called.clone();
            let pending = q.run(workers.clone(), move || {
                flag.store(true, Ordering::Release);
                success()
            });
            tokio::pin!(pending);
            poll_waiting(pending.as_mut()).await;
        }
        drop(release);
        blocker.await.unwrap();
        q.wait_idle().await;
        assert!(!called.load(Ordering::Acquire));
        assert_eq!(q.slots.available_permits(), 2);
        assert_eq!(workers.available_permits(), 1);
    });
}

#[tokio::test]
async fn full_queue_is_a_readable_http_error_with_retry_after() {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;
    let queue = Arc::new(EventQueue::new(1));
    let reserved = queue.slots.clone().acquire_many_owned(2).await.unwrap();
    let store = Arc::new(
        reconcile_cli::events::EventStore::in_memory(std::time::Duration::from_secs(30)).unwrap(),
    );
    let app = super::super::router_with_queue(
        super::super::Config::default(),
        store.clone(),
        queue.clone(),
    );
    let body = r#"{"event_id":"e","source":"Internal","version":1,"payment":{"id":"p","currency":"GBP","amount_minor":1,"status":"Completed"}}"#;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/events")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()["retry-after"], "1");
    let value: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16384).await.unwrap()).unwrap();
    assert_eq!(value["error"]["code"], "event_queue_full");
    assert!(matches!(
        store.status("p"),
        Err(reconcile_cli::events::EventError::NotFound)
    ));
    drop(reserved);
}

#[tokio::test(flavor = "current_thread")]
async fn completed_storage_work_wakes_an_idle_io_loop() {
    let queue = EventQueue::new(1);
    let workers = Arc::new(Semaphore::new(1));
    for _ in 0..64 {
        let (value, stages) = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            queue.run(workers.clone(), || {
                // Ensure the async request has to suspend before completion.
                std::thread::sleep(std::time::Duration::from_millis(1));
                Ok((42, TransactionTimings::default()))
            }),
        )
        .await
        .expect("Storage completion must wake the waiting request")
        .unwrap_or_else(|_| panic!("Storage operation failed"));
        assert_eq!(value, 42);
        assert!(
            stages.resume_ms < 1000.,
            "Completion resumed too late: {stages:?}"
        );
    }
}
