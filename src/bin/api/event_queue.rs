use super::{ApiError, StatusCode, milliseconds};
use reconcile_cli::events::TransactionTimings;
use serde::Serialize;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::sync::Semaphore;

#[derive(Clone, Debug, Default, Serialize)]
pub(super) struct EventStages {
    pub body_ms: f64,
    pub resume_ms: f64,
    pub queue_ms: f64,
    pub dispatch_ms: f64,
    pub service_ms: f64,
    pub storage: TransactionTimings,
    pub encode_ms: f64,
}

/// One active storage operation plus a bounded number of FIFO waiters.
/// Both reads and writes use this gate because they share one SQLite connection.
pub(crate) struct EventQueue {
    slots: Arc<Semaphore>,
    writer: Arc<Semaphore>,
}
struct Cancellation(Arc<AtomicBool>);
impl Drop for Cancellation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
impl EventQueue {
    pub(crate) fn new(waiting_capacity: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(waiting_capacity + 1)),
            writer: Arc::new(Semaphore::new(1)),
        }
    }
    pub(crate) async fn wait_idle(&self) {
        // Called after HTTP graceful shutdown, when no request can enqueue.
        let _permit = self.writer.clone().acquire_owned().await;
    }
    pub(super) async fn run<T: Send + 'static>(
        &self,
        workers: Arc<Semaphore>,
        work: impl FnOnce() -> Result<(T, TransactionTimings), ApiError> + Send + 'static,
    ) -> Result<(T, EventStages), ApiError> {
        let slot = self.slots.clone().try_acquire_owned().map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "event_queue_full",
                "Event storage queue is full; retry the same event",
            )
        })?;
        let queued = Instant::now();
        // Tokio semaphore acquisition is FIFO. Cancellation removes the waiter.
        let serial = self.writer.clone().acquire_owned().await.map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "storage_unavailable",
                "Event storage is unavailable",
            )
        })?;
        let queue_ms = milliseconds(queued);
        let worker = workers.try_acquire_owned().map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "busy",
                "Worker capacity exhausted; retry later",
            )
        })?;
        let cancellation = Cancellation(Arc::new(AtomicBool::new(false)));
        let cancelled = cancellation.0.clone();
        let dispatched = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            // Keep every permit until the blocking work actually ends, even if
            // the HTTP request times out. No hidden unbounded blocking queue.
            let (_slot, _serial, _worker) = (slot, serial, worker);
            if cancelled.load(Ordering::Acquire) {
                return Err(ApiError::new(
                    StatusCode::GATEWAY_TIMEOUT,
                    "timeout",
                    "Request cancelled before storage work started",
                ));
            }
            let dispatch_ms = milliseconds(dispatched);
            let started = Instant::now();
            let (value, storage) = work()?;
            Ok((
                value,
                EventStages {
                    body_ms: 0.,
                    resume_ms: 0.,
                    queue_ms,
                    dispatch_ms,
                    service_ms: milliseconds(started),
                    storage,
                    encode_ms: 0.,
                },
                Instant::now(),
            ))
        })
        .await
        .map_err(|_| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Storage worker failed",
            )
        })?;
        drop(cancellation);
        result.map(|(value, mut stages, completed)| {
            stages.resume_ms = milliseconds(completed);
            (value, stages)
        })
    }
}
#[cfg(test)]
#[path = "event_queue/tests.rs"]
mod tests;
