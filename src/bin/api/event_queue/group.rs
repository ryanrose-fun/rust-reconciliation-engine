use super::*;
use reconcile_cli::events::{EventReceipt, EventStore, PaymentEvent};
use std::collections::VecDeque;
use tokio::sync::{OwnedSemaphorePermit, oneshot};
struct Job {
    event: PaymentEvent,
    reply: oneshot::Sender<Result<(EventReceipt, EventStages), ApiError>>,
    queued: Instant,
    _slot: OwnedSemaphorePermit,
}
#[derive(Default)]
pub(super) struct Pending {
    jobs: VecDeque<Job>,
    pub(super) running: bool,
}
impl EventQueue {
    pub(in crate::routes) async fn ingest(
        self: &Arc<Self>,
        store: Arc<EventStore>,
        workers: Arc<Semaphore>,
        event: PaymentEvent,
    ) -> Result<(EventReceipt, EventStages), ApiError> {
        let slot = self.slots.clone().try_acquire_owned().map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "event_queue_full",
                "Event storage queue is full; retry the same event",
            )
        })?;
        let (reply, receive) = oneshot::channel();
        let start_worker = {
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            pending.jobs.push_back(Job {
                event,
                reply,
                queued: Instant::now(),
                _slot: slot,
            });
            let start = !pending.running;
            pending.running = true;
            start
        };
        if start_worker {
            let queue = self.clone();
            tokio::spawn(async move {
                queue.flush_groups(store, workers).await;
            });
        }
        receive.await.map_err(|_| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage_worker_failed",
                "Storage worker failed; retry the same event",
            )
        })?
    }
    async fn flush_groups(self: Arc<Self>, store: Arc<EventStore>, workers: Arc<Semaphore>) {
        loop {
            // Maximum deliberate accumulation delay; queue/service time can be longer.
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            let serial = self
                .writer
                .clone()
                .acquire_owned()
                .await
                .expect("writer never closed");
            let jobs: Vec<_> = {
                let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
                let count = pending.jobs.len().min(64);
                pending
                    .jobs
                    .drain(..count)
                    .filter(|j| !j.reply.is_closed())
                    .collect()
            };
            if !jobs.is_empty() {
                match workers.clone().try_acquire_owned() {
                    Err(_) => {
                        for job in jobs {
                            let _ = job.reply.send(Err(ApiError::new(
                                StatusCode::SERVICE_UNAVAILABLE,
                                "busy",
                                "Worker capacity exhausted; retry the same event",
                            )));
                        }
                    }
                    Ok(worker) => {
                        let batch_store = store.clone();
                        let dispatched = Instant::now();
                        // The blocking task owns all admission permits through commit and response delivery.
                        let outcome = tokio::task::spawn_blocking(move || {
                            let (_writer, _worker) = (serial, worker);
                            let dispatch_ms = milliseconds(dispatched);
                            let started = Instant::now();
                            let group_size = jobs.len();
                            let events = jobs.iter().map(|j| j.event.clone()).collect();
                            let result = batch_store.apply_group(events);
                            let service_ms = milliseconds(started);
                            match result {
                                Ok((results, storage)) => {
                                    for (job, result) in jobs.into_iter().zip(results) {
                                        let stages = EventStages {
                                            group_size,
                                            queue_ms: started
                                                .duration_since(job.queued)
                                                .as_secs_f64()
                                                * 1000.,
                                            dispatch_ms,
                                            service_ms,
                                            storage: storage.clone(),
                                            ..Default::default()
                                        };
                                        let _ = job.reply.send(
                                            result
                                                .map(|receipt| (receipt, stages))
                                                .map_err(super::super::event_routes::storage_error),
                                        );
                                    }
                                }
                                Err(error) => {
                                    let error = super::super::event_routes::storage_error(error);
                                    for job in jobs {
                                        let _ = job.reply.send(Err(ApiError::new(
                                            error.status,
                                            error.code,
                                            error.message.clone(),
                                        )));
                                    }
                                }
                            }
                        })
                        .await;
                        if outcome.is_err() {
                            eprintln!(
                                "Group commit worker failed; clients must retry stable event IDs"
                            );
                        }
                        // serial was moved into the task and released there.
                        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
                        if pending.jobs.is_empty() {
                            drop(store);
                            pending.running = false;
                            return;
                        }
                        continue;
                    }
                }
            }
            drop(serial);
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            if pending.jobs.is_empty() {
                drop(store);
                pending.running = false;
                return;
            }
        }
    }
}
