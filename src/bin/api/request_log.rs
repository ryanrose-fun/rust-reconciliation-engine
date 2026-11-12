//! Operational request logs must never block an async request worker.
//! Durable payment evidence remains in SQLite, independently of these logs.
use std::{
    io::Write,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicUsize, Ordering},
        mpsc::{SyncSender, sync_channel},
    },
};

struct Logger {
    sender: SyncSender<String>,
    dropped: Arc<AtomicUsize>,
}
impl Logger {
    fn start(
        mut writer: impl Write + Send + 'static,
        capacity: usize,
    ) -> (Self, std::thread::JoinHandle<()>) {
        let (sender, receiver) = sync_channel::<String>(capacity);
        let dropped = Arc::new(AtomicUsize::new(0));
        let counter = dropped.clone();
        let thread = std::thread::Builder::new()
            .name("request-log-writer".into())
            .spawn(move || {
                let mut reported = 0;
                while let Ok(line) = receiver.recv() {
                    if writeln!(writer, "{line}").is_err() {
                        break;
                    }
                    let total = counter.load(Ordering::Relaxed);
                    if total > reported {
                        if writeln!(
                            writer,
                            "{{\"event\":\"request_logs_dropped\",\"count\":{}}}",
                            total - reported
                        )
                        .is_err()
                        {
                            break;
                        }
                        reported = total;
                    }
                }
            })
            .expect("Could not start request log writer");
        (Self { sender, dropped }, thread)
    }
    fn emit(&self, line: String) {
        if self.sender.try_send(line).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
pub(super) fn emit(line: String) {
    static LOGGER: OnceLock<Logger> = OnceLock::new();
    LOGGER
        .get_or_init(|| Logger::start(std::io::stderr(), 1024).0)
        .emit(line);
}

#[cfg(test)]
mod tests {
    use super::*;
    struct SlowWriter {
        started: Option<std::sync::mpsc::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl Write for SlowWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Some(started) = self.started.take() {
                started.send(()).unwrap();
                self.release
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn slow_sink_drops_overflow_without_blocking_producers() {
        let (started, ready) = std::sync::mpsc::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let (logger, thread) = Logger::start(
            SlowWriter {
                started: Some(started),
                release: wait,
            },
            2,
        );
        logger.emit("first".into());
        ready
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        logger.emit("queued-1".into());
        logger.emit("queued-2".into());
        logger.emit("overflow".into());
        assert_eq!(logger.dropped.load(Ordering::Relaxed), 1);
        release.send(()).unwrap();
        drop(logger);
        thread.join().unwrap();
    }
}
