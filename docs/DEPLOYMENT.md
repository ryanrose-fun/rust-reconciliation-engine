# Deployment and operations

Use the source build, Windows archive, or Docker image for a controlled pilot. The snapshot API is stateless; the event API persists state in SQLite. Operators must back up and retain that database. It uses one shared bearer token and has no tenant-specific authorization.

See [EVENTS.md](EVENTS.md) for event recovery, retry and backup semantics.

## Configuration

| Variable | Default | Allowed values |
| --- | --- | --- |
| RECONCILE_DB_PATH | state/reconciliation.sqlite3 | Writable local SQLite file path |
| RECONCILE_EVENT_QUEUE_CAPACITY | 32 | 1 through 1024 waiting storage operations, plus one active |
| RECONCILE_EVENT_WAIT_MS | 30000 | 1 through 86400000; new payments only |
| RECONCILE_BIND | 127.0.0.1:3000 | Numeric IP and port; non-loopback requires a token |
| RECONCILE_API_KEY | unset | At least 32 printable non-space ASCII characters |
| RECONCILE_MAX_BODY_BYTES | 1048576 | 1 through 16777216 |
| RECONCILE_MAX_PAYMENTS | 5000 | 1 through 100000, per source |
| RECONCILE_CONCURRENCY | 8 | 1 through 64 |
| RECONCILE_TIMEOUT_MS | 10000 | 1 through 120000 |
| RECONCILE_REQUESTS_PER_SECOND | 100 | 1 through 10000, global fixed window |

Invalid configuration fails startup. Generate tokens using a cryptographically secure generator; do not choose memorable passwords or commit secrets. Inject them using an environment variable from your deployment secret store. Rotate by replacing the token and restarting; existing tokens immediately cease to work on the restarted process.

The server hashes expected and supplied credentials and compares fixed-size hashes in constant time. It does not log them. This is service authentication, not verification that transaction records originated from a bank.

## Containers and HTTPS

```powershell
# Set RECONCILE_API_KEY in this shell before starting.
docker compose -f deploy/compose.yaml up --build -d
```

The container runs as UID 10001, with a read-only root filesystem and a writable persistent named volume at /var/lib/reconcile, dropped capabilities, and CPU/memory/process limits. The host port is published on loopback only. The Dockerfile and CI build job are supplied, but Docker was not available on the development machine for local validation.

For remote clients, place a trusted HTTPS reverse proxy in front. `deploy/Caddyfile.example` shows the routing; replace its example domain, configure DNS/certificates, and restrict direct access to port 3000. Do not expose plain HTTP publicly. No hosted deployment or TLS endpoint has been provisioned.

## Limits and failure behavior

The request body is bounded before JSON extraction. Record counts are checked after parsing. Requests exceeding active capacity return 503; requests exceeding the global rate return 429. Use bounded retries with backoff for 429/503 and inspect timeouts rather than retrying indefinitely.

HTTP timeouts do not forcibly stop a Rust blocking worker. Its worker permit remains held until computation actually finishes, preventing timed-out work from creating an unbounded worker queue. Both request and worker counts are bounded. A front proxy should enforce connection/header/idle limits too: the application's controls do not replace network-level protection.

## Observability

Structured stderr events contain event type, random request ID, route template, status, and elapsed milliseconds. They exclude payment IDs, amounts, source labels, bodies, query strings, and authorization headers. Restrict and rotate logs in your hosting environment. `/health` reports liveness, not external dependency readiness. No external dependency is called by reconciliation.

Ctrl+C initiates graceful shutdown. Existing work is allowed to finish; choose a process-manager termination grace period that exceeds the request timeout and expected worker completion time. Monitor latency, errors, resource use, and saturation under your own workload before extending limits.

## Pilot acceptance

Verify authentication, HTTPS, access rules, token rotation, memory limits, report retention, and shutdown on the target environment. Run the included smoke tests with synthetic data before using company data. Dependency audit and container/hosted CI files are provided; they are not a substitute for executing those checks in the deployment environment.
## Request log backpressure

Operational request logs use a bounded 1,024-line background queue. Slow stderr or file capture cannot block an HTTP worker. If this queue fills, operational log lines are dropped; once the sink resumes, a request_logs_dropped record reports the overflow count. A broken sink or forced shutdown may prevent delivery of queued lines or the overflow notice. These logs are best-effort diagnostics, not a durable financial audit trail. Committed payment events remain in SQLite and are not dropped by this logging policy.
