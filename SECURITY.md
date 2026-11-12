# Security

This is a pilot reconciliation service, not a payment execution system. Do not submit security reports containing real payment data or credentials. Before public release, configure a private reporting contact or enable private vulnerability reporting on the repository.

Remote deployment requires a random bearer token and trusted HTTPS termination. Shared-token authentication is not multi-tenant isolation. Keep body and concurrency limits enabled, restrict direct service access, and use environment-specific secret injection and log retention. The API does not persist data or verify its origin cryptographically.

Run dependency audit in the supplied CI workflow and resolve findings before release. Rotate credentials after exposure. Review configuration and migration notes before upgrading.