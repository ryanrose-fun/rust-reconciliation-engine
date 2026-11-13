FROM rust:1.99.1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked --bin api --bin reconcile-cli --bin maintenance
FROM debian:bookworm-slim
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home app && mkdir -p /var/lib/reconcile && chown 10001:10001 /var/lib/reconcile
COPY --from=build /app/target/release/api /usr/local/bin/reconcile-api
COPY --from=build /app/target/release/reconcile-cli /usr/local/bin/reconcile-cli
COPY --from=build /app/target/release/maintenance /usr/local/bin/reconcile-maintenance
USER 10001:10001
ENV RECONCILE_BIND=0.0.0.0:3000
ENV RECONCILE_DB_PATH=/var/lib/reconcile/events.sqlite3
EXPOSE 3000
ENTRYPOINT ["reconcile-api"]