# Multi-stage Dockerfile for InferenceOS Gateway
FROM rust:1.80-slim AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
# Pre-build dummy source for layer caching
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src

# Copy real source code
COPY src ./src
RUN cargo build --release

# Minimal runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/inference-os /usr/local/bin/inference-os

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=2s --retries=3 \
  CMD curl -f http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/inference-os"]
