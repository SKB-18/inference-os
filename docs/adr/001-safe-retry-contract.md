# ADR-001: Safe Retry & Irreversible Streaming Commitment

## Status
Accepted

## Context
When serving LLM responses via Server-Sent Events (SSE), requests can experience mid-stream network partitions, worker crashes, or socket drops. In standard REST APIs, idempotent POSTs might be retried. However, for streaming LLM generation, once the first token has reached the client, the generation context and output have been exposed to the user application. 

Attempting to fail over to another backend mid-stream leads to either:
1. Duplicate output (if restarted from scratch).
2. Fragmented or nonsensical completions (if concatenated).
3. Inconsistent tokenizer and state discrepancies between different model engines.

## Decision
InferenceOS enforces an **Irreversible Streaming Commitment** policy:
1. **Pre-Stream Retry**: If a worker fails, times out, or returns a 5xx error *before* any HTTP headers or the first SSE token chunk is flushed to the client, the gateway may safely retry the request on an alternate healthy worker up to `max_retries` (default: 1).
2. **Post-Stream Abort**: Once the first token is flushed to the downstream client, the request is marked `Committed`. If the upstream connection drops thereafter, the gateway will:
   - Immediately terminate the SSE stream with a structured error frame or close the transport.
   - **Never** route the request to another backend.
   - Bill/audit only the tokens successfully generated before the crash.
   - Record the failure reason in `route_decisions` and metrics.

## Consequences
- Guaranteed zero duplicated text or corrupted token sequences for clients.
- Clear, predictable error semantics for client applications.
