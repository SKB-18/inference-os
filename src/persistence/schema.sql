-- InferenceOS Relational Persistence Schema
-- Target: PostgreSQL 14+

CREATE TABLE IF NOT EXISTS backends (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    url TEXT NOT NULL,
    model_ids JSONB NOT NULL DEFAULT '["*"]'::jsonb,
    concurrency_limit INT NOT NULL DEFAULT 10,
    weight INT NOT NULL DEFAULT 1,
    state VARCHAR(32) NOT NULL DEFAULT 'healthy',
    average_ttft_ms DOUBLE PRECISION NOT NULL DEFAULT 50.0,
    total_served BIGINT NOT NULL DEFAULT 0,
    total_errors BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    api_key VARCHAR(128) UNIQUE NOT NULL,
    token_budget BIGINT NOT NULL DEFAULT 1000000,
    tokens_used BIGINT NOT NULL DEFAULT 0,
    max_inflight INT NOT NULL DEFAULT 20,
    allowed_models JSONB NOT NULL DEFAULT '["*"]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS requests (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    idempotency_key VARCHAR(128),
    selected_backend VARCHAR(64),
    model VARCHAR(128) NOT NULL,
    state VARCHAR(32) NOT NULL,
    latency_ms DOUBLE PRECISION,
    ttft_ms DOUBLE PRECISION,
    tokens_estimated INT NOT NULL,
    tokens_used INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS route_decisions (
    request_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    model VARCHAR(128) NOT NULL,
    policy VARCHAR(64) NOT NULL,
    selected_backend_id VARCHAR(64),
    selected_backend_url TEXT,
    candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    exclusion_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_requests_tenant ON requests(tenant_id);
CREATE INDEX IF NOT EXISTS idx_requests_idempotency ON requests(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_route_decisions_tenant ON route_decisions(tenant_id);
