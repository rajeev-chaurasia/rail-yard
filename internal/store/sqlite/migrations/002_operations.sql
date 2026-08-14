CREATE TABLE operation_requests (
    idempotency_key TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    committed_at INTEGER NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    target_state TEXT,
    target_version INTEGER,
    response_json TEXT NOT NULL
);

CREATE INDEX operation_requests_target_idx
    ON operation_requests (target_type, target_id, committed_at);

CREATE TABLE audit_events (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE
        REFERENCES operation_requests(idempotency_key) ON DELETE RESTRICT,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    details_json TEXT NOT NULL
);

CREATE INDEX audit_events_actor_time_idx
    ON audit_events (actor, occurred_at, id);

CREATE INDEX audit_events_target_idx
    ON audit_events (target_type, target_id, occurred_at);

CREATE TABLE dag_runs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_digest TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE dag_jobs (
    dag_id TEXT NOT NULL REFERENCES dag_runs(id) ON DELETE RESTRICT,
    job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE RESTRICT,
    node_key TEXT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (dag_id, job_id),
    UNIQUE (dag_id, node_key)
);

CREATE INDEX dag_jobs_run_idx ON dag_jobs (dag_id, node_key);
