CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    priority INTEGER NOT NULL,
    slot_cost INTEGER NOT NULL CHECK (slot_cost > 0),
    payload_json TEXT NOT NULL,
    retry_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'PENDING',
            'SCHEDULED',
            'RUNNING',
            'RETRYING',
            'SUCCEEDED',
            'FAILED',
            'DEAD_LETTER'
        )
    ),
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK (attempt_no >= 0),
    state_version INTEGER NOT NULL DEFAULT 1 CHECK (state_version > 0),
    lease_generation INTEGER NOT NULL DEFAULT 0 CHECK (lease_generation >= 0),
    available_at INTEGER NOT NULL,
    ready_seq INTEGER NOT NULL DEFAULT 0 CHECK (ready_seq >= 0),
    recovery_pending INTEGER NOT NULL DEFAULT 0 CHECK (recovery_pending IN (0, 1)),
    execution_key TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    terminal_at INTEGER,
    failure_json TEXT
);

CREATE INDEX jobs_ready_idx
    ON jobs (state, recovery_pending DESC, priority DESC, ready_seq, id)
    WHERE state = 'PENDING' AND ready_seq > 0;

CREATE INDEX jobs_due_idx
    ON jobs (state, available_at, id)
    WHERE ready_seq = 0 AND state IN ('PENDING', 'RETRYING');

CREATE TABLE job_dependencies (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    depends_on_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    PRIMARY KEY (job_id, depends_on_id),
    CHECK (job_id <> depends_on_id)
);

CREATE INDEX job_dependencies_parent_idx
    ON job_dependencies (depends_on_id, job_id);

CREATE TABLE attempts (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    worker_id TEXT NOT NULL,
    lease_generation INTEGER NOT NULL CHECK (lease_generation > 0),
    token_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('LEASED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'EXPIRED')
    ),
    leased_at INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    failure_json TEXT,
    completion_request_digest TEXT,
    receipt_state TEXT,
    receipt_state_version INTEGER,
    receipt_committed_at INTEGER,
    PRIMARY KEY (job_id, attempt_no),
    UNIQUE (job_id, lease_generation)
);

CREATE INDEX attempts_expiry_idx
    ON attempts (expires_at, job_id)
    WHERE state IN ('LEASED', 'RUNNING');

CREATE TABLE job_completions (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK (state IN ('SUCCEEDED', 'FAILED', 'DEAD_LETTER')),
    state_version INTEGER NOT NULL,
    attempt_no INTEGER NOT NULL,
    output_digest TEXT NOT NULL,
    failure_json TEXT,
    committed_at INTEGER NOT NULL
);

CREATE TABLE idempotency_requests (
    tenant_id TEXT NOT NULL,
    submission_key TEXT NOT NULL,
    request_kind TEXT NOT NULL CHECK (request_kind IN ('job', 'workflow', 'cron')),
    request_digest TEXT NOT NULL,
    job_id TEXT REFERENCES jobs(id) ON DELETE RESTRICT,
    response_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, submission_key)
);

CREATE TABLE tenant_limits (
    tenant_id TEXT PRIMARY KEY,
    max_depth INTEGER NOT NULL DEFAULT 0 CHECK (max_depth >= 0),
    max_slots INTEGER NOT NULL DEFAULT 0 CHECK (max_slots >= 0),
    active_slots INTEGER NOT NULL DEFAULT 0 CHECK (active_slots >= 0)
);

CREATE TABLE queue_state (
    tenant_id TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    weight INTEGER NOT NULL DEFAULT 1 CHECK (weight > 0),
    deficit INTEGER NOT NULL DEFAULT 0,
    active_slots INTEGER NOT NULL DEFAULT 0 CHECK (active_slots >= 0),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, queue_name),
    FOREIGN KEY (tenant_id) REFERENCES tenant_limits(tenant_id) ON DELETE CASCADE
);

CREATE TABLE events (
    event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL,
    state TEXT NOT NULL,
    state_version INTEGER NOT NULL,
    occurred_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE INDEX events_job_idx ON events (job_id, event_seq);

CREATE TABLE decision_log (
    sequence INTEGER PRIMARY KEY,
    previous_hash TEXT NOT NULL,
    hash TEXT NOT NULL UNIQUE,
    record_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE dead_letters (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE RESTRICT,
    failure_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    redriven_job_id TEXT REFERENCES jobs(id) ON DELETE RESTRICT
);

CREATE TABLE cron_triggers (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    expression TEXT NOT NULL,
    job_spec_json TEXT NOT NULL,
    next_fire_at INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE cron_occurrences (
    trigger_id TEXT NOT NULL REFERENCES cron_triggers(id) ON DELETE CASCADE,
    nominal_at INTEGER NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (trigger_id, nominal_at)
);

CREATE TABLE redis_deliveries (
    trigger_id TEXT NOT NULL,
    stream TEXT NOT NULL,
    message_id TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (trigger_id, stream, message_id)
);

CREATE TABLE counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL CHECK (value >= 0)
);

INSERT INTO counters (name, value) VALUES ('ready_seq', 0);
INSERT INTO counters (name, value) VALUES ('scheduler_seq', 0);
INSERT INTO counters (name, value) VALUES ('scheduler_cursor', 0);
