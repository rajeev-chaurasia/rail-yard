CREATE TABLE operation_requests_v3 (
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    committed_at INTEGER NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    target_state TEXT,
    target_version INTEGER,
    response_json TEXT NOT NULL,
    PRIMARY KEY (tenant_id, action, idempotency_key)
);

INSERT INTO operation_requests_v3 (
    tenant_id, idempotency_key, action, actor, reason, request_digest,
    committed_at, target_type, target_id, target_state, target_version,
    response_json
)
SELECT
    COALESCE(
        (SELECT tenant_id FROM jobs
         WHERE id = operation_requests.target_id
           AND operation_requests.target_type IN ('job', 'dead_letter')),
        (SELECT tenant_id FROM dag_runs
         WHERE id = operation_requests.target_id
           AND operation_requests.target_type = 'dag'),
        'default'
    ),
    idempotency_key, action, actor, reason, request_digest, committed_at,
    target_type, target_id, target_state, target_version, response_json
FROM operation_requests;

CREATE TABLE audit_events_v3 (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    details_json TEXT NOT NULL,
    UNIQUE (tenant_id, action, idempotency_key),
    FOREIGN KEY (tenant_id, action, idempotency_key)
        REFERENCES operation_requests_v3 (tenant_id, action, idempotency_key)
        ON DELETE RESTRICT
);

INSERT INTO audit_events_v3 (
    id, tenant_id, idempotency_key, action, actor, occurred_at,
    target_type, target_id, details_json
)
SELECT
    audit_events.id,
    operation_requests_v3.tenant_id,
    audit_events.idempotency_key,
    audit_events.action,
    audit_events.actor,
    audit_events.occurred_at,
    audit_events.target_type,
    audit_events.target_id,
    audit_events.details_json
FROM audit_events
JOIN operation_requests_v3
  ON operation_requests_v3.idempotency_key = audit_events.idempotency_key
 AND operation_requests_v3.action = audit_events.action;

DROP TABLE audit_events;

DROP TABLE operation_requests;

ALTER TABLE operation_requests_v3 RENAME TO operation_requests;

ALTER TABLE audit_events_v3 RENAME TO audit_events;

CREATE INDEX operation_requests_target_idx
    ON operation_requests (tenant_id, target_type, target_id, committed_at);

CREATE INDEX audit_events_actor_time_idx
    ON audit_events (actor, occurred_at, id);

CREATE INDEX audit_events_target_idx
    ON audit_events (tenant_id, target_type, target_id, occurred_at);
