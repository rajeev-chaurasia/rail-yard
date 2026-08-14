CREATE TABLE workers (
    worker_id TEXT PRIMARY KEY,
    capacity_slots INTEGER NOT NULL CHECK (capacity_slots > 0),
    registered_at INTEGER NOT NULL,
    last_heartbeat_at INTEGER NOT NULL
);

CREATE INDEX workers_heartbeat_idx
    ON workers (last_heartbeat_at, worker_id);
