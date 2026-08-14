# Rail Yard Design

## Scope

Rail Yard is a single-control-plane job orchestrator for one host. It is built
to make scheduling and crash behavior inspectable, not to imitate a globally
available production service.

The server is the only process that opens SQLite. Workers use HTTP and can be
added or killed independently. Redis carries external trigger notifications but
is not authoritative job storage.

## Guarantees

For every durably accepted logical job, Rail Yard records exactly one canonical
terminal outcome. Submission, Redis delivery, cron occurrence, and completion
requests are idempotent. Lease ownership is fenced by attempt, generation,
worker, and token rather than by replaying an acquisition request.

Payload execution is at least once. A worker can finish an external side effect
and die before its completion transaction. The successor lease can then repeat
that payload. Rail Yard passes an execution idempotency key to payloads but
cannot make an arbitrary command idempotent.

The durability threat model covers process and container crashes on a healthy
local filesystem. It does not claim survival from disk loss, corrupt hardware,
or a filesystem that lies about `fsync`.

## Components

### Server

The server contains:

- an HTTP control API for submissions and inspection;
- an HTTP worker API for lease acquisition, heartbeats, and completion;
- a priority-aware serialized write gate for durable state changes;
- a DAG readiness evaluator and fair scheduler;
- a lease reaper;
- cron and Redis Stream trigger pollers;
- an immutable decision log; and
- Prometheus instrumentation.

The HTTP listeners may serve concurrently, but all state-changing commands pass
through the same store transaction boundary. Long polling never holds a
database transaction open.

### Worker

A worker advertises a fixed slot capacity and asks for a bounded lease batch.
Each running attempt consumes its job's slot cost. The worker heartbeats active
leases once per second and reports a content digest with each completion.

No-op execution waits for an optional duration. Command execution accepts an
argv array, never an interpolated shell string. It is disabled unless
`--allow-shell` is set, receives `RAILYARD_IDEMPOTENCY_KEY`, has bounded output,
and is canceled when the worker shuts down or confirms lease loss.

### Replay

Replay reads canonical JSON Lines records, reconstructs each scheduler input,
runs the production scheduling function, and compares canonical decision bytes.
It exits on the first mismatch and reports the record sequence and byte offset.

## Domain model

### Job states

- `PENDING`: accepted but blocked by dependencies or release time.
- `SCHEDULED`: ready and assigned to a worker under a lease.
- `RUNNING`: the worker acknowledged that payload execution started.
- `RETRYING`: an attempt failed and the next release time is in the future.
- `SUCCEEDED`: one canonical successful terminal outcome.
- `FAILED`: one canonical non-retryable terminal failure.
- `DEAD_LETTER`: retry budget exhausted or an upstream dependency failed.

`SUCCEEDED`, `FAILED`, and `DEAD_LETTER` are terminal. Terminal jobs never move
back to active states. Redrive creates a new job and records that job ID on the
source `dead_letters` row.

### Identifiers and fencing

- `job_id`: random 128-bit lowercase hexadecimal identifier.
- `submission_key`: client idempotency key scoped to a tenant.
- `attempt_no`: one-based, strictly increasing per job.
- `lease_generation`: one-based fencing number, strictly increasing per job.
- `lease_token`: random secret returned only to the current worker.
- `state_version`: optimistic-concurrency version incremented on every state
  mutation.
- `event_seq`: global commit order allocated inside the write transaction.
- `ready_seq`: durable FIFO order allocated when a job becomes schedulable.

Completion requires job ID, attempt number, lease generation, lease token,
worker ID, outcome, and output digest. An identical duplicate returns the
original receipt. A conflicting or stale completion returns `409`.

## SQLite

The canonical database uses:

```text
journal_mode=WAL
synchronous=FULL
foreign_keys=ON
busy_timeout=5000
```

The server validates these settings at startup. The write pool is limited to
one connection. Read transactions are short, and passive checkpoints prevent
unbounded WAL growth. Docker Compose stores the database in a named local
volume.

### Tables

`jobs`
: Logical job definition, current materialized state, priority, slot cost,
  retry policy, release time, state version, lease generation, and terminal
  summary.

`job_dependencies`
: Immutable parent-child edges. The primary key is `(job_id, depends_on_id)`.

`attempts`
: Worker, lease generation/token hash, lease timestamps, attempt state, failure
  context, and completion digest. The primary key is `(job_id, attempt_no)`.

`job_completions`
: Canonical terminal outcome. `job_id` is the primary key.

`tenant_limits`
: Queue depth and slot limits.

`queue_state`
: Per-tenant queue fairness weight, deficit, active slots, and update time.

`idempotency_requests`
: `(tenant_id, submission_key)`, request digest, job ID, and stable response.

`cron_triggers` and `cron_occurrences`
: Parsed schedule metadata, durable next fire time, and unique nominal UTC
  occurrence.

`redis_deliveries`
: Unique `(trigger_id, stream, message_id)` delivery records. Stream and
  consumer-group configuration comes from process configuration, not a
  `redis_triggers` table.

`dead_letters`
: Terminal failure context and optional redrive linkage.

`decision_log`
: Global sequence, one canonical record JSON value, and its SHA-256 chain.

`operation_requests` and `audit_events`
: Tenant and action scoped idempotency receipts plus actor-attributed audit
  events for the operations and dashboard mutation surfaces.

`dag_runs` and `dag_jobs`
: Durable operations-facade DAG identity and node-to-job mappings.

`workers`
: Durable worker capacity registration and latest heartbeat time.

`counters`
: Durable ready sequence, scheduler sequence, and scheduler cursor values.

`schema_migrations`
: Monotonic migration version and checksum.

### Migration order

Embedded migrations are read in filename order and applied in one transaction.
The current forward-only sequence is:

1. `001_initial.sql`, core jobs, attempts, completions, scheduling, triggers,
   decision log, and counters;
2. `002_operations.sql`, operation receipts, audit events, and DAG run mapping;
3. `003_operation_scope.sql`, tenant and action scoped operation idempotency,
   with existing rows backfilled from their targets or assigned to `default`
   when no target tenant can be resolved; and
4. `004_workers.sql`, durable worker registrations.

Startup stores each migration's name and SHA-256 checksum in
`schema_migrations`. A changed checksum for an applied version prevents startup.
There is no downgrade path, and compatibility with a newer database is not
claimed.

### Transaction boundaries

Submission atomically deduplicates the key, enforces tenant depth, inserts the
job and edges, computes initial readiness, and records its event.

Lease grant atomically verifies readiness and capacity, increments the
generation, inserts the attempt, reserves slots, moves the job to `SCHEDULED`,
and appends the decision.

Completion atomically verifies the fencing tuple, closes the attempt, releases
slots, and either writes the unique terminal row or moves the job to
`RETRYING`. A successful parent activates newly unblocked children in the same
transaction.

Reaping atomically expires only the current generation, closes that attempt,
releases slots, and requeues or dead-letters the job.

Redis ingestion atomically deduplicates a delivery and creates its graph before
the poller acknowledges the stream entry. A crash before acknowledgement leaves
the entry pending, and normal pending-entry recovery retries it against the
durable delivery key.

## Scheduling

Scheduling is deterministic for a recorded input.

1. Build a snapshot containing logical time, available worker slots, all ready
   jobs, tenant/queue capacities, and queue deficits.
2. Visit non-empty tenant queues through equal-weight deficit round robin.
3. Add each queue's quantum to its deficit.
4. Within a selected queue, sort by priority descending, `ready_seq` ascending,
   then job ID ascending.
5. Grant the queue head only when its slot cost fits both the deficit and worker
   capacity. Charge its slot cost to the deficit.
6. Stop at the lease batch limit or when no candidate can fit.

Priority is strict only inside a queue. Fairness across queues takes precedence
over global priority. The queue head is not bypassed for a smaller later job,
which preserves FIFO among equal priorities.

Jobs whose slot cost exceeds cluster maximum are rejected. Tenant admission
depth includes all nonterminal jobs and returns HTTP `429` with stable error
code `queue_full`.

## DAG behavior

A workflow submission is one transaction. Kahn's algorithm rejects cycles
before persistence. Edges are immutable after acceptance.

A child becomes ready only when every parent is `SUCCEEDED`. If any parent
finishes `FAILED` or `DEAD_LETTER`, the child receives a `DEAD_LETTER` outcome
with reason `upstream_failed`; this propagation is bounded in transaction-sized
batches for large fan-out graphs.

## Leases and recovery

Workers heartbeat every second. A lease expires 2.5 seconds after its latest
successful heartbeat. The reaper scans every 250 milliseconds and also runs
immediately at server startup.

Workers with more than one slot reserve 25% of capacity for successor leases.
Normal work cannot consume that reserve, while recovery work can use every
available slot. Reaping and recovery dispatch also receive priority over
admission writes.

Recovery time is the interval from the chaos controller's confirmed worker
`SIGKILL` timestamp to the commit timestamp of the successor lease. The p99 is
nearest-rank over every affected job across the ten qualification runs.

## Retries and dead letters

The default maximum is five attempts. Retry caps are 2, 4, 8, and 16 seconds.
Jitter is a deterministic value in `[80%, 100%]` of the cap, derived from
SHA-256 of job ID and attempt number. The chosen release time is persisted and
recorded.

Non-retryable failures enter `FAILED`. Exhausted retryable failures enter
`DEAD_LETTER` with attempt summaries, bounded stderr, exit code, and timestamps.

## Triggers

Cron accepts standard five-field expressions and optional `CRON_TZ`. The
application parses schedules with `robfig/cron/v3`, stores nominal fire times in
UTC, and coalesces missed occurrences into at most one job on restart.

Redis uses a consumer group with blocking reads. The poller recovers abandoned
pending entries, deduplicates using the stream entry ID, commits before `XACK`,
and exposes lag and pending-entry metrics. Stream retention before ingestion is
an operator responsibility.

## HTTP protocol

All JSON requests reject unknown fields and have body-size limits. Mutating
control requests require `Idempotency-Key`. Every mutation under
`/v1/operations` also requires `X-Rail-Yard-Actor`. The older `/v1` mutations
do not accept an actor, so operator workflows should use the operations facade
when actor attribution is required.

Control endpoints:

- `POST /v1/jobs`
- `POST /v1/workflows`
- `GET /v1/jobs/{job_id}`
- `GET /v1/dead-letters`
- `POST /v1/dead-letters/{job_id}/redrive`
- `POST /v1/triggers/cron`
- `GET /health/live`
- `GET /health/ready`
- `GET /metrics`

Operations endpoints:

- `POST /v1/operations/jobs`
- `POST /v1/operations/dags`
- `GET /v1/operations/jobs/{job_id}`
- `GET /v1/operations/jobs/{job_id}/history`
- `POST /v1/operations/jobs/{job_id}/cancel`
- `POST /v1/operations/jobs/{job_id}/force`
- `POST /v1/operations/dead-letters/{job_id}/redrive`
- `GET /v1/operations/tenants/{tenant_id}/queues`
- `GET /v1/operations/workers`
- `GET /v1/operations/dags/{dag_id}`
- `POST /v1/operations/operator-actions`
- `GET /v1/operations/audit-events`
- `GET /ops` (redirects to `/ops/`)
- `GET /ops/`
- `GET /ops/assets/app.css`
- `GET /ops/assets/app.js`
- `GET /ops/api/snapshot`
- `GET /ops/api/dead-letters`
- `GET /ops/api/runs/{run_id}`
- `POST /ops/api/actions`

Operations API mutations require an actor header and idempotency key. Dashboard
mutations require an actor in the request body and the dashboard CSRF cookie
and header. State history reports `system` for scheduler and worker transitions
and the supplied actor for operator transitions.

Worker endpoints:

- `POST /v1/workers/register`
- `POST /v1/workers/{worker_id}/leases/acquire`
- `POST /v1/workers/{worker_id}/heartbeats`
- `POST /v1/workers/{worker_id}/attempts/start`
- `POST /v1/workers/{worker_id}/attempts/start-batch`
- `POST /v1/workers/{worker_id}/attempts/complete`
- `POST /v1/workers/{worker_id}/attempts/complete-batch`

Error responses contain `code`, `message`, and optional retry metadata. Stable
codes include `invalid_request`, `idempotency_conflict`, `queue_full`,
`cycle_detected`, `stale_lease`, and `not_found`.

## Shutdown and readiness

On graceful shutdown the server stops admission, trigger polling, and lease
grants; drains in-flight write commands; checkpoints opportunistically; then
closes SQLite. Workers stop acquiring, cancel payloads, send a final heartbeat
when possible, and exit.

Liveness reports process health. Readiness requires successful migration,
validated SQLite settings, initial telemetry collection, and Redis connectivity
when Redis triggers are configured before the listener starts. Once listening,
readiness reflects whether graceful shutdown has begun. It does not perform a
fresh SQLite write or Redis probe for each request.
