# Rail Yard Maintainer Handoff

## Current snapshot

Branch: `main`

Current source commit before this handoff: `347853c`

Remote:

```text
https://github.com/rajeev-chaurasia/rail-yard.git
```

The implementation and reduced portfolio qualification are complete.

Final evidence status:

```text
Evidence validity: valid
Combined qualification: measured miss
Benchmark median: 9,807.35 durable lease grants/min
Chaos correctness: zero lost, zero duplicate canonical outcomes
Recovery: 16.084s p99 across 282 samples
Replay: 50,000 decisions, 3 processes, 100% byte match
P5 lifecycle: passed
SLO rule tests: passed
```

No qualification stack should remain active after handoff.

## Implemented system

### Control plane

`cmd/railyard-server` provides:

- SQLite WAL persistence with `synchronous=FULL`
- sequence-ordered job events
- durable scheduling decisions and hash chaining
- DAG dependency release and upstream failure propagation
- priority-then-FIFO queue ordering
- deficit round-robin queue fairness
- tenant queue-depth admission limits
- fenced leases and monotonic lease generations
- one-second heartbeats
- 2.5-second default recovery lease
- priority reaping and reserved recovery capacity
- deterministic retries and DLQ insertion
- cron and Redis Streams triggers
- operator API, audit trail, and dashboard
- Prometheus metrics and health endpoints

### Worker

`cmd/railyard-worker` provides:

- no-op and argv command execution
- opt-in command jobs
- bounded stdout and stderr
- idempotency-key injection
- slot-aware concurrent execution
- batched attempt starts and completions
- batched heartbeats, including idle heartbeats
- automatic re-registration after control-plane restart
- graceful cancellation and shutdown

### Replay

`cmd/railyard-replay` consumes canonical decision JSONL and:

- verifies the SHA-256 decision chain
- reruns the production scheduler
- byte-compares canonical output
- stops at the first divergence

`test/replay` builds the real CLI and launches three distinct operating-system
processes for qualification.

### Operations

The operations surface is mounted under `/v1/operations/`.

The dashboard is mounted under `/ops/`.

Implemented capabilities:

- submit jobs and DAGs
- query status and transition history
- cancel and force state changes
- list tenant queue depth
- inspect durable worker health
- inspect DAG topology
- browse and redrive the DLQ
- record operator actions
- query actor-attributed audit events

All mutation routes, including the legacy `/v1` routes, require
`X-Rail-Yard-Actor`.

## Important correctness semantics

Rail Yard guarantees exactly one durable terminal ledger outcome per accepted
logical job.

Payload execution is at least once. A worker can complete an external side
effect and die before acknowledging completion. A successor attempt can run
the payload again. External side effects must honor the supplied idempotency
key.

Chaos reconciliation counts canonical terminal outcomes. Attempt repeats are
reported separately.

## Repository map

```text
cmd/
  railyard-server/
  railyard-worker/
  railyard-replay/
internal/
  admission/
  api/
  control/
  dag/
  dashboard/
  decisionlog/
  domain/
  evidence/
  executor/
  lease/
  operations/
  replay/
  retry/
  scheduler/
  server/
  store/sqlite/
  telemetry/
  trigger/
  worker/
test/
  benchmark/
  chaos/
  integration/
  p1/
  p5/
  reconcile/
  replay/
deploy/
  compose.yaml
  prometheus/
  grafana/
docs/
results/
```

## Database migrations

The migration order is:

```text
001_initial.sql
002_operations.sql
003_operation_scope.sql
004_workers.sql
```

Migration checksums are persisted and validated at startup. Never edit a
released migration after evidence has been produced. Add a new migration.

## Build and test environment

Pinned Go version:

```text
Go 1.26.6
```

On this Windows host, a portable toolchain is available at:

```text
%TEMP%\rail-yard-go-tar\go\bin
```

Normal setup:

```powershell
$env:GOROOT="$env:TEMP\rail-yard-go-tar\go"
$env:PATH="$env:GOROOT\bin;$env:PATH"
go version
docker info
```

## Quality gates

All of these passed on the integrated release tree before reduced
qualification:

```powershell
go run ./hack/ci format
go test ./...
go vet ./...
go run ./hack/ci lint
```

Linux race verification:

```powershell
docker run --rm `
  --mount "type=bind,source=$($PWD.Path),target=/src" `
  -w /src `
  golang:1.26.6 `
  go test -race ./...
```

Prometheus and dashboard validation:

```powershell
$rules="$($PWD.Path)\deploy\prometheus"

docker run --rm --entrypoint promtool `
  --mount "type=bind,source=$rules,target=/rules,readonly" `
  prom/prometheus:v3.5.0 `
  check rules /rules/alerts.yml

docker run --rm --entrypoint promtool `
  --mount "type=bind,source=$rules,target=/rules,readonly" `
  prom/prometheus:v3.5.0 `
  test rules /rules/slo-tests.yml

Get-Content -Raw deploy\grafana\dashboards\railyard-overview.json |
  ConvertFrom-Json |
  Out-Null
```

## Reduced qualification scope

The user explicitly reduced qualification to a one-hour portfolio scope.

### Benchmark

- one warm-up
- three measured runs
- 5,000 no-op jobs per run
- eight workers
- 256 slots per worker
- report every sample and the median

Command:

```powershell
go run ./test/benchmark `
  --compose-file=deploy/compose.yaml `
  --project-prefix=railyard-reduced-benchmark `
  --runs=3 `
  --jobs=5000 `
  --workers=8 `
  --worker-slots=256 `
  --submit-concurrency=8 `
  --poll-concurrency=128 `
  --request-timeout=30s `
  --host-port=0 `
  --output=results/_work/benchmark-<commit>
```

Resume:

```powershell
go run ./test/benchmark `
  --resume `
  --compose-file=deploy/compose.yaml `
  --project-prefix=railyard-reduced-benchmark `
  --runs=3 `
  --jobs=5000 `
  --workers=8 `
  --worker-slots=256 `
  --submit-concurrency=8 `
  --poll-concurrency=128 `
  --request-timeout=30s `
  --output=results/_work/benchmark-<commit>
```

### Chaos

- one seeded run
- 5,000 no-op jobs
- 20 worker SIGKILLs
- one server SIGKILL
- zero lost canonical outcomes
- zero duplicate canonical outcomes
- report recovery p99 and sample count

Use a 10-second no-op duration on Docker Desktop. The original 250ms duration
is shorter than Docker CLI kill latency and creates boundary ambiguity.

Command:

```powershell
$env:RAILYARD_HTTP_PORT="18080"

go run ./test/chaos `
  --compose-file=deploy/compose.yaml `
  --project-prefix=railyard-reduced-chaos `
  --server-url=http://127.0.0.1:18080 `
  --runs=1 `
  --jobs=5000 `
  --worker-kills=20 `
  --job-duration=10s `
  --action-min=100ms `
  --action-max=500ms `
  --seed=26081401 `
  --output=results/_work/chaos-<commit>
```

Resume uses the same flags plus `--resume`.

### Replay

```powershell
go run ./test/replay `
  --output="$env:TEMP\railyard-replay-<commit>"
```

Expected:

```text
50,000 decisions
3 separate processes
100% byte match
SHA-256 06ea0a236743ed4a9782879f9fff78242fd6e29da24ed78cc9c90e46a6376bdf
```

### P5 lifecycle and SLOs

The reduced scope requires:

- live DAG execution
- worker kill and successor attempt
- forced DLQ insertion
- redrive
- actor audit verification
- deterministic promtool fire and recovery cases

It does not require waiting through live Prometheus `for` windows.

Use `docs/operations-walkthrough.md`.

## Evidence already observed

### Replay

A three-process 50,000-decision replay passed with:

```text
SHA-256 06ea0a236743ed4a9782879f9fff78242fd6e29da24ed78cc9c90e46a6376bdf
```

Raw local evidence:

```text
%TEMP%\railyard-replay-final-9d66718
```

Replay must be regenerated after any scheduler or decision-log change.

### Benchmark

A valid reduced-scope benchmark suite completed on commit `347853c`.

Measured medians:

```text
Admissions: 9,816.136 jobs/min
Durable lease grants: 9,807.350 jobs/min
Successful completions: 9,823.744 jobs/min
```

The target was 10,000 jobs/min. This is a measured miss and must not be rounded
into compliance.

Local evidence:

```text
results/_work/benchmark-347853c
```

### Final chaos evidence

```text
5,000 accepted
5,000 canonical terminal outcomes
20 worker kills
1 server kill
0 lost
0 duplicate canonical outcomes
16.084s recovery p99
282 recovery samples
```

Correctness passed. The five-second recovery target was missed.

## Known local port conflicts

Other local projects can occupy common ports.

Use:

```text
Rail Yard API: 18080 or 18081
Prometheus: 19090
Grafana: 13000
```

The benchmark orchestrator can choose a free API port with `--host-port=0`.

## Optional follow-up work

1. Raise benchmark median above 10,000 durable lease grants/min.
2. Reduce recovery p99 below five seconds under the same chaos scope.
3. Rerun qualification with identical flags after performance changes.
4. Replace the measured-miss evidence only after the new set validates.

## Final evidence command

```powershell
go run ./hack/results `
  --benchmark-suite=<benchmark-summary.json> `
  --chaos-campaign=<chaos-summary.json> `
  --replay-summary=<replay-summary.json> `
  --slo-summary=<slo-summary.json> `
  --p5-walkthrough=<walkthrough.json> `
  --output=<new-qualification-summary.json>
```

The command is fail-closed. It emits activation text only when all evidence is
valid. A target miss remains a measured miss.

## Git and release notes

Recent implementation commits:

```text
347853c Align benchmark evidence with reduced scope
adedd84 Attribute internal harness mutations
7506618 Align reduced qualification and actor contracts
f81c8e6 Handle empty SLO histogram buckets
9d66718 Fix restart and evidence correctness gaps
ad4cfcf Add resumable qualification checkpoints
fb59b73 Stabilize high-concurrency benchmark leases
b861df3 Allow qualification on recorded Docker hosts
14f4f71 Build durable orchestration and operations platform
23102d9 Document Rail Yard architecture and qualification targets
```

Do not publish target numbers as measured results.

Do not push runtime databases, WAL files, temporary qualification directories,
or `results/_work`.

## Handoff checklist

- confirm `git status --short` is clean before qualification
- confirm no stale `railyard-*` containers are running
- use unique Compose project names
- use fresh volumes for each scored run
- preserve failed evidence for diagnosis
- use `--resume` only with identical immutable flags
- run final tests and lint before evidence commit
- keep README claims aligned with committed evidence
