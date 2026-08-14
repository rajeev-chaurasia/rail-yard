# Benchmark and Qualification Methodology

## Canonical environment

Qualification runs use the committed Docker Compose Linux topology on a single
host:

- one `railyard-server`;
- eight `railyard-worker` processes;
- one Redis process;
- one Prometheus process; and
- one Grafana process.

The run manifest records the Git commit and dirty state, binary and image
digests, Go version, Docker and Compose versions, kernel, CPU model/count,
memory, filesystem, cgroup limits, SQLite pragmas, worker concurrency, payload
size, configuration hash, timezone, and random seed.

SQLite uses a fresh Docker named volume for each scored run. Source code may be
stored in a synchronized directory, but the live WAL database may not.

## Throughput

The workload contains 50,000 independent no-op jobs with fixed serialized
payload size, one tenant, one queue, and slot cost one. Eight workers use the
same frozen capacity of 256 slots each for all throughput runs. Chaos runs use
the reference 16-slot worker configuration. Throughput runs use a 10-second
lease to absorb high-concurrency heartbeat latency; recovery qualification uses
the default 2.5-second lease.

Report these rates separately:

- durable admissions per minute;
- durable lease grants per minute; and
- successful terminal completions per minute.

The acceptance number is durable lease grants divided by the interval from the
first to the last scored grant. A run remains valid only when every accepted job
reconciles to `SUCCEEDED`, so dispatch cannot hide lost work.

Protocol:

1. start from a fresh volume and wait on health predicates;
2. run one unscored warm-up;
3. run three independent scored workloads;
4. drain and reconcile each workload before accepting its sample; and
5. report every sample and the median without discarding outliers.

The target is a three-run median of at least 10,000 durable no-op lease grants
per minute. Also report p50, p95, and p99 admission-to-lease,
lease-to-completion, and end-to-end latency.

Absolute performance is qualified on a documented dedicated host. Shared CI
runners execute only a smoke benchmark and do not create resume evidence.

## Chaos campaign

Each of ten runs uses a fresh volume, a recorded seed, and exactly 50,000
accepted no-op jobs.

While work remains, the controller:

- sends `SIGKILL` to a uniformly selected live worker at deterministic seeded
  intervals;
- performs at least twenty worker kills per run;
- restarts each worker with the same slot capacity;
- sends one `SIGKILL` to the server between 20% and 80% progress; and
- restarts the server against the same SQLite volume.

The controller records planned and observed timestamps, victim container,
active leases, restart, readiness, successor leases, and completions. No
graceful pre-kill hook is used.

After submission stops, the run waits on an explicit drain predicate. It then
stops writers, takes a consistent SQLite/WAL snapshot, and runs the independent
SQL reconciliation oracle from `docs/invariants.md`.

Random kills supplement deterministic failpoint tests at every commit boundary;
they do not replace them.

## Recovery

Dead-worker recovery is measured for every job leased by a worker at the
controller's confirmed kill timestamp:

```text
recovery = successor_lease_committed_at - worker_kill_confirmed_at
```

Use the nearest-rank p99 over all samples from the ten runs. Publish the full
sample set. The target is p99 below five seconds.

Server recovery is reported as the later of readiness restoration and the first
post-restart durable lease. Ten server kills are insufficient for a meaningful
p99, so this value is reported as a maximum rather than folded into the worker
recovery target.

## Deterministic replay

Use a captured stream containing at least 50,000 scheduler decisions. Start
three clean replay processes from the same input. Each writes canonical UTF-8
JSONL and a SHA-256 digest.

The test passes only when all three outputs are byte-identical to the captured
canonical decisions and all four digests match. Database, WAL, archive, and
human log bytes are not replay targets.

## Evidence layout

Every run writes a unique immutable directory:

```text
results/<kind>/<UTC timestamp>-<short commit>/
  manifest.json
  submitted.jsonl
  events.jsonl
  reconciliation.json
  recovery-samples.jsonl
  benchmark-samples.jsonl
  benchmark-summary.json
  replay-input.jsonl
  replay-output.jsonl
  replay.sha256
  metrics.prom
  logs/
  SHA256SUMS
```

Not every artifact applies to every run. Missing required artifacts invalidate
a scored result.

Random seeds, exact action traces, and failure output are always preserved.
Tests use predicate polling with deadlines, never fixed readiness sleeps. A
failed random test is rerun only for diagnosis; the original run remains
failed.

## Publishing

README and resume claims include:

- the measured value;
- no-op workload;
- one host;
- eight workers;
- the number of runs and aggregation; and
- a link to the raw evidence directory.

Targets are never substituted for measurements. A miss is published as a miss,
with the measured result and environment unchanged.
