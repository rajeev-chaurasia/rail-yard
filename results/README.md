# Performance evidence

This directory holds measured evidence, not target values. Qualification output
is valid only after the benchmark runner drains the workload, an operator stops
all database writers, and the reconciliation command validates a consistent
SQLite snapshot.

## Command contract

The Compose orchestrator performs the warm-up, measured fresh-volume runs,
quiesced snapshots, reconciliation, and summary:

```sh
go run ./test/benchmark \
  --compose-file deploy/compose.yaml \
  --project-prefix railyard-benchmark \
  --runs 3 \
  --jobs 50000 \
  --workers 8 \
  --worker-slots 256 \
  --output results/throughput/<suite-directory>
```

Build the tool once on the qualification host:

```sh
go build -o ./bin/railyard-benchmark ./test/benchmark/cmd/railyard-benchmark
```

The integration layer owns deployment lifecycle. For each run it must:

1. create a fresh SQLite volume;
2. start one server and exactly eight workers with frozen slot counts;
3. preserve the same deployment configuration and no-op request body;
4. invoke `run`, which polls liveness and readiness before submission;
5. wait for `run` to complete its explicit per-job terminal drain predicate;
6. stop the server, workers, trigger writers, and any other SQLite writer;
7. checkpoint and copy the database plus any WAL content into one consistent
   snapshot outside the run artifact directory; and
8. invoke `reconcile --quiesced` against that snapshot.

Run one unscored warm-up and three measured workloads. Every invocation needs a
unique run ID and artifact directory.

```sh
./bin/railyard-benchmark run \
  -server-url http://127.0.0.1:8080 \
  -metrics-url http://127.0.0.1:8080/metrics \
  -output results/throughput/<warmup-directory> \
  -run-id <warmup-run-id> \
  -phase warmup \
  -jobs 50000 \
  -workers 8 \
  -worker-slots <frozen-slots-per-worker> \
  -submit-concurrency <bounded-client-concurrency> \
  -environment <environment.json> \
  -configuration-sha256 <frozen-configuration-sha256> \
  -qualification

./bin/railyard-benchmark reconcile \
  -run-dir results/throughput/<warmup-directory> \
  -db-snapshot <quiesced-snapshot.db> \
  -quiesced
```

Repeat those commands with `-phase measured` for measured run IDs 1, 2, and 3,
using a fresh volume each time. Then create the three-run aggregate:

```sh
./bin/railyard-benchmark summarize \
  -output results/throughput/<suite-summary-directory> \
  -run-dir results/throughput/<warmup-directory> \
  -run-dir results/throughput/<measured-directory-1> \
  -run-dir results/throughput/<measured-directory-2> \
  -run-dir results/throughput/<measured-directory-3>
```

The `-workers` value records the integration contract. The current control API
does not expose worker-pool membership, so the deployment layer must verify the
process count independently and record that evidence in the environment
manifest.

## Environment manifest

`-environment` accepts a JSON object with the fields below. Replace every
placeholder with observed data from the qualification host. Missing canonical
fields invalidate a run when `-qualification` is set.

```json
{
  "git_commit": "<full commit>",
  "git_dirty": null,
  "binary_digests": {
    "railyard-server": "<sha256>",
    "railyard-worker": "<sha256>"
  },
  "image_digests": {
    "server": "<digest>",
    "worker": "<digest>"
  },
  "go_version": "<version>",
  "docker_version": "<version>",
  "compose_version": "<version>",
  "hostname": "<host>",
  "os": "<host OS>",
  "architecture": "<architecture>",
  "kernel": "<host or container kernel when available>",
  "cpu_model": "<model>",
  "cpu_count": 0,
  "memory_bytes": 0,
  "filesystem": "<filesystem and mount>",
  "cgroup_limits": "<effective limits>",
  "sqlite_pragmas": {
    "journal_mode": "wal",
    "synchronous": "full",
    "foreign_keys": "on",
    "busy_timeout": "5000"
  },
  "timezone": "<IANA zone>",
  "operator_details": {
    "worker_count_evidence": "<process or container inspection record>"
  }
}
```

The runner fills locally observable Go, OS, architecture, CPU-count, hostname,
timezone, and build metadata when they are omitted. It lists every unavailable
field instead of substituting a value.

## Timing definitions

All scored lifecycle timestamps come from integer nanosecond commit timestamps
in the quiesced SQLite snapshot.

- Admissions per minute equals the number of durable job admissions divided by
  the interval from the first to the last `jobs.created_at`.
- Durable lease grants per minute equals the number of attempt rows divided by
  the interval from the first to the last `attempts.leased_at`.
- Successful completions per minute equals the number of successful canonical
  completions divided by the interval from the first to the last
  `job_completions.committed_at`.
- Admission-to-lease latency is first `attempts.leased_at` minus
  `jobs.created_at`.
- Lease-to-completion latency is `job_completions.committed_at` minus the
  completing attempt's `leased_at`.
- End-to-end latency is `job_completions.committed_at` minus
  `jobs.created_at`.

The suite reports each measured rate and the median of the three measured
rates. Latency p50, p95, and p99 use the nearest-rank definition over all raw
job samples from the three measured runs. No interpolation is used.

## Artifacts and validity

`submitted.jsonl` contains raw HTTP submission receipts and stable,
deterministically derived idempotency keys. `drain-samples.jsonl` contains the
terminal observations used by the explicit drain predicate.
`benchmark-samples.jsonl` contains the database-backed per-job timing samples.
`benchmark-summary.json` keeps admission, durable lease-grant, and completion
rates separate.

`reconciliation.json` records the independent oracle result, SQLite integrity
and foreign-key checks, event and attempt ordering, terminal counts, active
state, and residual slot reservations. Any mismatch marks the manifest and
summary invalid. A failed or incomplete run remains in place for diagnosis.

`SHA256SUMS` covers every regular artifact in its directory. Summary generation
verifies each run's checksums and refuses invalid, incomplete, or modified
evidence. The manifest also records separate SHA-256 digests for the database,
WAL, and shared-memory snapshot components that exist. Lease rates and
lease-based latency are unavailable until a valid SQLite snapshot supplies
exact durable timestamps.
