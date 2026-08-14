# Performance evidence

This directory holds measured evidence, not target values. Qualification output
is valid only after the benchmark runner drains the workload, an operator stops
all database writers, and the reconciliation command validates a consistent
SQLite snapshot.

The final portfolio qualification is deliberately reduced to fit about one
hour on the documented host. It covers 5,000 jobs per benchmark workload and
one 5,000-job chaos run. It must not be described as a 50,000-job benchmark or
a ten-run, 50,000-job chaos campaign.

## Published evidence status

No committed throughput or chaos directory currently satisfies the
qualification contract. Content under `results/_work/` is scratch output and
must not be cited as a published result.

The committed SLO summary at `results/slo/20260814T104500Z/summary.json`
records deterministic promtool rule tests. It is not a complete P5 evidence
directory by itself. The committed P5 walkthrough at
`results/p5/20260814T124900Z/walkthrough.json` skipped live alert waits and
lacks co-located retained promtool logs and checksums. It is useful historical
lifecycle output, but it is not reduced portfolio qualification evidence.

## Replay qualification

`results/replay/20260814T102800Z/summary.json` is superseded. Its command used
in-process replay calls, so its `clean_process_replays` value is not
clean-process evidence and the summary must not be used in a release audit.

Create replacement evidence in a new directory:

```sh
go run ./test/replay \
  --output results/_work/replay-qualification-<run-id>
```

Supply `--input <decision-log.jsonl>` to consume an existing capture. The
qualifier verifies and canonicalizes exactly 50,000 chained decisions, or
creates that input when the flag is omitted. It builds the real
`railyard-replay` command, starts three separate operating system processes,
records each process ID and command, and byte-compares every output with the
captured canonical decisions and with the other outputs. It publishes the
evidence directory only after every comparison and reported digest passes.
`SHA256SUMS` is written atomically and verified before publication.

Use the generated `summary.json` as the release audit replay input. The
`manifest.json`, replay input, canonical decisions, three replay outputs, and
`SHA256SUMS` are required supporting evidence.

Replay qualification intentionally has no resume mode. The output directory
must not already exist. Start a replacement in a new directory after an
interruption.

## Chaos qualification

Run the resumable chaos tool directly because the bounded CI wrapper does not
expose the resume flag:

```sh
go run ./test/chaos \
  --resume \
  --compose-file deploy/compose.yaml \
  --project-prefix railyard-qualification-chaos \
  --runs 1 \
  --jobs 5000 \
  --worker-kills 20 \
  --job-duration 10s \
  --action-min 100ms \
  --action-max 500ms \
  --seed <recorded-base-seed> \
  --output results/_work/chaos-qualification-<run-id>
```

`--resume` is safe for a new chaos output directory. It reuses only completed
runs whose configuration hash, finalized manifest, checksums, submitted
records, reconciliation, action trace, recovery samples, and database snapshot
all validate. Invalid prior run directories are moved under `.invalid/`.

The output root contains `summary.json`. Each timestamped run directory
contains `manifest.json`, `submitted.jsonl`, `events.jsonl`,
`recovery-samples.jsonl`, `reconciliation.json`, the database snapshot,
Compose logs, and `SHA256SUMS`. The campaign summary alone is not sufficient
release evidence.

The release-audit reader requires manifest version 3, including host-to-server
clock mapping, and derives `recovery_ms` from confirmed mapped kill time to the
durable successor lease time. It requires exactly one run, 5,000 jobs, 20
worker kills, one server kill, zero lost or duplicate canonical terminal
outcomes, and a nonempty recovery sample set. The output reports both p99 and
sample count.

## Command contract

The Compose orchestrator performs the warm-up, measured fresh-volume runs,
quiesced snapshots, reconciliation, and summary:

```sh
go run ./test/benchmark \
  --compose-file deploy/compose.yaml \
  --project-prefix railyard-benchmark \
  --runs 3 \
  --jobs 5000 \
  --workers 8 \
  --worker-slots 256 \
  --output results/_work/benchmark-qualification-<run-id>
```

Resume an interrupted suite with the same immutable options and output
directory:

```sh
go run ./test/benchmark \
  --resume \
  --compose-file deploy/compose.yaml \
  --project-prefix railyard-benchmark \
  --runs 3 \
  --jobs 5000 \
  --workers 8 \
  --worker-slots 256 \
  --output results/_work/benchmark-qualification-<run-id>
```

Resume verifies the orchestration checkpoint and every completed run's
checksums, finalized manifest, reconciliation, summary, and sample count. It
reuses only valid runs with the requested configuration. An incomplete current
run is quarantined and restarted, and the suite summary is always regenerated.
Changed configuration or corrupted completed evidence stops the resume.
Partial timing data is never included in the summary.

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
  -jobs 5000 \
  -workers 8 \
  -worker-slots 256 \
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

## P5 qualification

The current walkthrough writes one self-contained evidence directory:

```sh
go run ./test/p5/cmd/walkthrough \
  --repo-root . \
  --compose-file deploy/compose.yaml \
  --compose-project <fresh-project> \
  --run-id <run-id> \
  --actor qualification \
  --slo-rule-evidence slo-summary.json \
  --output results/_work/p5/<run-id>/walkthrough.json \
  --timeout 25m
```

A passing directory contains `walkthrough.json`, `slo-summary.json`,
`promtool-check.log`, `promtool-test.log`, and `SHA256SUMS`. The walkthrough
report must prove the live three-node DAG, worker reassignment,
dead-letter/redrive, and actor audit lifecycle, and `passed` must be true. The
SLO summary must report exactly two alerts and two deterministic
fire-and-recovery cases. Live alert timestamps are supplemental and are not a
reduced portfolio qualifier. P5 has no resume mode and requires a fresh
disposable Compose project.

## Final release audit

Generate the immutable activation decision from the five checked summaries:

```sh
go run ./hack/results \
  --benchmark-suite <benchmark-output>/suite/benchmark-summary.json \
  --chaos-campaign <chaos-output>/summary.json \
  --replay-summary <replay-output>/summary.json \
  --slo-summary <p5-output>/slo-summary.json \
  --p5-walkthrough <p5-output>/walkthrough.json \
  --output results/_work/qualification-<run-id>.json
```

The output path must be new. The command verifies each input directory's
checksums and exits nonzero for invalid evidence or a measured miss. It rejects
old 50,000-job benchmark evidence, old ten-run chaos evidence, mixed workload
shapes, nonexact replay counts, and incomplete P5 rule evidence.
