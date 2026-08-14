# Rail Yard SLOs and Alerts

## Scope

Rail Yard has exactly two service level objectives. Both use a rolling
30-minute reporting window. The short window is intended for the local
reference deployment and gives operators actionable feedback during a run. It
is not qualification evidence.

Prometheus evaluates the rules every five seconds. Alerts also require a
sustained breach before they fire. The rule tests use a one-minute evaluation
interval only to keep the fixtures small and deterministic.

## SLO 1: ready-job start latency

At least 99% of ready jobs must start within five seconds in each rolling
30-minute window that contains at least 20 observations.

The Prometheus indicator is:

```promql
sum(increase(railyard_job_latency_seconds_bucket{stage="ready_to_lease",le="5"}[30m]))
/
sum(increase(railyard_job_latency_seconds_count{stage="ready_to_lease"}[30m]))
```

The `le="5"` bucket counts jobs granted a durable lease within five seconds.
The count series is the total observed population. The recording rules expose
the numerator, denominator, and ratio. The minimum population prevents an
alert based on a few jobs.

`RailYardReadyStartSLOBreach` is critical. It fires after the ratio remains
below 0.99 for five minutes with at least 20 observations. A sustained breach
means runnable work is waiting too long for capacity or the scheduler.

### Measurement limitations

- The scheduler records the durable ready timestamp and observes latency when
  the successor lease commits.
- A durable lease grant is used as the start event. The metric does not measure
  the worker's later transition to `RUNNING`.
- Missed scrapes and a Prometheus outage leave gaps. This dashboard is
  operational telemetry, not the raw qualification sample set described in
  [the benchmark methodology](benchmark-methodology.md).

### On-call actions

1. Confirm the target is up and the recorded population is nonzero:

   ```promql
   up{job="railyard-server"}
   ```

   ```promql
   railyard:slo:ready_start:observations30m
   ```

2. Compare pending and retrying work with lease grants:

   ```promql
   sum(railyard_queue_depth{state=~"pending|retrying"})
   ```

   ```promql
   sum(rate(railyard_lease_grants_total[5m]))
   ```

3. Check worker health, lease expirations, and SQLite transaction latency.
4. Pause or rate-limit producers if the queue is growing.
5. Follow [Admission overload](runbook.md#admission-overload) when queue-full
   rejections rise. Follow [Lease expiry storm](runbook.md#lease-expiry-storm)
   when expirations rise.
6. Preserve metrics and logs before changing worker capacity.

## SLO 2: dead-letter queue depth

The unredriven dead-letter queue depth must remain below 10 entries at every
evaluation in the rolling 30-minute reporting window. A depth of 10 or more is
a breach.

Rail Yard exports the current count of unredriven dead letters:

```promql
railyard_dlq_depth
```

`RailYardDLQDepthHigh` is warning severity. It fires after the gauge remains at
10 or more for 10 minutes.

### Measurement limitations

- The gauge is restored from durable storage at server startup.
- Dead-letter creation and redrive refresh the gauge after their durable
  transactions complete.
- A scrape gap can delay alert evaluation but does not change stored depth.

### On-call actions

1. Confirm the warning and creation reasons:

   ```promql
   railyard_dlq_depth
   ```

   ```promql
   sum by (reason) (increase(railyard_dead_letters_total[30m]))
   ```

2. Query `GET /v1/dead-letters`. Count entries with no `redriven_job_id` to
   confirm current depth. If 100 entries are returned, treat the result as a
   lower bound because the endpoint is capped.
3. Inspect failure classes and recent retry and lease-expiration rates.
4. Stop redriving repeated failures until the payload, dependency, or worker
   fault is understood.
5. Redrive only entries whose side effects are safe under the documented
   idempotency contract.
6. Capture incident evidence using
   [Incident and qualification evidence](runbook.md#incident-and-qualification-evidence).

## Validation

Run from the repository root:

```sh
docker run --rm \
  -v "$PWD/deploy/prometheus:/work:ro" \
  --entrypoint promtool \
  prom/prometheus:v3.5.0 check rules /work/alerts.yml

docker run --rm \
  -v "$PWD/deploy/prometheus:/work:ro" \
  --entrypoint promtool \
  prom/prometheus:v3.5.0 test rules /work/slo-tests.yml

docker run --rm \
  -v "$PWD/deploy/grafana/dashboards/railyard-overview.json:/dashboard.json:ro" \
  ghcr.io/jqlang/jq:1.7.1 empty /dashboard.json
```
