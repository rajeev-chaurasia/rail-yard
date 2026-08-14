# Rail Yard P5 Operations Walkthrough

## Scope

This walkthrough is a qualification drill for a fresh disposable stack. It:

1. submits a three-node DAG and observes every node running in dependency order;
2. sends `SIGKILL` to a worker and observes a fenced successor attempt;
3. drives a one-attempt job into `DEAD_LETTER`;
4. lists and redrives that dead letter through the control API;
5. verifies durable actor and timestamp records for each operator action; and
6. observes live fire and recovery for `RailYardReadyStartSLOBreach` and
   `RailYardDLQDepthHigh`; and
7. pairs the lifecycle report with retained promtool fire-and-recovery
   evidence for both rules.

The recovery drill intentionally leaves the killed worker unavailable for nine
seconds. It is supposed to breach the five-second recovery target. Do not use
its timing as a qualification sample.

Run this only against a disposable local stack. The harness stops, starts, and
kills worker containers. It restores all workers before exit, including after
an acceptance failure.

## Required P5 production hooks

The acceptance client uses only the P5 operations HTTP surface, the operator
dashboard, Prometheus, and Docker Compose. It does not inspect SQLite. The
`internal/operations` and `internal/dashboard` handlers must be backed by
durable store adapters and mounted by `railyard-server`.

### Operator identity and audit

Every mutating operations request carrying an operator identity must accept:

```text
X-Rail-Yard-Actor: <non-empty actor>
Idempotency-Key: <stable request key>
```

The server must durably append an audit event in the same transaction as each
successful control mutation. Failed mutations must not produce successful
action records. Duplicate idempotency requests must return the original event
without appending another event.

The audit feed contract is:

```http
GET /v1/operations/audit-events?since=<RFC3339Nano>&actor=<actor>
```

```json
{
  "events": [
    {
      "id": "immutable-event-id",
      "tenant_id": "default",
      "action": "dead_letter.redrive",
      "actor": "p5-acceptance",
      "occurred_at": "2032-03-14T15:09:26.123456789Z",
      "target_type": "job",
      "target_id": "job-id",
      "details": {}
    }
  ]
}
```

Events must be returned in commit order. `occurred_at` must be a nonzero UTC
timestamp from the durable commit, not a value supplied by the client.

The required automatic action names are:

- `dag.submit` for `POST /v1/operations/dags`;
- `job.submit` for `POST /v1/operations/jobs`;
- `job.force.dead_letter` for
  `POST /v1/operations/jobs/{job_id}/force`; and
- `dead_letter.redrive` for
  `POST /v1/operations/dead-letters/{job_id}/redrive`.

External actions such as a container kill need a durable ingress endpoint:

```http
POST /v1/operations/operator-actions
X-Rail-Yard-Actor: p5-acceptance
Idempotency-Key: p5-unique-key
Content-Type: application/json

{
  "action": "worker.kill",
  "target_type": "worker",
  "target_id": "worker-1",
  "details": {
    "signal": "SIGKILL",
    "confirmed_at": "2032-03-14T15:09:26.123456789Z"
  }
}
```

The response is `201 Created`, or `200 OK` for an identical duplicate:

```json
{
  "event": {
    "id": "immutable-event-id",
    "tenant_id": "default",
    "action": "worker.kill",
    "actor": "p5-acceptance",
    "occurred_at": "2032-03-14T15:09:26.223456789Z",
    "target_type": "worker",
    "target_id": "worker-1",
    "details": {
      "signal": "SIGKILL",
      "confirmed_at": "2032-03-14T15:09:26.123456789Z"
    }
  }
}
```

The same endpoint records `alert.exercise.start` and
`alert.exercise.recover`. Forced dead-letter and redrive actions are recorded
by their existing operations handlers. The endpoint enforces non-empty bounded
action and target identifiers, at most 64 detail entries, a 1 MiB body limit,
an actor header, and an idempotency key. It does not enforce an action or target
type allowlist, authentication, or authorization. Deployments must provide the
authentication and authorization boundary before exposing operator controls.

The walkthrough uses this subset of the mounted P5 and dashboard routes:

```text
POST /v1/operations/dags
POST /v1/operations/jobs
GET  /v1/operations/jobs/{job_id}
POST /v1/operations/jobs/{job_id}/force
POST /v1/operations/dead-letters/{job_id}/redrive
GET  /v1/operations/workers
GET  /ops/api/dead-letters
GET  /ops/
```

The force request uses
`{"action":"dead_letter","reason":"P5 acceptance forced dead-letter drill"}`.
Its `ActionReceipt` must include the actor and a nonzero commit timestamp. The
dashboard dead-letter API must show the same job before redrive.

### SLO validation

Promtool validates both SLO alert state machines with deterministic breach and
recovery series:

```sh
promtool check rules deploy/prometheus/alerts.yml
promtool test rules deploy/prometheus/slo-tests.yml
```

Prometheus must load these alert names:

- `RailYardReadyStartSLOBreach`;
- `RailYardDLQDepthHigh`.

The walkthrough command runs both promtool checks before the live drill and
retains `promtool-check.log`, `promtool-test.log`, `slo-summary.json`,
`walkthrough.json`, and `SHA256SUMS` in the report directory.

The live drill stops the workers, creates 10 current unredriven dead letters,
and submits 20 jobs that remain ready for more than five seconds. It then
starts the workers and observes the ready-start alert after its five-minute
`for` duration. Recovery uses 2,400 bounded no-op observations in batches of
100, which raises the controlled population above the 99 percent threshold
without waiting for the 30-minute window to expire.

The 10 dead letters remain unredriven through the alert's ten-minute `for`
duration. After Prometheus reports the alert firing, the harness redrives all
10 entries, verifies the current depth is zero, and observes the alert become
inactive.

The walkthrough report retains actual observation times only. For compatibility
with the release audit schema, `recovery_alert_*` contains the ready-start SLO
times and `queue_alert_*` contains the DLQ depth times. A normal run takes about
12 to 18 minutes. The acceptance command uses a 35-minute bound.

## Start the disposable stack

Run from the repository root on the canonical Linux Docker host:

```sh
export RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
export COMPOSE_PROJECT_NAME="railyard-p5"

cp deploy/.env.example deploy/.env
chmod 600 deploy/.env
${EDITOR:-vi} deploy/.env

docker compose \
  --env-file deploy/.env \
  -f deploy/compose.yaml \
  -p "$COMPOSE_PROJECT_NAME" \
  up -d --build

deadline=$((SECONDS + 180))
until curl -fsS http://127.0.0.1:8080/health/ready >/dev/null; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    docker compose -f deploy/compose.yaml -p "$COMPOSE_PROJECT_NAME" ps
    exit 1
  fi
  sleep 1
done
```

Use a fresh Compose project or remove its volumes before the run. Existing jobs
can be leased by the synthetic DLQ worker and correctly cause the test to fail.

## Run the walkthrough harness

The harness writes a report only after every assertion passes:

```sh
go run ./test/p5/cmd/walkthrough \
  -repo-root . \
  -compose-file deploy/compose.yaml \
  -compose-project "$COMPOSE_PROJECT_NAME" \
  -run-id "$RUN_ID" \
  -actor p5-walkthrough \
  -slo-rule-evidence slo-summary.json \
  -output "results/_work/p5/$RUN_ID/walkthrough.json" \
  -timeout 35m
```

The DLQ redrive uses
`POST /v1/operations/dead-letters/{job_id}/redrive`. The operator dashboard at
`/ops/` exposes the same redrive operation. The harness verifies the dashboard
dead-letter read model and the operations API mutation without bypassing CSRF.

The independent Go acceptance test exercises the same black-box path:

```sh
RAILYARD_P5_ACCEPTANCE=1 \
RAILYARD_P5_RUN_ID="$RUN_ID" \
RAILYARD_P5_ACTOR=p5-acceptance \
RAILYARD_P5_COMPOSE_PROJECT="$COMPOSE_PROJECT_NAME" \
go test ./test/p5 \
  -run '^TestOperationsWalkthrough$' \
  -count=1 \
  -v \
  -timeout 40m
```

Do not run the harness and acceptance test concurrently. Both control the same
worker pool. If an audit route or recovery metric integration is absent, the
test fails with the missing endpoint, action, or alert name.

## Screenshot reservations and capture commands

Screenshots are optional evidence. These paths are reservations only. No
screenshot or successful result is committed by this walkthrough.

Install the pinned Chromium used by the capture commands:

```sh
npx --yes playwright@1.61.0 install chromium
mkdir -p "results/_work/p5/$RUN_ID/screenshots"
```

Start each alert capture function in a separate terminal before starting the
harness. It waits for a real firing state, captures it, waits for recovery, and
then captures the inactive rule page.

```sh
capture_alert() {
  alert_name="$1"
  prefix="$2"
  output_dir="results/_work/p5/$RUN_ID/screenshots"

  until curl -fsS http://127.0.0.1:9090/api/v1/alerts |
    jq -e --arg name "$alert_name" \
      '.data.alerts[] | select(.labels.alertname == $name and .state == "firing")' \
      >/dev/null; do
    sleep 5
  done

  npx --yes playwright@1.61.0 screenshot \
    --browser chromium \
    --viewport-size "1600,1000" \
    --wait-for-timeout 2000 \
    --full-page \
    "http://127.0.0.1:9090/alerts?search=$alert_name" \
    "$output_dir/${prefix}-firing.png"

  until ! curl -fsS http://127.0.0.1:9090/api/v1/alerts |
    jq -e --arg name "$alert_name" \
      '.data.alerts[] | select(.labels.alertname == $name)' \
      >/dev/null; do
    sleep 5
  done

  npx --yes playwright@1.61.0 screenshot \
    --browser chromium \
    --viewport-size "1600,1000" \
    --wait-for-timeout 2000 \
    --full-page \
    "http://127.0.0.1:9090/rules?search=$alert_name" \
    "$output_dir/${prefix}-recovered.png"
}

capture_alert RailYardReadyStartSLOBreach 02-ready-start-slo
```

In another terminal:

```sh
capture_alert RailYardDLQDepthHigh 03-dlq-depth
```

Capture the Rail Yard operator dashboard while the harness is active:

```sh
npx --yes playwright@1.61.0 screenshot \
  --browser chromium \
  --viewport-size "1600,1000" \
  --wait-for-timeout 3000 \
  --full-page \
  "http://127.0.0.1:8080/ops/" \
  "results/_work/p5/$RUN_ID/screenshots/01-operator-dashboard.png"
```

Reserved locations:

```text
results/_work/p5/<RUN_ID>/screenshots/
  01-operator-dashboard.png
  02-ready-start-slo-firing.png
  02-ready-start-slo-recovered.png
  03-dlq-depth-firing.png
  03-dlq-depth-recovered.png
```

Inspect each image before retaining it.

## Cleanup

```sh
docker compose \
  -f deploy/compose.yaml \
  -p "$COMPOSE_PROJECT_NAME" \
  down --volumes --remove-orphans
```

Removing volumes is appropriate here only because this procedure requires a
disposable stack.
