# Rail Yard Operations Runbook

## Scope and topology

The reference deployment is one Rail Yard server, eight workers, Redis with
append-only persistence, Prometheus, and Grafana. SQLite is stored in the
`railyard-sqlite` Docker volume; it must not be placed in a synchronized host
directory. Redis transports triggers but is not a backup of canonical job
state.

Default local endpoints are:

- Rail Yard API and metrics: `http://127.0.0.1:8080`
- Prometheus: `http://127.0.0.1:9090`
- Grafana: `http://127.0.0.1:3000`

All published ports bind to loopback. Do not expose them on a shared host
without adding authentication and transport security.

Run commands from the repository root on the canonical Linux Docker host.
Create a shell helper once per terminal:

```sh
dc() {
  docker compose --env-file deploy/.env -f deploy/compose.yaml "$@"
}
```

## First start

Create local configuration and replace the Grafana password before shared use:

```sh
cp deploy/.env.example deploy/.env
chmod 600 deploy/.env
${EDITOR:-vi} deploy/.env
```

Render the fully interpolated Compose model before starting anything:

```sh
dc config --quiet
dc config > /tmp/railyard-compose.resolved.yaml
```

Build and start the stack, then wait on predicates rather than sleeping for a
fixed interval:

```sh
dc up -d --build

deadline=$((SECONDS + 180))
until curl -fsS http://127.0.0.1:8080/health/ready >/dev/null; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    dc ps
    dc logs --no-color --tail=200 server
    exit 1
  fi
  sleep 1
done

dc ps
```

`dc ps` should show the server, all eight workers, Redis, Prometheus, and
Grafana as healthy. If an image build fails because
`cmd/railyard-server` or `cmd/railyard-worker` is absent, complete the binary
integration contract at the end of this runbook before treating the deployment
as runnable.

## Stop and restart

A normal stop preserves all named volumes:

```sh
dc stop
dc start
```

Remove containers and the network while preserving data:

```sh
dc down --remove-orphans
```

`dc down --volumes` destroys SQLite, Redis AOF, Prometheus history, and Grafana
state. Use it only for an explicitly disposable run after evidence and backups
have been copied off the volumes.

For a controlled application restart:

```sh
dc stop worker-1 worker-2 worker-3 worker-4 worker-5 worker-6 worker-7 worker-8
dc restart server
curl -fsS http://127.0.0.1:8080/health/ready
dc start worker-1 worker-2 worker-3 worker-4 worker-5 worker-6 worker-7 worker-8
```

## Health checks

Check each layer independently:

```sh
curl -fsS http://127.0.0.1:8080/health/live
curl -fsS http://127.0.0.1:8080/health/ready
curl -fsS http://127.0.0.1:8080/metrics |
  grep -E '^(railyard_|go_|process_)' |
  sed -n '1,30p'

curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=up%7Bjob%3D%22railyard-server%22%7D'
curl -fsS http://127.0.0.1:3000/api/health
dc exec -T redis redis-cli ping
dc ps
```

Expected results are HTTP `200`, Redis `PONG`, Prometheus value `1`, and no
unhealthy container. Readiness is the traffic gate; liveness alone does not
mean SQLite is writable or configured dependencies are available.

Inspect active Prometheus alerts at
`http://127.0.0.1:9090/alerts` and the provisioned **Rail Yard Operations**
dashboard in Grafana. Prometheus is intentionally not connected to an external
Alertmanager in this local deployment.

## Admission overload

Symptoms include HTTP `429` with code `queue_full`, increasing pending depth,
and a sustained queue-full rejection rate. There is no separate admission
overload alert in the committed rules.

Capture the current rates before changing capacity:

```sh
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(railyard_queue_depth) by (state)'
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(rate(railyard_rejections_total{reason="queue_full"}[5m]))'
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(rate(railyard_lease_grants_total[5m]))'
dc logs --since=15m --no-color server
```

1. Pause or rate-limit producers. Clients must not immediately retry `429`
   responses.
2. Confirm workers are healthy and that lease grants and completions are still
   moving.
3. Check SQLite transaction p95 and busy events. More worker concurrency can
   worsen a storage bottleneck.
4. If CPU and SQLite latency are healthy, raise the frozen worker slot count
   and recreate all workers consistently:

   ```sh
   RAILYARD_WORKER_SLOTS=32 dc up -d --force-recreate \
     worker-1 worker-2 worker-3 worker-4 \
     worker-5 worker-6 worker-7 worker-8
   ```

5. Resume producers gradually only after pending depth has a sustained negative
   slope and queue-full rejections stop.

Do not raise tenant queue caps to hide overload. A cap change increases the
maximum memory, recovery, and drain burden and requires a separate capacity
decision.

## Lease expiry storm

Symptoms are repeated worker registration, rising lease-expiration rate,
falling completion throughput, or recovery p99 approaching five seconds. There
is no separate lease-storm alert in the committed rules.

```sh
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(rate(railyard_lease_expirations_total[5m])) by (disposition)'
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=histogram_quantile(0.99,sum by (le)(rate(railyard_lease_recovery_duration_seconds_bucket[10m])))'
dc ps
dc logs --since=15m --no-color \
  worker-1 worker-2 worker-3 worker-4 \
  worker-5 worker-6 worker-7 worker-8
```

1. Stop admission so the incident population is bounded.
2. Check worker restart counts, host CPU throttling, memory pressure, and
   server-to-worker network errors.
3. Verify host clock synchronization. Recovery evidence uses controller
   timestamps and is invalid when clocks are not comparable.
4. Stop repeatedly failing workers. Leave healthy workers running so fenced
   successor leases can drain the queue.
5. Restore workers one at a time and watch expiry rate, grant rate, and recovery
   p99 after each restart.

Do not delete lease rows, force-complete attempts, or shorten the reaper
interval during an incident. Do not lengthen the 2.5-second lease merely to
silence the alert; first establish whether heartbeat latency, worker stalls, or
server transaction latency is the cause.

## Redis lag and pending entries

Set the configured stream and consumer group, then inspect Redis directly:

```sh
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=railyard_redis_stream_lag'
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=railyard_redis_pending_entries'

STREAM="${STREAM:?set STREAM to the configured Redis stream}"
GROUP="${GROUP:?set GROUP to the configured consumer group}"

dc exec -T redis redis-cli XINFO STREAM "$STREAM"
dc exec -T redis redis-cli XINFO GROUPS "$STREAM"
dc exec -T redis redis-cli XPENDING "$STREAM" "$GROUP"
dc exec -T redis redis-cli INFO persistence
```

On Redis 7, `XINFO GROUPS` reports `lag`; `XPENDING` reports unacknowledged
entries. Also check `aof_last_write_status`, `aof_rewrite_in_progress`, and
`aof_current_size` in the persistence output.

1. If lag rises with no pending entries, check server readiness and trigger
   polling.
2. If pending rises, check ingestion transaction latency and SQLite busy
   events. Rail Yard acknowledges only after the delivery and jobs commit.
3. If AOF writes fail, stop producers and resolve Redis disk or filesystem
   errors before restarting Redis.
4. Allow the application to reclaim and deduplicate abandoned entries.

Never run `XACK`, `XDEL`, `XTRIM`, `FLUSHDB`, or recreate the consumer group as
an incident shortcut. Confirm durable SQLite ingestion and capture evidence
before any retention operation.

## SQLite WAL, busy events, and disk pressure

Start with telemetry and filesystem state:

```sh
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(increase(railyard_sqlite_busy_total[15m])) by (operation)'
curl -fsSG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=histogram_quantile(0.95,sum by (le,operation)(rate(railyard_sqlite_transaction_duration_seconds_bucket[10m])))'

dc exec -T server sh -ceu '
  df -h /var/lib/railyard
  ls -lh "$RAILYARD_DB_PATH" "$RAILYARD_DB_PATH-wal" "$RAILYARD_DB_PATH-shm" 2>/dev/null || true
'
```

A growing WAL is not corruption. It commonly means a long reader is preventing
checkpoint progress. To request and inspect a passive checkpoint, use a short
operator connection with a busy timeout:

```sh
dc exec -T server sh -ceu '
  sqlite3 "$RAILYARD_DB_PATH" \
    "PRAGMA busy_timeout=10000;" \
    "PRAGMA wal_checkpoint(PASSIVE);"
'
```

The checkpoint row is `busy | log frames | checkpointed frames`. A nonzero
`busy` value or a persistent gap between log and checkpointed frames requires
finding the long reader; it does not justify deleting WAL files.

For disk pressure:

1. Pause producers and preserve at least enough free space for the current WAL
   and a backup.
2. Capture metrics, logs, and file sizes.
3. Let active work drain, then stop workers before restarting the server.
4. Remove unrelated host data or move exported backups off the Docker storage
   filesystem.
5. Restart the server, verify readiness and a passive checkpoint, then restart
   workers.

Never copy only the live main database, truncate the WAL, or remove `-wal` and
`-shm` while the server is running.

## Consistent backup

SQLite's online backup API produces a consistent database while the server is
running:

```sh
mkdir -p backups
backup="railyard-$(date -u +%Y%m%dT%H%M%SZ).db"

dc exec -T -e BACKUP_NAME="$backup" server sh -ceu '
  sqlite3 "$RAILYARD_DB_PATH" \
    ".timeout 10000" \
    ".backup /var/lib/railyard/backups/$BACKUP_NAME"
  test "$(sqlite3 "/var/lib/railyard/backups/$BACKUP_NAME" "PRAGMA integrity_check;")" = "ok"
  test -z "$(sqlite3 "/var/lib/railyard/backups/$BACKUP_NAME" "PRAGMA foreign_key_check;")"
'

dc cp "server:/var/lib/railyard/backups/$backup" "backups/$backup"
sha256sum "backups/$backup" | tee "backups/$backup.sha256"
```

Store the database and checksum outside the Docker host. A complete operational
backup also records the Git commit, resolved Compose configuration, image
digests, and Redis trigger configuration. Prometheus and Grafana volumes are
diagnostic history, not part of canonical recovery.

## Restore and recovery

A restore rolls canonical state back to the backup timestamp. Redis entries
already acknowledged after that timestamp will not automatically recreate the
rolled-back jobs. Preserve submission manifests and reconcile all external
producers before choosing point-in-time restore.

1. Stop producers.
2. Take and export a final online backup if the current database is readable.
3. Stop all workers and the server:

   ```sh
   dc stop worker-1 worker-2 worker-3 worker-4 \
     worker-5 worker-6 worker-7 worker-8 server
   ```

4. Verify the selected backup checksum:

   ```sh
   sha256sum -c "backups/$backup.sha256"
   ```

5. Copy, validate, and atomically install the backup through a one-shot
   non-root server container:

   ```sh
   dc run --rm --no-deps \
     -e RESTORE_FILE="$backup" \
     -v "$(pwd)/backups:/restore:ro" \
     --entrypoint sh server -ceu '
       candidate="$RAILYARD_DB_PATH.restore"
       cp "/restore/$RESTORE_FILE" "$candidate"
       test "$(sqlite3 "$candidate" "PRAGMA integrity_check;")" = "ok"
       test -z "$(sqlite3 "$candidate" "PRAGMA foreign_key_check;")"
       rm -f "$RAILYARD_DB_PATH-wal" "$RAILYARD_DB_PATH-shm"
       mv "$candidate" "$RAILYARD_DB_PATH"
     '
   ```

6. Start only the server and verify storage settings and readiness:

   ```sh
   dc start server
   curl -fsS http://127.0.0.1:8080/health/ready
   dc exec -T server sh -ceu '
     sqlite3 "$RAILYARD_DB_PATH" \
       "PRAGMA journal_mode;" \
       "PRAGMA synchronous;" \
       "PRAGMA foreign_keys;" \
       "PRAGMA integrity_check;"
   '
   ```

   Expected values are `wal`, synchronous level `2` (`FULL`), foreign keys
   `1`, and `ok`.

7. Reconcile accepted manifests against canonical completions. Account for work
   submitted after the backup before enabling producers.
8. Start workers and watch retries, lease expirations, and completion rate:

   ```sh
   dc start worker-1 worker-2 worker-3 worker-4 \
     worker-5 worker-6 worker-7 worker-8
   ```

## Incident and qualification evidence

Create an immutable UTC-stamped directory and capture state before remediation:

```sh
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
run_dir="results/incident/$stamp"
mkdir -p "$run_dir/logs"

git rev-parse HEAD > "$run_dir/git-commit.txt"
git status --short > "$run_dir/git-status.txt"
docker version > "$run_dir/docker-version.txt"
dc version > "$run_dir/compose-version.txt"
dc config > "$run_dir/compose.resolved.yaml"
dc ps --all > "$run_dir/compose-ps.txt"
dc images > "$run_dir/images.txt"
dc logs --no-color --timestamps > "$run_dir/logs/compose.log" 2>&1

curl -fsS http://127.0.0.1:8080/metrics > "$run_dir/metrics.prom"
curl -fsS http://127.0.0.1:9090/api/v1/alerts > "$run_dir/prometheus-alerts.json"
dc exec -T redis redis-cli INFO all > "$run_dir/redis-info.txt"
dc exec -T server sh -ceu '
  df -h /var/lib/railyard
  ls -ln "$RAILYARD_DB_PATH" "$RAILYARD_DB_PATH-wal" "$RAILYARD_DB_PATH-shm" 2>/dev/null || true
' > "$run_dir/sqlite-filesystem.txt"

find "$run_dir" -type f ! -name SHA256SUMS -print0 |
  sort -z |
  xargs -0 sha256sum > "$run_dir/SHA256SUMS"
```

Take a consistent SQLite backup using the backup procedure and copy it into the
evidence directory when policy permits. Qualification evidence must also
include the workload manifest, seeds, action trace, reconciliation output,
recovery samples, benchmark samples, and replay digests described in
`docs/benchmark-methodology.md`. Logs can contain payload output; restrict the
raw evidence and create a separate redacted copy for broad sharing.

## Binary and telemetry integration contract

The deployment expects these executable and flag contracts:

- `cmd/railyard-server` builds `railyard-server` and accepts `--listen`,
  `--db-path`, `--redis-url`, and `--allow-shell`.
- `cmd/railyard-worker` builds `railyard-worker` and accepts `--server-url`,
  `--worker-id`, `--slots`, and `--allow-shell`.
- The server serves `/health/live`, `/health/ready`, and `/metrics` on the
  configured listener.

Create `telemetry.Metrics` once with `telemetry.New`, route
`Metrics.Handler()` at `/metrics`, and keep its custom registry rather than the
Prometheus default registry. Record counters only after the corresponding
durable transaction commits. Idempotent duplicates increment the duplicate
admission series but must not increment durable grants or completion outcomes.
Refresh queue and dead-letter gauges every five seconds from aggregate SQLite
queries. Start the lifecycle event cursor at the current durable sequence, then
consume new events incrementally. Events committed after startup use persisted
ready, lease, completion, and lease-loss timestamps even when their origin
predates the process. Historical observations are not replayed after a restart.
SQLite latency and busy outcomes cover instrumented job-protocol storage calls.
Poll the configured Redis group every five seconds; publish pending entries and
lag only when Redis reports them, and remove the series when the group state
cannot be observed.
