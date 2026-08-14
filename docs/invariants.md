# Correctness Invariants

These invariants are the contract shared by production code, model tests, the
independent reconciliation tool, and deterministic replay.

## Durable state

1. Every accepted logical job has exactly one `jobs` row and one admission
   event.
2. A `(tenant_id, submission_key)` identifies at most one request digest and one
   stable response.
3. A job has at most one canonical terminal row in `job_completions`.
4. Terminal states never transition to nonterminal states.
5. Folding a job's ordered events yields the same state and state version as its
   materialized row.
6. Event sequence numbers are unique, positive, and strictly increasing in
   commit order.
7. A transaction never persists a materialized state change without its
   corresponding event.

## Attempts and leases

8. Attempt numbers and lease generations are unique and strictly increasing per
   job.
9. At most one unexpired database lease is current for a job.
10. A heartbeat, start, or completion can mutate a job only when job ID,
    attempt number, worker ID, lease generation, and lease token all match.
11. A stale completion never creates a terminal outcome.
12. An identical duplicate completion returns its first receipt; a conflicting
    duplicate returns `409`.
13. Slot reservations are acquired and released in the same transaction as the
    state transition that owns them.
14. Reaping expires only the current lease generation.

## DAGs

15. Accepted workflow graphs are acyclic and immutable.
16. A child is schedulable only after all parents are `SUCCEEDED`.
17. A child is released at most once.
18. A terminal unsuccessful parent eventually gives every nonterminal
    descendant a terminal `upstream_failed` dead-letter outcome.

## Scheduling

19. Every scheduler input has a complete total ordering.
20. Queue-local selection is priority descending, ready sequence ascending,
    then job ID ascending.
21. Queue fairness state changes only as part of a recorded scheduling
    decision.
22. A lease never exceeds worker, queue, or tenant slot capacity.
23. Admission never allows tenant nonterminal depth above its configured cap.
24. No random value, map iteration, unrecorded clock read, or floating-point
    calculation can affect a scheduling decision.

## Retries and dead letters

25. Every failed attempt consumes at most one retry-budget entry.
26. Retry release time is deterministic from recorded inputs and is persisted.
27. A job with exhausted retries receives one `DEAD_LETTER` completion.
28. Redrive creates a new job and never reopens a terminal job.
29. Failure context is bounded but includes failure class, attempt, exit code,
    output digest, and truncated stderr.

## Triggers

30. A cron trigger creates at most one job for a nominal UTC occurrence.
31. A Redis stream entry creates at most one logical job per trigger.
32. A Redis entry is acknowledged only after its delivery and jobs commit in
    SQLite.
33. A crash between commit and acknowledgement is resolved through
    deduplication on redelivery.

## Decision log and replay

34. Decision records use contiguous sequence numbers and a SHA-256 hash chain.
35. Canonical records use fixed-field JSON structs, integer timestamps, sorted
    slices, UTF-8, and LF line endings.
36. Replaying a record through the production scheduler reproduces the stored
    decision bytes or fails at that record.
37. Algorithm, schema, and configuration versions are part of every replay
    input.

## Qualification oracle

For the reduced portfolio 5,000-job no-op chaos run, define:

- `A`: durably accepted job IDs from the submitted manifest and idempotency
  receipts;
- `T`: job IDs in `job_completions`; and
- `C(j)`: canonical completion count for job `j`.

The run passes only when:

```text
|A| = 5,000
|T| = 5,000
lost = |A - T| = 0
orphan = |T - A| = 0
duplicates = sum(max(0, C(j) - 1)) = 0
SUCCEEDED = 5,000
FAILED = DEAD_LETTER = active = 0
```

In addition, SQLite `integrity_check` and `foreign_key_check` pass, every
transition sequence is contiguous, lease generations are strictly increasing,
and no slot reservation remains.

Attempt-level repeats are reported separately. They do not violate canonical
terminal uniqueness, but a cooperative idempotency test target is used to
detect repeated business side effects.
