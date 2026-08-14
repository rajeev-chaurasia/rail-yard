package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/operations"
	storepkg "github.com/rajeev-chaurasia/rail-yard/internal/store"
)

func TestLegacySubmissionActorsPersistInHistoryAndDeduplicate(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "legacy-actors.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 18, 0, 0, 0, time.UTC)

	jobSubmission := storepkg.Submission{
		Job: domain.JobSpec{
			TenantID: "tenant-a",
			Payload:  domain.Payload{Type: domain.PayloadNoop},
		},
		Actor:          "job-actor",
		IdempotencyKey: "job-key",
		RequestDigest:  "job-digest",
	}
	job, duplicate, err := jobStore.SubmitJob(ctx, jobSubmission, now)
	if err != nil || duplicate {
		t.Fatalf("first job submission = %+v, duplicate = %t, error = %v", job, duplicate, err)
	}
	repeated, duplicate, err := jobStore.SubmitJob(ctx, jobSubmission, now.Add(time.Minute))
	if err != nil || !duplicate || repeated.ID != job.ID {
		t.Fatalf("duplicate job = %+v, duplicate = %t, error = %v", repeated, duplicate, err)
	}
	assertOnlyHistoryActor(t, jobStore, job.ID, "job-actor")

	workflowSubmission := storepkg.WorkflowSubmission{
		Request: api.SubmitWorkflowRequest{
			TenantID: "tenant-a",
			Nodes: []api.WorkflowNode{
				{
					Key: "a",
					Job: domain.JobSpec{Payload: domain.Payload{Type: domain.PayloadNoop}},
				},
				{
					Key: "b",
					Job: domain.JobSpec{
						Payload:   domain.Payload{Type: domain.PayloadNoop},
						DependsOn: []string{"a"},
					},
				},
			},
		},
		Actor:          "workflow-actor",
		IdempotencyKey: "workflow-key",
		RequestDigest:  "workflow-digest",
	}
	jobs, duplicate, err := jobStore.SubmitWorkflow(ctx, workflowSubmission, now)
	if err != nil || duplicate || len(jobs) != 2 {
		t.Fatalf("first workflow jobs = %+v, duplicate = %t, error = %v", jobs, duplicate, err)
	}
	repeatedJobs, duplicate, err := jobStore.SubmitWorkflow(
		ctx,
		workflowSubmission,
		now.Add(time.Minute),
	)
	if err != nil || !duplicate || len(repeatedJobs) != len(jobs) {
		t.Fatalf(
			"duplicate workflow jobs = %+v, duplicate = %t, error = %v",
			repeatedJobs,
			duplicate,
			err,
		)
	}
	for _, workflowJob := range jobs {
		assertOnlyHistoryActor(t, jobStore, workflowJob.ID, "workflow-actor")
	}
}

func TestCronActorPersistsThroughDuplicateAndFire(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "cron-actor.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 18, 0, 30, 0, time.UTC)
	submission := storepkg.CronSubmission{
		Trigger: domain.CronTrigger{
			TenantID:   "tenant-a",
			Expression: "* * * * *",
			Job: domain.JobSpec{
				Payload: domain.Payload{Type: domain.PayloadNoop},
			},
		},
		Actor:          "cron-actor",
		IdempotencyKey: "cron-key",
		RequestDigest:  "cron-digest",
	}
	created, duplicate, err := jobStore.CreateCronTrigger(ctx, submission, now)
	if err != nil || duplicate {
		t.Fatalf("first cron trigger = %+v, duplicate = %t, error = %v", created, duplicate, err)
	}
	repeated, duplicate, err := jobStore.CreateCronTrigger(ctx, submission, now.Add(time.Minute))
	if err != nil || !duplicate || repeated.ID != created.ID {
		t.Fatalf("duplicate cron trigger = %+v, duplicate = %t, error = %v", repeated, duplicate, err)
	}
	jobIDs, err := jobStore.FireDueCron(ctx, created.NextFireAt, 1)
	if err != nil || len(jobIDs) != 1 {
		t.Fatalf("fired jobs = %v, error = %v", jobIDs, err)
	}
	assertOnlyHistoryActor(t, jobStore, jobIDs[0], "cron-actor")
}

func TestLegacyRedriveActorPersistsInHistoryAndAudit(t *testing.T) {
	ctx := context.Background()
	jobStore := openTestStore(t, filepath.Join(t.TempDir(), "redrive-actor.db"))
	t.Cleanup(func() { _ = jobStore.Close() })
	now := time.Date(2026, time.August, 14, 18, 30, 0, 0, time.UTC)
	source := createDeadLetterForOperationTest(t, jobStore, now, "actor-source")
	attributedContext := storepkg.WithActor(ctx, "redrive-actor")

	created, duplicate, err := jobStore.RedriveDeadLetter(
		attributedContext,
		source.ID,
		"redrive-key",
		"redrive-digest",
		now.Add(time.Second),
	)
	if err != nil || duplicate {
		t.Fatalf("first redrive = %+v, duplicate = %t, error = %v", created, duplicate, err)
	}
	repeated, duplicate, err := jobStore.RedriveDeadLetter(
		attributedContext,
		source.ID,
		"redrive-key",
		"redrive-digest",
		now.Add(2*time.Second),
	)
	if err != nil || !duplicate || repeated.ID != created.ID {
		t.Fatalf("duplicate redrive = %+v, duplicate = %t, error = %v", repeated, duplicate, err)
	}

	sourceHistory, err := jobStore.JobHistory(
		ctx,
		source.ID,
		operations.HistoryQuery{Limit: 20},
	)
	if err != nil {
		t.Fatal(err)
	}
	redriveEvents := 0
	for _, event := range sourceHistory.Events {
		if event.Type == "operator_redrive" {
			redriveEvents++
			if event.Actor != "redrive-actor" {
				t.Fatalf("redrive history actor = %q", event.Actor)
			}
		}
	}
	if redriveEvents != 1 {
		t.Fatalf("redrive history events = %d, want 1", redriveEvents)
	}
	assertOnlyHistoryActor(t, jobStore, created.ID, "redrive-actor")

	audits, err := jobStore.ListAuditEvents(
		ctx,
		now.Add(-time.Hour),
		"redrive-actor",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(audits) != 1 ||
		audits[0].Action != "dead_letter.redrive" ||
		audits[0].Actor != "redrive-actor" ||
		audits[0].Details["actor"] != "redrive-actor" {
		t.Fatalf("redrive audits = %+v", audits)
	}
}

func assertOnlyHistoryActor(t *testing.T, jobStore *Store, jobID, actor string) {
	t.Helper()
	history, err := jobStore.JobHistory(
		context.Background(),
		jobID,
		operations.HistoryQuery{Limit: 20},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(history.Events) != 1 {
		t.Fatalf("history for %s has %d events, want 1", jobID, len(history.Events))
	}
	if history.Events[0].Actor != actor {
		t.Fatalf("history actor for %s = %q, want %q", jobID, history.Events[0].Actor, actor)
	}
}
