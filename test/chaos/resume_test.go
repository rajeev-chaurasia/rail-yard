package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/test/reconcile"
)

func TestResumeReusesValidRun(t *testing.T) {
	cfg := resumeConfig(t, 2)
	writeCompletedRun(t, cfg, 1)
	var executed []int
	summary, err := runCampaignWithExecutor(
		context.Background(),
		cfg,
		&recordingRunner{},
		func(_ context.Context, cfg config, _ commandRunner, run int, seed int64) (runSummary, error) {
			executed = append(executed, run)
			return completedSummary(cfg, run, seed, "new-run"), nil
		},
	)
	if err != nil {
		t.Fatalf("resume campaign: %v", err)
	}
	if len(executed) != 1 || executed[0] != 2 {
		t.Fatalf("executed runs=%v want=[2]", executed)
	}
	if len(summary.Runs) != 2 || summary.Runs[0].Run != 1 || summary.Runs[1].Run != 2 {
		t.Fatalf("unexpected resumed summary: %#v", summary.Runs)
	}
	if summary.RecoverySamples != 2 || summary.RecoveryP99MS != 10 {
		t.Fatalf("unexpected aggregate recovery: %#v", summary)
	}
}

func TestResumeRejectsSeedAndConfigurationMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*runManifest)
	}{
		{
			name: "seed",
			mutate: func(manifest *runManifest) {
				manifest.Seed++
			},
		},
		{
			name: "configuration",
			mutate: func(manifest *runManifest) {
				manifest.ConfigurationHash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := resumeConfig(t, 1)
			runDirectory := writeCompletedRun(t, cfg, 1)
			var manifest runManifest
			if err := readJSONFile(filepath.Join(runDirectory, "manifest.json"), &manifest); err != nil {
				t.Fatalf("read manifest: %v", err)
			}
			test.mutate(&manifest)
			if err := writeJSONAtomic(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
				t.Fatalf("rewrite manifest: %v", err)
			}
			rewriteChecksums(t, runDirectory, cfg)

			executed := 0
			_, err := runCampaignWithExecutor(
				context.Background(),
				cfg,
				&recordingRunner{},
				func(_ context.Context, cfg config, _ commandRunner, run int, seed int64) (runSummary, error) {
					executed++
					return completedSummary(cfg, run, seed, "replacement"), nil
				},
			)
			if err != nil {
				t.Fatalf("resume campaign: %v", err)
			}
			if executed != 1 {
				t.Fatalf("executed runs=%d want=1", executed)
			}
			assertIsolated(t, cfg.OutputDirectory, filepath.Base(runDirectory))
		})
	}
}

func TestResumeRejectsCorruptArtifact(t *testing.T) {
	cfg := resumeConfig(t, 1)
	runDirectory := writeCompletedRun(t, cfg, 1)
	file, err := os.OpenFile(
		filepath.Join(runDirectory, "recovery-samples.jsonl"),
		os.O_APPEND|os.O_WRONLY,
		0,
	)
	if err != nil {
		t.Fatalf("open recovery samples: %v", err)
	}
	if _, err := file.WriteString("{}\n"); err != nil {
		t.Fatalf("corrupt recovery samples: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close recovery samples: %v", err)
	}

	executed := 0
	_, err = runCampaignWithExecutor(
		context.Background(),
		cfg,
		&recordingRunner{},
		func(_ context.Context, cfg config, _ commandRunner, run int, seed int64) (runSummary, error) {
			executed++
			return completedSummary(cfg, run, seed, "replacement"), nil
		},
	)
	if err != nil {
		t.Fatalf("resume campaign: %v", err)
	}
	if executed != 1 {
		t.Fatalf("executed runs=%d want=1", executed)
	}
	assertIsolated(t, cfg.OutputDirectory, filepath.Base(runDirectory))
}

func TestResumeReplacesPartialRun(t *testing.T) {
	cfg := resumeConfig(t, 1)
	runDirectory := filepath.Join(
		cfg.OutputDirectory,
		"20260101T000000.000000000Z-r01-s1",
	)
	if err := os.MkdirAll(runDirectory, 0o755); err != nil {
		t.Fatalf("create partial run: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDirectory, "manifest.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write partial manifest: %v", err)
	}

	executed := 0
	_, err := runCampaignWithExecutor(
		context.Background(),
		cfg,
		&recordingRunner{},
		func(_ context.Context, cfg config, _ commandRunner, run int, seed int64) (runSummary, error) {
			executed++
			if _, err := os.Stat(runDirectory); !os.IsNotExist(err) {
				t.Fatalf("partial run was not isolated before replacement: %v", err)
			}
			return completedSummary(cfg, run, seed, "replacement"), nil
		},
	)
	if err != nil {
		t.Fatalf("resume campaign: %v", err)
	}
	if executed != 1 {
		t.Fatalf("executed runs=%d want=1", executed)
	}
	assertIsolated(t, cfg.OutputDirectory, filepath.Base(runDirectory))
}

func TestResumeCompletedCampaignIsNoOp(t *testing.T) {
	cfg := resumeConfig(t, 3)
	for run := 1; run <= cfg.Runs; run++ {
		writeCompletedRun(t, cfg, run)
	}
	summary, err := runCampaignWithExecutor(
		context.Background(),
		cfg,
		&recordingRunner{},
		func(context.Context, config, commandRunner, int, int64) (runSummary, error) {
			t.Fatal("completed campaign executed a run")
			return runSummary{}, nil
		},
	)
	if err != nil {
		t.Fatalf("resume completed campaign: %v", err)
	}
	if !summary.Passed || len(summary.Runs) != cfg.Runs {
		t.Fatalf("unexpected completed summary: %#v", summary)
	}
	if _, err := os.Stat(filepath.Join(cfg.OutputDirectory, "summary.json")); err != nil {
		t.Fatalf("regenerated summary: %v", err)
	}
}

func resumeConfig(t *testing.T, runs int) config {
	t.Helper()
	cfg := validConfig()
	cfg.OutputDirectory = t.TempDir()
	cfg.Runs = runs
	cfg.Jobs = 3
	cfg.BaseSeed = 1
	cfg.Resume = true
	return cfg
}

func writeCompletedRun(t *testing.T, cfg config, run int) string {
	t.Helper()
	seed := cfg.BaseSeed + int64(run-1)
	runDirectory := filepath.Join(
		cfg.OutputDirectory,
		fmt.Sprintf("20260101T00000%d.000000000Z-r%02d-s%s", run, run, seedToken(seed)),
	)
	if err := os.MkdirAll(filepath.Join(runDirectory, "database"), 0o755); err != nil {
		t.Fatalf("create database directory: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(runDirectory, "logs"), 0o755); err != nil {
		t.Fatalf("create logs directory: %v", err)
	}
	started := time.Date(2026, 1, 1, 0, 0, run, 0, time.UTC)
	configurationHash, err := cfg.configurationHash()
	if err != nil {
		t.Fatalf("configuration hash: %v", err)
	}
	manifest := runManifest{
		Version:           3,
		Run:               run,
		Seed:              seed,
		Project:           projectName(cfg.ProjectPrefix, run, seed),
		TenantID:          fmt.Sprintf("chaos-r%02d-s%s", run, seedToken(seed)),
		Queue:             "noop",
		Jobs:              cfg.Jobs,
		Workers:           append([]string(nil), cfg.Workers...),
		WorkerKills:       cfg.WorkerKills,
		SubmitConcurrency: cfg.SubmitConcurrency,
		JobDuration:       cfg.JobDuration,
		MaxRecovery:       cfg.MaxRecovery,
		ComposeFile:       cfg.ComposeFile,
		ConfigurationHash: configurationHash,
		StartedAt:         started,
		CompletedAt:       started.Add(time.Minute),
	}
	if err := writeJSONAtomic(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	submitted, err := createJSONLines(filepath.Join(runDirectory, "submitted.jsonl"))
	if err != nil {
		t.Fatalf("create submitted records: %v", err)
	}
	for index := 1; index <= cfg.Jobs; index++ {
		if err := submitted.write(reconcile.AcceptedRecord{
			Sequence:      index,
			SubmissionKey: stableSubmissionKey(run, seed, index),
			JobID:         fmt.Sprintf("%032x", index),
			TenantID:      manifest.TenantID,
			AcceptedAt:    started,
		}); err != nil {
			t.Fatalf("write submitted record: %v", err)
		}
	}
	if err := submitted.close(); err != nil {
		t.Fatalf("close submitted records: %v", err)
	}
	mapping, err := newClockMapping(
		started.Add(-time.Second+5*time.Millisecond),
		started.Add(-time.Second),
		started.Add(-time.Second+10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("create clock mapping: %v", err)
	}
	events, err := createJSONLines(filepath.Join(runDirectory, "events.jsonl"))
	if err != nil {
		t.Fatalf("create events: %v", err)
	}
	for sequence := 1; sequence <= cfg.WorkerKills; sequence++ {
		details := map[string]any{
			"kill_sequence":     sequence,
			"kill_confirmed_at": started,
			"clock_mapping":     mapping,
		}
		if sequence == 1 {
			details["active_leases"] = []activeLease{{
				JobID:      fmt.Sprintf("%032x", 1),
				AttemptNo:  1,
				Generation: 1,
				LeasedAt:   started.Add(-time.Second),
			}}
		}
		if err := events.write(actionEvent{
			Sequence:   sequence,
			Type:       "worker_killed",
			ObservedAt: started,
			Details:    details,
		}); err != nil {
			t.Fatalf("write worker event: %v", err)
		}
	}
	if err := events.write(actionEvent{
		Sequence:   cfg.WorkerKills + 1,
		Type:       "server_killed",
		ObservedAt: started,
	}); err != nil {
		t.Fatalf("write server event: %v", err)
	}
	if err := events.close(); err != nil {
		t.Fatalf("close events: %v", err)
	}
	recovery, err := createJSONLines(filepath.Join(runDirectory, "recovery-samples.jsonl"))
	if err != nil {
		t.Fatalf("create recovery samples: %v", err)
	}
	if err := recovery.write(recoverySample{
		KillSequence:        1,
		Worker:              cfg.Workers[0],
		VictimContainerID:   "container-1",
		JobID:               fmt.Sprintf("%032x", 1),
		KilledAttempt:       1,
		KilledGeneration:    1,
		KillConfirmedHostAt: started,
		KillConfirmedAt:     started,
		ClockMapping:        mapping,
		SuccessorAttempt:    2,
		SuccessorGeneration: 2,
		SuccessorLeasedAt:   started.Add(10 * time.Millisecond),
		SuccessorObservedAt: started.Add(15 * time.Millisecond),
		CompletionAt:        started.Add(time.Second),
		RecoveryMS:          10,
	}); err != nil {
		t.Fatalf("write recovery sample: %v", err)
	}
	if err := recovery.close(); err != nil {
		t.Fatalf("close recovery samples: %v", err)
	}
	report := reconcile.Report{
		Version:        1,
		GeneratedAt:    started,
		Passed:         true,
		ExpectedJobs:   cfg.Jobs,
		ViolationCount: 0,
		Counts: reconcile.Counts{
			Accepted:    cfg.Jobs,
			Jobs:        cfg.Jobs,
			Completions: cfg.Jobs,
		},
	}
	if err := writeJSONAtomic(filepath.Join(runDirectory, "reconciliation.json"), report); err != nil {
		t.Fatalf("write reconciliation: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDirectory, "database", "railyard.db"), []byte("db"), 0o644); err != nil {
		t.Fatalf("write database: %v", err)
	}
	if err := os.WriteFile(filepath.Join(runDirectory, "logs", "compose.log"), []byte("logs"), 0o644); err != nil {
		t.Fatalf("write logs: %v", err)
	}
	if err := finalizeRunArtifacts(runDirectory, cfg); err != nil {
		t.Fatalf("finalize run artifacts: %v", err)
	}
	return runDirectory
}

func completedSummary(cfg config, run int, seed int64, directory string) runSummary {
	started := time.Date(2026, 1, 1, 0, 0, run, 0, time.UTC)
	return runSummary{
		Run:                run,
		Seed:               seed,
		Project:            projectName(cfg.ProjectPrefix, run, seed),
		TenantID:           fmt.Sprintf("chaos-r%02d-s%s", run, seedToken(seed)),
		StartedAt:          started,
		CompletedAt:        started.Add(time.Minute),
		Accepted:           cfg.Jobs,
		WorkerKills:        cfg.WorkerKills,
		ServerKills:        1,
		RecoverySamples:    1,
		RecoveryP99MS:      10,
		RecoveryTargetMS:   float64(cfg.MaxRecovery) / float64(time.Millisecond),
		ReconciliationPass: true,
		ArtifactDirectory:  directory,
		recoveryValues:     []float64{10},
	}
}

func rewriteChecksums(t *testing.T, runDirectory string, cfg config) {
	t.Helper()
	if err := os.Remove(filepath.Join(runDirectory, checksumFile)); err != nil {
		t.Fatalf("remove checksums: %v", err)
	}
	if err := finalizeRunArtifacts(runDirectory, cfg); err != nil {
		t.Fatalf("rewrite checksums: %v", err)
	}
}

func assertIsolated(t *testing.T, outputDirectory, name string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(outputDirectory, ".invalid", name)); err != nil {
		t.Fatalf("isolated run %s: %v", name, err)
	}
}
