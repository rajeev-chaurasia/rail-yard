package evidence

import (
	"strings"
	"testing"
)

func TestReducedQualificationAcceptsRecordedDockerHost(t *testing.T) {
	clean := false
	manifest := RunManifest{
		Config: RunConfig{
			Qualification:       true,
			JobCount:            5_000,
			WorkerCount:         8,
			WorkerSlots:         256,
			ConfigurationSHA256: strings.Repeat("a", 64),
		},
		Environment: EnvironmentManifest{
			GitCommit:      strings.Repeat("b", 40),
			GitDirty:       &clean,
			GoVersion:      "go1.26.6",
			DockerVersion:  "29.7.2",
			ComposeVersion: "5.3.1",
			Hostname:       "qualification-host",
			OS:             "windows",
			Architecture:   "amd64",
			CPUCount:       16,
			Filesystem:     "ext4",
			Timezone:       "UTC",
			BinaryDigests: map[string]string{
				"server": strings.Repeat("c", 64),
				"worker": strings.Repeat("d", 64),
			},
			ImageDigests: map[string]string{
				"redis":    "1",
				"server":   "2",
				"worker-1": "3",
				"worker-2": "4",
				"worker-3": "5",
				"worker-4": "6",
				"worker-5": "7",
				"worker-6": "8",
				"worker-7": "9",
				"worker-8": "10",
			},
			SQLitePragmas: map[string]string{
				"journal_mode": "wal",
				"synchronous":  "2",
				"foreign_keys": "1",
				"busy_timeout": "5000",
			},
			OperatorDetails: map[string]string{
				"worker_count_evidence": "worker-1,worker-2,worker-3,worker-4,worker-5,worker-6,worker-7,worker-8",
			},
		},
	}

	if violations := validateQualificationEnvironment(manifest); len(violations) != 0 {
		t.Fatalf("valid reduced qualification violations: %v", violations)
	}

	manifest.Config.JobCount = 50_000
	violations := validateQualificationEnvironment(manifest)
	if len(violations) != 1 || violations[0] != "qualification job_count must be 5000" {
		t.Fatalf("unexpected legacy-scope violations: %v", violations)
	}
}
