package evidence

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArtifactChecksums(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := WriteJSON(filepath.Join(root, "manifest.json"), map[string]int{"version": 1}); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "logs"), 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "logs", "run.log"), []byte("ok\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "snapshot.db-shm"), []byte("mutable"), 0o644); err != nil {
		t.Fatalf("write SHM: %v", err)
	}
	if err := GenerateChecksums(root); err != nil {
		t.Fatalf("GenerateChecksums: %v", err)
	}
	if err := VerifyChecksums(root); err != nil {
		t.Fatalf("VerifyChecksums: %v", err)
	}

	checksums, err := os.ReadFile(filepath.Join(root, ChecksumsFile))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(checksums), "logs/run.log") {
		t.Fatalf("checksums do not use slash-separated paths:\n%s", checksums)
	}
	if strings.Contains(string(checksums), "snapshot.db-shm") {
		t.Fatalf("checksums include mutable SQLite SHM:\n%s", checksums)
	}
}

func TestArtifactChecksumsDetectTamperingAndUnlistedFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	artifact := filepath.Join(root, "sample.jsonl")
	if err := os.WriteFile(artifact, []byte("{\"value\":1}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := GenerateChecksums(root); err != nil {
		t.Fatalf("GenerateChecksums: %v", err)
	}
	if err := os.WriteFile(artifact, []byte("{\"value\":2}\n"), 0o644); err != nil {
		t.Fatalf("tamper artifact: %v", err)
	}
	if err := VerifyChecksums(root); err == nil {
		t.Error("VerifyChecksums accepted a modified artifact")
	}

	if err := GenerateChecksums(root); err != nil {
		t.Fatalf("regenerate checksums: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "unlisted.txt"), []byte("extra"), 0o644); err != nil {
		t.Fatalf("write unlisted artifact: %v", err)
	}
	if err := VerifyChecksums(root); err == nil {
		t.Error("VerifyChecksums accepted an unlisted artifact")
	}
}

func TestVerifyChecksumsRejectsEscapingPath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	line := strings.Repeat("0", 64) + "  ../outside\n"
	if err := os.WriteFile(filepath.Join(root, ChecksumsFile), []byte(line), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := VerifyChecksums(root); err == nil {
		t.Error("VerifyChecksums accepted an escaping path")
	}
}

func TestSQLiteSnapshotDigestsIncludeWAL(t *testing.T) {
	t.Parallel()

	database := filepath.Join(t.TempDir(), "snapshot.db")
	if err := os.WriteFile(database, []byte("database"), 0o644); err != nil {
		t.Fatalf("write database: %v", err)
	}
	if err := os.WriteFile(database+"-wal", []byte("wal"), 0o644); err != nil {
		t.Fatalf("write WAL: %v", err)
	}
	if err := os.WriteFile(database+"-shm", []byte("mutable"), 0o644); err != nil {
		t.Fatalf("write SHM: %v", err)
	}

	digests, err := SQLiteSnapshotDigests(database)
	if err != nil {
		t.Fatalf("SQLiteSnapshotDigests: %v", err)
	}
	if digests["database"] == "" || digests["wal"] == "" {
		t.Fatalf("snapshot digests omit a durable component: %#v", digests)
	}
	if _, found := digests["shm"]; found {
		t.Fatalf("snapshot digests include absent SHM: %#v", digests)
	}
}
