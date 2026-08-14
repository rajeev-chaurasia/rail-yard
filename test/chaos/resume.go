package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/test/reconcile"
)

const checksumFile = "SHA256SUMS"

var runDirectoryPattern = regexp.MustCompile(`-r([0-9]+)-s[^/\\]+$`)

var requiredRunArtifactFiles = []string{
	"events.jsonl",
	"logs/compose.log",
	"manifest.json",
	"reconciliation.json",
	"recovery-samples.jsonl",
	"submitted.jsonl",
}

type runConfiguration struct {
	Version           int           `json:"version"`
	Runs              int           `json:"runs"`
	Jobs              int           `json:"jobs"`
	BaseSeed          int64         `json:"base_seed"`
	ProjectPrefix     string        `json:"project_prefix"`
	ServerURL         string        `json:"server_url"`
	DatabasePath      string        `json:"database_path"`
	ComposeFile       string        `json:"compose_file"`
	DockerExecutable  string        `json:"docker_executable"`
	Workers           []string      `json:"workers"`
	WorkerKills       int           `json:"worker_kills"`
	SubmitConcurrency int           `json:"submit_concurrency"`
	JobDuration       time.Duration `json:"job_duration"`
	ActionMinimum     time.Duration `json:"action_minimum"`
	ActionMaximum     time.Duration `json:"action_maximum"`
	PollInterval      time.Duration `json:"poll_interval"`
	StartupTimeout    time.Duration `json:"startup_timeout"`
	DrainTimeout      time.Duration `json:"drain_timeout"`
	RequestTimeout    time.Duration `json:"request_timeout"`
	MaxRecovery       time.Duration `json:"max_recovery"`
	KeepStack         bool          `json:"keep_stack"`
}

type resumedRun struct {
	summary runSummary
	path    string
}

type expectedRecoverySample struct {
	lease               activeLease
	killConfirmedHostAt time.Time
	killConfirmedAt     time.Time
	clockMapping        clockMapping
}

func (c config) runConfiguration() runConfiguration {
	return runConfiguration{
		Version:           1,
		Runs:              c.Runs,
		Jobs:              c.Jobs,
		BaseSeed:          c.BaseSeed,
		ProjectPrefix:     c.ProjectPrefix,
		ServerURL:         c.ServerURL,
		DatabasePath:      c.DatabasePath,
		ComposeFile:       c.ComposeFile,
		DockerExecutable:  c.DockerExecutable,
		Workers:           append([]string(nil), c.Workers...),
		WorkerKills:       c.WorkerKills,
		SubmitConcurrency: c.SubmitConcurrency,
		JobDuration:       c.JobDuration,
		ActionMinimum:     c.ActionMinimum,
		ActionMaximum:     c.ActionMaximum,
		PollInterval:      c.PollInterval,
		StartupTimeout:    c.StartupTimeout,
		DrainTimeout:      c.DrainTimeout,
		RequestTimeout:    c.RequestTimeout,
		MaxRecovery:       c.MaxRecovery,
		KeepStack:         c.KeepStack,
	}
}

func (c config) configurationHash() (string, error) {
	body, err := json.Marshal(c.runConfiguration())
	if err != nil {
		return "", fmt.Errorf("encode run configuration: %w", err)
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func loadResumedRuns(cfg config) (map[int]runSummary, error) {
	entries, err := os.ReadDir(cfg.OutputDirectory)
	if err != nil {
		return nil, fmt.Errorf("read resume directory: %w", err)
	}
	expectedHash, err := cfg.configurationHash()
	if err != nil {
		return nil, err
	}
	valid := make(map[int]resumedRun)
	var rejected []string
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == ".invalid" {
			continue
		}
		run, ok := runNumberFromDirectory(entry.Name())
		if !ok || run < 1 || run > cfg.Runs {
			continue
		}
		path := filepath.Join(cfg.OutputDirectory, entry.Name())
		summary, verifyErr := verifyCompletedRun(path, cfg, run, expectedHash)
		if verifyErr != nil {
			rejected = append(rejected, path)
			continue
		}
		current, exists := valid[run]
		if !exists || summary.CompletedAt.After(current.summary.CompletedAt) {
			valid[run] = resumedRun{summary: summary, path: path}
		}
	}
	for _, path := range rejected {
		if err := isolateRun(cfg.OutputDirectory, path); err != nil {
			return nil, err
		}
	}
	result := make(map[int]runSummary, len(valid))
	for run, candidate := range valid {
		result[run] = candidate.summary
	}
	return result, nil
}

func runNumberFromDirectory(name string) (int, bool) {
	match := runDirectoryPattern.FindStringSubmatch(name)
	if len(match) != 2 {
		return 0, false
	}
	run, err := strconv.Atoi(match[1])
	return run, err == nil
}

func isolateRun(outputDirectory, path string) error {
	directory := filepath.Join(outputDirectory, ".invalid")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create invalid run directory: %w", err)
	}
	base := filepath.Base(path)
	destination := filepath.Join(directory, base)
	for sequence := 1; ; sequence++ {
		if _, err := os.Stat(destination); errors.Is(err, os.ErrNotExist) {
			break
		} else if err != nil {
			return fmt.Errorf("inspect invalid run destination: %w", err)
		}
		destination = filepath.Join(directory, fmt.Sprintf("%s-%d", base, sequence))
	}
	if err := os.Rename(path, destination); err != nil {
		return fmt.Errorf("isolate invalid run %s: %w", base, err)
	}
	return nil
}

func verifyCompletedRun(
	runDirectory string,
	cfg config,
	run int,
	expectedHash string,
) (runSummary, error) {
	if err := verifyChecksums(runDirectory, cfg); err != nil {
		return runSummary{}, err
	}
	var manifest runManifest
	if err := readJSONFile(filepath.Join(runDirectory, "manifest.json"), &manifest); err != nil {
		return runSummary{}, err
	}
	seed := cfg.BaseSeed + int64(run-1)
	if manifest.Version != 3 || manifest.Run != run || manifest.Seed != seed {
		return runSummary{}, errors.New("manifest run or seed does not match")
	}
	if manifest.ConfigurationHash != expectedHash {
		return runSummary{}, errors.New("manifest configuration does not match")
	}
	if manifest.CompletedAt.IsZero() || manifest.CompletedAt.Before(manifest.StartedAt) {
		return runSummary{}, errors.New("manifest is not finalized")
	}
	if manifest.Project != projectName(cfg.ProjectPrefix, run, seed) ||
		manifest.TenantID != fmt.Sprintf("chaos-r%02d-s%s", run, seedToken(seed)) ||
		manifest.Jobs != cfg.Jobs ||
		manifest.WorkerKills != cfg.WorkerKills {
		return runSummary{}, errors.New("manifest identity does not match")
	}

	var report reconcile.Report
	if err := readJSONFile(filepath.Join(runDirectory, "reconciliation.json"), &report); err != nil {
		return runSummary{}, err
	}
	if !report.Passed || report.ViolationCount != 0 ||
		report.ExpectedJobs != cfg.Jobs ||
		report.Counts.Accepted != cfg.Jobs ||
		report.Counts.Jobs != cfg.Jobs ||
		report.Counts.Completions != cfg.Jobs {
		return runSummary{}, errors.New("reconciliation report does not pass")
	}
	accepted, err := readAccepted(filepath.Join(runDirectory, "submitted.jsonl"))
	if err != nil {
		return runSummary{}, err
	}
	if len(accepted) != cfg.Jobs {
		return runSummary{}, fmt.Errorf("submitted records=%d want=%d", len(accepted), cfg.Jobs)
	}
	for _, record := range accepted {
		if record.TenantID != manifest.TenantID {
			return runSummary{}, errors.New("submitted record tenant does not match")
		}
	}
	samples, err := readRecoverySamples(filepath.Join(runDirectory, "recovery-samples.jsonl"))
	if err != nil {
		return runSummary{}, err
	}
	if len(samples) == 0 {
		return runSummary{}, errors.New("recovery evidence is empty")
	}
	workerKills, serverKills, expectedSamples, err := readActionEvidence(
		filepath.Join(runDirectory, "events.jsonl"),
	)
	if err != nil {
		return runSummary{}, err
	}
	if workerKills != cfg.WorkerKills || serverKills != 1 {
		return runSummary{}, fmt.Errorf(
			"action counts worker=%d server=%d want=%d and 1",
			workerKills,
			serverKills,
			cfg.WorkerKills,
		)
	}
	values := make([]float64, len(samples))
	observedSamples := make(map[string]struct{}, len(samples))
	for index, sample := range samples {
		key := recoverySampleKey(sample.KillSequence, sample.JobID)
		expected, exists := expectedSamples[key]
		if !exists {
			return runSummary{}, fmt.Errorf("recovery sample %d has no killed lease", index+1)
		}
		if _, duplicate := observedSamples[key]; duplicate {
			return runSummary{}, fmt.Errorf("recovery sample %d is duplicated", index+1)
		}
		observedSamples[key] = struct{}{}
		validatedMapping, mappingErr := newClockMapping(
			sample.ClockMapping.ServerTime,
			sample.ClockMapping.HostLowerBound,
			sample.ClockMapping.HostUpperBound,
		)
		mappedKill := sample.ClockMapping.serverTime(sample.KillConfirmedHostAt)
		recovery := sample.SuccessorLeasedAt.Sub(sample.KillConfirmedAt)
		if sample.Worker == "" || sample.VictimContainerID == "" ||
			sample.KillConfirmedHostAt.IsZero() || sample.KillConfirmedAt.IsZero() ||
			sample.ClockMapping.ServerTime.IsZero() ||
			sample.ClockMapping.HostLowerBound.IsZero() ||
			sample.ClockMapping.HostUpperBound.IsZero() ||
			sample.ClockMapping.HostUpperBound.Before(sample.ClockMapping.HostLowerBound) ||
			sample.ClockMapping.Uncertainty < 0 ||
			sample.ClockMapping.Uncertainty > maxClockMappingUncertainty ||
			mappingErr != nil ||
			validatedMapping.Offset != sample.ClockMapping.Offset ||
			validatedMapping.Uncertainty != sample.ClockMapping.Uncertainty ||
			sample.KilledAttempt != expected.lease.AttemptNo ||
			sample.KilledGeneration != expected.lease.Generation ||
			!sample.KillConfirmedHostAt.Equal(expected.killConfirmedHostAt) ||
			!sample.KillConfirmedAt.Equal(expected.killConfirmedAt) ||
			!clockMappingsEqual(sample.ClockMapping, expected.clockMapping) ||
			!mappedKill.Equal(sample.KillConfirmedAt) ||
			sample.SuccessorAttempt <= sample.KilledAttempt ||
			sample.SuccessorGeneration <= sample.KilledGeneration ||
			!sample.SuccessorLeasedAt.After(
				sample.KillConfirmedAt.Add(sample.ClockMapping.Uncertainty),
			) ||
			sample.SuccessorObservedAt.IsZero() ||
			sample.CompletionAt.Before(sample.SuccessorLeasedAt) ||
			recovery < 0 ||
			sample.RecoveryMS != float64(recovery)/float64(time.Millisecond) {
			return runSummary{}, fmt.Errorf("recovery sample %d is incomplete", index+1)
		}
		values[index] = sample.RecoveryMS
	}
	if len(observedSamples) != len(expectedSamples) {
		return runSummary{}, fmt.Errorf(
			"recovery samples=%d want=%d killed leases",
			len(observedSamples),
			len(expectedSamples),
		)
	}
	return runSummary{
		Run:                run,
		Seed:               seed,
		Project:            manifest.Project,
		TenantID:           manifest.TenantID,
		StartedAt:          manifest.StartedAt,
		CompletedAt:        manifest.CompletedAt,
		Accepted:           len(accepted),
		WorkerKills:        workerKills,
		ServerKills:        serverKills,
		RecoverySamples:    len(samples),
		RecoveryP99MS:      nearestRank(values, 0.99),
		RecoveryTargetMS:   float64(cfg.MaxRecovery) / float64(time.Millisecond),
		ReconciliationPass: true,
		ArtifactDirectory:  runDirectory,
		recoveryValues:     values,
	}, nil
}

func readAccepted(path string) ([]reconcile.AcceptedRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open submitted records: %w", err)
	}
	records, readErr := reconcile.ReadManifest(file)
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close submitted records: %w", closeErr)
	}
	return records, nil
}

func readRecoverySamples(path string) ([]recoverySample, error) {
	var samples []recoverySample
	if err := readJSONLines(path, func(line []byte) error {
		var sample recoverySample
		if err := decodeJSON(line, &sample); err != nil {
			return err
		}
		samples = append(samples, sample)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("read recovery samples: %w", err)
	}
	return samples, nil
}

func readActionEvidence(path string) (int, int, map[string]expectedRecoverySample, error) {
	workerKills := 0
	serverKills := 0
	expectedSamples := make(map[string]expectedRecoverySample)
	err := readJSONLines(path, func(line []byte) error {
		var event actionEvent
		if err := decodeJSON(line, &event); err != nil {
			return err
		}
		switch event.Type {
		case "worker_killed":
			workerKills++
			body, err := json.Marshal(event.Details)
			if err != nil {
				return err
			}
			var details struct {
				KillSequence  int           `json:"kill_sequence"`
				KillConfirmed time.Time     `json:"kill_confirmed_at"`
				ClockMapping  clockMapping  `json:"clock_mapping"`
				ActiveLeases  []activeLease `json:"active_leases"`
			}
			if err := json.Unmarshal(body, &details); err != nil {
				return err
			}
			if details.KillSequence < 1 {
				return errors.New("worker kill sequence is missing")
			}
			for _, lease := range details.ActiveLeases {
				if lease.JobID == "" {
					return errors.New("worker kill has an incomplete active lease")
				}
				key := recoverySampleKey(details.KillSequence, lease.JobID)
				if _, duplicate := expectedSamples[key]; duplicate {
					return fmt.Errorf("killed lease %s is duplicated", key)
				}
				expectedSamples[key] = expectedRecoverySample{
					lease:               lease,
					killConfirmedHostAt: event.ObservedAt,
					killConfirmedAt:     details.KillConfirmed,
					clockMapping:        details.ClockMapping,
				}
			}
		case "server_killed":
			serverKills++
		}
		return nil
	})
	if err != nil {
		return 0, 0, nil, fmt.Errorf("read action events: %w", err)
	}
	if len(expectedSamples) == 0 {
		return 0, 0, nil, errors.New("worker kills captured no active leases")
	}
	return workerKills, serverKills, expectedSamples, nil
}

func recoverySampleKey(killSequence int, jobID string) string {
	return strconv.Itoa(killSequence) + "\x00" + jobID
}

func clockMappingsEqual(left, right clockMapping) bool {
	return left.HostLowerBound.Equal(right.HostLowerBound) &&
		left.HostUpperBound.Equal(right.HostUpperBound) &&
		left.ServerTime.Equal(right.ServerTime) &&
		left.Offset == right.Offset &&
		left.Uncertainty == right.Uncertainty
}

func readJSONLines(path string, consume func([]byte) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	line := 0
	for scanner.Scan() {
		line++
		body := []byte(strings.TrimSpace(scanner.Text()))
		if len(body) == 0 {
			return fmt.Errorf("line %d is empty", line)
		}
		if err := consume(body); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
	}
	return scanner.Err()
}

func readJSONFile(path string, value any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", filepath.Base(path), err)
	}
	defer func() {
		_ = file.Close()
	}()
	decoder := json.NewDecoder(io.LimitReader(file, 16*1024*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	if err := ensureDecoderEOF(decoder); err != nil {
		return fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	return nil
}

func decodeJSON(body []byte, value any) error {
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	return ensureDecoderEOF(decoder)
}

func ensureDecoderEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("multiple JSON values")
}

func finalizeRunArtifacts(runDirectory string, cfg config) error {
	artifacts, err := artifactFiles(runDirectory)
	if err != nil {
		return err
	}
	for _, required := range requiredArtifacts(cfg) {
		path := filepath.Join(runDirectory, filepath.FromSlash(required))
		info, statErr := os.Stat(path)
		if statErr != nil {
			return fmt.Errorf("required artifact %s: %w", required, statErr)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("required artifact %s is not a regular file", required)
		}
	}
	var body strings.Builder
	for _, relative := range artifacts {
		digest, err := fileDigest(filepath.Join(runDirectory, filepath.FromSlash(relative)))
		if err != nil {
			return err
		}
		fmt.Fprintf(&body, "%s  %s\n", digest, relative)
	}
	if err := writeFileAtomic(filepath.Join(runDirectory, checksumFile), []byte(body.String())); err != nil {
		return fmt.Errorf("write checksums: %w", err)
	}
	return nil
}

func verifyChecksums(runDirectory string, cfg config) error {
	file, err := os.Open(filepath.Join(runDirectory, checksumFile))
	if err != nil {
		return fmt.Errorf("open checksums: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()
	expected := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		body := scanner.Text()
		if len(body) < 67 || body[64:66] != "  " {
			return fmt.Errorf("checksum line %d is malformed", line)
		}
		digest := body[:64]
		if _, err := hex.DecodeString(digest); err != nil {
			return fmt.Errorf("checksum line %d has invalid digest", line)
		}
		relative := body[66:]
		if !safeRelativePath(relative) {
			return fmt.Errorf("checksum line %d has unsafe path", line)
		}
		if _, duplicate := expected[relative]; duplicate {
			return fmt.Errorf("checksum path %s is duplicated", relative)
		}
		expected[relative] = digest
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read checksums: %w", err)
	}
	actual, err := artifactFiles(runDirectory)
	if err != nil {
		return err
	}
	if len(expected) != len(actual) {
		return errors.New("checksum file does not cover every artifact")
	}
	for _, relative := range actual {
		want, exists := expected[relative]
		if !exists {
			return fmt.Errorf("artifact %s has no checksum", relative)
		}
		got, err := fileDigest(filepath.Join(runDirectory, filepath.FromSlash(relative)))
		if err != nil {
			return err
		}
		if got != want {
			return fmt.Errorf("artifact %s checksum does not match", relative)
		}
	}
	for _, required := range requiredArtifacts(cfg) {
		path := filepath.Join(runDirectory, filepath.FromSlash(required))
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("required artifact %s: %w", required, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("required artifact %s is not a regular file", required)
		}
	}
	return nil
}

func requiredArtifacts(cfg config) []string {
	result := append([]string(nil), requiredRunArtifactFiles...)
	result = append(result, "database/"+filepath.Base(cfg.DatabasePath))
	return result
}

func artifactFiles(runDirectory string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(runDirectory, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == runDirectory {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact %s is a symbolic link", entry.Name())
		}
		if entry.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("artifact %s is not a regular file", entry.Name())
		}
		relative, err := filepath.Rel(runDirectory, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative != checksumFile {
			files = append(files, relative)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("enumerate run artifacts: %w", err)
	}
	sort.Strings(files)
	return files, nil
}

func fileDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open artifact %s: %w", filepath.Base(path), err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil {
		return "", fmt.Errorf("hash artifact %s: %w", filepath.Base(path), copyErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close artifact %s: %w", filepath.Base(path), closeErr)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func safeRelativePath(path string) bool {
	if path == "" || filepath.IsAbs(filepath.FromSlash(path)) || strings.Contains(path, `\`) {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(path)))
	return clean == path && clean != "." && clean != ".." && !strings.HasPrefix(clean, "../")
}

func writeFileAtomic(path string, body []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".artifact-*.tmp")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer func() {
		_ = os.Remove(temporary)
	}()
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	return nil
}
