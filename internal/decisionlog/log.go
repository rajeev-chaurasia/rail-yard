package decisionlog

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/rajeev-chaurasia/rail-yard/internal/scheduler"
)

const SchemaVersion = 1

type Record struct {
	SchemaVersion int                `json:"schema_version"`
	Sequence      int64              `json:"sequence"`
	PreviousHash  string             `json:"previous_hash"`
	Input         scheduler.Snapshot `json:"input"`
	Decision      scheduler.Decision `json:"decision"`
	Hash          string             `json:"hash"`
}

type hashMaterial struct {
	SchemaVersion int                `json:"schema_version"`
	Sequence      int64              `json:"sequence"`
	PreviousHash  string             `json:"previous_hash"`
	Input         scheduler.Snapshot `json:"input"`
	Decision      scheduler.Decision `json:"decision"`
}

func NewRecord(previousHash string, input scheduler.Snapshot, decision scheduler.Decision) (Record, error) {
	if input.Sequence != decision.Sequence {
		return Record{}, errors.New("input and decision sequences differ")
	}
	material := hashMaterial{
		SchemaVersion: SchemaVersion,
		Sequence:      input.Sequence,
		PreviousHash:  previousHash,
		Input:         normalizeSnapshot(input),
		Decision:      normalizeDecision(decision),
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		return Record{}, fmt.Errorf("marshal decision record: %w", err)
	}
	hash := sha256.Sum256(encoded)
	return Record{
		SchemaVersion: material.SchemaVersion,
		Sequence:      material.Sequence,
		PreviousHash:  material.PreviousHash,
		Input:         material.Input,
		Decision:      material.Decision,
		Hash:          hex.EncodeToString(hash[:]),
	}, nil
}

func (record Record) Verify(previousHash string) error {
	if record.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported decision log schema %d", record.SchemaVersion)
	}
	if record.PreviousHash != previousHash {
		return fmt.Errorf("previous hash %q, want %q", record.PreviousHash, previousHash)
	}
	expected, err := NewRecord(record.PreviousHash, record.Input, record.Decision)
	if err != nil {
		return err
	}
	if record.Hash != expected.Hash {
		return fmt.Errorf("record hash %q, want %q", record.Hash, expected.Hash)
	}
	return nil
}

func CanonicalDecision(decision scheduler.Decision) ([]byte, error) {
	encoded, err := json.Marshal(normalizeDecision(decision))
	if err != nil {
		return nil, fmt.Errorf("marshal canonical decision: %w", err)
	}
	return append(encoded, '\n'), nil
}

type Writer struct {
	mu       sync.Mutex
	writer   io.Writer
	lastHash string
}

func NewWriter(writer io.Writer, previousHash string) *Writer {
	return &Writer{writer: writer, lastHash: previousHash}
}

func (writer *Writer) Append(input scheduler.Snapshot, decision scheduler.Decision) (Record, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()

	record, err := NewRecord(writer.lastHash, input, decision)
	if err != nil {
		return Record{}, err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return Record{}, fmt.Errorf("marshal decision record: %w", err)
	}
	encoded = append(encoded, '\n')
	if _, err := writer.writer.Write(encoded); err != nil {
		return Record{}, fmt.Errorf("write decision record: %w", err)
	}
	writer.lastHash = record.Hash
	return record, nil
}

func (writer *Writer) LastHash() string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.lastHash
}

func ReadAll(reader io.Reader) ([]Record, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	records := make([]Record, 0)
	previousHash := ""
	var previousSequence int64

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var record Record
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return nil, fmt.Errorf("decode record %d: %w", len(records)+1, err)
		}
		if len(records) == 0 && record.Sequence != 1 {
			return nil, fmt.Errorf("decision log starts at sequence %d", record.Sequence)
		}
		if len(records) > 0 && record.Sequence != previousSequence+1 {
			return nil, fmt.Errorf("record sequence %d follows %d", record.Sequence, previousSequence)
		}
		if err := record.Verify(previousHash); err != nil {
			return nil, fmt.Errorf("verify record %d: %w", record.Sequence, err)
		}
		records = append(records, record)
		previousHash = record.Hash
		previousSequence = record.Sequence
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan decision log: %w", err)
	}
	return records, nil
}

func normalizeSnapshot(input scheduler.Snapshot) scheduler.Snapshot {
	return scheduler.CanonicalSnapshot(input)
}

func normalizeDecision(decision scheduler.Decision) scheduler.Decision {
	if decision.Grants == nil {
		decision.Grants = []scheduler.Grant{}
	}
	if decision.Queues == nil {
		decision.Queues = []scheduler.QueueState{}
	}
	return decision
}
