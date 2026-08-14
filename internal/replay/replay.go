package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/rajeev-chaurasia/rail-yard/internal/decisionlog"
	"github.com/rajeev-chaurasia/rail-yard/internal/scheduler"
)

type Result struct {
	Records int    `json:"records"`
	Digest  string `json:"digest"`
}

type DivergenceError struct {
	Sequence int64
	Offset   int
	Expected byte
	Actual   byte
}

func (err *DivergenceError) Error() string {
	return fmt.Sprintf(
		"decision %d diverged at byte %d: expected 0x%02x, got 0x%02x",
		err.Sequence,
		err.Offset,
		err.Expected,
		err.Actual,
	)
}

func Run(input io.Reader, output io.Writer) (Result, error) {
	records, err := decisionlog.ReadAll(input)
	if err != nil {
		return Result{}, err
	}

	hasher := sha256.New()
	target := io.MultiWriter(output, hasher)
	for _, record := range records {
		decision, err := scheduler.Decide(record.Input)
		if err != nil {
			return Result{}, fmt.Errorf("recompute decision %d: %w", record.Sequence, err)
		}
		actual, err := decisionlog.CanonicalDecision(decision)
		if err != nil {
			return Result{}, err
		}
		expected, err := decisionlog.CanonicalDecision(record.Decision)
		if err != nil {
			return Result{}, err
		}
		if !bytes.Equal(actual, expected) {
			return Result{}, divergence(record.Sequence, expected, actual)
		}
		if _, err := target.Write(actual); err != nil {
			return Result{}, fmt.Errorf("write replay output: %w", err)
		}
	}

	return Result{
		Records: len(records),
		Digest:  hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func divergence(sequence int64, expected, actual []byte) error {
	limit := len(expected)
	if len(actual) < limit {
		limit = len(actual)
	}
	for index := 0; index < limit; index++ {
		if expected[index] != actual[index] {
			return &DivergenceError{
				Sequence: sequence,
				Offset:   index,
				Expected: expected[index],
				Actual:   actual[index],
			}
		}
	}
	var expectedByte byte
	var actualByte byte
	if limit < len(expected) {
		expectedByte = expected[limit]
	}
	if limit < len(actual) {
		actualByte = actual[limit]
	}
	return &DivergenceError{
		Sequence: sequence,
		Offset:   limit,
		Expected: expectedByte,
		Actual:   actualByte,
	}
}
