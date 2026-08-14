package retry

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"
)

var caps = [...]time.Duration{
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
}

func Delay(jobID string, failedAttempt int) (time.Duration, error) {
	if jobID == "" {
		return 0, errors.New("job ID is required")
	}
	if failedAttempt < 1 {
		return 0, errors.New("failed attempt must be positive")
	}
	index := failedAttempt - 1
	if index >= len(caps) {
		index = len(caps) - 1
	}
	cap := caps[index]

	input := []byte(jobID)
	input = append(input, 0)
	var attempt [8]byte
	binary.BigEndian.PutUint64(attempt[:], uint64(failedAttempt))
	input = append(input, attempt[:]...)
	sum := sha256.Sum256(input)

	basisPoints := int64(8_000 + binary.BigEndian.Uint16(sum[:2])%2_001)
	return time.Duration(int64(cap) * basisPoints / 10_000), nil
}

func ReleaseAt(now time.Time, jobID string, failedAttempt int) (time.Time, error) {
	delay, err := Delay(jobID, failedAttempt)
	if err != nil {
		return time.Time{}, err
	}
	return now.UTC().Add(delay), nil
}
