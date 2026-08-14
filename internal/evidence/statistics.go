package evidence

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

type DurationDistribution struct {
	Count int           `json:"count"`
	Min   time.Duration `json:"min_ns"`
	P50   time.Duration `json:"p50_ns"`
	P95   time.Duration `json:"p95_ns"`
	P99   time.Duration `json:"p99_ns"`
	Max   time.Duration `json:"max_ns"`
}

func NearestRank(samples []time.Duration, percentile int) (time.Duration, error) {
	if len(samples) == 0 {
		return 0, errors.New("nearest rank requires at least one sample")
	}
	if percentile < 1 || percentile > 100 {
		return 0, fmt.Errorf("percentile must be between 1 and 100, got %d", percentile)
	}

	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	rank := (percentile*len(ordered) + 99) / 100
	return ordered[rank-1], nil
}

func SummarizeDurations(samples []time.Duration) (DurationDistribution, error) {
	if len(samples) == 0 {
		return DurationDistribution{}, errors.New("duration summary requires at least one sample")
	}
	for _, sample := range samples {
		if sample < 0 {
			return DurationDistribution{}, errors.New("duration summary does not accept negative samples")
		}
	}

	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	p50, _ := NearestRank(ordered, 50)
	p95, _ := NearestRank(ordered, 95)
	p99, _ := NearestRank(ordered, 99)
	return DurationDistribution{
		Count: len(ordered),
		Min:   ordered[0],
		P50:   p50,
		P95:   p95,
		P99:   p99,
		Max:   ordered[len(ordered)-1],
	}, nil
}

func Median(values []float64) (float64, error) {
	if len(values) == 0 {
		return 0, errors.New("median requires at least one value")
	}
	ordered := slices.Clone(values)
	for _, value := range ordered {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, errors.New("median requires finite values")
		}
	}
	slices.Sort(ordered)
	middle := len(ordered) / 2
	if len(ordered)%2 == 1 {
		return ordered[middle], nil
	}
	return (ordered[middle-1] + ordered[middle]) / 2, nil
}

func RatePerMinute(count int, first, last time.Time) (float64, error) {
	if count < 2 {
		return 0, errors.New("rate requires at least two samples")
	}
	interval := last.Sub(first)
	if interval <= 0 {
		return 0, errors.New("rate interval must be positive")
	}
	return float64(count) / interval.Minutes(), nil
}
