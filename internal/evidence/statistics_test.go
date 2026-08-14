package evidence

import (
	"math"
	"testing"
	"time"
)

func TestNearestRank(t *testing.T) {
	t.Parallel()

	samples := make([]time.Duration, 100)
	for index := range samples {
		samples[index] = time.Duration(100-index) * time.Millisecond
	}

	tests := []struct {
		percentile int
		want       time.Duration
	}{
		{percentile: 50, want: 50 * time.Millisecond},
		{percentile: 95, want: 95 * time.Millisecond},
		{percentile: 99, want: 99 * time.Millisecond},
		{percentile: 100, want: 100 * time.Millisecond},
	}
	for _, test := range tests {
		got, err := NearestRank(samples, test.percentile)
		if err != nil {
			t.Fatalf("NearestRank(%d): %v", test.percentile, err)
		}
		if got != test.want {
			t.Errorf("NearestRank(%d) = %s, want %s", test.percentile, got, test.want)
		}
	}
}

func TestNearestRankRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	if _, err := NearestRank(nil, 50); err == nil {
		t.Error("NearestRank(nil, 50) succeeded")
	}
	if _, err := NearestRank([]time.Duration{time.Second}, 0); err == nil {
		t.Error("NearestRank percentile 0 succeeded")
	}
}

func TestMedian(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{name: "odd", values: []float64{30, 10, 20}, want: 20},
		{name: "even", values: []float64{4, 1, 3, 2}, want: 2.5},
	}
	for _, test := range tests {
		got, err := Median(test.values)
		if err != nil {
			t.Fatalf("%s: Median: %v", test.name, err)
		}
		if got != test.want {
			t.Errorf("%s: Median = %v, want %v", test.name, got, test.want)
		}
	}

	if _, err := Median([]float64{math.NaN()}); err == nil {
		t.Error("Median accepted NaN")
	}
}

func TestRatePerMinuteUsesFirstToLastInterval(t *testing.T) {
	t.Parallel()

	first := time.Unix(1_000, 0)
	got, err := RatePerMinute(500, first, first.Add(30*time.Second))
	if err != nil {
		t.Fatalf("RatePerMinute: %v", err)
	}
	if got != 1_000 {
		t.Fatalf("RatePerMinute = %v, want 1000", got)
	}
}
