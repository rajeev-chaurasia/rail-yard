package trigger

import (
	"testing"
	"time"
)

func TestCronScheduleUsesUTC(t *testing.T) {
	schedule, err := ParseCron("CRON_TZ=America/New_York 0 9 * * *")
	if err != nil {
		t.Fatal(err)
	}
	after := time.Date(2026, time.January, 2, 13, 0, 0, 0, time.UTC)
	got := schedule.Next(after)
	want := time.Date(2026, time.January, 2, 14, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("next = %s, want %s", got, want)
	}
	if got.Location() != time.UTC {
		t.Fatalf("location = %s, want UTC", got.Location())
	}
}

func TestCronOccurrenceKeyIsStable(t *testing.T) {
	nominal := time.Date(2026, time.January, 2, 14, 0, 0, 0, time.UTC)
	first := CronOccurrenceKey("trigger", nominal)
	second := CronOccurrenceKey("trigger", nominal.In(time.FixedZone("offset", -5*60*60)))
	if first != second {
		t.Fatalf("equivalent instants produced %q and %q", first, second)
	}
}

func TestParseCronRejectsInvalidExpression(t *testing.T) {
	if _, err := ParseCron("not a schedule"); err == nil {
		t.Fatal("invalid schedule accepted")
	}
}
