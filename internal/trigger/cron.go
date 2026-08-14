package trigger

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

type CronSchedule struct {
	expression string
	schedule   cron.Schedule
}

func ParseCron(expression string) (CronSchedule, error) {
	parser := cron.NewParser(
		cron.Minute |
			cron.Hour |
			cron.Dom |
			cron.Month |
			cron.Dow |
			cron.Descriptor,
	)
	schedule, err := parser.Parse(expression)
	if err != nil {
		return CronSchedule{}, fmt.Errorf("parse cron expression: %w", err)
	}
	return CronSchedule{expression: expression, schedule: schedule}, nil
}

func (schedule CronSchedule) Next(after time.Time) time.Time {
	return schedule.schedule.Next(after).UTC()
}

func (schedule CronSchedule) Expression() string {
	return schedule.expression
}

func CronOccurrenceKey(triggerID string, nominal time.Time) string {
	sum := sha256.Sum256([]byte(triggerID + "\x00" + nominal.UTC().Format(time.RFC3339Nano)))
	return "cron:" + hex.EncodeToString(sum[:])
}
