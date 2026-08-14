package domain

import "time"

type CronTrigger struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id"`
	Expression string    `json:"expression"`
	Job        JobSpec   `json:"job"`
	NextFireAt time.Time `json:"next_fire_at"`
	Enabled    bool      `json:"enabled"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}
