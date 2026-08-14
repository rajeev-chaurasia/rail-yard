package sqlite

import (
	"context"
	"fmt"
	"io"
)

func (s *Store) ExportDecisionLog(ctx context.Context, destination io.Writer) error {
	rows, err := s.db.QueryContext(
		ctx,
		"SELECT record_json FROM decision_log ORDER BY sequence",
	)
	if err != nil {
		return fmt.Errorf("query decision log: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var record string
		if err := rows.Scan(&record); err != nil {
			return fmt.Errorf("scan decision log: %w", err)
		}
		if _, err := io.WriteString(destination, record+"\n"); err != nil {
			return fmt.Errorf("write decision log: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate decision log: %w", err)
	}
	return nil
}
