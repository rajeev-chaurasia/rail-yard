package p1_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/store"
	sqlitestore "github.com/rajeev-chaurasia/rail-yard/internal/store/sqlite"
	qa "github.com/rajeev-chaurasia/rail-yard/qa/p1"
	_ "modernc.org/sqlite"
)

type sqliteFixture struct {
	path   string
	store  *sqlitestore.Store
	reader *sql.DB
}

func TestSQLiteP1Contract(t *testing.T) {
	suite := qa.ContractSuite{
		NewFixture: newSQLiteFixture,
		LeaseTTL:   3 * time.Second,
	}
	suite.Run(t)
}

func newSQLiteFixture(t testing.TB, _ *qa.FakeClock) (qa.Fixture, error) {
	path := filepath.Join(t.TempDir(), "railyard.db")
	fixture := &sqliteFixture{path: path}
	if err := fixture.open(); err != nil {
		return nil, err
	}
	return fixture, nil
}

func (fixture *sqliteFixture) Store() store.Store {
	return fixture.store
}

func (fixture *sqliteFixture) Reader() *sql.DB {
	return fixture.reader
}

func (fixture *sqliteFixture) Crash(context.Context) error {
	return fixture.closeHandles()
}

func (fixture *sqliteFixture) Reopen(context.Context) error {
	return fixture.open()
}

func (fixture *sqliteFixture) Close() error {
	return fixture.closeHandles()
}

func (fixture *sqliteFixture) open() error {
	jobStore, err := sqlitestore.Open(fixture.path)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	reader, err := sql.Open("sqlite", fixture.path)
	if err != nil {
		_ = jobStore.Close()
		return fmt.Errorf("open reader: %w", err)
	}
	reader.SetMaxOpenConns(1)
	if err := reader.Ping(); err != nil {
		_ = reader.Close()
		_ = jobStore.Close()
		return fmt.Errorf("ping reader: %w", err)
	}
	fixture.store = jobStore
	fixture.reader = reader
	return nil
}

func (fixture *sqliteFixture) closeHandles() error {
	var first error
	if fixture.reader != nil {
		if err := fixture.reader.Close(); err != nil {
			first = err
		}
		fixture.reader = nil
	}
	if fixture.store != nil {
		if err := fixture.store.Close(); err != nil && first == nil {
			first = err
		}
		fixture.store = nil
	}
	return first
}
