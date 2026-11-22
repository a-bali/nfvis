package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
)

// TestBeginImmediateRespectsItsBudget is the regression test for a retry loop
// that multiplied busy_timeout by the attempt count.
//
// Each contended BEGIN waits for busy_timeout (5s) before failing, so a naive
// retry of six attempts blocked a caller for 30 seconds. That does not recover
// from contention; it stalls the ingest writer and pushes the batch past the
// flush deadline. beginImmediate must fail within a small fixed budget instead.
func TestBeginImmediateRespectsItsBudget(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "beginbudget.db")
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	// A second pool holds the write lock for the duration of the test.
	holder, err := sql.Open("sqlite", "file:"+cfg.Database.Path+
		"?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatalf("holder: %v", err)
	}
	defer holder.Close()
	holder.SetMaxOpenConns(1)
	htx, err := holder.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	defer htx.Rollback()
	if _, err := htx.ExecContext(context.Background(),
		`INSERT INTO flows (received_ms,start_ms,end_ms,time_source,version,exporter,exporter_ip)
		 VALUES (1,1,1,'t',9,'a','a')`); err != nil {
		t.Fatalf("holder insert: %v", err)
	}

	// A generous caller budget: the point is that beginImmediate does not use it
	// all up retrying.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	tx, err := st.beginImmediate(ctx)
	elapsed := time.Since(start)
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("beginImmediate succeeded while another connection held the write lock")
	}
	// One attempt waits busy_timeout (5s) once. The point is that it does not
	// multiply that by a retry count, so allow a little over one timeout but well
	// under two.
	const budget = 7 * time.Second
	if elapsed > budget {
		t.Errorf("beginImmediate blocked for %v; one attempt should cost about one "+
			"busy_timeout (5s), not a multiple of it", elapsed.Round(time.Millisecond))
	}
	t.Logf("failed in %v with %v, as intended", elapsed.Round(time.Millisecond), err)
}

// TestBeginImmediateSucceedsWhenFree is the positive control: the budget must not
// make the normal path fail.
func TestBeginImmediateSucceedsWhenFree(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "beginfree.db")
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := st.beginImmediate(ctx)
	if err != nil {
		t.Fatalf("beginImmediate failed with an uncontended lock: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}
