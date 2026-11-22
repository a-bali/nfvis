package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
)

// TestBeginImmediateWaitsAtMostOneBusyTimeout pins the cost of a contended
// BEGIN. The comment on beginImmediate claims one attempt costing one
// busy_timeout (5s), not six attempts costing 30s.
//
// Measured: a held write lock makes BeginTx return after ~5s (the full
// busy_timeout), which is why a retry loop multiplies the stall. This test fails
// if the retry is reinstated, and it also fails if busy_timeout is lost from the
// DSN — in that case BEGIN returns immediately and the wait collapses to
// milliseconds, which would mean writers fail on any contention instead of
// queueing.
func TestBeginImmediateWaitsAtMostOneBusyTimeout(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	cfg := config.Default()
	cfg.Database.Path = path
	cfg.DNS.Enabled = true
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	// Hold the write lock from a separate connection.
	holder, err := sql.Open("sqlite", "file:"+path+"?_txlock=immediate&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open holder: %v", err)
	}
	defer holder.Close()
	holder.SetMaxOpenConns(1)
	hctx := context.Background()
	htx, err := holder.BeginTx(hctx, nil)
	if err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	defer func() { _ = htx.Rollback() }()
	if _, err := htx.ExecContext(hctx, `CREATE TABLE IF NOT EXISTS blocker(x INTEGER)`); err != nil {
		t.Fatalf("holder write: %v", err)
	}

	start := time.Now()
	tx, err := st.beginImmediate(context.Background())
	elapsed := time.Since(start)
	if err == nil {
		_ = tx.Rollback()
		t.Fatalf("beginImmediate succeeded against a held write lock after %v; the lock was not held", elapsed)
	}

	// One busy_timeout is 5s. Allow generous slack above, but the point is that a
	// second attempt would push this past 10s.
	if elapsed > 9*time.Second {
		t.Errorf("contended BEGIN took %v; more than one busy_timeout (5s) elapsed, which "+
			"means beginImmediate retried — every extra attempt costs another full 5s and "+
			"stalls the single ingest goroutine", elapsed)
	}
	// Below 1s would mean busy_timeout is not applied to BEGIN at all, so writers
	// would fail instantly under contention instead of waiting.
	if elapsed < time.Second {
		t.Errorf("contended BEGIN failed after only %v; busy_timeout does not appear to "+
			"apply to BEGIN, so concurrent writers would fail rather than queue", elapsed)
	}
	t.Logf("contended BEGIN cost %v (one busy_timeout = 5s)", elapsed.Round(time.Millisecond))
}
