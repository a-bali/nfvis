package ingest

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
)

// lockTestCollector builds a collector over a fresh database whose busy_timeout is
// short, so the test does not burn 5s per attempt while still exercising the real
// code path.
func lockTestCollector(t *testing.T) *Collector {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "lock.db")
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return New(cfg, nil, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// holdWriteLock takes and holds the write lock from a separate connection,
// returning a function that releases it.
func holdWriteLock(t *testing.T, st *store.Store, path string) func() {
	t.Helper()
	holder, err := sql.Open("sqlite", "file:"+path+"?_txlock=immediate")
	if err != nil {
		t.Fatalf("open holder: %v", err)
	}
	holder.SetMaxOpenConns(1)
	ctx := context.Background()
	tx, err := holder.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS blocker(x INTEGER)`); err != nil {
		t.Fatalf("holder write: %v", err)
	}
	return func() {
		_ = tx.Rollback()
		_ = holder.Close()
	}
}

// TestFlushDoesNotRetryAnExhaustedLockWait is the regression test for a stall that
// was three busy_timeouts long.
//
// beginImmediate makes a single attempt, but insertWithRetry retried on IsBusy up
// to three times. BEGIN IMMEDIATE is subject to busy_timeout, so each retry paid
// the full wait: a contention event that should stall a flush for one timeout
// stalled it for three (measured 15.49s at the 5s default). Retrying cannot help —
// the lock was held for the entire timeout, so there is no transient state left to
// recover from.
//
// Elapsed time and the sentinel are both asserted, so this fails if the retry comes
// back and also if the error stops being marked.
func TestFlushDoesNotRetryAnExhaustedLockWait(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the 5s busy_timeout by design")
	}
	// busy_timeout is a hard-coded DSN literal, so the test inherits the real 5s and
	// the three-attempt shape would take 15s. Bound the run instead: one attempt must
	// finish well inside it, three cannot.
	busy := 5 * time.Second // the DSN's busy_timeout(5000)
	c := lockTestCollector(t)
	release := holdWriteLock(t, c.store, c.cfg.Database.Path)
	defer release()

	start := time.Now()
	err := c.insertWithRetry(context.Background(), []model.Flow{{}})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("insert succeeded while the write lock was held (after %v)", elapsed)
	}
	if !errors.Is(err, store.ErrLockWaitExhausted) {
		t.Errorf("error %v is not marked ErrLockWaitExhausted, so callers cannot distinguish an "+
			"exhausted wait from transient contention and will retry it", err)
	}
	// One attempt ~5s; three ~15s. 9s sits clearly between them.
	if elapsed > 9*time.Second {
		t.Errorf("flush stalled for %v with busy_timeout=%v; more than one timeout elapsed, so "+
			"insertWithRetry retried a wait that had already been paid in full", elapsed, busy)
	}
	t.Logf("flush failed after %v with busy_timeout=%v (single attempt)", elapsed.Round(time.Millisecond), busy)
}
