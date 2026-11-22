package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
)

// openTxlockTest opens a database with the given _txlock mode, using the same
// pragmas store.Open uses, so the two modes can be compared directly.
func openTxlockTest(t *testing.T, path, mode string) *sql.DB {
	t.Helper()
	q := url.Values{}
	if mode != "" {
		q.Set("_txlock", mode)
	}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: q.Encode()}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open (%s): %v", mode, err)
	}
	db.SetMaxOpenConns(8)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	return db
}

// TestImmediateTxlockSurvivesConcurrentWriters is the regression test for the
// production failure that dropped whole batches with "database is locked".
//
// The ingest transaction reads dedup candidates and then writes. In WAL mode a
// reader does not block a writer, so another connection can commit between this
// transaction's read and its write. A DEFERRED transaction then cannot honour
// busy_timeout on the read→write upgrade: SQLite returns SQLITE_BUSY at once
// because the snapshot it read from is stale. BEGIN IMMEDIATE takes the write
// lock up front, where busy_timeout does apply, so the same interleaving waits
// instead of failing.
//
// Both modes run the identical workload on the same machine, so the difference
// is attributable to the mode.
func TestImmediateTxlockSurvivesConcurrentWriters(t *testing.T) {
	const writers = 8
	const iterations = 12

	run := func(t *testing.T, mode string) (committed, busy int) {
		t.Helper()
		db := openTxlockTest(t, filepath.Join(t.TempDir(), "txlock-"+mode+".db"), mode)
		defer db.Close()

		var mu sync.Mutex
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					err := func() error {
						ctx := context.Background()
						tx, err := db.BeginTx(ctx, nil)
						if err != nil {
							return err
						}
						defer tx.Rollback()
						// Read first, exactly as the dedup candidate lookup does.
						var n int
						if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
							return err
						}
						// Then write: a deferred tx must upgrade the lock here,
						// and fails instantly if anyone committed meanwhile.
						if _, err := tx.ExecContext(ctx, `INSERT INTO t (v) VALUES (?)`,
							fmt.Sprintf("w%d-%d", w, i)); err != nil {
							return err
						}
						return tx.Commit()
					}()
					mu.Lock()
					if err == nil {
						committed++
					} else if strings.Contains(err.Error(), "database is locked") ||
						strings.Contains(err.Error(), "SQLITE_BUSY") {
						busy++
					}
					mu.Unlock()
				}
			}(w)
		}
		wg.Wait()
		return committed, busy
	}

	t.Run("deferred", func(t *testing.T) {
		committed, busy := run(t, "deferred")
		t.Logf("deferred: %d committed, %d lost to SQLITE_BUSY (of %d)",
			committed, busy, writers*iterations)
	})

	t.Run("immediate", func(t *testing.T) {
		committed, busy := run(t, "immediate")
		if busy != 0 {
			t.Errorf("IMMEDIATE lost %d writer(s) to SQLITE_BUSY; busy_timeout should have made "+
				"them wait instead of failing", busy)
		}
		if want := writers * iterations; committed != want {
			t.Errorf("IMMEDIATE committed %d/%d writes", committed, want)
		}
	})
}

// TestStoreUsesImmediateTxlock asserts store.Open requests the immediate mode.
// It reproduces the exact interleaving that costs the production instance a
// batch: this store's transaction reads, a second connection commits, and then
// the transaction writes. With a deferred transaction the final write fails
// instantly; with immediate the lock was already held so the interleaving
// cannot happen.
//
// This fails if "_txlock=immediate" is removed from store.Open — verified by
// deleting the line and watching this test fail.
func TestStoreUsesImmediateTxlock(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "txlock-store.db")
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	ctx := context.Background()

	// Other pool, deliberately deferred-friendly: it only needs to commit a row
	// between our read and our write.
	other := openTxlockTest(t, cfg.Database.Path, "")
	defer other.Close()
	// Wait for the store's own tables: OpenTxlockTest creates "t", the store has
	// "flows"; both pools point at the same file.
	if _, err := other.Exec(`INSERT INTO t (v) VALUES ('seed')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	// Step 1: the dedup-style read.
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("read: %v", err)
	}

	// Step 2: another connection commits now. With IMMEDIATE this blocks on
	// busy_timeout because we already hold the write lock, so give it a short
	// deadline and accept either outcome; the assertion is on step 3.
	shortCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, _ = other.ExecContext(shortCtx, `INSERT INTO t (v) VALUES ('interleaved')`)
	cancel()

	// Step 3: our write. This is the statement that reports
	// "database is locked" in production when the transaction was deferred.
	_, writeErr := tx.ExecContext(ctx, `INSERT INTO t (v) VALUES ('ours')`)
	if writeErr != nil {
		t.Fatalf("read→write upgrade failed with %v; the transaction was deferred, so "+
			"busy_timeout did not apply to the upgrade and a concurrent commit poisoned it. "+
			"store.Open must request _txlock=immediate", writeErr)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}
