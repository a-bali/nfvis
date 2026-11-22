package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/a-bali/nfvis/internal/config"
)

// openWithPragmas opens a store and returns it, so a test can read back the
// effective pragma values from SQLite rather than from the config or the DSN.
func openWithPragmas(t *testing.T, mutate func(*config.Config)) *Store {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "pragmas.db")
	if mutate != nil {
		mutate(&cfg)
	}
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestMmapSizeDefaultPreservesPreviousBehaviour pins the upgrade path: a config
// that says nothing must produce exactly the values that used to be hard-coded,
// or an upgrade would silently change the memory profile of every deployment.
func TestMmapSizeDefaultPreservesPreviousBehaviour(t *testing.T) {
	st := openWithPragmas(t, nil)
	ctx := context.Background()

	var mmap int64
	if err := st.DB().QueryRowContext(ctx, "PRAGMA mmap_size").Scan(&mmap); err != nil {
		t.Fatalf("read mmap_size: %v", err)
	}
	if want := int64(1024 * 1024 * 1024); mmap != want {
		t.Errorf("default mmap_size = %d, want %d (the previous hard-coded value)", mmap, want)
	}

	var cache int64
	if err := st.DB().QueryRowContext(ctx, "PRAGMA cache_size").Scan(&cache); err != nil {
		t.Fatalf("read cache_size: %v", err)
	}
	// PRAGMA cache_size reports the value as set, so a configured 128 MiB appears
	// as -131072 KiB — exactly the literal that used to be hard-coded.
	if want := int64(-128 * 1024); cache != want {
		t.Errorf("default cache_size = %d, want %d (128 MiB in KiB)", cache, want)
	}
}

// TestMmapSizeHonoursConfiguredValue proves the setting reaches SQLite, which is
// the whole point: a config key that is parsed and then ignored is worse than no
// key at all.
func TestMmapSizeHonoursConfiguredValue(t *testing.T) {
	for _, mb := range []int{256, 64, 0} {
		st := openWithPragmas(t, func(c *config.Config) {
			*c.Database.MmapSizeMB = mb
		})
		var got int64
		if err := st.DB().QueryRowContext(context.Background(), "PRAGMA mmap_size").Scan(&got); err != nil {
			t.Fatalf("read mmap_size: %v", err)
		}
		if want := int64(mb) * 1024 * 1024; got != want {
			t.Errorf("mmap_size_mb=%d produced mmap_size=%d, want %d", mb, got, want)
		}
	}
}

// TestCacheSizeHonoursConfiguredValue checks the KiB conversion, which is the
// easy thing to get wrong: SQLite reads a negative cache_size as KiB and a
// positive one as pages.
func TestCacheSizeHonoursConfiguredValue(t *testing.T) {
	st := openWithPragmas(t, func(c *config.Config) {
		*c.Database.CacheSizeMB = 16
	})
	var got int64
	if err := st.DB().QueryRowContext(context.Background(), "PRAGMA cache_size").Scan(&got); err != nil {
		t.Fatalf("read cache_size: %v", err)
	}
	if want := int64(-16 * 1024); got != want {
		t.Errorf("cache_size_mb=16 produced %d, want %d (16 MiB in KiB)", got, want)
	}
	if got > 0 {
		t.Errorf("cache_size = %d is positive, which SQLite reads as pages not KiB", got)
	}
}

// TestJournalModeAndBusyTimeoutSurviveTheRefactor guards the pragmas that are not
// tunable but must not be lost when the pragma list is rebuilt.
//
// It deliberately does NOT claim to cover _txlock: transaction mode is not
// observable through a PRAGMA (there is no such statement), so it cannot be read
// back. TestStoreUsesImmediateTxlock in txlock_test.go pins it behaviourally by
// reproducing the read-then-write interleaving that fails without it, and that is
// the test which fails if _txlock=immediate is removed.
func TestJournalModeAndBusyTimeoutSurviveTheRefactor(t *testing.T) {
	st := openWithPragmas(t, nil)
	ctx := context.Background()

	var mode string
	if err := st.DB().QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("read journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Errorf("journal_mode = %q, want wal", mode)
	}

	var busy int64
	if err := st.DB().QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busy); err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}
	if busy != 5000 {
		t.Errorf("busy_timeout = %d, want 5000", busy)
	}

	var sync int64
	if err := st.DB().QueryRowContext(ctx, "PRAGMA synchronous").Scan(&sync); err != nil {
		t.Fatalf("read synchronous: %v", err)
	}
	// synchronous=NORMAL is 1; the WAL default of FULL(2) would cost a sync per
	// commit and undo the batching work in the DNS path.
	if sync != 1 {
		t.Errorf("synchronous = %d, want 1 (NORMAL)", sync)
	}
}
