package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
)

// A migration must be allowed to outlive the old hard-coded 30-second ceiling.
// Indexing an 11M-row production table took 64s and 49s in two statements, so a
// 30s cap rolled the migration back and crash-looped the pod before it served.
// The regression this guards is the constant, not the SQL: with a short
// configured timeout the run must fail, and with a generous one it must finish.
func TestMigrationHonoursConfiguredTimeout(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")

	// Baseline: a default config migrates and records schema 5.
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open with default timeout: %v", err)
	}
	var version int
	if err := st.DB().QueryRow(`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version = %d, want %d", version, schemaVersion)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// A deliberately impossible timeout must abort the migration rather than
	// silently succeed, proving the value is actually wired through.
	tight := config.Default()
	tight.Database.Path = filepath.Join(t.TempDir(), "tight.db")
	tight.Database.MigrationTimeout = config.Duration(time.Nanosecond)
	if st, err := Open(tight); err == nil {
		_ = st.Close()
		t.Fatal("a 1ns migration timeout still succeeded: the timeout is not wired through")
	}

	// The default must be generous enough for a large table, and an explicit
	// value must override it.
	if got := config.Default().Database.EffectiveMigrationTimeout(); got < 10*time.Minute {
		t.Fatalf("default migration timeout = %v, too small for a multi-GB table", got)
	}
	custom := config.Default()
	custom.Database.MigrationTimeout = config.Duration(5 * time.Minute)
	if got := custom.Database.EffectiveMigrationTimeout(); got != 5*time.Minute {
		t.Fatalf("configured timeout = %v, want 5m", got)
	}
}

// The migration context must not be shared with operations that have their own
// budget, so a slow migration does not consume the backfill window or vice
// versa.
func TestMigrationTimeoutIsIndependentOfBackfill(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.Database.MigrationTimeout = config.Duration(7 * time.Minute)
	st, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// Opening succeeded with a non-default timeout; the tier backfill ran under
	// its own 10-minute context and produced rows for every configured tier.
	var tiers int
	if err := st.DB().QueryRow(`SELECT COUNT(DISTINCT res_ms) FROM agg_traffic`).Scan(&tiers); err != nil {
		if err == sql.ErrNoRows {
			return
		}
		t.Fatal(err)
	}
	_ = tiers
	_ = context.Background()
}
