// Package store persists normalized flows and hourly aggregates in SQLite.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	_ "modernc.org/sqlite"
)

// Store owns the SQLite connection pool.
type Store struct {
	db    *sql.DB
	cfg   config.Config
	path  string
	tiers []trafficTier

	// Row counts are an O(number of rows) scan, so the dashboard's five-second
	// polling must not run them every time. They are cached briefly; sizes are
	// always read fresh.
	statsMu     sync.Mutex
	statsAt     time.Time
	statsCached Stats

	// nat is nil unless pre-NAT/post-NAT duplicate suppression is configured.
	nat *natState
	// pc is nil unless parent/child duplicate suppression is configured with at
	// least one per-exporter interface link.
	pc *pcState
}

// statsTTL bounds how stale the cached row counts may be.
const statsTTL = 15 * time.Second

// trafficTier is the resolved form of one configured chart-resolution tier.
type trafficTier struct {
	resMS       int64
	retentionMS int64
}

func trafficTiers(cfg config.Config) []trafficTier {
	configured := cfg.Database.TrafficTiers()
	tiers := make([]trafficTier, 0, len(configured))
	for _, tier := range configured {
		tiers = append(tiers, trafficTier{
			resMS:       tier.Bucket.Duration().Milliseconds(),
			retentionMS: tier.Retention.Duration().Milliseconds(),
		})
	}
	if len(tiers) == 0 {
		tiers = append(tiers, trafficTier{resMS: hourMS, retentionMS: cfg.Database.Retention.Hourly.Duration().Milliseconds()})
	}
	return tiers
}

// Open creates the database directory, applies migrations, and configures WAL.
func Open(cfg config.Config) (*Store, error) {
	path, err := filepath.Abs(cfg.Database.Path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}
	q := url.Values{}
	// Every transaction this process opens writes. A deferred transaction whose
	// first statement is a read — which is what the dedup lookup is — cannot
	// honour busy_timeout when it later tries to upgrade to a write: SQLite
	// returns SQLITE_BUSY immediately instead of waiting, because the snapshot it
	// read from is already stale. With eight DNS writers committing alongside the
	// ingest batch this produced periodic "database is locked" failures that
	// discarded whole batches. Asking for BEGIN IMMEDIATE makes each transaction
	// take the write lock up front, where busy_timeout does apply, so contention
	// waits and succeeds instead of failing instantly.
	q.Set("_txlock", "immediate")
	// Both pragmas are configurable because they, not the row count, decide what
	// Kubernetes reports for this process: mmap'd database pages are charged as
	// file-backed memory and cache_size is unreclaimable heap. Measured on a
	// 1.2 GB database, the old 1024 MiB / 128 MiB pair kept 1156 MiB resident
	// where 256 MiB / 16 MiB kept 285 MiB, at 11-21% higher cost on wide scans.
	pragmas := []string{
		"busy_timeout(5000)",
		"journal_mode(WAL)",
		"synchronous(NORMAL)",
		"foreign_keys(ON)",
		"temp_store(MEMORY)",
		"wal_autocheckpoint(1000)",
	}
	if mb := cfg.Database.EffectiveCacheSizeMB(); mb > 0 {
		// SQLite takes a negative cache_size in KiB; a positive value would mean
		// "pages" and silently become a different size.
		pragmas = append(pragmas, fmt.Sprintf("cache_size(-%d)", int64(mb)*1024))
	} else if mb < 0 {
		// store.Open is exported and several callers (the seeder, most store
		// tests) build a config without calling Validate. A negative value would
		// format as `cache_size(--16384)`, which SQLite rejects silently — the
		// connection just keeps its default and the setting appears to be
		// ignored. Refuse instead.
		return nil, fmt.Errorf("database.cache_size_mb must be positive, got %d", mb)
	}
	// mmap_size 0 disables memory mapping, which is a supported choice: it trades
	// 10-20% on wide scans for the entire mapping's resident footprint.
	// int64 conversion: mb*1024*1024 overflows int on 32-bit builds, which would
	// emit mmap_size(0) or a negative value for a large configuration.
	pragmas = append(pragmas, fmt.Sprintf("mmap_size(%d)", int64(cfg.Database.EffectiveMmapSizeMB())*1024*1024))
	for _, pragma := range pragmas {
		q.Add("_pragma", pragma)
	}
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: q.Encode()}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(0)

	pingCtx, cancelPing := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelPing()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	// Migrations are one-time startup work whose cost scales with the existing
	// data, not with a request budget: indexing an 11M-row table takes minutes.
	// The timeout is configurable (database.migration_timeout) with a generous
	// default, because exceeding it rolls back and exits before serving.
	migrateCtx, cancelMigrate := context.WithTimeout(context.Background(), cfg.Database.EffectiveMigrationTimeout())
	defer cancelMigrate()
	// Log the budget so an operator reading startup logs knows what will happen
	// if a migration overruns, rather than discovering it as a crash loop.
	slog.Info("migration budget", "timeout", cfg.Database.EffectiveMigrationTimeout().String())
	if err := migrate(migrateCtx, db); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			slog.Error("migration exceeded its budget; the transaction was rolled back",
				"timeout", cfg.Database.EffectiveMigrationTimeout().String(),
				"hint", "raise database.migration_timeout")
		}
		_ = db.Close()
		return nil, err
	}
	tiers := trafficTiers(cfg)
	backfillCtx, cancelBackfill := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancelBackfill()
	if err := backfillTrafficTiers(backfillCtx, db, tiers); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, cfg: cfg, path: path, tiers: tiers,
		pc: newPCState(cfg.Database.Dedup.ParentChild, cfg.Database.Dedup.ParentChildLinks),
		nat: newNatState(cfg.Database.Dedup.NAT.WANAddresses, cfg.Database.Dedup.NAT.Exporters,
			cfg.Database.Dedup.NAT.Window.Duration())}, nil
}

// DB exposes the underlying pool for tests and maintenance tools.
func (s *Store) DB() *sql.DB { return s.db }

// Path returns the absolute database path.
func (s *Store) Path() string { return s.path }

// Close flushes and closes the pool.
func (s *Store) Close() error { return s.db.Close() }

// Stats summarizes database size and row counts.
type Stats struct {
	Path                string `json:"path"`
	SchemaVersion       int    `json:"schema_version"`
	FlowRows            int64  `json:"flow_rows"`
	HourlyAggregateRows int64  `json:"hourly_aggregate_rows"`
	SizeBytes           int64  `json:"size_bytes"`
	WALBytes            int64  `json:"wal_bytes"`
}

// Stats returns SQLite storage statistics. Counts are exact and may be
// expensive on very large databases; health checks should cache this result.
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	st.Path = s.path
	st.SchemaVersion = schemaVersion

	s.statsMu.Lock()
	cached, fresh := s.statsCached, time.Since(s.statsAt) < statsTTL && !s.statsAt.IsZero()
	s.statsMu.Unlock()
	if fresh {
		st.FlowRows = cached.FlowRows
		st.HourlyAggregateRows = cached.HourlyAggregateRows
	} else {
		err := s.db.QueryRowContext(ctx, `
			SELECT
				(SELECT COUNT(*) FROM flows),
				(SELECT COUNT(*) FROM agg_exporter) +
				(SELECT COUNT(*) FROM agg_interface) +
				(SELECT COUNT(*) FROM agg_host) +
				(SELECT COUNT(*) FROM agg_vlan) +
				(SELECT COUNT(*) FROM agg_protocol)
		`).Scan(&st.FlowRows, &st.HourlyAggregateRows)
		if err != nil {
			return st, fmt.Errorf("database stats: %w", err)
		}
		s.statsMu.Lock()
		s.statsCached = st
		s.statsAt = time.Now()
		s.statsMu.Unlock()
	}
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE((SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()), 0)`).Scan(&st.SizeBytes); err != nil {
		return st, fmt.Errorf("database size: %w", err)
	}
	if info, err := os.Stat(s.path + "-wal"); err == nil {
		st.WALBytes = info.Size()
	}
	return st, nil
}

// invalidateStats drops the cached row counts so the next Stats call rescans.
func (s *Store) invalidateStats() {
	s.statsMu.Lock()
	s.statsAt = time.Time{}
	s.statsMu.Unlock()
}

// IsDiskFull reports whether an error looks like an out-of-space failure so
// the collector can surface it prominently instead of claiming success.
func IsDiskFull(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "disk full") || strings.Contains(msg, "no space left") ||
		strings.Contains(msg, "sqlite_full") || strings.Contains(msg, "database or disk is full")
}

// IsBusy reports transient SQLite lock contention.
func IsBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "locked") || strings.Contains(msg, "busy")
}
