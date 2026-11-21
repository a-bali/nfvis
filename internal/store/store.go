// Package store persists normalized flows and hourly aggregates in SQLite.
package store

import (
	"context"
	"database/sql"
	"fmt"
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
	// parentChildDedup collapses the per-interface copies of one frame.
	parentChildDedup bool
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
	for _, pragma := range []string{
		"busy_timeout(5000)",
		"journal_mode(WAL)",
		"synchronous(NORMAL)",
		"foreign_keys(ON)",
		"temp_store(MEMORY)",
		"cache_size(-131072)",
		// Map the database read-only into memory so repeated wide scans hit the
		// OS page cache instead of re-reading the file for every pass.
		"mmap_size(1073741824)",
		"wal_autocheckpoint(1000)",
	} {
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	if err := migrate(ctx, db); err != nil {
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
		parentChildDedup: cfg.Database.Dedup.ParentChild, nat: newNatState(NatPolicy{
			WANAddresses: cfg.Database.Dedup.NAT.WANAddresses,
			Window:       cfg.Database.Dedup.NAT.Window.Duration(),
		})}, nil
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
