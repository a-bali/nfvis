package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// backfillTrafficTiers seeds each missing resolution when a database is opened. Databases created before the tier table only
// hold raw flows and hourly per-dimension aggregates, so without this one-time
// pass the charts would show no history until the new tiers accumulate.
//
// The cost is bounded: each fine tier is grouped from raw flows inside its own
// retention window (about 10k rows at 1 minute and 9k at 5 minutes) and the
// hourly tier is copied from the existing hourly aggregates (at most one year of
// hours). Resolutions with existing rows are skipped, so restarts are cheap.
func backfillTrafficTiers(ctx context.Context, db *sql.DB, tiers []trafficTier) error {
	var hourlyRows, flowRows int64
	if err := db.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM agg_exporter), (SELECT COUNT(*) FROM flows)
	`).Scan(&hourlyRows, &flowRows); err != nil {
		return fmt.Errorf("count backfill sources: %w", err)
	}
	if hourlyRows == 0 && flowRows == 0 {
		return nil
	}

	started := time.Now()
	now := time.Now().UnixMilli()
	for _, tier := range tiers {
		var existing int64
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agg_traffic WHERE res_ms = ?)`, tier.resMS).Scan(&existing); err != nil {
			return fmt.Errorf("check traffic tier: %w", err)
		}
		if existing != 0 {
			continue
		}
		cutoff := now - tier.retentionMS
		var (
			res sql.Result
			err error
		)
		if tier.resMS >= hourMS {
			// The per-exporter hourly rollup already stores whole-hour buckets, so
			// sum it instead of re-grouping raw flows.
			res, err = db.ExecContext(ctx, `
				INSERT INTO agg_traffic (res_ms, bucket_ms, bytes, packets, flows, sampled_flows, unknown_flows)
				SELECT ?, hour_ms, SUM(bytes), SUM(packets), SUM(flows), SUM(sampled_flows), SUM(unknown_flows)
				FROM agg_exporter
				WHERE hour_ms >= ?
				GROUP BY hour_ms
				ON CONFLICT(res_ms, bucket_ms) DO NOTHING`,
				tier.resMS, cutoff)
		} else {
			res, err = db.ExecContext(ctx, `
				INSERT INTO agg_traffic (res_ms, bucket_ms, bytes, packets, flows, sampled_flows, unknown_flows)
				SELECT ?, (end_ms / ?) * ?, SUM(bytes), SUM(packets), COUNT(*), SUM(sampled), 0
				FROM flows
				WHERE end_ms >= ?
				GROUP BY (end_ms / ?) * ?
				ON CONFLICT(res_ms, bucket_ms) DO NOTHING`,
				tier.resMS, tier.resMS, tier.resMS, cutoff, tier.resMS, tier.resMS)
		}
		if err != nil {
			return fmt.Errorf("backfill traffic tier %dms: %w", tier.resMS, err)
		}
		if n, err := res.RowsAffected(); err == nil {
			slog.Info("seeded traffic resolution tier",
				"bucket_ms", tier.resMS, "retention_ms", tier.retentionMS,
				"rows", n, "source_flows", flowRows, "source_hours", hourlyRows)
		}
	}
	slog.Info("traffic tier backfill complete", "elapsed", time.Since(started).String())
	return nil
}
