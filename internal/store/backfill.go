package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// backfillFineInterface runs in migration 6's transaction. Hour-aligned
// chunks bound the GROUP BY scratch space and never split a fine bucket.
func backfillFineInterface(ctx context.Context, tx *sql.Tx) error {
	var newest sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(end_ms) FROM flows`).Scan(&newest); err != nil || !newest.Valid {
		return err
	}
	for _, tier := range fineInterfaceTiers {
		var first int64
		if err := tx.QueryRowContext(ctx, `SELECT from_ms FROM interface_fine_coverage WHERE res_ms=?`, tier.resMS).Scan(&first); err != nil {
			return err
		}
		for from := first; from <= newest.Int64; {
			to := floorToHour(from) + hourMS
			_, err := tx.ExecContext(ctx, `INSERT INTO agg_interface_fine
				SELECT ?, f.end_ms / ? * ?, f.exporter,
					CASE side.dir WHEN 'ingress' THEN f.input_if ELSE f.output_if END,
					side.dir, SUM(f.bytes), SUM(f.packets), COUNT(*)
				FROM flows f CROSS JOIN (SELECT 'ingress' AS dir UNION ALL SELECT 'egress') side
				WHERE f.end_ms >= ? AND f.end_ms < ?
				GROUP BY 2,3,4,5`, tier.resMS, tier.resMS, tier.resMS, from, to)
			if err != nil {
				return err
			}
			from = to
		}
	}
	return nil
}

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
