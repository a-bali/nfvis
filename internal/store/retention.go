package store

import (
	"context"
	"fmt"
	"time"
)

// RetentionResult reports rows removed by one maintenance pass.
type RetentionResult struct {
	RawDeleted       int64 `json:"raw_deleted"`
	AggregateDeleted int64 `json:"aggregate_deleted"`
	TrafficDeleted   int64 `json:"traffic_deleted"`
}

// SweepRetention deletes expired raw flows and hourly aggregates in bounded
// batches. The two retention windows are independent.
func (s *Store) SweepRetention(ctx context.Context, now time.Time) (RetentionResult, error) {
	var result RetentionResult
	rawCutoff := now.Add(-s.cfg.Database.Retention.Raw.Duration()).UnixMilli()
	batch := s.cfg.Database.Retention.CleanupBatch

	for {
		res, err := s.db.ExecContext(ctx,
			`DELETE FROM flows WHERE id IN (
				SELECT id FROM flows WHERE end_ms < ? LIMIT ?
			)`, rawCutoff, batch)
		if err != nil {
			return result, fmt.Errorf("delete expired flows: %w", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return result, err
		}
		result.RawDeleted += n
		if n < int64(batch) {
			break
		}
	}

	aggCutoff := now.Add(-s.cfg.Database.Retention.Hourly.Duration()).UnixMilli()
	aggCutoff = (aggCutoff / hourMS) * hourMS
	for _, table := range []string{"agg_exporter", "agg_interface", "agg_host", "dns_host_history", "agg_vlan", "agg_protocol"} {
		for {
			res, err := s.db.ExecContext(ctx, fmt.Sprintf(
				`DELETE FROM %s WHERE hour_ms IN (
					SELECT hour_ms FROM %s WHERE hour_ms < ? LIMIT ?
				)`, table, table), aggCutoff, batch)
			if err != nil {
				return result, fmt.Errorf("delete expired %s: %w", table, err)
			}
			n, err := res.RowsAffected()
			if err != nil {
				return result, err
			}
			result.AggregateDeleted += n
			if n < int64(batch) {
				break
			}
		}
	}

	for _, tier := range s.tiers {
		cutoff := now.Add(-time.Duration(tier.retentionMS) * time.Millisecond).UnixMilli()
		cutoff = (cutoff / tier.resMS) * tier.resMS
		for {
			res, err := s.db.ExecContext(ctx, `DELETE FROM agg_traffic WHERE res_ms = ? AND bucket_ms IN (
				SELECT bucket_ms FROM agg_traffic WHERE res_ms = ? AND bucket_ms < ? LIMIT ?
			)`, tier.resMS, tier.resMS, cutoff, batch)
			if err != nil {
				return result, fmt.Errorf("delete expired agg_traffic %dms: %w", tier.resMS, err)
			}
			n, err := res.RowsAffected()
			if err != nil {
				return result, err
			}
			result.TrafficDeleted += n
			if n < int64(batch) {
				break
			}
		}
	}

	for _, tier := range fineInterfaceTiers {
		cutoff := (now.UnixMilli() - tier.retentionMS) / tier.resMS * tier.resMS
		for {
			res, err := s.db.ExecContext(ctx, `DELETE FROM agg_interface_fine WHERE (res_ms,bucket_ms,exporter,if_index,direction) IN
				(SELECT res_ms,bucket_ms,exporter,if_index,direction FROM agg_interface_fine WHERE res_ms=? AND bucket_ms<? LIMIT ?)`, tier.resMS, cutoff, batch)
			if err != nil {
				return result, fmt.Errorf("delete expired fine interface totals: %w", err)
			}
			n, err := res.RowsAffected()
			if err != nil {
				return result, err
			}
			result.TrafficDeleted += n
			if n < int64(batch) {
				break
			}
		}
	}

	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO collector_state(key, value) VALUES('last_retention_sweep', ?)
		 ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
		now.UTC().Format(time.RFC3339Nano)); err != nil {
		return result, fmt.Errorf("record retention sweep: %w", err)
	}
	if result.RawDeleted > 0 || result.AggregateDeleted > 0 || result.TrafficDeleted > 0 {
		s.invalidateStats()
	}
	return result, nil
}

// TierCoverage describes the retained window of one chart-resolution tier.
type TierCoverage struct {
	BucketMS    int64 `json:"bucket_ms"`
	RetentionMS int64 `json:"retention_ms"`
	OldestMS    int64 `json:"oldest_ms"`
	NewestMS    int64 `json:"newest_ms"`
	Available   bool  `json:"available"`
}

// Coverage describes the retained raw and aggregate time ranges.
type Coverage struct {
	RawOldestMS           int64          `json:"raw_oldest_ms"`
	RawNewestMS           int64          `json:"raw_newest_ms"`
	AggregateOldestMS     int64          `json:"aggregate_oldest_ms"`
	AggregateNewestMS     int64          `json:"aggregate_newest_ms"`
	RawAvailable          bool           `json:"raw_available"`
	AggregateAvailable    bool           `json:"aggregate_available"`
	RawRetentionMS        int64          `json:"raw_retention_ms"`
	HourlyRetentionMS     int64          `json:"hourly_retention_ms"`
	RawDetailsRecoverable bool           `json:"raw_details_recoverable"`
	Resolution            string         `json:"resolution"`
	Tiers                 []TierCoverage `json:"tiers"`
}

// Coverage returns current retention coverage from stored data.
func (s *Store) Coverage(ctx context.Context) (Coverage, error) {
	c := Coverage{
		RawRetentionMS:    s.cfg.Database.Retention.Raw.Duration().Milliseconds(),
		HourlyRetentionMS: s.cfg.Database.Retention.Hourly.Duration().Milliseconds(),
	}
	// MIN and MAX as separate scalar subqueries so the planner can seek the end
	// time index twice; combining both aggregates in one scan reads every row.
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE((SELECT MIN(end_ms) FROM flows),0), COALESCE((SELECT MAX(end_ms) FROM flows),0)`).Scan(&c.RawOldestMS, &c.RawNewestMS); err != nil {
		return c, fmt.Errorf("read raw coverage: %w", err)
	}
	c.RawAvailable = c.RawOldestMS > 0
	c.RawDetailsRecoverable = c.RawAvailable

	var minHour, maxHour int64
	for i, table := range []string{"agg_exporter", "agg_interface", "agg_host", "agg_vlan", "agg_protocol"} {
		var lo, hi int64
		if err := s.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COALESCE((SELECT MIN(hour_ms) FROM %s),0), COALESCE((SELECT MAX(hour_ms) FROM %s),0)`, table, table)).Scan(&lo, &hi); err != nil {
			return c, fmt.Errorf("read %s coverage: %w", table, err)
		}
		if lo == 0 {
			continue
		}
		if i == 0 || minHour == 0 || lo < minHour {
			minHour = lo
		}
		if hi > maxHour {
			maxHour = hi
		}
	}
	c.AggregateOldestMS, c.AggregateNewestMS = minHour, maxHour
	c.AggregateAvailable = minHour > 0

	// Chart-resolution tiers: only report what is actually retained so the
	// chart never claims a fine resolution it cannot serve.
	tiers := make([]TierCoverage, 0, len(s.tiers))
	stored := map[int64]TierCoverage{}
	rows, err := s.db.QueryContext(ctx,
		`SELECT res_ms, COALESCE(MIN(bucket_ms),0), COALESCE(MAX(bucket_ms),0) FROM agg_traffic GROUP BY res_ms`)
	if err != nil {
		return c, fmt.Errorf("read agg_traffic coverage: %w", err)
	}
	for rows.Next() {
		var tc TierCoverage
		if err := rows.Scan(&tc.BucketMS, &tc.OldestMS, &tc.NewestMS); err != nil {
			_ = rows.Close()
			return c, fmt.Errorf("scan agg_traffic coverage: %w", err)
		}
		tc.Available = tc.OldestMS > 0
		stored[tc.BucketMS] = tc
	}
	if err := rows.Close(); err != nil {
		return c, fmt.Errorf("close agg_traffic coverage: %w", err)
	}
	if err := rows.Err(); err != nil {
		return c, fmt.Errorf("read agg_traffic coverage: %w", err)
	}
	for _, tier := range s.tiers {
		tc := stored[tier.resMS]
		tc.BucketMS = tier.resMS
		tc.RetentionMS = tier.retentionMS
		tiers = append(tiers, tc)
	}
	c.Tiers = tiers
	switch {
	case c.RawAvailable && c.AggregateAvailable && c.AggregateOldestMS < (c.RawOldestMS/hourMS)*hourMS:
		c.Resolution = "mixed"
	case c.RawAvailable:
		c.Resolution = "raw"
	case c.AggregateAvailable:
		c.Resolution = "aggregate"
	default:
		c.Resolution = "empty"
	}
	return c, nil
}
