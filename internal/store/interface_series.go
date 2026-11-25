package store

import (
	"context"
	"fmt"
	"time"
)

var fineInterfaceTiers = [...]trafficTier{{minuteMS, 7 * 24 * hourMS}, {5 * minuteMS, 30 * 24 * hourMS}}

// InterfaceSeries keeps each direction's counters unsigned and on the same grid.
// A flow with the selected interface on both sides contributes to both series.
type InterfaceSeries struct {
	Range
	Ingress     []Point  `json:"ingress"`
	Egress      []Point  `json:"egress"`
	Limitations []string `json:"limitations"`
}

// InterfaceTimeseries uses one resolution per chart from the available
// fine or hourly rollups. Incomplete buckets do not contribute to rates.
func (s *Store) InterfaceTimeseries(ctx context.Context, r Range, exporter string, index uint32) (InterfaceSeries, error) {
	result := InterfaceSeries{Range: r, Ingress: []Point{}, Egress: []Point{}, Limitations: []string{}}
	var coverage Coverage
	// Exporter and interface rollups are written in the same transaction.
	// The interface chart does not need the collector-wide tier inventory.
	if err := s.db.QueryRowContext(ctx, `SELECT
		COALESCE((SELECT MIN(hour_ms) FROM agg_exporter),0),
		COALESCE((SELECT MAX(hour_ms) FROM agg_exporter),0)`).
		Scan(&coverage.AggregateOldestMS, &coverage.AggregateNewestMS); err != nil {
		return result, fmt.Errorf("read interface chart coverage: %w", err)
	}
	coverage.AggregateAvailable = coverage.AggregateOldestMS > 0
	emit := func(from, to, width int64, source string) error {
		// Counters are attributed to the bucket containing the flow's end
		// time, not distributed across its lifetime. Don't divide them by
		// the few seconds of a bucket clipped by the query or retention.
		from = ceilDiv(from, width) * width
		to = (to / width) * width
		if from >= to {
			return nil
		}
		var args []any
		var sql string
		switch source {
		case "hourly":
			sql = `SELECT hour_ms, direction, bytes, packets, flows FROM agg_interface
           WHERE exporter = ? AND if_index = ? AND hour_ms >= ? AND hour_ms < ?`
			args = []any{exporter, index, from, to}
		case "fine":
			sql = `SELECT bucket_ms, direction, bytes, packets, flows FROM agg_interface_fine
				WHERE res_ms = ? AND exporter = ? AND if_index = ? AND bucket_ms >= ? AND bucket_ms < ?`
			args = []any{width, exporter, index, from, to}
		default:
			sql = `WITH matching AS (
     SELECT end_ms / ? * ? AS bucket, input_if, output_if, bytes, packets
     FROM flows WHERE end_ms >= ? AND end_ms < ? AND exporter = ? AND (input_if = ? OR output_if = ?)
    )
    SELECT bucket, 'ingress', SUM(bytes), SUM(packets), COUNT(*) FROM matching WHERE input_if = ? GROUP BY bucket
    UNION ALL
    SELECT bucket, 'egress', SUM(bytes), SUM(packets), COUNT(*) FROM matching WHERE output_if = ? GROUP BY bucket`
			args = []any{width, width, from, to, exporter, index, index, index, index}
		}
		rows, err := s.db.QueryContext(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		in, out := make(map[int64]*Point), make(map[int64]*Point)
		for rows.Next() {
			var p Point
			var direction string
			if err := rows.Scan(&p.BucketMS, &direction, &p.Bytes, &p.Packets, &p.Flows); err != nil {
				return err
			}
			switch direction {
			case "ingress":
				in[p.BucketMS] = &p
			case "egress":
				out[p.BucketMS] = &p
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		appendBuckets(&result.Ingress, in, from, to, width, maxSeriesPoints, false)
		appendBuckets(&result.Egress, out, from, to, width, maxSeriesPoints, false)
		return nil
	}
	// Compare against the collector's oldest known hour so a bucket clipped
	// during migration doesn't silently replace existing hourly history.
	first := r.FromMS
	if coverage.AggregateAvailable && first < coverage.AggregateOldestMS {
		first = coverage.AggregateOldestMS
	}
	span := r.ToMS - r.FromMS
	now := time.Now().UnixMilli()
	for i, tier := range fineInterfaceTiers {
		if (i == 0 && span > 6*hourMS) || span > tier.retentionMS {
			continue
		}
		aligned := ceilDiv(first, tier.resMS) * tier.resMS
		cutoff := (now - tier.retentionMS) / tier.resMS * tier.resMS
		if !coverage.AggregateAvailable || aligned < max64(s.fineInterfaceFrom[i], cutoff) {
			continue
		}
		if first > r.FromMS {
			result.Limitations = appendChartCoverageLimitation(result.Limitations, first)
		}
		if err := emit(first, r.ToMS, tier.resMS, "fine"); err != nil {
			return result, fmt.Errorf("fine interface chart: %w", err)
		}
		return result, nil
	}

	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MIN(end_ms),0) FROM flows`).Scan(&coverage.RawOldestMS); err != nil {
		return result, fmt.Errorf("read raw interface chart coverage: %w", err)
	}
	coverage.RawAvailable = coverage.RawOldestMS > 0

	// Do not show an unavailable partial hour as zero. Clamp raw reads to
	// retained detail, and account for any missing portion of the window.
	cursor := r.FromMS
	raw := func(from, to int64) error {
		if from >= to {
			return nil
		}
		if !coverage.RawAvailable || coverage.RawOldestMS >= to {
			result.Limitations = append(result.Limitations, "Interface detail is unavailable for part of this range; only complete retained hours can use hourly totals.")
			return nil
		}
		first := max64(from, coverage.RawOldestMS)
		if first > from {
			result.Limitations = appendChartCoverageLimitation(result.Limitations, first)
		}
		return emit(first, to, hourMS, "raw")
	}
	if coverage.AggregateAvailable {
		from := max64(ceilToHour(r.FromMS), coverage.AggregateOldestMS)
		to := min64(floorToHour(r.ToMS), coverage.AggregateNewestMS+hourMS)
		if from < to {
			if err := raw(cursor, from); err != nil {
				return result, fmt.Errorf("interface chart start: %w", err)
			}
			if err := emit(from, to, hourMS, "hourly"); err != nil {
				return result, fmt.Errorf("hourly interface chart: %w", err)
			}
			cursor = to
		}
	}
	if err := raw(cursor, r.ToMS); err != nil {
		return result, fmt.Errorf("interface chart end: %w", err)
	}
	return result, nil
}
