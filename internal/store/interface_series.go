package store

import (
	"context"
	"fmt"
)

// InterfaceSeries keeps each direction's counters unsigned and on the same grid.
// A flow with the selected interface on both sides contributes to both series.
type InterfaceSeries struct {
	Range
	Ingress     []Point  `json:"ingress"`
	Egress      []Point  `json:"egress"`
	Limitations []string `json:"limitations"`
}

// InterfaceTimeseries uses indexed hourly interface totals for complete older
// hours. The most recent hour and partial edge hours use minute buckets from
// stored flows. Only complete buckets are returned; a clipped bucket cannot
// turn counters for a whole export interval into a reliable rate.
func (s *Store) InterfaceTimeseries(ctx context.Context, r Range, exporter string, index uint32) (InterfaceSeries, error) {
	result := InterfaceSeries{Range: r, Ingress: []Point{}, Egress: []Point{}, Limitations: []string{}}
	coverage, err := s.Coverage(ctx)
	if err != nil {
		return result, err
	}
	emit := func(from, to, width int64, hourly bool) error {
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
		if hourly {
			sql = `SELECT hour_ms, direction, bytes, packets, flows FROM agg_interface
           WHERE exporter = ? AND if_index = ? AND hour_ms >= ? AND hour_ms < ?`
			args = []any{exporter, index, from, to}
		} else {
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
		// Around a retention seam the only raw segments are partial edge hours;
		// for wider raw-only windows use an appropriately coarse grid.
		width := minuteMS
		if to-first > 3*hourMS {
			width = hourMS
		}
		return emit(first, to, width, false)
	}
	if coverage.AggregateAvailable {
		from := max64(ceilToHour(r.FromMS), coverage.AggregateOldestMS)
		until := floorToHour(r.ToMS)
		if coverage.RawAvailable {
			until = min64(until, floorToHour(max64(r.ToMS-hourMS, ceilToHour(coverage.RawOldestMS))))
		}
		to := min64(until, coverage.AggregateNewestMS+hourMS)
		if from < to {
			if err := raw(cursor, from); err != nil {
				return result, fmt.Errorf("interface chart start: %w", err)
			}
			if err := emit(from, to, hourMS, true); err != nil {
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
