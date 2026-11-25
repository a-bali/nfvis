package store

import (
	"context"
	"fmt"
	"net"
	"strings"
)

// ipHostTerm lets a user explicitly request a substring of the address only.
// Bare partial terms also search stored DNS names and cannot use agg_host.
func ipHostTerm(term string) (address string, exact, ok bool) {
	if net.ParseIP(term) != nil {
		return term, true, true
	}
	if strings.HasPrefix(term, "ip:") && len(term) > 3 {
		return term[3:], false, true
	}
	return "", false, false
}

// hourlyHostFilter accepts only a single endpoint and no other predicates.
func hourlyHostFilter(q Query) (direction, address string, exact, ok bool) {
	if len(q.SrcHostIPs) > 0 || len(q.DstHostIPs) > 0 || q.SrcHostNoMatch || q.DstHostNoMatch {
		return
	}
	for key, value := range q.Filters {
		if value == "" {
			continue
		}
		if direction != "" {
			return "", "", false, false
		}
		switch key {
		case "src_host":
			direction = "source"
		case "dst_host":
			direction = "destination"
		default:
			return "", "", false, false
		}
		address, exact, ok = ipHostTerm(value)
		if !ok {
			return
		}
	}
	return
}

// hostHistory uses hourly totals only before raw coverage. Raw records still
// answer the newer portion, on the same hourly grid and with no hour counted
// twice. A partial historical hour without raw coverage cannot be recovered.
func (s *Store) hostHistory(ctx context.Context, q Query, c Coverage, result TimeseriesResult) (TimeseriesResult, bool, error) {
	direction, address, exact, ok := hourlyHostFilter(q)
	if !ok || !c.AggregateAvailable {
		return result, false, nil
	}
	first := max64(ceilToHour(q.FromMS), c.AggregateOldestMS)
	last := min64(floorToHour(q.ToMS), c.AggregateNewestMS+hourMS)
	if c.RawAvailable {
		// Include the hour cut by raw retention so the stored sum represents the
		// whole hour rather than just the raw rows that survived.
		last = min64(last, ceilToHour(c.RawOldestMS))
	}
	if first >= last {
		return result, false, nil
	}
	predicate := "host = ?"
	if !exact {
		predicate = "instr(lower(host), lower(?)) > 0"
	}
	rows, err := s.db.QueryContext(ctx, `SELECT hour_ms, SUM(bytes), SUM(packets), SUM(flows), SUM(sampled_flows), SUM(unknown_flows)
  FROM agg_host WHERE direction=? AND hour_ms>=? AND hour_ms<? AND `+predicate+` GROUP BY hour_ms`, direction, first, last, address)
	if err != nil {
		return result, true, fmt.Errorf("query historical host chart: %w", err)
	}
	points := make(map[int64]*Point)
	if err := scanPoints(rows, points); err != nil {
		return result, true, err
	}
	result.Resolution = "aggregate"
	result.BucketMS = hourMS
	result.AggregateBucketMS = hourMS
	result.AggregateAvailableTo = last
	if first > q.FromMS {
		result.Limitations = appendChartCoverageLimitation(result.Limitations, first)
	}
	appendBuckets(&result.Points, points, first, last, hourMS, maxSeriesPoints, false)
	if c.RawAvailable && max64(last, c.RawOldestMS) < q.ToMS {
		rawQ := q
		rawQ.FromMS = max64(last, c.RawOldestMS)
		if rawQ.FromMS > last {
			result.Limitations = append(result.Limitations, "Some hours between hourly totals and retained flows are unavailable.")
		}
		rawQ.BucketMS = hourMS
		raw, err := s.timeseriesRaw(ctx, rawQ, c, TimeseriesResult{Range: rawQ.Range})
		if err != nil {
			return result, true, err
		}
		result.Resolution = "mixed"
		result.Points = append(result.Points, raw.Points...)
		result.Limitations = append(result.Limitations, raw.Limitations...)
	} else if last < q.ToMS {
		result.Limitations = append(result.Limitations, "Only complete historical hours are available for this filter.")
	}
	return result, true, nil
}
