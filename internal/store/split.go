package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	// maxSplitSeries bounds a stacked chart to ten named bands plus "other" so
	// the legend stays readable and the payload stays small.
	maxSplitSeries = 10
	// maxSplitPoints bounds bucket x series points in one response.
	maxSplitPoints = 60000
	// splitRawSpanMS bounds how much raw history a stacked chart scans. Older
	// hours come from the hourly dimension aggregates, which keep every
	// interface, VLAN, host, and protocol per hour.
	splitRawSpanMS = 6 * hourMS
)

// splitDimensions maps a stacked-chart dimension to the SQL expression that
// identifies one band in the flows table. Interfaces prefer the configured
// label so one physical interface keeps one band even when exporters report
// different indexes; hosts and protocols use their raw value and are labelled
// by the API.
var splitDimensions = map[string]string{
	"input_if":    "COALESCE(NULLIF(input_label, ''), CAST(input_if AS TEXT))",
	"output_if":   "COALESCE(NULLIF(output_label, ''), CAST(output_if AS TEXT))",
	"input_vlan":  "CAST(src_vlan AS TEXT)",
	"output_vlan": "CAST(dst_vlan AS TEXT)",
	"src_host":    "src_addr",
	"dst_host":    "dst_addr",
	"protocol":    "CAST(protocol AS TEXT)",
}

// splitAggregateSource describes the hourly aggregate table that can serve a
// dimension without scanning individual flows. The key expression must produce
// the same values as splitDimensions so bands merge across the seam.
type splitAggregateSource struct {
	table string
	key   string
	where string
}

var splitAggregates = map[string]splitAggregateSource{
	"input_if":    {"agg_interface", "COALESCE(NULLIF(label, ''), CAST(if_index AS TEXT))", "direction = 'ingress'"},
	"output_if":   {"agg_interface", "COALESCE(NULLIF(label, ''), CAST(if_index AS TEXT))", "direction = 'egress'"},
	"input_vlan":  {"agg_vlan", "CAST(vlan AS TEXT)", "direction = 'source'"},
	"output_vlan": {"agg_vlan", "CAST(vlan AS TEXT)", "direction = 'destination'"},
	"src_host":    {"agg_host", "host", "direction = 'source'"},
	"dst_host":    {"agg_host", "host", "direction = 'destination'"},
	"protocol":    {"agg_protocol", "CAST(protocol AS TEXT)", ""},
}

// ValidSplitBy reports whether a stacked-chart dimension is supported.
func ValidSplitBy(v string) bool {
	_, ok := splitDimensions[v]
	return ok
}

// SplitByDimensions lists the supported stacked-chart dimensions.
func SplitByDimensions() []string {
	out := make([]string, 0, len(splitDimensions))
	for k := range splitDimensions {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// SeriesPoint is one bucket of one band: bucket, bytes, packets, flows. It is
// encoded as a tuple to keep stacked responses small.
type SeriesPoint [4]int64

// SplitSeries is one stacked band.
type SplitSeries struct {
	Key     string        `json:"key"`
	Label   string        `json:"label,omitempty"`
	Other   bool          `json:"other,omitempty"`
	Bytes   uint64        `json:"bytes"`
	Packets uint64        `json:"packets"`
	Flows   uint64        `json:"flows"`
	Points  []SeriesPoint `json:"points"`
}

// SplitResult is a stacked timeseries. Points holds the bucket totals so a
// chart can compute share or draw a total line without a second request.
type SplitResult struct {
	Range
	Resolution           string        `json:"resolution"`
	RawAvailableFromMS   int64         `json:"raw_available_from_ms"`
	AggregateAvailableTo int64         `json:"aggregate_available_to_ms"`
	BucketMS             int64         `json:"bucket_ms"`
	SplitBy              string        `json:"split_by"`
	Metric               string        `json:"metric"`
	Limitations          []string      `json:"limitations"`
	Points               []Point       `json:"points"`
	Series               []SplitSeries `json:"series"`
}

const splitDeadlineLimitation = "The stacked chart stopped early because the query exceeded its time budget; narrow the range or remove filters for complete data."

// splitSegment is one contiguous part of the requested range served from a
// single source. Hourly segments read the dimension aggregates; the remaining
// (most recent) segment reads individual flows.
type splitSegment struct {
	from, to int64
	bucket   int64
	hourly   bool
}

type splitTotals struct {
	bytes   uint64
	packets uint64
	flows   uint64
}

func (t *splitTotals) add(p *Point) {
	t.bytes += p.Bytes
	t.packets += p.Packets
	t.flows += p.Flows
}

func (t *splitTotals) metric(metric string) uint64 {
	return seriesMetric(t.bytes, t.packets, t.flows, metric)
}

func seriesMetric(bytes, packets, flows uint64, metric string) uint64 {
	switch metric {
	case "packets":
		return packets
	case "flows":
		return flows
	default:
		return bytes
	}
}

func normalizeMetric(metric string) string {
	switch strings.ToLower(strings.TrimSpace(metric)) {
	case "packets":
		return "packets"
	case "flows":
		return "flows"
	default:
		return "bytes"
	}
}

func andWhere(clause string) string {
	if clause == "" {
		return ""
	}
	return " AND " + clause
}

func ceilToHour(ms int64) int64  { return ceilDiv(ms, hourMS) * hourMS }
func floorToHour(ms int64) int64 { return (ms / hourMS) * hourMS }

// splitSegments decides which part of the range comes from hourly aggregates
// and which parts are scanned from raw flows. Without filters the aggregate
// tables can serve whole hours, so a long range only scans raw flows for the
// most recent hours and for a partial hour at the start of the range.
func (s *Store) splitSegments(q Query, coverage Coverage) ([]splitSegment, []string) {
	var segments []splitSegment

	tailFrom := max64(q.FromMS, q.ToMS-splitRawSpanMS)
	if coverage.RawAvailable {
		tailFrom = max64(tailFrom, coverage.RawOldestMS)
	} else {
		tailFrom = q.ToMS
	}
	aggFrom := ceilToHour(max64(q.FromMS, coverage.AggregateOldestMS))
	aggTo := min64(floorToHour(q.ToMS), coverage.AggregateNewestMS+hourMS)
	if aggTo > tailFrom {
		aggTo = floorToHour(tailFrom)
	}
	hasAgg := coverage.AggregateAvailable && aggTo > aggFrom

	if !hasAgg {
		// Without hourly aggregates the whole range has to come from raw flows.
		if !coverage.RawAvailable {
			return nil, nil
		}
		rawFrom := max64(q.FromMS, coverage.RawOldestMS)
		if rawFrom >= q.ToMS {
			return nil, nil
		}
		return []splitSegment{{
			from: rawFrom, to: q.ToMS, bucket: s.splitBucketWidth(q, rawFrom),
		}}, coverageNotes(q, rawFrom)
	}

	// The aggregate segment is whole hours; a partial hour at either end of the
	// range is cheap to read from raw flows and keeps the chart exact.
	if coverage.RawAvailable && q.FromMS < aggFrom {
		headFrom := max64(q.FromMS, coverage.RawOldestMS)
		if headFrom < aggFrom {
			segments = append(segments, splitSegment{
				from: headFrom, to: aggFrom, bucket: s.splitBucketWidth(q, headFrom),
			})
		}
	}
	segments = append(segments, splitSegment{from: aggFrom, to: aggTo, bucket: hourMS, hourly: true})
	if coverage.RawAvailable {
		rawFrom := max64(tailFrom, aggTo)
		if rawFrom < q.ToMS {
			segments = append(segments, splitSegment{
				from: rawFrom, to: q.ToMS, bucket: s.splitBucketWidth(q, rawFrom),
			})
		}
	}
	return segments, coverageNotes(q, segments[0].from)
}

// coverageNotes explains a range that starts before any retained data.
func coverageNotes(q Query, first int64) []string {
	if first <= q.FromMS {
		return nil
	}
	return appendChartCoverageLimitation(nil, first)
}

// splitBucketWidth starts from the finest sensible width and only coarsens to
// keep bucket x series points inside the payload budget.
func (s *Store) splitBucketWidth(q Query, from int64) int64 {
	bucket := s.desiredBucket(q.ToMS - from)
	if q.BucketMS > bucket {
		bucket = ceilDiv(q.BucketMS, minuteMS) * minuteMS
	}
	budget := int64(maxSplitPoints / (maxSplitSeries + 1))
	for _, candidate := range niceBuckets {
		if candidate < bucket {
			continue
		}
		if ceilDiv(q.ToMS-from, candidate) <= budget {
			return candidate
		}
	}
	return niceBuckets[len(niceBuckets)-1]
}

func caseBand(expr string, keys []string) (string, []interface{}) {
	if len(keys) == 0 {
		return "''", nil
	}
	placeholders := make([]string, len(keys))
	args := make([]interface{}, len(keys))
	for i, key := range keys {
		placeholders[i] = "?"
		args[i] = key
	}
	return fmt.Sprintf("CASE WHEN %s IN (%s) THEN %s ELSE '' END", expr, strings.Join(placeholders, ","), expr), args
}

// TimeseriesSplit returns a stacked chart for one dimension. Ranges longer than
// six hours read whole hours from the hourly dimension aggregates and only scan
// raw flows for the most recent hours; filtered queries scan raw flows for the
// whole range because aggregates hold no filter dimensions.
func (s *Store) TimeseriesSplit(ctx context.Context, q Query) (SplitResult, error) {
	rawKey, ok := splitDimensions[q.SplitBy]
	if !ok {
		return SplitResult{}, fmt.Errorf("unsupported split dimension %q", q.SplitBy)
	}
	metric := normalizeMetric(q.Metric)
	result := SplitResult{
		Range: q.Range, SplitBy: q.SplitBy, Metric: metric,
		Limitations: []string{}, Points: []Point{}, Series: []SplitSeries{},
	}
	coverage, err := s.Coverage(ctx)
	if err != nil {
		return result, err
	}
	result.RawAvailableFromMS = coverage.RawOldestMS
	result.AggregateAvailableTo = coverage.AggregateNewestMS + hourMS

	aggregateOnly := len(q.Filters) == 0 && len(q.SrcHostIPs) == 0 && len(q.DstHostIPs) == 0 &&
		!q.SrcHostNoMatch && !q.DstHostNoMatch
	segments, notes := s.splitSegments(q, coverage)
	if !aggregateOnly {
		// A filtered chart cannot use the aggregates, so scan the whole range.
		segments, notes = nil, nil
		rawFrom := q.FromMS
		if coverage.RawAvailable {
			rawFrom = max64(q.FromMS, coverage.RawOldestMS)
		}
		if coverage.RawAvailable && rawFrom < q.ToMS {
			notes = append(notes, coverageNotes(q, rawFrom)...)
			segments = []splitSegment{{from: rawFrom, to: q.ToMS, bucket: s.splitBucketWidth(q, rawFrom)}}
		}
	}
	result.Limitations = append(result.Limitations, notes...)
	if len(segments) == 0 {
		result.BucketMS = s.desiredBucket(q.ToMS - q.FromMS)
		result.Limitations = append(result.Limitations, "No individual flows or hourly aggregates cover this period.")
		result.Limitations = append(result.Limitations, "No traffic was recorded in this period.")
		return result, nil
	}

	filterSQL, filterArgs := flowFilterSQL(q.Filters, q.SrcHostIPs, q.DstHostIPs, q.SrcHostNoMatch, q.DstHostNoMatch)
	aggSource := splitAggregates[q.SplitBy]

	// Rank the bands by the requested metric across every segment before
	// deciding which ones get their own band.
	totals := make(map[string]*splitTotals)
	truncated := false
	for _, seg := range segments {
		var err error
		if seg.hourly {
			err = s.splitAggregateKeyTotals(ctx, aggSource, seg, totals)
		} else {
			truncated, err = s.splitRawKeyTotals(ctx, rawKey, seg, filterSQL, filterArgs, metric, totals)
		}
		if err != nil {
			return result, err
		}
		if truncated {
			break
		}
	}
	keys := make([]string, 0, len(totals))
	for key := range totals {
		keys = append(keys, key)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		ai, aj := totals[keys[i]].metric(metric), totals[keys[j]].metric(metric)
		if ai != aj {
			return ai > aj
		}
		return keys[i] < keys[j]
	})
	if len(keys) > maxSplitSeries {
		keys = keys[:maxSplitSeries]
	}

	bandPoints := make(map[string]map[int64]*Point, len(keys)+1)
	bandTotals := make(map[string]*Point, len(keys)+1)
	pointTotals := make(map[int64]*Point)
	buckets := make([]int64, 0, 64)
	durations := make(map[int64]int64)
	finest := int64(0)
	for _, seg := range segments {
		if seg.hourly {
			err = s.splitAggregateBandPoints(ctx, aggSource, seg, keys, bandPoints, bandTotals, pointTotals)
		} else {
			truncated, err = s.splitRawBandPoints(ctx, rawKey, seg, keys, filterSQL, filterArgs, bandPoints, bandTotals, pointTotals)
		}
		if err != nil {
			return result, err
		}
		if finest == 0 || seg.bucket < finest {
			finest = seg.bucket
		}
		for b := (seg.from / seg.bucket) * seg.bucket; b < seg.to; b += seg.bucket {
			buckets = append(buckets, b)
			durations[b] = min64(b+seg.bucket, seg.to) - max64(b, seg.from)
		}
		if truncated {
			break
		}
	}
	if truncated {
		result.Limitations = append(result.Limitations, splitDeadlineLimitation)
	}
	if finest == 0 {
		finest = segments[0].bucket
	}
	result.BucketMS = finest
	if len(segments) == 1 {
		if segments[0].hourly {
			result.Resolution = "aggregate"
		} else {
			result.Resolution = "raw"
		}
	} else {
		result.Resolution = "mixed"
	}
	for _, seg := range segments {
		if !seg.hourly {
			continue
		}
		result.Limitations = append(result.Limitations, fmt.Sprintf(
			"Stacked bands before %s UTC are hourly because those hours come from hourly aggregates instead of individual flows.",
			time.UnixMilli(seg.to).UTC().Format("2006-01-02 15:04")))
	}

	bands := make([]string, 0, len(keys)+1)
	for _, key := range keys {
		if agg := bandTotals[key]; agg != nil && agg.Flows > 0 {
			bands = append(bands, key)
		}
	}
	if agg := bandTotals[""]; agg != nil && agg.Flows > 0 {
		bands = append(bands, "")
	}
	width := func(key string) uint64 {
		if agg := bandTotals[key]; agg != nil {
			return seriesMetric(agg.Bytes, agg.Packets, agg.Flows, metric)
		}
		return 0
	}
	sort.SliceStable(bands, func(i, j int) bool {
		if bands[i] == "" {
			return false
		}
		if bands[j] == "" {
			return true
		}
		return width(bands[i]) > width(bands[j])
	})

	for _, key := range bands {
		agg := bandTotals[key]
		series := SplitSeries{
			Key: key, Other: key == "", Bytes: agg.Bytes, Packets: agg.Packets, Flows: agg.Flows,
			Points: make([]SeriesPoint, 0, len(buckets)),
		}
		points := bandPoints[key]
		for _, b := range buckets {
			if p := points[b]; p != nil {
				series.Points = append(series.Points, SeriesPoint{b, int64(p.Bytes), int64(p.Packets), int64(p.Flows)})
			} else {
				series.Points = append(series.Points, SeriesPoint{b, 0, 0, 0})
			}
		}
		result.Series = append(result.Series, series)
	}
	for _, b := range buckets {
		if p := pointTotals[b]; p != nil {
			point := *p
			point.DurationMS = durations[b]
			result.Points = append(result.Points, point)
		} else {
			result.Points = append(result.Points, Point{BucketMS: b, DurationMS: durations[b]})
		}
	}
	if len(result.Series) == 0 {
		result.Limitations = append(result.Limitations, "No traffic was recorded in this period.")
	}
	return result, nil
}

func (s *Store) splitAggregateKeyTotals(ctx context.Context, src splitAggregateSource, seg splitSegment, totals map[string]*splitTotals) error {
	query := fmt.Sprintf(`
		SELECT %s AS band, COALESCE(SUM(bytes),0), COALESCE(SUM(packets),0), COALESCE(SUM(flows),0)
		FROM %s WHERE hour_ms >= ? AND hour_ms < ?%s
		GROUP BY band`, src.key, src.table, andWhere(src.where))
	rows, err := s.db.QueryContext(ctx, query, seg.from, seg.to)
	if err != nil {
		return fmt.Errorf("query split key totals: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var band string
		var p Point
		if err := rows.Scan(&band, &p.Bytes, &p.Packets, &p.Flows); err != nil {
			return fmt.Errorf("scan split key totals: %w", err)
		}
		total := totals[band]
		if total == nil {
			total = &splitTotals{}
			totals[band] = total
		}
		total.add(&p)
	}
	return rows.Err()
}

func (s *Store) splitAggregateBandPoints(ctx context.Context, src splitAggregateSource, seg splitSegment, keys []string,
	bandPoints map[string]map[int64]*Point, bandTotals map[string]*Point, pointTotals map[int64]*Point) error {
	bandExpr, args := caseBand(src.key, keys)
	query := fmt.Sprintf(`
		SELECT hour_ms, %s AS band, COALESCE(SUM(bytes),0), COALESCE(SUM(packets),0), COALESCE(SUM(flows),0)
		FROM %s WHERE hour_ms >= ? AND hour_ms < ?%s
		GROUP BY hour_ms, band`, bandExpr, src.table, andWhere(src.where))
	args = append(args, seg.from, seg.to)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query stacked aggregate points: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var band string
		var p Point
		if err := rows.Scan(&p.BucketMS, &band, &p.Bytes, &p.Packets, &p.Flows); err != nil {
			return fmt.Errorf("scan stacked aggregate point: %w", err)
		}
		recordSplitPoint(bandPoints, band, &p)
		recordSplitTotal(bandTotals, band, &p)
		recordSplitTotal2(pointTotals, p.BucketMS, &p)
	}
	return rows.Err()
}

func (s *Store) splitRawKeyTotals(ctx context.Context, keyExpr string, seg splitSegment, filterSQL string, filterArgs []interface{}, metric string, totals map[string]*splitTotals) (bool, error) {
	query := fmt.Sprintf(`
		SELECT %s AS band, COALESCE(SUM(bytes),0), COALESCE(SUM(packets),0), COUNT(*)
		FROM flows WHERE end_ms >= ? AND end_ms < ?%s
		GROUP BY band`, keyExpr, filterSQL)
	for start := seg.from; start < seg.to; start += filteredChartChunk {
		if ctx.Err() != nil {
			return true, nil
		}
		end := min64(start+filteredChartChunk, seg.to)
		args := append([]interface{}{start, end}, filterArgs...)
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			if ctx.Err() != nil {
				return true, nil
			}
			return false, fmt.Errorf("query split keys: %w", err)
		}
		band := ""
		var p Point
		for rows.Next() {
			if err := rows.Scan(&band, &p.Bytes, &p.Packets, &p.Flows); err != nil {
				_ = rows.Close()
				return false, fmt.Errorf("scan split key: %w", err)
			}
			total := totals[band]
			if total == nil {
				total = &splitTotals{}
				totals[band] = total
			}
			total.add(&p)
		}
		rowErr := rows.Err()
		_ = rows.Close()
		if rowErr != nil {
			if ctx.Err() != nil {
				return true, nil
			}
			return false, rowErr
		}
	}
	return false, nil
}

func (s *Store) splitRawBandPoints(ctx context.Context, keyExpr string, seg splitSegment, keys []string, filterSQL string,
	filterArgs []interface{}, bandPoints map[string]map[int64]*Point, bandTotals map[string]*Point, pointTotals map[int64]*Point) (bool, error) {
	bandExpr, bandArgs := caseBand(keyExpr, keys)
	query := fmt.Sprintf(`
		SELECT (end_ms / ?) * ? AS bucket, %s AS band,
		       COALESCE(SUM(bytes),0), COALESCE(SUM(packets),0), COUNT(*)
		FROM flows WHERE end_ms >= ? AND end_ms < ?%s
		GROUP BY bucket, band`, bandExpr, filterSQL)
	leading := make([]interface{}, 0, len(bandArgs)+2)
	leading = append(leading, seg.bucket, seg.bucket)
	leading = append(leading, bandArgs...)
	chunk := max64(seg.bucket, filteredChartChunk)
	for start := (seg.from / seg.bucket) * seg.bucket; start < seg.to; start += chunk {
		if ctx.Err() != nil {
			return true, nil
		}
		end := min64(start+chunk, seg.to)
		args := append(append([]interface{}{}, leading...), start, end)
		args = append(args, filterArgs...)
		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			if ctx.Err() != nil {
				return true, nil
			}
			return false, fmt.Errorf("query stacked timeseries: %w", err)
		}
		band := ""
		var p Point
		for rows.Next() {
			if err := rows.Scan(&p.BucketMS, &band, &p.Bytes, &p.Packets, &p.Flows); err != nil {
				_ = rows.Close()
				return false, fmt.Errorf("scan stacked point: %w", err)
			}
			recordSplitPoint(bandPoints, band, &p)
			recordSplitTotal(bandTotals, band, &p)
			recordSplitTotal2(pointTotals, p.BucketMS, &p)
		}
		rowErr := rows.Err()
		_ = rows.Close()
		if rowErr != nil {
			if ctx.Err() != nil {
				return true, nil
			}
			return false, rowErr
		}
	}
	return false, nil
}

func recordSplitPoint(points map[string]map[int64]*Point, band string, p *Point) {
	buckets := points[band]
	if buckets == nil {
		buckets = make(map[int64]*Point)
		points[band] = buckets
	}
	recordSplitTotal2(buckets, p.BucketMS, p)
}

func recordSplitTotal(totals map[string]*Point, band string, p *Point) {
	if dst := totals[band]; dst != nil {
		dst.Bytes += p.Bytes
		dst.Packets += p.Packets
		dst.Flows += p.Flows
		return
	}
	totals[band] = &Point{Bytes: p.Bytes, Packets: p.Packets, Flows: p.Flows}
}

func recordSplitTotal2(totals map[int64]*Point, bucket int64, p *Point) {
	if dst := totals[bucket]; dst != nil {
		dst.Bytes += p.Bytes
		dst.Packets += p.Packets
		dst.Flows += p.Flows
		return
	}
	totals[bucket] = &Point{BucketMS: bucket, Bytes: p.Bytes, Packets: p.Packets, Flows: p.Flows}
}
