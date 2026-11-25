package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

const (
	minuteMS           = int64(time.Minute / time.Millisecond)
	maxSeriesPoints    = 20000
	filteredChartChunk = int64(6 * time.Hour / time.Millisecond)
	maxFlowHosts       = 2000
)

// Range is a half-open UTC time interval in Unix milliseconds.
type Range struct {
	FromMS int64 `json:"from_ms"`
	ToMS   int64 `json:"to_ms"`
}

// Query describes a bounded analytical query.
type Query struct {
	Range
	Dimension string
	Direction string
	BucketMS  int64
	Limit     int
	Offset    int
	AllRows   bool // internal: caller performs filtering and pagination
	Filters   map[string]string
	// SplitBy and Metric drive stacked charts: SplitBy names the dimension
	// whose bands are stacked, Metric ranks those bands.
	SplitBy string
	Metric  string

	SrcHostIPs     []string
	DstHostIPs     []string
	SrcHostNoMatch bool
	DstHostNoMatch bool
}

// Point is one time bucket.
type Point struct {
	BucketMS     int64  `json:"bucket_ms"`
	DurationMS   int64  `json:"duration_ms"`
	Bytes        uint64 `json:"bytes"`
	Packets      uint64 `json:"packets"`
	Flows        uint64 `json:"flows"`
	SampledFlows uint64 `json:"sampled_flows"`
	UnknownFlows uint64 `json:"unknown_flows"`
}

// TimeseriesResult combines raw and aggregate coverage without double counting.
type TimeseriesResult struct {
	Range
	Resolution           string   `json:"resolution"`
	RawAvailableFromMS   int64    `json:"raw_available_from_ms"`
	AggregateAvailableTo int64    `json:"aggregate_available_to_ms"`
	BucketMS             int64    `json:"bucket_ms"`
	AggregateBucketMS    int64    `json:"aggregate_bucket_ms"`
	Limitations          []string `json:"limitations"`
	Points               []Point  `json:"points"`
}

// TopRow is one ranked dimension value.
type TopRow struct {
	Key     string `json:"key"`
	Label   string `json:"label,omitempty"`
	Unknown bool   `json:"unknown,omitempty"`
	Bytes   uint64 `json:"bytes"`
	Packets uint64 `json:"packets"`
	Flows   uint64 `json:"flows"`
	Sampled uint64 `json:"sampled_flows"`
}

// TopResult is a paginated ranking over mixed raw/aggregate coverage.
type TopResult struct {
	Range
	Resolution           string   `json:"resolution"`
	RawAvailableFromMS   int64    `json:"raw_available_from_ms"`
	AggregateAvailableTo int64    `json:"aggregate_available_to_ms"`
	Limitations          []string `json:"limitations"`
	Dimension            string   `json:"dimension"`
	Direction            string   `json:"direction"`
	Total                int      `json:"total"`
	Rows                 []TopRow `json:"rows"`
}

// FlowRecord is one retained individual flow.
type FlowRecord struct {
	ID       int64  `json:"id"`
	SrcName  string `json:"src_name,omitempty"`
	DstName  string `json:"dst_name,omitempty"`
	SrcDNSMS int64  `json:"src_dns_ms,omitempty"`
	DstDNSMS int64  `json:"dst_dns_ms,omitempty"`
	model.Flow
}

// FlowPage is a paginated flow-detail result.
type FlowPage struct {
	Range
	DetailAvailable bool         `json:"detail_available"`
	Limitation      string       `json:"limitation,omitempty"`
	Total           int64        `json:"total"`
	Limit           int          `json:"limit"`
	Offset          int          `json:"offset"`
	Rows            []FlowRecord `json:"rows"`
}

type coverageSplit struct {
	aggFrom       int64
	aggTo         int64
	rawHeadTo     int64
	rawTailFrom   int64
	rawDetailFrom int64
	resolution    string
	limitations   []string
}

// splitCoverage finds the legacy boundary between aggregate-only history and
// raw flow detail. It is used for queries that cannot be answered from hourly
// aggregates, such as unscoped rankings where source and destination would be
// double counted.
func (s *Store) splitCoverage(c Coverage, from, to, bucketMS int64) coverageSplit {
	rawFrom := to
	if c.RawAvailable {
		rawFrom = (c.RawOldestMS / bucketMS) * bucketMS
	}
	split := coverageSplit{
		aggFrom:       from,
		aggTo:         from,
		rawHeadTo:     from,
		rawTailFrom:   rawFrom,
		rawDetailFrom: rawFrom,
	}
	if from >= rawFrom {
		split.resolution = "raw"
	} else if to <= rawFrom {
		split.resolution = "aggregate"
	} else {
		split.resolution = "mixed"
	}
	if from < rawFrom {
		split.aggTo = min64(to, rawFrom)
		split.limitations = appendRawDetailLimitation(split.limitations, rawFrom)
	}
	if !c.AggregateAvailable && !c.RawAvailable && from < rawFrom {
		split.limitations = append(split.limitations, "No hourly aggregates cover the requested older period.")
	}
	if !c.RawAvailable && to > rawFrom {
		split.limitations = append(split.limitations, "No individual flows are retained; only hourly aggregates are available.")
	}
	return split
}

// fastSplitCoverage uses hourly aggregates for every complete hour they
// cover, even while raw flows are still retained. That keeps long dashboard
// ranges bounded: only the current, still-changing hour needs raw scanning.
func (s *Store) fastSplitCoverage(c Coverage, from, to, nowMS int64) coverageSplit {
	split := coverageSplit{
		aggFrom:       from,
		aggTo:         from,
		rawHeadTo:     from,
		rawTailFrom:   to,
		rawDetailFrom: to,
	}
	if c.RawAvailable {
		split.rawDetailFrom = (c.RawOldestMS / hourMS) * hourMS
	}

	aggStart := c.AggregateOldestMS
	aggEnd := (nowMS / hourMS) * hourMS
	if c.AggregateAvailable && c.AggregateNewestMS > 0 {
		aggEnd = min64(aggEnd, c.AggregateNewestMS+hourMS)
	}
	if !c.AggregateAvailable || aggEnd <= aggStart {
		aggEnd = aggStart
	}
	if !c.RawAvailable && aggEnd < to {
		// With no raw detail, empty future hours are represented as empty
		// aggregate buckets rather than as a raw scan that can never work.
		aggEnd = to
	}
	// Only whole hours contained by the request can come from aggregate
	// rows; a partial first or last hour must stay on raw flows.
	firstWholeHour := ceilDiv(from, hourMS) * hourMS
	lastWholeHour := (to / hourMS) * hourMS
	split.aggFrom = max64(firstWholeHour, aggStart)
	split.aggTo = min64(lastWholeHour, aggEnd)
	if !c.RawAvailable {
		split.aggTo = min64(to, aggEnd)
	}
	if split.aggTo < split.aggFrom {
		split.aggTo = split.aggFrom
	}

	if c.RawAvailable && from < split.aggFrom {
		split.rawHeadTo = min64(to, split.aggFrom)
	}
	split.rawTailFrom = to
	if c.RawAvailable {
		split.rawTailFrom = max64(from, split.aggTo)
	}

	hasAggregate := split.aggTo > split.aggFrom
	hasRawHead := split.rawHeadTo > from
	hasRawTail := to > split.rawTailFrom
	switch {
	case hasAggregate && (hasRawHead || hasRawTail):
		split.resolution = "mixed"
	case hasAggregate:
		split.resolution = "aggregate"
	default:
		split.resolution = "raw"
	}

	if from < split.rawDetailFrom {
		split.limitations = appendRawDetailLimitation(split.limitations, split.rawDetailFrom)
	}
	// Older than the first aggregate does not imply missing aggregates: it
	// may predate collection entirely, or retained raw flows may cover it.
	if !c.AggregateAvailable && !c.RawAvailable && from < to {
		split.limitations = append(split.limitations, "No hourly aggregates cover the requested period.")
	}
	if !c.RawAvailable && from < to {
		split.limitations = append(split.limitations, "No individual flows are retained; only hourly aggregates are available.")
	}
	return split
}

func appendRawDetailLimitation(limitations []string, rawFrom int64) []string {
	return append(limitations, fmt.Sprintf(
		"Flow details are unavailable before %s UTC.",
		time.UnixMilli(rawFrom).UTC().Format("2006-01-02 15:04"),
	))
}

func rawOnlySplit(split coverageSplit, from, to int64) coverageSplit {
	split.aggTo = split.aggFrom
	split.rawHeadTo = from
	split.rawTailFrom = from
	split.resolution = "raw"
	return split
}

// Note: rawOnlySplit is kept for ranking queries that cannot use aggregates.

func (split coverageSplit) rawSegments(from, to int64) [][2]int64 {
	var segments [][2]int64
	if split.rawHeadTo > from {
		segments = append(segments, [2]int64{from, min64(split.rawHeadTo, to)})
	}
	if split.rawTailFrom < to {
		start := max64(from, split.rawTailFrom)
		if start < to {
			segments = append(segments, [2]int64{start, to})
		}
	}
	return segments
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func ceilDiv(a, b int64) int64 {
	if b <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

// niceBuckets is the ladder of chart bucket widths, finest first. Buckets are
// rounded up from the finest tier that covers the range.
var niceBuckets = []int64{
	minuteMS, 2 * minuteMS, 5 * minuteMS, 10 * minuteMS, 15 * minuteMS, 30 * minuteMS,
	hourMS, 2 * hourMS, 3 * hourMS, 6 * hourMS, 12 * hourMS, 24 * hourMS, 7 * 24 * hourMS,
}

// appendBuckets fills missing buckets on one UTC-aligned grid.
func appendBuckets(out *[]Point, points map[int64]*Point, from, to, bucketMS int64, limit int, partial bool) {
	if bucketMS <= 0 {
		return
	}
	for bucket := (from / bucketMS) * bucketMS; bucket < to && len(*out) < limit; bucket += bucketMS {
		duration := bucketMS
		if partial {
			duration = min64(bucket+bucketMS, to) - max64(bucket, from)
		}
		if p, ok := points[bucket]; ok {
			point := *p
			point.DurationMS = duration
			*out = append(*out, point)
		} else {
			*out = append(*out, Point{BucketMS: bucket, DurationMS: duration})
		}
	}
}

// tsSegment is one part of a chart range served by a single resolution tier.
type tsSegment struct {
	from  int64
	to    int64
	resMS int64
}

// desiredBucket returns the finest tier bucket whose retention covers spanMS.
func (s *Store) desiredBucket(spanMS int64) int64 {
	if spanMS <= 0 {
		return minuteMS
	}
	for _, tier := range s.tiers {
		if spanMS <= tier.retentionMS {
			return tier.resMS
		}
	}
	if len(s.tiers) > 0 {
		return s.tiers[len(s.tiers)-1].resMS
	}
	return hourMS
}

// tierSegments assigns each part of a range to the finest tier that retains
// it, newest first. Boundaries are aligned to the next coarser tier so that no
// coarse bucket straddles two segments and double counts. The returned gapFrom
// marks the start of the oldest retained data when the request reaches further
// back than anything still stored.
func (s *Store) tierSegments(c Coverage, from, to, nowMS int64) ([]tsSegment, int64) {
	retained := make(map[int64]TierCoverage, len(c.Tiers))
	for _, tier := range c.Tiers {
		retained[tier.BucketMS] = tier
	}
	if len(s.tiers) == 0 {
		return nil, 0
	}
	segments := make([]tsSegment, 0, len(s.tiers)+1)
	cursor := to
	for i, tier := range s.tiers {
		tc := retained[tier.resMS]
		if !tc.Available {
			continue
		}
		boundary := nowMS - tier.retentionMS
		if tc.OldestMS > boundary {
			boundary = tc.OldestMS
		}
		if i+1 < len(s.tiers) {
			boundary = (boundary / s.tiers[i+1].resMS) * s.tiers[i+1].resMS
		} else {
			boundary = (boundary / tier.resMS) * tier.resMS
		}
		start := max64(from, boundary)
		if start >= cursor {
			continue
		}
		segments = append(segments, tsSegment{from: start, to: cursor, resMS: tier.resMS})
		cursor = start
	}
	gapFrom := int64(0)
	if cursor > from {
		// Nothing is retained between from and cursor. The chart starts at the
		// oldest retained bucket and the caller adds a limitation explaining the
		// gap, so no empty filler segment is emitted (it would otherwise force a
		// coarser width onto the whole series).
		gapFrom = cursor
	}
	return segments, gapFrom
}

// planSegments chooses one chart bucket width per segment. Widths start at the
// finest tier covering the range and only coarsen to stay inside the point
// budget, so tiers are never shown finer than they are retained.
func planSegments(segments []tsSegment, desired int64) ([]int64, int64) {
	widths := make([]int64, len(segments))
	if len(segments) == 0 {
		return widths, desired
	}
	for _, bucket := range niceBuckets {
		if bucket < desired {
			continue
		}
		total := 0
		for i, seg := range segments {
			width := ceilDiv(bucket, seg.resMS) * seg.resMS
			widths[i] = width
			total += int(ceilDiv(seg.to-seg.from, width))
		}
		if total <= maxSeriesPoints {
			return widths, bucket
		}
	}
	coarsest := niceBuckets[len(niceBuckets)-1]
	for i, seg := range segments {
		widths[i] = ceilDiv(coarsest, seg.resMS) * seg.resMS
	}
	return widths, coarsest
}

func appendChartCoverageLimitation(limitations []string, from int64) []string {
	return append(limitations, fmt.Sprintf(
		"No data is available before %s UTC.",
		time.UnixMilli(from).UTC().Format("2006-01-02 15:04"),
	))
}

// Timeseries returns traffic totals, mixing resolution tiers so that the most
// recent part of a range is as fine as retention allows and older parts fall
// back to coarser buckets without double counting.
func (s *Store) Timeseries(ctx context.Context, q Query) (TimeseriesResult, error) {
	result := TimeseriesResult{Range: q.Range, Limitations: []string{}, Points: []Point{}}
	coverage, err := s.Coverage(ctx)
	if err != nil {
		return result, err
	}
	result.RawAvailableFromMS = coverage.RawOldestMS
	result.AggregateAvailableTo = coverage.AggregateNewestMS + hourMS
	filtered := hasNonAggregateFilter(q.Filters) || len(q.SrcHostIPs) > 0 || len(q.DstHostIPs) > 0 || q.SrcHostNoMatch || q.DstHostNoMatch
	if filtered {
		if history, used, err := s.hostHistory(ctx, q, coverage, result); used || err != nil {
			return history, err
		}
		return s.timeseriesRaw(ctx, q, coverage, result)
	}

	segments, gapFrom := s.tierSegments(coverage, q.FromMS, q.ToMS, time.Now().UnixMilli())
	if len(segments) == 0 {
		result.Resolution = "aggregate"
		result.BucketMS = s.desiredBucket(q.ToMS - q.FromMS)
		result.AggregateBucketMS = result.BucketMS
		result.Limitations = append(result.Limitations, "No traffic was recorded in this period.")
		return result, nil
	}
	if gapFrom > 0 {
		result.Limitations = appendChartCoverageLimitation(result.Limitations, gapFrom)
	}
	coveredFrom := segments[len(segments)-1].from
	desired := s.desiredBucket(q.ToMS - coveredFrom)
	if q.BucketMS > desired {
		desired = ceilDiv(q.BucketMS, minuteMS) * minuteMS
	}
	widths, bucket := planSegments(segments, desired)
	points := make(map[int64]*Point)
	coarsest := int64(0)
	finest := int64(0)
	for i, seg := range segments {
		if err := s.scanTrafficSegment(ctx, seg, widths[i], points); err != nil {
			return result, err
		}
		if widths[i] > coarsest {
			coarsest = widths[i]
		}
		if finest == 0 || widths[i] < finest {
			finest = widths[i]
		}
	}
	result.BucketMS = finest
	result.AggregateBucketMS = coarsest
	if result.BucketMS == 0 {
		result.BucketMS = bucket
		result.AggregateBucketMS = bucket
	}
	// "mixed" describes what the caller receives: several bucket widths in one
	// series. Overlapping tiers rendered at the same width are still uniform.
	if finest != coarsest {
		result.Resolution = "mixed"
	} else {
		result.Resolution = "aggregate"
	}

	filled := make([]Point, 0, maxSeriesPoints)
	for i := len(segments) - 1; i >= 0; i-- {
		appendBuckets(&filled, points, segments[i].from, segments[i].to, widths[i], maxSeriesPoints, false)
	}
	result.Points = filled
	if len(result.Points) == 0 {
		result.Limitations = append(result.Limitations, "No traffic was recorded in this period.")
	}
	return result, nil
}

func (s *Store) scanTrafficSegment(ctx context.Context, seg tsSegment, bucketMS int64, points map[int64]*Point) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT (bucket_ms / ?) * ? AS bucket,
		       SUM(bytes), SUM(packets), SUM(flows), SUM(sampled_flows), SUM(unknown_flows)
		FROM agg_traffic
		WHERE res_ms = ? AND bucket_ms >= ? AND bucket_ms < ?
		GROUP BY bucket`, bucketMS, bucketMS, seg.resMS, seg.from, seg.to)
	if err != nil {
		return fmt.Errorf("query traffic timeseries: %w", err)
	}
	return scanPoints(rows, points)
}

// timeseriesRaw serves filtered charts from retained individual flows. Results
// are scanned in bounded chunks so an expensive filter still returns partial
// data plus an explicit limitation instead of failing the request.
func (s *Store) timeseriesRaw(ctx context.Context, q Query, coverage Coverage, result TimeseriesResult) (TimeseriesResult, error) {
	result.Resolution = "raw"
	from := q.FromMS
	if !coverage.RawAvailable {
		result.BucketMS = s.desiredBucket(q.ToMS - q.FromMS)
		result.AggregateBucketMS = result.BucketMS
		result.Limitations = append(result.Limitations, "No individual flows are retained; only hourly aggregates are available.")
		result.Limitations = append(result.Limitations, "No traffic was recorded in this period.")
		return result, nil
	}
	from = max64(from, coverage.RawOldestMS)
	if from >= q.ToMS {
		result.BucketMS = s.desiredBucket(q.ToMS - q.FromMS)
		result.AggregateBucketMS = result.BucketMS
		result.Limitations = append(result.Limitations, "No retained individual flows cover this period.")
		result.Limitations = append(result.Limitations, "No traffic was recorded in this period.")
		return result, nil
	}
	if from > q.FromMS {
		result.Limitations = appendChartCoverageLimitation(result.Limitations, from)
	}
	bucket := s.desiredBucket(q.ToMS - from)
	if q.BucketMS > bucket {
		bucket = ceilDiv(q.BucketMS, minuteMS) * minuteMS
	}
	if coarsest := niceBuckets[len(niceBuckets)-1]; bucket > coarsest {
		bucket = coarsest
	}
	result.BucketMS = bucket
	result.AggregateBucketMS = bucket

	filterSQL, filterArgs := flowFilterSQL(q.Filters, q.SrcHostIPs, q.DstHostIPs, q.SrcHostNoMatch, q.DstHostNoMatch)
	chunk := ceilDiv(max64(bucket, filteredChartChunk), bucket) * bucket
	points := make(map[int64]*Point)
	truncated := false
	for start := from; start < q.ToMS; start += chunk {
		if ctx.Err() != nil {
			truncated = true
			break
		}
		end := min64(start+chunk, q.ToMS)
		args := append([]interface{}{bucket, bucket, start, end}, filterArgs...)
		rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
			SELECT (end_ms / ?) * ? AS bucket,
			       SUM(bytes), SUM(packets), COUNT(*), COALESCE(SUM(sampled),0),
			       COALESCE(SUM(CASE WHEN src_vlan < 0 OR dst_vlan < 0 THEN 1 ELSE 0 END),0)
			FROM flows
			WHERE end_ms >= ? AND end_ms < ?%s
			GROUP BY bucket`, filterSQL), args...)
		if err != nil {
			if ctx.Err() != nil {
				truncated = true
				break
			}
			return result, fmt.Errorf("query raw timeseries: %w", err)
		}
		if err := scanPoints(rows, points); err != nil {
			if ctx.Err() != nil {
				truncated = true
				break
			}
			return result, err
		}
	}
	if truncated {
		result.Limitations = append(result.Limitations, "The filtered chart stopped early because the query exceeded its time budget; narrow the range or remove filters for complete data.")
	}
	filled := make([]Point, 0, maxSeriesPoints)
	appendBuckets(&filled, points, from, q.ToMS, bucket, maxSeriesPoints, true)
	result.Points = filled
	if len(result.Points) == 0 {
		result.Limitations = append(result.Limitations, "No traffic was recorded in this period.")
	}
	return result, nil
}

func scanPoints(rows *sql.Rows, points map[int64]*Point) error {
	defer rows.Close()
	for rows.Next() {
		var p Point
		if err := rows.Scan(&p.BucketMS, &p.Bytes, &p.Packets, &p.Flows, &p.SampledFlows, &p.UnknownFlows); err != nil {
			return fmt.Errorf("scan timeseries point: %w", err)
		}
		if dst := points[p.BucketMS]; dst != nil {
			dst.Bytes += p.Bytes
			dst.Packets += p.Packets
			dst.Flows += p.Flows
			dst.SampledFlows += p.SampledFlows
			dst.UnknownFlows += p.UnknownFlows
		} else {
			points[p.BucketMS] = &p
		}
	}
	return rows.Err()
}

func hasNonAggregateFilter(filters map[string]string) bool {
	for key, value := range filters {
		if value == "" || key == "direction" {
			continue
		}
		return true
	}
	return false
}

// topCanUseAggregates reports whether an hourly aggregate table can answer a
// ranking without counting each flow twice. With no explicit direction,
// aggregate tables contain both source/destination or ingress/egress rows for
// the same flow, while the raw query picks one side per flow.
func topCanUseAggregates(q Query) bool {
	if hasNonAggregateFilter(q.Filters) || len(q.SrcHostIPs) > 0 || len(q.DstHostIPs) > 0 || q.SrcHostNoMatch || q.DstHostNoMatch {
		return false
	}
	dir := strings.ToLower(strings.TrimSpace(q.Direction))
	switch q.Dimension {
	case "interface":
		return dir == "ingress" || dir == "egress"
	case "host", "vlan":
		return dir == "source" || dir == "destination"
	default:
		return true
	}
}

var validDimensions = map[string]bool{
	"exporter": true, "interface": true, "host": true, "vlan": true, "protocol": true,
}

// ValidDimension reports whether a dimension name is supported.
func ValidDimension(d string) bool { return validDimensions[d] }

// Top returns a mixed-coverage ranking for one dimension.
func (s *Store) Top(ctx context.Context, q Query) (TopResult, error) {
	result := TopResult{
		Range: q.Range, Dimension: q.Dimension, Direction: q.Direction,
		Limitations: []string{}, Rows: []TopRow{},
	}
	coverage, err := s.Coverage(ctx)
	if err != nil {
		return result, err
	}
	var split coverageSplit
	if topCanUseAggregates(q) {
		split = s.fastSplitCoverage(coverage, q.FromMS, q.ToMS, time.Now().UnixMilli())
	} else {
		split = s.splitCoverage(coverage, q.FromMS, q.ToMS, hourMS)
	}
	result.Resolution = split.resolution
	result.RawAvailableFromMS = split.rawDetailFrom
	result.AggregateAvailableTo = split.aggTo
	result.Limitations = append(result.Limitations, split.limitations...)

	rowsByKey := make(map[string]*TopRow)
	if split.aggTo > split.aggFrom {
		if err := s.topAggregate(ctx, q, split.aggFrom, split.aggTo, rowsByKey); err != nil {
			return result, err
		}
	}
	for _, segment := range split.rawSegments(q.FromMS, q.ToMS) {
		if err := s.topRaw(ctx, q, segment[0], segment[1], rowsByKey); err != nil {
			return result, err
		}
	}
	rows := make([]TopRow, 0, len(rowsByKey))
	for _, row := range rowsByKey {
		rows = append(rows, *row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Bytes != rows[j].Bytes {
			return rows[i].Bytes > rows[j].Bytes
		}
		return rows[i].Key < rows[j].Key
	})
	result.Total = len(rows)
	limit, offset := normalizePage(q.Limit, q.Offset, 100, 1000)
	if q.AllRows {
		limit, offset = len(rows), 0
	}
	for i := offset; i < len(rows) && i < offset+limit; i++ {
		result.Rows = append(result.Rows, rows[i])
	}
	if len(rows) == 0 {
		result.Limitations = append(result.Limitations, "No traffic was recorded in this period.")
	}
	return result, nil
}

func normalizePage(limit, offset, defaultLimit, maxLimit int) (int, int) {
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func (s *Store) topRaw(ctx context.Context, q Query, from, to int64, rows map[string]*TopRow) error {
	expr, args := rawDimensionExpr(q)
	where := "end_ms >= ? AND end_ms < ?"
	args = append([]interface{}{}, args...)
	filterSQL, filterArgs := flowFilterSQL(q.Filters, nil, nil, false, false)
	where += filterSQL
	args = append(args, from, to)
	args = append(args, filterArgs...)
	query := fmt.Sprintf(`SELECT %s, COALESCE(MAX(%s),''), SUM(bytes), SUM(packets), COUNT(*), COALESCE(SUM(sampled),0)
		FROM flows WHERE %s GROUP BY 1`, expr, rawLabelExpr(q), where)
	r, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query raw top %s: %w", q.Dimension, err)
	}
	defer r.Close()
	for r.Next() {
		var key, label string
		var row TopRow
		if err := r.Scan(&key, &label, &row.Bytes, &row.Packets, &row.Flows, &row.Sampled); err != nil {
			return fmt.Errorf("scan raw top: %w", err)
		}
		mergeTopRow(rows, key, label, row)
	}
	return r.Err()
}

func rawDimensionExpr(q Query) (string, []interface{}) {
	dir := strings.ToLower(q.Direction)
	switch q.Dimension {
	case "exporter":
		return "exporter", nil
	case "interface":
		// Interface indexes are only meaningful together with the exporter.
		switch dir {
		case "ingress":
			return "CAST(input_if AS TEXT) || '@' || exporter", nil
		case "egress":
			return "CAST(output_if AS TEXT) || '@' || exporter", nil
		default:
			return "CASE WHEN ? = 'ingress' THEN CAST(input_if AS TEXT) || '@' || exporter ELSE CAST(output_if AS TEXT) || '@' || exporter END", []interface{}{"ingress"}
		}
	case "host":
		switch dir {
		case "source":
			return "src_addr", nil
		case "destination":
			return "dst_addr", nil
		default:
			return "CASE WHEN ? = 'source' THEN src_addr ELSE dst_addr END", []interface{}{"source"}
		}
	case "vlan":
		switch dir {
		case "source":
			return "CAST(src_vlan AS TEXT)", nil
		case "destination":
			return "CAST(dst_vlan AS TEXT)", nil
		default:
			return "CASE WHEN ? = 'source' THEN CAST(src_vlan AS TEXT) ELSE CAST(dst_vlan AS TEXT) END", []interface{}{"source"}
		}
	case "protocol":
		return "CAST(protocol AS TEXT)", nil
	default:
		return "''", nil
	}
}

func rawLabelExpr(q Query) string {
	if q.Dimension == "interface" {
		dir := strings.ToLower(q.Direction)
		switch dir {
		case "egress":
			return "output_label"
		case "ingress":
			return "input_label"
		default:
			return "CASE WHEN input_if > 0 THEN input_label ELSE output_label END"
		}
	}
	return "''"
}

func (s *Store) topAggregate(ctx context.Context, q Query, from, to int64, rows map[string]*TopRow) error {
	dir := strings.ToLower(q.Direction)
	var table, keyExpr, labelExpr string
	switch q.Dimension {
	case "exporter":
		table, keyExpr, labelExpr = "agg_exporter", "exporter", "''"
	case "interface":
		table, keyExpr, labelExpr = "agg_interface", "CAST(if_index AS TEXT) || '@' || exporter", "label"
	case "host":
		table, keyExpr, labelExpr = "agg_host", "host", "''"
	case "vlan":
		table, keyExpr, labelExpr = "agg_vlan", "CAST(vlan AS TEXT)", "''"
	case "protocol":
		table, keyExpr, labelExpr = "agg_protocol", "CAST(protocol AS TEXT)", "''"
	default:
		return nil
	}
	directionFilter := ""
	if q.Dimension != "exporter" && q.Dimension != "protocol" && (dir == "ingress" || dir == "egress" || dir == "source" || dir == "destination") {
		directionFilter = " AND direction = ?"
	}
	query := fmt.Sprintf(`SELECT %s, COALESCE(MAX(%s),''), SUM(bytes), SUM(packets), SUM(flows), SUM(sampled_flows)
		FROM %s WHERE hour_ms >= ? AND hour_ms < ?%s GROUP BY 1`, keyExpr, labelExpr, table, directionFilter)
	queryArgs := []interface{}{from, to}
	if directionFilter != "" {
		queryArgs = append(queryArgs, dir)
	}
	r, err := s.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return fmt.Errorf("query aggregate top %s: %w", q.Dimension, err)
	}
	defer r.Close()
	for r.Next() {
		var key, label string
		var row TopRow
		if err := r.Scan(&key, &label, &row.Bytes, &row.Packets, &row.Flows, &row.Sampled); err != nil {
			return fmt.Errorf("scan aggregate top: %w", err)
		}
		mergeTopRow(rows, key, label, row)
	}
	return r.Err()
}

func mergeTopRow(rows map[string]*TopRow, key, label string, row TopRow) {
	row.Key = key
	if label != "" {
		row.Label = label
	}
	row.Unknown = unknownTopKey(key)
	if existing := rows[key]; existing != nil {
		existing.Bytes += row.Bytes
		existing.Packets += row.Packets
		existing.Flows += row.Flows
		existing.Sampled += row.Sampled
		if existing.Label == "" {
			existing.Label = row.Label
		}
		existing.Unknown = existing.Unknown || row.Unknown
		return
	}
	rows[key] = &row
}

// unknownTopKey recognizes sentinel values, including composite interface
// keys such as "0@192.0.2.1" where the exporter did not report an ifIndex.
func unknownTopKey(key string) bool {
	if index, _, ok := strings.Cut(key, "@"); ok {
		key = index
	}
	return key == "" || key == "0" || key == "-1"
}

func flowFilterSQL(filters map[string]string, srcIPs, dstIPs []string, srcNoMatch, dstNoMatch bool) (string, []interface{}) {
	var b strings.Builder
	var args []interface{}
	add := func(sql string, values ...interface{}) {
		b.WriteString(" AND ")
		b.WriteString(sql)
		args = append(args, values...)
	}
	// Direction only narrows filters that have distinct source/ingress and
	// destination/egress columns. It is ignored for protocol, exporter, and
	// sampling, and mismatched pairs fall back to matching either side.
	dir := strings.ToLower(strings.TrimSpace(filters["direction"]))
	if v := filters["exporter"]; v != "" {
		add("exporter = ?", v)
	}
	if v := filters["host"]; v != "" {
		switch dir {
		case "source":
			add("src_addr = ?", v)
		case "destination":
			add("dst_addr = ?", v)
		default:
			add("(src_addr = ? OR dst_addr = ?)", v, v)
		}
	}
	if v := filters["src_host"]; v != "" {
		if address, exact, ok := ipHostTerm(v); ok {
			if exact {
				add("src_addr = ?", address)
			} else {
				add("instr(lower(src_addr), lower(?)) > 0", address)
			}
		} else {
			add("(instr(lower(src_addr), lower(?)) > 0 OR instr(lower(src_name), lower(?)) > 0)", v, v)
		}
	}
	if v := filters["dst_host"]; v != "" {
		if address, exact, ok := ipHostTerm(v); ok {
			if exact {
				add("dst_addr = ?", address)
			} else {
				add("instr(lower(dst_addr), lower(?)) > 0", address)
			}
		} else {
			add("(instr(lower(dst_addr), lower(?)) > 0 OR instr(lower(dst_name), lower(?)) > 0)", v, v)
		}
	}
	addIPSet := func(column string, values []string, noMatch bool) {
		if noMatch {
			add("1 = 0")
			return
		}
		if len(values) == 0 {
			return
		}
		placeholders := make([]string, len(values))
		args := make([]interface{}, len(values))
		for i, value := range values {
			placeholders[i] = "?"
			args[i] = value
		}
		add(column+" IN ("+strings.Join(placeholders, ",")+")", args...)
	}
	addIPSet("src_addr", srcIPs, srcNoMatch)
	addIPSet("dst_addr", dstIPs, dstNoMatch)
	if v := filters["interface"]; v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			switch dir {
			case "ingress":
				add("input_if = ?", n)
			case "egress":
				add("output_if = ?", n)
			default:
				add("(input_if = ? OR output_if = ?)", n, n)
			}
		} else {
			switch dir {
			case "ingress":
				add("input_label = ?", v)
			case "egress":
				add("output_label = ?", v)
			default:
				add("(input_label = ? OR output_label = ?)", v, v)
			}
		}
	}
	if v := filters["vlan"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			switch dir {
			case "source":
				add("src_vlan = ?", n)
			case "destination":
				add("dst_vlan = ?", n)
			default:
				add("(src_vlan = ? OR dst_vlan = ?)", n, n)
			}
		}
	}
	if v := filters["input_interface"]; v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			add("input_if = ?", n)
		} else {
			add("input_label = ?", v)
		}
	}
	if v := filters["output_interface"]; v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			add("output_if = ?", n)
		} else {
			add("output_label = ?", v)
		}
	}
	if v := filters["input_vlan"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			add("src_vlan = ?", n)
		}
	}
	if v := filters["output_vlan"]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			add("dst_vlan = ?", n)
		}
	}
	if v := filters["protocol"]; v != "" {
		if n, err := strconv.ParseUint(v, 10, 8); err == nil {
			add("protocol = ?", n)
		}
	}
	if v := filters["src_port"]; v != "" {
		if n, err := strconv.ParseUint(v, 10, 16); err == nil {
			add("src_port = ?", n)
		}
	}
	if v := filters["dst_port"]; v != "" {
		if n, err := strconv.ParseUint(v, 10, 16); err == nil {
			add("dst_port = ?", n)
		}
	}
	switch strings.ToLower(strings.TrimSpace(filters["sampled"])) {
	case "true", "1":
		add("sampled = 1")
	case "false", "0":
		add("sampled = 0")
	}
	return b.String(), args
}

const flowColumns = `id, received_ms, start_ms, end_ms, time_source, version, exporter,
	exporter_ip, exporter_id, sequence, engine_type, engine_id, input_if, input_label,
	output_if, output_label, direction, direction_known, src_addr, dst_addr, src_port,
	dst_port, protocol, tcp_flags, src_tos, dst_tos, bytes, packets, src_as, dst_as,
	src_vlan, dst_vlan, src_vlan_source, dst_vlan_source, sampled, sampling_interval,
	sampling_algorithm, sampling_source, src_name, dst_name, src_dns_ms, dst_dns_ms`

func scanFlowRow(scanner interface{ Scan(...interface{}) error }) (FlowRecord, error) {
	var r FlowRecord
	f := &r.Flow
	err := scanner.Scan(
		&r.ID, &f.ReceivedAtMS, &f.StartMS, &f.EndMS, &f.TimeSource, &f.Version,
		&f.Exporter, &f.ExporterIP, &f.ExporterID, &f.Sequence, &f.EngineType, &f.EngineID,
		&f.InputIf, &f.InputLabel, &f.OutputIf, &f.OutputLabel, &f.Direction, &f.DirectionKnown,
		&f.SrcAddr, &f.DstAddr, &f.SrcPort, &f.DstPort, &f.Protocol, &f.TCPFlags,
		&f.SrcTOS, &f.DstTOS, &f.Bytes, &f.Packets, &f.SrcAS, &f.DstAS,
		&f.SrcVLAN, &f.DstVLAN, &f.SrcVLANSource, &f.DstVLANSource,
		&f.Sampled, &f.SamplingInterval, &f.SamplingAlgorithm, &f.SamplingSource,
		&r.SrcName, &r.DstName, &r.SrcDNSMS, &r.DstDNSMS,
	)
	return r, err
}

// rawFlowStart uses the retention cutoff, not the oldest observed flow: an
// empty interval before the first recorded flow is not necessarily expired.
// Rows older than the cutoff may still exist until the next retention sweep.
func (s *Store) rawFlowStart(ctx context.Context, requested Range) (int64, int64, bool, error) {
	coverage, err := s.Coverage(ctx)
	if err != nil {
		return 0, 0, false, err
	}
	rawFrom := time.Now().UnixMilli() - s.cfg.Database.Retention.Raw.Duration().Milliseconds()
	if coverage.RawAvailable && coverage.RawOldestMS < rawFrom {
		rawFrom = coverage.RawOldestMS
	}
	if requested.ToMS <= rawFrom {
		return rawFrom, rawFrom, false, nil
	}
	return max64(requested.FromMS, rawFrom), rawFrom, true, nil
}

// FlowHosts returns the distinct retained endpoint addresses that can feed a
// DNS-host filter. The cap keeps a name search bounded on large databases.
func (s *Store) FlowHosts(ctx context.Context, q FlowQuery) ([]string, error) {
	from, _, available, err := s.rawFlowStart(ctx, q.Range)
	if err != nil || !available {
		return []string{}, err
	}
	filterSQL, args := flowFilterSQL(q.Filters, nil, nil, false, false)
	where := "end_ms >= ? AND end_ms < ?" + filterSQL
	base := append([]interface{}{from, q.ToMS}, args...)
	queryArgs := append(append([]interface{}{}, base...), base...)
	queryArgs = append(queryArgs, maxFlowHosts)
	rows, err := s.db.QueryContext(ctx, `
		SELECT addr FROM (
			SELECT src_addr AS addr FROM flows WHERE `+where+`
			UNION
			SELECT dst_addr AS addr FROM flows WHERE `+where+`
		) ORDER BY addr LIMIT ?`, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("query flow hosts: %w", err)
	}
	defer rows.Close()
	hosts := []string{}
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, fmt.Errorf("scan flow host: %w", err)
		}
		hosts = append(hosts, addr)
	}
	return hosts, rows.Err()
}

// Flows returns paginated individual flows from retained raw coverage.
func (s *Store) Flows(ctx context.Context, q FlowQuery) (FlowPage, error) {
	page := FlowPage{Range: q.Range, DetailAvailable: true, Rows: []FlowRecord{}}
	limit, offset := normalizePage(q.Limit, q.Offset, 100, 1000)
	page.Limit, page.Offset = limit, offset
	from, rawFrom, available, err := s.rawFlowStart(ctx, q.Range)
	if err != nil {
		return page, err
	}
	if q.FromMS < rawFrom {
		page.Limitation = "Only retained individual flows are returned; earlier periods may have aged out of raw retention."
	}
	if !available {
		page.DetailAvailable = false
		return page, nil
	}
	filterSQL, args := flowFilterSQL(q.Filters, q.SrcHostIPs, q.DstHostIPs, q.SrcHostNoMatch, q.DstHostNoMatch)
	where := "end_ms >= ? AND end_ms < ?" + filterSQL
	baseArgs := append([]interface{}{from, q.ToMS}, args...)

	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM flows WHERE "+where, baseArgs...).Scan(&page.Total); err != nil {
		return page, fmt.Errorf("count flows: %w", err)
	}
	columns := map[string]string{"time": "end_ms", "interface": "input_if, output_if", "vlan": "src_vlan, dst_vlan", "source": "src_addr", "destination": "dst_addr", "proto": "protocol", "bytes": "bytes", "packets": "packets"}
	order := columns[q.Sort]
	if order == "" {
		order = "end_ms"
	}
	dir := "DESC"
	if q.SortDir == "asc" {
		dir = "ASC"
	}
	parts := strings.Split(order, ", ")
	for i := range parts {
		parts[i] += " " + dir
	}
	order = strings.Join(parts, ", ") + ", id " + dir
	rows, err := s.db.QueryContext(ctx,
		"SELECT "+flowColumns+" FROM flows WHERE "+where+" ORDER BY "+order+" LIMIT ? OFFSET ?",
		append(baseArgs, limit, offset)...)
	if err != nil {
		return page, fmt.Errorf("query flows: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		r, err := scanFlowRow(rows)
		if err != nil {
			return page, fmt.Errorf("scan flow: %w", err)
		}
		page.Rows = append(page.Rows, r)
	}
	return page, rows.Err()
}

// FlowQuery describes a flow-detail request.
type FlowQuery struct {
	Range
	Limit          int
	Offset         int
	Sort           string
	SortDir        string
	Filters        map[string]string
	SrcHostIPs     []string
	DstHostIPs     []string
	SrcHostNoMatch bool
	DstHostNoMatch bool
}

// LastRetentionSweep returns the timestamp recorded by the last cleanup pass.
func (s *Store) LastRetentionSweep(ctx context.Context) (time.Time, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM collector_state WHERE key='last_retention_sweep'`).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, value)
}
