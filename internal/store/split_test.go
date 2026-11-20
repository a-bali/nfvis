package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/balia/nfvis/internal/model"
)

func protocolFlow(end time.Time, src, dst string, protocol uint8, bytes uint64) model.Flow {
	f := flow(end, src, dst, bytes)
	f.Protocol = protocol
	return f
}

func insertFlows(t *testing.T, st *Store, flows ...model.Flow) {
	t.Helper()
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatalf("insert flows: %v", err)
	}
}

// splitRange returns a range around the given buckets that covers only the
// inserted raw flows.
func splitRange(base time.Time, span time.Duration) Range {
	return Range{FromMS: base.UnixMilli(), ToMS: base.Add(span).UnixMilli()}
}

func TestTimeseriesSplitStacksBands(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	insertFlows(t, st,
		protocolFlow(base.Add(10*time.Second), "10.0.0.1", "10.0.0.2", 6, 400),
		protocolFlow(base.Add(20*time.Second), "10.0.0.3", "10.0.0.4", 17, 100),
		protocolFlow(base.Add(70*time.Second), "10.0.0.1", "10.0.0.2", 6, 200),
	)
	res, err := st.TimeseriesSplit(context.Background(), Query{
		Range: splitRange(base, 2*time.Minute), SplitBy: "protocol", Metric: "bytes",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.SplitBy != "protocol" || res.Metric != "bytes" {
		t.Fatalf("split=%q metric=%q", res.SplitBy, res.Metric)
	}
	if len(res.Series) != 2 {
		t.Fatalf("series=%d want 2 (%+v)", len(res.Series), res.Series)
	}
	if res.Series[0].Key != "6" || res.Series[0].Bytes != 600 {
		t.Fatalf("top band=%+v want protocol 6 with 600 bytes", res.Series[0])
	}
	if res.Series[1].Key != "17" || res.Series[1].Bytes != 100 {
		t.Fatalf("second band=%+v want protocol 17 with 100 bytes", res.Series[1])
	}
	var total uint64
	for _, p := range res.Points {
		total += p.Bytes
	}
	if total != 700 {
		t.Fatalf("total bytes=%d want 700", total)
	}
	// Every band carries one point per bucket so the stack aligns.
	for _, band := range res.Series {
		if len(band.Points) != len(res.Points) {
			t.Fatalf("band %q points=%d totals=%d", band.Key, len(band.Points), len(res.Points))
		}
	}
	// Bucket values must add up to the band total.
	for _, band := range res.Series {
		var sum uint64
		for _, p := range band.Points {
			sum += uint64(p[1])
		}
		if sum != band.Bytes {
			t.Fatalf("band %q points sum=%d total=%d", band.Key, sum, band.Bytes)
		}
	}
}

func TestTimeseriesSplitFoldsRemainingIntoOther(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	flows := make([]model.Flow, 0, maxSplitSeries+5)
	for i := 0; i < maxSplitSeries+5; i++ {
		flows = append(flows, flow(
			base.Add(time.Duration(i)*time.Second),
			fmt.Sprintf("10.0.0.%d", i+1), "10.1.0.1",
			uint64(1000*(i+1)),
		))
	}
	insertFlows(t, st, flows...)

	res, err := st.TimeseriesSplit(context.Background(), Query{
		Range: splitRange(base, 2*time.Minute), SplitBy: "src_host",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Series) != maxSplitSeries+1 {
		t.Fatalf("series=%d want %d", len(res.Series), maxSplitSeries+1)
	}
	last := res.Series[len(res.Series)-1]
	if !last.Other || last.Label != "" {
		// Labels are applied by the API; the store marks the band only.
		if !last.Other {
			t.Fatalf("last band is not the fold: %+v", last)
		}
	}
	var expected uint64 = 15_000 // hosts 1..5 (1000..5000 bytes) fold into "other"
	if last.Bytes != expected {
		t.Fatalf("other bytes=%d want %d", last.Bytes, expected)
	}
	var stacked uint64
	for _, band := range res.Series {
		stacked += band.Bytes
	}
	var total uint64
	for _, p := range res.Points {
		total += p.Bytes
	}
	if stacked != total {
		t.Fatalf("stacked=%d total=%d; the fold must preserve the total", stacked, total)
	}
}

func TestTimeseriesSplitRespectsFilters(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	insertFlows(t, st,
		protocolFlow(base.Add(5*time.Second), "10.0.0.1", "10.0.0.2", 6, 500),
		protocolFlow(base.Add(6*time.Second), "10.0.0.3", "10.0.0.4", 17, 300),
	)
	res, err := st.TimeseriesSplit(context.Background(), Query{
		Range: splitRange(base, 2*time.Minute), SplitBy: "src_host",
		Filters: map[string]string{"protocol": "17"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Series) != 1 || res.Series[0].Key != "10.0.0.3" {
		t.Fatalf("series=%+v want the UDP source only", res.Series)
	}
	if res.Series[0].Bytes != 300 {
		t.Fatalf("bytes=%d want 300", res.Series[0].Bytes)
	}
}

func TestTimeseriesSplitRanksByMetric(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	insertFlows(t, st,
		// Fewer, larger TCP flows beat many small UDP flows on bytes but not on
		// flow count.
		protocolFlow(base.Add(1*time.Second), "10.0.0.1", "10.0.0.2", 6, 10_000),
	)
	for i := 0; i < 5; i++ {
		insertFlows(t, st, protocolFlow(base.Add(time.Duration(10+i)*time.Second), "10.0.0.5", "10.0.0.6", 17, 10))
	}
	rng := splitRange(base, 2*time.Minute)
	byBytes, err := st.TimeseriesSplit(context.Background(), Query{Range: rng, SplitBy: "protocol", Metric: "bytes"})
	if err != nil {
		t.Fatal(err)
	}
	if byBytes.Series[0].Key != "6" {
		t.Fatalf("by bytes top=%q want 6", byBytes.Series[0].Key)
	}
	byFlows, err := st.TimeseriesSplit(context.Background(), Query{Range: rng, SplitBy: "protocol", Metric: "flows"})
	if err != nil {
		t.Fatal(err)
	}
	if byFlows.Series[0].Key != "17" {
		t.Fatalf("by flows top=%q want 17", byFlows.Series[0].Key)
	}
}

func TestTimeseriesSplitUsesInterfaceLabels(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	base := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	a := flow(base.Add(time.Second), "10.0.0.1", "10.0.0.2", 100)
	a.InputIf, a.InputLabel = 10, "opt3"
	b := flow(base.Add(2*time.Second), "10.0.0.3", "10.0.0.4", 50)
	b.InputIf, b.InputLabel = 11, "lan"
	insertFlows(t, st, a, b)

	res, err := st.TimeseriesSplit(context.Background(), Query{
		Range: splitRange(base, 2*time.Minute), SplitBy: "input_if",
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{res.Series[0].Key, res.Series[1].Key}
	if keys[0] != "opt3" || keys[1] != "lan" {
		t.Fatalf("keys=%v want the interface labels", keys)
	}
}

func TestTimeseriesSplitRejectsUnknownDimension(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	if _, err := st.TimeseriesSplit(context.Background(), Query{
		Range: Range{FromMS: 1, ToMS: 2}, SplitBy: "exporter",
	}); err == nil {
		t.Fatal("want an error for an unsupported split dimension")
	}
}

func TestTimeseriesSplitWithoutRawData(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC()
	res, err := st.TimeseriesSplit(context.Background(), Query{
		Range: Range{FromMS: now.Add(-time.Hour).UnixMilli(), ToMS: now.UnixMilli()}, SplitBy: "protocol",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Series) != 0 || len(res.Points) != 0 {
		t.Fatalf("want an empty result, got %d series / %d points", len(res.Series), len(res.Points))
	}
	if len(res.Limitations) == 0 {
		t.Fatal("want limitations explaining the empty chart")
	}
}

// A long range reads whole hours from the hourly dimension aggregates and only
// scans raw flows for the partial hours at each end, so the chart stays fast and
// the stack still sums to the totals.
func TestTimeseriesSplitUsesHourlyAggregatesForLongRanges(t *testing.T) {
	st, _ := testStore(t, 72*time.Hour, 30*24*time.Hour)
	base := time.Now().UTC().Add(-10 * time.Hour).Truncate(time.Hour)
	var flows []model.Flow
	for hour := 0; hour < 8; hour++ {
		at := base.Add(time.Duration(hour)*time.Hour + 5*time.Minute)
		flows = append(flows,
			protocolFlow(at, "10.1.0.1", "10.1.0.2", 6, 1000),
			protocolFlow(at.Add(time.Second), "10.1.0.3", "10.1.0.4", 17, 500),
		)
	}
	insertFlows(t, st, flows...)

	q := Query{
		Range:   Range{FromMS: base.UnixMilli(), ToMS: base.Add(8 * time.Hour).UnixMilli()},
		SplitBy: "protocol",
	}
	res, err := st.TimeseriesSplit(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if res.Resolution != "aggregate" && res.Resolution != "mixed" {
		t.Fatalf("resolution=%q want an aggregate-backed chart", res.Resolution)
	}
	if len(res.Series) != 2 {
		t.Fatalf("series=%d want 2", len(res.Series))
	}
	var total uint64
	for _, p := range res.Points {
		total += p.Bytes
	}
	if total != 12000 {
		t.Fatalf("total bytes=%d want 12000", total)
	}
	var bandSum uint64
	for _, band := range res.Series {
		bandSum += band.Bytes
		for _, p := range band.Points {
			if p[1] == 0 {
				continue
			}
			if p[0]%hourMS != 0 && p[0]%minuteMS != 0 {
				t.Fatalf("bucket %d is not aligned", p[0])
			}
		}
	}
	if bandSum != total {
		t.Fatalf("bands sum=%d totals=%d; the stack must match the chart", bandSum, total)
	}
	if len(res.Limitations) == 0 {
		t.Fatal("want a limitation explaining hourly bands")
	}
}

// Filters cannot be answered from the hourly aggregates, so a filtered stacked
// chart scans raw flows only.
func TestTimeseriesSplitFilteredStaysRaw(t *testing.T) {
	st, _ := testStore(t, 72*time.Hour, 30*24*time.Hour)
	base := time.Now().UTC().Add(-10 * time.Hour).Truncate(time.Hour)
	var flows []model.Flow
	for hour := 0; hour < 8; hour++ {
		at := base.Add(time.Duration(hour)*time.Hour + 5*time.Minute)
		flows = append(flows, protocolFlow(at, "10.2.0.1", "10.2.0.2", 6, 700))
	}
	insertFlows(t, st, flows...)

	res, err := st.TimeseriesSplit(context.Background(), Query{
		Range:   Range{FromMS: base.UnixMilli(), ToMS: base.Add(8 * time.Hour).UnixMilli()},
		SplitBy: "protocol",
		Filters: map[string]string{"protocol": "6"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Resolution != "raw" {
		t.Fatalf("resolution=%q want raw for a filtered chart", res.Resolution)
	}
	if len(res.Series) != 1 || res.Series[0].Bytes != 5600 {
		t.Fatalf("series=%+v want one band with 5600 bytes", res.Series)
	}
}
