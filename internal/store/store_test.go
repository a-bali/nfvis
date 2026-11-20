package store

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

func testStore(t *testing.T, raw, hourly time.Duration) (*Store, config.Config) {
	t.Helper()
	return testStoreTiers(t, raw, hourly)
}

func testStoreTiers(t *testing.T, raw, hourly time.Duration, tiers ...config.ResolutionTier) (*Store, config.Config) {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.Database.Retention.Raw = config.Duration(raw)
	cfg.Database.Retention.Hourly = config.Duration(hourly)
	cfg.Database.Retention.CleanupBatch = 10
	if len(tiers) > 0 {
		cfg.Database.Resolution = tiers
	}
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, cfg
}

func flow(end time.Time, src, dst string, bytes uint64) model.Flow {
	hour := (end.UnixMilli() / hourMS) * hourMS
	return model.Flow{
		ReceivedAtMS: end.UnixMilli(), StartMS: hour, EndMS: end.UnixMilli(), TimeSource: "exported",
		Version: 9, Exporter: "192.0.2.1", ExporterIP: "192.0.2.1", ExporterID: 1,
		InputIf: 1, InputLabel: "wan0", OutputIf: 2, OutputLabel: "lan0",
		SrcAddr: src, DstAddr: dst, SrcPort: 1234, DstPort: 80, Protocol: 6, Bytes: bytes, Packets: bytes / 100,
		SrcVLAN: 10, SrcVLANSource: "rule", DstVLAN: 20, DstVLANSource: "rule",
		Sampled: bytes > 1000, SamplingInterval: 100,
	}
}

func countRows(t *testing.T, st *Store, table string) int64 {
	t.Helper()
	var n int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestInsertBatchWritesRawAndAggregates(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC()
	flows := []model.Flow{flow(now, "10.0.0.1", "203.0.113.1", 1000), flow(now, "10.0.0.2", "203.0.113.2", 2000)}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("raw rows = %d", got)
	}
	for _, table := range []string{"agg_exporter", "agg_interface", "agg_host", "agg_vlan", "agg_protocol"} {
		if got := countRows(t, st, table); got == 0 {
			t.Errorf("%s is empty", table)
		}
	}

	var bytes, pkts, n, sampled int64
	if err := st.DB().QueryRow(`SELECT bytes, packets, flows, sampled_flows FROM agg_exporter`).Scan(&bytes, &pkts, &n, &sampled); err != nil {
		t.Fatal(err)
	}
	if bytes != 3000 || n != 2 || sampled != 1 {
		t.Fatalf("exporter aggregate: bytes=%d flows=%d sampled=%d", bytes, n, sampled)
	}
	var srcHosts int64
	if err := st.DB().QueryRow(`SELECT SUM(flows) FROM agg_host WHERE direction='source'`).Scan(&srcHosts); err != nil {
		t.Fatal(err)
	}
	if srcHosts != 2 {
		t.Fatalf("source host total = %d", srcHosts)
	}
}

func TestTopMarksUnknownInterfaceAndVLANKeys(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC()
	unknown := flow(now, "10.0.0.1", "203.0.113.1", 1000)
	unknown.InputIf = 0
	unknown.InputLabel = ""
	unknown.SrcVLAN = -1
	unknown.SrcVLANSource = "unknown"
	if err := st.InsertBatch(context.Background(), []model.Flow{unknown}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rng := Range{FromMS: now.Add(-time.Hour).UnixMilli(), ToMS: now.Add(time.Hour).UnixMilli()}

	interfaces, err := st.Top(context.Background(), Query{Range: rng, Dimension: "interface", Direction: "ingress", Limit: 10})
	if err != nil {
		t.Fatalf("top interface: %v", err)
	}
	if len(interfaces.Rows) != 1 || interfaces.Rows[0].Key != "0@192.0.2.1" || !interfaces.Rows[0].Unknown {
		t.Fatalf("unknown interface row = %+v", interfaces.Rows)
	}

	vLANs, err := st.Top(context.Background(), Query{Range: rng, Dimension: "vlan", Direction: "source", Limit: 10})
	if err != nil {
		t.Fatalf("top vlan: %v", err)
	}
	if len(vLANs.Rows) != 1 || vLANs.Rows[0].Key != "-1" || !vLANs.Rows[0].Unknown {
		t.Fatalf("unknown vlan row = %+v", vLANs.Rows)
	}
}

func TestRestartConsistencyAndRetentionBoundaries(t *testing.T) {
	st, cfg := testStore(t, time.Hour, 24*time.Hour)
	now := time.Now().UTC()
	old := now.Add(-2 * time.Hour)
	if err := st.InsertBatch(context.Background(), []model.Flow{flow(old, "10.0.0.1", "203.0.113.1", 500)}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertBatch(context.Background(), []model.Flow{flow(now, "10.0.0.2", "203.0.113.2", 700)}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := countRows(t, reopened, "flows"); got != 2 {
		t.Fatalf("flows lost across restart: %d", got)
	}
	res, err := reopened.SweepRetention(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if res.RawDeleted != 1 || res.AggregateDeleted != 0 {
		t.Fatalf("retention: %+v", res)
	}
	if got := countRows(t, reopened, "flows"); got != 1 {
		t.Fatalf("raw retention boundary: %d", got)
	}
	if got := countRows(t, reopened, "agg_exporter"); got != 2 {
		t.Fatalf("aggregate retention boundary: %d", got)
	}
}

func TestMixedCoverageDoesNotDoubleCount(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC().Truncate(time.Hour).Add(30 * time.Minute)
	oldFlow := flow(now.Add(-48*time.Hour), "10.0.0.1", "203.0.113.1", 1000)
	newFlow := flow(now.Add(-30*time.Minute), "10.0.0.2", "203.0.113.2", 2000)
	if err := st.InsertBatch(context.Background(), []model.Flow{oldFlow, newFlow}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SweepRetention(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, st, "flows"); got != 1 {
		t.Fatalf("expected only new raw flow, got %d", got)
	}

	q := Query{Range: Range{FromMS: now.Add(-72 * time.Hour).UnixMilli(), ToMS: now.UnixMilli()}}
	ts, err := st.Timeseries(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if ts.Resolution != "mixed" {
		t.Fatalf("resolution = %s", ts.Resolution)
	}
	var bytes, flows uint64
	for _, p := range ts.Points {
		bytes += p.Bytes
		flows += p.Flows
	}
	if bytes != 3000 || flows != 2 {
		t.Fatalf("mixed totals: bytes=%d flows=%d points=%d", bytes, flows, len(ts.Points))
	}
	if len(ts.Limitations) == 0 || !strings.Contains(ts.Limitations[0], "No data is available before") {
		t.Fatalf("coverage limitation missing: %+v", ts.Limitations)
	}
	// The recent part uses the finest tier and the old part stays coarse, so the
	// chart never claims a resolution the store cannot serve.
	if len(ts.Points) < 2 || ts.Points[len(ts.Points)-1].BucketMS-ts.Points[len(ts.Points)-2].BucketMS != minuteMS {
		t.Fatalf("newest points should be one minute apart: %+v", ts.Points[len(ts.Points)-3:])
	}
	if step := ts.Points[1].BucketMS - ts.Points[0].BucketMS; step != 5*minuteMS {
		t.Fatalf("oldest points should use the five-minute tier, got step=%d", step)
	}
	if ts.BucketMS != minuteMS || ts.AggregateBucketMS != 5*minuteMS {
		t.Fatalf("buckets=%d/%d want %d/%d", ts.BucketMS, ts.AggregateBucketMS, minuteMS, 5*minuteMS)
	}

	top, err := st.Top(context.Background(), Query{Range: q.Range, Dimension: "host", Direction: "source", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if top.Total != 2 {
		t.Fatalf("mixed host rows = %d: %+v", top.Total, top.Rows)
	}
}

func TestTimeseriesHonorsFlowFilters(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	base := time.Now().UTC().Truncate(time.Hour)
	first := flow(base.Add(time.Minute), "10.0.0.1", "203.0.113.1", 1000)
	second := flow(base.Add(2*time.Minute), "10.0.0.2", "203.0.113.2", 2000)
	if err := st.InsertBatch(context.Background(), []model.Flow{first, second}); err != nil {
		t.Fatal(err)
	}
	rng := Range{FromMS: base.UnixMilli(), ToMS: base.Add(time.Hour).UnixMilli()}

	ts, err := st.Timeseries(context.Background(), Query{Range: rng, Filters: map[string]string{"src_host": "10.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	var bytes, flows uint64
	for _, p := range ts.Points {
		bytes += p.Bytes
		flows += p.Flows
	}
	if bytes != 1000 || flows != 1 {
		t.Fatalf("filtered totals: bytes=%d flows=%d points=%d", bytes, flows, len(ts.Points))
	}

	ts, err = st.Timeseries(context.Background(), Query{Range: rng, Filters: map[string]string{"src_host": "10.0.0.9"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ts.Points {
		bytes += p.Bytes
		flows += p.Flows
	}
	if bytes != 1000 || flows != 1 {
		t.Fatalf("unmatched filter should add nothing: bytes=%d flows=%d", bytes, flows)
	}
}

func TestTimeseriesUsesMinuteBuckets(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	base := time.Now().UTC().Truncate(time.Hour)
	first := flow(base.Add(time.Minute), "10.0.0.1", "203.0.113.1", 1000)
	second := flow(base.Add(2*time.Minute), "10.0.0.2", "203.0.113.2", 2000)
	if err := st.InsertBatch(context.Background(), []model.Flow{first, second}); err != nil {
		t.Fatal(err)
	}
	rng := Range{FromMS: base.UnixMilli(), ToMS: base.Add(time.Hour).UnixMilli()}

	for _, requested := range []int64{0, minuteMS} {
		ts, err := st.Timeseries(context.Background(), Query{Range: rng, BucketMS: requested})
		if err != nil {
			t.Fatal(err)
		}
		if ts.BucketMS != minuteMS {
			t.Fatalf("bucket=%d want %d (requested %d)", ts.BucketMS, minuteMS, requested)
		}
		if len(ts.Points) != 60 {
			t.Fatalf("points=%d want 60", len(ts.Points))
		}
		for i, p := range ts.Points {
			if i > 0 && p.BucketMS-ts.Points[i-1].BucketMS != minuteMS {
				t.Fatalf("points are not one minute apart: %+v", ts.Points)
			}
		}
		byBucket := make(map[int64]Point, len(ts.Points))
		for _, p := range ts.Points {
			byBucket[p.BucketMS] = p
		}
		if got := byBucket[base.Add(time.Minute).UnixMilli()/minuteMS*minuteMS].Bytes; got != 1000 {
			t.Fatalf("first minute bytes=%d want 1000", got)
		}
		if got := byBucket[base.Add(2*time.Minute).UnixMilli()/minuteMS*minuteMS].Bytes; got != 2000 {
			t.Fatalf("second minute bytes=%d want 2000", got)
		}
	}
}

func TestTimeseriesFallsBackToHourlyTier(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	base := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	if _, err := st.DB().Exec(`
		INSERT INTO agg_traffic (res_ms, bucket_ms, bytes, packets, flows, sampled_flows, unknown_flows)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		hourMS, base.UnixMilli(), 1000, 10, 1, 0, 0); err != nil {
		t.Fatal(err)
	}
	rng := Range{FromMS: base.UnixMilli(), ToMS: base.Add(2 * time.Hour).UnixMilli()}
	ts, err := st.Timeseries(context.Background(), Query{Range: rng, BucketMS: minuteMS})
	if err != nil {
		t.Fatal(err)
	}
	if ts.Resolution != "aggregate" {
		t.Fatalf("resolution=%s want aggregate", ts.Resolution)
	}
	if ts.BucketMS != hourMS || ts.AggregateBucketMS != hourMS {
		t.Fatalf("buckets raw=%d aggregate=%d want hourly", ts.BucketMS, ts.AggregateBucketMS)
	}
	if len(ts.Points) != 2 {
		t.Fatalf("points=%d want 2 hourly points", len(ts.Points))
	}
}

func TestTimeseriesUsesFinestTierForRecentRange(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	currentHour := time.Now().UTC().Truncate(time.Hour)
	first := flow(currentHour.Add(-2*time.Hour+time.Minute), "10.0.0.1", "203.0.113.1", 1000)
	second := flow(currentHour.Add(-time.Hour+time.Minute), "10.0.0.2", "203.0.113.2", 2000)
	if err := st.InsertBatch(context.Background(), []model.Flow{first, second}); err != nil {
		t.Fatal(err)
	}

	q := Query{Range: Range{
		FromMS: currentHour.Add(-2 * time.Hour).UnixMilli(),
		ToMS:   currentHour.UnixMilli(),
	}}
	ts, err := st.Timeseries(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if ts.Resolution != "aggregate" {
		t.Fatalf("resolution=%s want aggregate", ts.Resolution)
	}
	if ts.BucketMS != minuteMS {
		t.Fatalf("bucket=%d want minute %d", ts.BucketMS, minuteMS)
	}
	var bytes, flows uint64
	for _, p := range ts.Points {
		bytes += p.Bytes
		flows += p.Flows
	}
	if bytes != 3000 || flows != 2 {
		t.Fatalf("totals: bytes=%d flows=%d points=%+v", bytes, flows, ts.Points)
	}
	if len(ts.Points) != 120 {
		t.Fatalf("points=%d want 120 minute points", len(ts.Points))
	}
}

func TestTimeseriesPartialHoursUseTiers(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	base := time.Now().UTC().Truncate(time.Hour).Add(-5 * time.Hour)
	flows := []model.Flow{
		flow(base.Add(time.Minute), "10.0.0.1", "203.0.113.1", 1000),
		flow(base.Add(5*time.Minute), "10.0.0.2", "203.0.113.2", 2000),
	}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatal(err)
	}

	ts, err := st.Timeseries(context.Background(), Query{Range: Range{
		FromMS: base.UnixMilli(),
		ToMS:   base.Add(3 * time.Minute).UnixMilli(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	var bytes, n uint64
	for _, p := range ts.Points {
		bytes += p.Bytes
		n += p.Flows
	}
	if bytes != 1000 || n != 1 {
		t.Fatalf("partial-hour totals: bytes=%d flows=%d points=%+v", bytes, n, ts.Points)
	}
	if ts.BucketMS != minuteMS || len(ts.Points) != 3 {
		t.Fatalf("bucket=%d points=%d want 3 minute points", ts.BucketMS, len(ts.Points))
	}
	for i, p := range ts.Points {
		if i > 0 && p.BucketMS-ts.Points[i-1].BucketMS != minuteMS {
			t.Fatalf("points are not one minute apart: %+v", ts.Points)
		}
	}
}

func TestTopUsesAggregatesForCompleteHours(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	currentHour := time.Now().UTC().Truncate(time.Hour)
	flows := []model.Flow{
		flow(currentHour.Add(-2*time.Hour+time.Minute), "10.0.0.1", "203.0.113.1", 1000),
		flow(currentHour.Add(-time.Hour+time.Minute), "10.0.0.1", "203.0.113.2", 2000),
	}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatal(err)
	}

	top, err := st.Top(context.Background(), Query{
		Range: Range{
			FromMS: currentHour.Add(-2 * time.Hour).UnixMilli(),
			ToMS:   currentHour.UnixMilli(),
		},
		Dimension: "host", Direction: "source", Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if top.Resolution != "aggregate" {
		t.Fatalf("resolution=%s want aggregate while raw rows are retained", top.Resolution)
	}
	if top.Total != 1 || len(top.Rows) != 1 {
		t.Fatalf("host rows=%d: %+v", top.Total, top.Rows)
	}
	if top.Rows[0].Bytes != 3000 || top.Rows[0].Flows != 2 {
		t.Fatalf("host totals: %+v", top.Rows[0])
	}
}

func TestFlowPaginationAndAggregateOnlyDetail(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC()
	flows := make([]model.Flow, 5)
	for i := range flows {
		flows[i] = flow(now.Add(time.Duration(i)*time.Minute), "10.0.0.1", "203.0.113.1", uint64(100+i))
	}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatal(err)
	}
	first, err := st.Flows(context.Background(), FlowQuery{Range: Range{FromMS: now.Add(-time.Hour).UnixMilli(), ToMS: now.Add(time.Hour).UnixMilli()}, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if first.Total != 5 || len(first.Rows) != 2 {
		t.Fatalf("page 1: total=%d rows=%d", first.Total, len(first.Rows))
	}
	second, err := st.Flows(context.Background(), FlowQuery{Range: Range{FromMS: now.Add(-time.Hour).UnixMilli(), ToMS: now.Add(time.Hour).UnixMilli()}, Limit: 2, Offset: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Rows) != 2 || second.Rows[0].ID == first.Rows[0].ID {
		t.Fatal("pagination did not advance")
	}

	// A recent empty interval before the first observed row is not expired.
	emptyRange := Range{FromMS: now.Add(-2 * time.Hour).UnixMilli(), ToMS: now.Add(-time.Hour).UnixMilli()}
	empty, err := st.Flows(context.Background(), FlowQuery{Range: emptyRange})
	if err != nil || !empty.DetailAvailable || empty.Total != 0 || empty.Limitation != "" {
		t.Fatalf("recent empty interval: page=%+v err=%v", empty, err)
	}

	oldRange := Range{FromMS: now.Add(-72 * time.Hour).UnixMilli(), ToMS: now.Add(-48 * time.Hour).UnixMilli()}
	page, err := st.Flows(context.Background(), FlowQuery{Range: oldRange, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if page.DetailAvailable || page.Limitation == "" {
		t.Fatalf("aggregate-only detail should be unavailable: %+v", page)
	}
}

func TestFlowFiltersRespectDirection(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC()
	first := flow(now, "10.0.0.1", "203.0.113.1", 1000)
	second := flow(now.Add(time.Minute), "203.0.113.1", "10.0.0.1", 2000)
	first.SrcPort, first.DstPort = 1111, 80
	second.InputIf, second.OutputIf = 2, 1
	second.InputLabel, second.OutputLabel = "lan0", "wan0"
	second.SrcPort, second.DstPort = 2222, 443
	second.SrcVLAN, second.DstVLAN = 20, 10
	if err := st.InsertBatch(context.Background(), []model.Flow{first, second}); err != nil {
		t.Fatal(err)
	}
	rng := Range{FromMS: now.Add(-time.Hour).UnixMilli(), ToMS: now.Add(time.Hour).UnixMilli()}

	tests := []struct {
		name      string
		filters   map[string]string
		wantRows  int64
		wantBytes uint64
	}{
		{"interface ingress", map[string]string{"interface": "1", "direction": "ingress"}, 1, 1000},
		{"interface egress", map[string]string{"interface": "1", "direction": "egress"}, 1, 2000},
		{"interface any", map[string]string{"interface": "1"}, 2, 3000},
		{"interface label ingress", map[string]string{"interface": "wan0", "direction": "ingress"}, 1, 1000},
		{"interface label egress", map[string]string{"interface": "wan0", "direction": "egress"}, 1, 2000},
		{"host source", map[string]string{"host": "10.0.0.1", "direction": "source"}, 1, 1000},
		{"host destination", map[string]string{"host": "10.0.0.1", "direction": "destination"}, 1, 2000},
		{"source host exact", map[string]string{"src_host": "10.0.0.1"}, 1, 1000},
		{"destination host exact", map[string]string{"dst_host": "10.0.0.1"}, 1, 2000},
		{"vlan source", map[string]string{"vlan": "10", "direction": "source"}, 1, 1000},
		{"vlan destination", map[string]string{"vlan": "10", "direction": "destination"}, 1, 2000},
		{"input interface", map[string]string{"input_interface": "1"}, 1, 1000},
		{"output interface", map[string]string{"output_interface": "1"}, 1, 2000},
		{"input interface label", map[string]string{"input_interface": "wan0"}, 1, 1000},
		{"output interface label", map[string]string{"output_interface": "wan0"}, 1, 2000},
		{"input vlan", map[string]string{"input_vlan": "10"}, 1, 1000},
		{"output vlan", map[string]string{"output_vlan": "10"}, 1, 2000},
		{"source port", map[string]string{"src_port": "1111"}, 1, 1000},
		{"destination port", map[string]string{"dst_port": "443"}, 1, 2000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			page, err := st.Flows(context.Background(), FlowQuery{Range: rng, Filters: tc.filters, Limit: 10})
			if err != nil {
				t.Fatal(err)
			}
			if page.Total != tc.wantRows || int64(len(page.Rows)) != tc.wantRows {
				t.Fatalf("total=%d rows=%d want %d", page.Total, len(page.Rows), tc.wantRows)
			}
			var bytes uint64
			for _, row := range page.Rows {
				bytes += row.Bytes
			}
			if bytes != tc.wantBytes {
				t.Fatalf("bytes=%d want %d", bytes, tc.wantBytes)
			}
		})
	}
}

func TestDirectionalAggregatesAreSeparateViews(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC()
	if err := st.InsertBatch(context.Background(), []model.Flow{flow(now, "10.0.0.1", "10.0.0.2", 1000)}); err != nil {
		t.Fatal(err)
	}
	var src, dst int64
	if err := st.DB().QueryRow(`SELECT SUM(flows) FROM agg_host WHERE host='10.0.0.1' AND direction='source'`).Scan(&src); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(`SELECT SUM(flows) FROM agg_host WHERE host='10.0.0.2' AND direction='destination'`).Scan(&dst); err != nil {
		t.Fatal(err)
	}
	if src != 1 || dst != 1 {
		t.Fatalf("directional views: src=%d dst=%d", src, dst)
	}
	var exporterTotal int64
	if err := st.DB().QueryRow(`SELECT SUM(flows) FROM agg_exporter`).Scan(&exporterTotal); err != nil {
		t.Fatal(err)
	}
	if exporterTotal != 1 {
		t.Fatalf("network total must not sum directional views: %d", exporterTotal)
	}
}

func TestFlowHostsAndResolvedNameFilters(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC()
	flows := []model.Flow{
		flow(now.Add(-time.Minute), "10.0.0.1", "203.0.113.1", 1000),
		flow(now.Add(-2*time.Minute), "203.0.113.1", "10.0.0.2", 2000),
	}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatal(err)
	}
	rng := Range{FromMS: now.Add(-time.Hour).UnixMilli(), ToMS: now.Add(time.Hour).UnixMilli()}

	hosts, err := st.FlowHosts(context.Background(), FlowQuery{Range: rng})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(hosts)
	want := []string{"10.0.0.1", "10.0.0.2", "203.0.113.1"}
	if len(hosts) != len(want) {
		t.Fatalf("hosts=%v want %v", hosts, want)
	}
	for i := range want {
		if hosts[i] != want[i] {
			t.Fatalf("hosts=%v want %v", hosts, want)
		}
	}

	cases := []struct {
		name  string
		query FlowQuery
		total int64
	}{
		{"source IP set", FlowQuery{SrcHostIPs: []string{"10.0.0.1"}}, 1},
		{"destination IP set", FlowQuery{DstHostIPs: []string{"10.0.0.2"}}, 1},
		{"source no match", FlowQuery{SrcHostNoMatch: true}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := tc.query
			q.Range = rng
			page, err := st.Flows(context.Background(), q)
			if err != nil {
				t.Fatal(err)
			}
			if page.Total != tc.total {
				t.Fatalf("total=%d want %d", page.Total, tc.total)
			}
		})
	}
}

func TestTimeseriesMixesResolutionTiers(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 8760*time.Hour)
	now := time.Now().UTC().Truncate(time.Hour)
	insert := func(res, bucket int64, bytes uint64) {
		t.Helper()
		if _, err := st.DB().Exec(`
			INSERT INTO agg_traffic (res_ms, bucket_ms, bytes, packets, flows, sampled_flows, unknown_flows)
			VALUES (?, ?, ?, ?, 1, 0, 0)`, res, bucket, bytes, bytes/100); err != nil {
			t.Fatal(err)
		}
	}
	// A minute tier that only covers the last half hour, a five-minute tier for
	// the hours before it, and hourly history beyond that.
	insert(minuteMS, now.Add(-30*time.Minute).UnixMilli(), 1000)
	insert(5*minuteMS, now.Add(-3*time.Hour).UnixMilli(), 2000)
	insert(hourMS, now.Add(-8*time.Hour).UnixMilli(), 4000)

	ts, err := st.Timeseries(context.Background(), Query{Range: Range{FromMS: now.Add(-10 * time.Hour).UnixMilli(), ToMS: now.UnixMilli()}})
	if err != nil {
		t.Fatal(err)
	}
	if ts.Resolution != "mixed" {
		t.Fatalf("resolution=%s want mixed", ts.Resolution)
	}
	if ts.BucketMS != minuteMS || ts.AggregateBucketMS != hourMS {
		t.Fatalf("buckets=%d/%d want %d/%d", ts.BucketMS, ts.AggregateBucketMS, minuteMS, hourMS)
	}
	var bytes, flowsCount uint64
	for _, p := range ts.Points {
		bytes += p.Bytes
		flowsCount += p.Flows
	}
	if bytes != 7000 || flowsCount != 3 {
		t.Fatalf("tier totals: bytes=%d flows=%d", bytes, flowsCount)
	}
	// Every bucket must appear exactly once even though tiers overlap in time.
	seen := map[int64]bool{}
	for _, p := range ts.Points {
		if seen[p.BucketMS] {
			t.Fatalf("duplicate bucket %d in %+v", p.BucketMS, ts.Points)
		}
		seen[p.BucketMS] = true
	}
	if got := ts.Points[len(ts.Points)-1].BucketMS - ts.Points[len(ts.Points)-2].BucketMS; got != minuteMS {
		t.Fatalf("newest spacing=%d want %d", got, minuteMS)
	}
	if got := ts.Points[1].BucketMS - ts.Points[0].BucketMS; got != hourMS {
		t.Fatalf("oldest spacing=%d want %d", got, hourMS)
	}
	if ts.Points[0].DurationMS != hourMS || ts.Points[len(ts.Points)-1].DurationMS != minuteMS {
		t.Fatalf("mixed point durations: first=%d last=%d", ts.Points[0].DurationMS, ts.Points[len(ts.Points)-1].DurationMS)
	}
}

func TestCoverageNoticesBeforeCollection(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	base := time.Now().UTC().Truncate(time.Hour).UnixMilli()
	coverage := Coverage{RawAvailable: true, RawOldestMS: base + minuteMS, AggregateAvailable: true, AggregateOldestMS: base, AggregateNewestMS: base + hourMS}
	for _, split := range []coverageSplit{
		st.fastSplitCoverage(coverage, base-hourMS, base+2*hourMS, base+3*hourMS),
		st.splitCoverage(coverage, base-hourMS, base+2*hourMS, hourMS),
	} {
		joined := strings.Join(split.limitations, " ")
		if strings.Contains(joined, "No hourly aggregates") || !strings.Contains(joined, "Flow details are unavailable before") {
			t.Fatalf("unexpected limitations: %v", split.limitations)
		}
	}
}

func TestAppendBucketsPartialDurations(t *testing.T) {
	var points []Point
	appendBuckets(&points, map[int64]*Point{}, 15000, 75000, minuteMS, 10, true)
	if len(points) != 2 || points[0].DurationMS != 45000 || points[1].DurationMS != 15000 {
		t.Fatalf("partial bucket durations: %+v", points)
	}
}

func TestTrafficRetentionPrunesEachTier(t *testing.T) {
	tiers := []config.ResolutionTier{
		{Bucket: config.Duration(time.Minute), Retention: config.Duration(time.Hour)},
		{Bucket: config.Duration(5 * time.Minute), Retention: config.Duration(3 * time.Hour)},
	}
	st, _ := testStoreTiers(t, 6*time.Hour, 6*time.Hour, tiers...)
	now := time.Now().UTC().Truncate(time.Hour)
	if err := st.InsertBatch(context.Background(), []model.Flow{flow(now.Add(-2*time.Hour), "10.0.0.1", "203.0.113.1", 1000)}); err != nil {
		t.Fatal(err)
	}
	res, err := st.SweepRetention(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if res.TrafficDeleted != 1 {
		t.Fatalf("traffic rows deleted=%d want 1 (minute tier only)", res.TrafficDeleted)
	}
	counts := map[int64]int64{}
	rows, err := st.DB().Query(`SELECT res_ms, COUNT(*) FROM agg_traffic GROUP BY res_ms`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var bucket, n int64
		if err := rows.Scan(&bucket, &n); err != nil {
			t.Fatal(err)
		}
		counts[bucket] = n
	}
	if counts[minuteMS] != 0 || counts[5*minuteMS] != 1 || counts[hourMS] != 1 {
		t.Fatalf("tier rows after sweep: %+v", counts)
	}
}

func TestFilteredTimeseriesStopsOnDeadline(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC().Truncate(time.Minute)
	if err := st.InsertBatch(context.Background(), []model.Flow{flow(now.Add(-30*time.Minute), "10.0.0.1", "203.0.113.1", 1000)}); err != nil {
		t.Fatal(err)
	}
	coverage, err := st.Coverage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	q := Query{Range: Range{FromMS: now.Add(-time.Hour).UnixMilli(), ToMS: now.UnixMilli()}, Filters: map[string]string{"protocol": "6"}}
	ts, err := st.timeseriesRaw(ctx, q, coverage, TimeseriesResult{Range: q.Range})
	if err != nil {
		t.Fatalf("truncated filtered chart must not fail: %v", err)
	}
	found := false
	for _, l := range ts.Limitations {
		if strings.Contains(l, "stopped early") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a truncation limitation: %+v", ts.Limitations)
	}
	if ts.BucketMS <= 0 || ts.Points == nil {
		t.Fatalf("partial result must still describe the chart: %+v", ts)
	}
}

func TestCoverageReportsTierAvailability(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC().Truncate(time.Minute)
	if err := st.InsertBatch(context.Background(), []model.Flow{flow(now, "10.0.0.1", "203.0.113.1", 1000)}); err != nil {
		t.Fatal(err)
	}
	cov, err := st.Coverage(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(cov.Tiers) != 3 {
		t.Fatalf("tiers=%+v", cov.Tiers)
	}
	wantBuckets := []int64{minuteMS, 5 * minuteMS, hourMS}
	for i, tier := range cov.Tiers {
		if tier.BucketMS != wantBuckets[i] || !tier.Available || tier.OldestMS%tier.BucketMS != 0 || tier.OldestMS > now.UnixMilli() {
			t.Fatalf("tier %d = %+v", i, tier)
		}
	}
	if cov.Tiers[0].RetentionMS != (7*24*time.Hour).Milliseconds() || cov.Tiers[2].RetentionMS != (72*time.Hour).Milliseconds() {
		t.Fatalf("tier retentions: %+v", cov.Tiers)
	}
}

func TestMigrationAddsTrafficTable(t *testing.T) {
	st, cfg := testStore(t, 24*time.Hour, 72*time.Hour)
	if _, err := st.DB().Exec(`DROP TABLE dns_host_history; ALTER TABLE flows DROP COLUMN src_name; ALTER TABLE flows DROP COLUMN dst_name; ALTER TABLE flows DROP COLUMN src_dns_ms; ALTER TABLE flows DROP COLUMN dst_dns_ms`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`DELETE FROM schema_migrations WHERE version >= 2`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`DROP TABLE IF EXISTS agg_traffic`); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	stats, err := reopened.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.SchemaVersion != schemaVersion {
		t.Fatalf("schema version=%d want %d", stats.SchemaVersion, schemaVersion)
	}
	if err := reopened.InsertBatch(context.Background(), []model.Flow{flow(time.Now().UTC(), "10.0.0.1", "203.0.113.1", 10)}); err != nil {
		t.Fatalf("insert after migration: %v", err)
	}
	if got := countRows(t, reopened, "agg_traffic"); got == 0 {
		t.Fatal("agg_traffic is empty after migration")
	}
}

func TestBackfillSeedsTiersOnUpgrade(t *testing.T) {
	st, cfg := testStore(t, 24*time.Hour, 72*time.Hour)
	now := time.Now().UTC().Truncate(time.Hour)
	old := now.Add(-2 * time.Hour)
	if err := st.InsertBatch(context.Background(), []model.Flow{flow(old.Add(time.Minute), "10.0.0.1", "203.0.113.1", 1000)}); err != nil {
		t.Fatal(err)
	}
	// Simulate a database written before the tiered totals table existed.
	if _, err := st.DB().Exec(`DELETE FROM agg_traffic`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`DROP TABLE dns_host_history; ALTER TABLE flows DROP COLUMN src_name; ALTER TABLE flows DROP COLUMN dst_name; ALTER TABLE flows DROP COLUMN src_dns_ms; ALTER TABLE flows DROP COLUMN dst_dns_ms`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`DELETE FROM schema_migrations WHERE version >= 2`); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if got := countRows(t, reopened, "agg_traffic"); got != 3 {
		t.Fatalf("seeded tier rows=%d want one row per tier", got)
	}
	ts, err := reopened.Timeseries(context.Background(), Query{Range: Range{FromMS: now.Add(-6 * time.Hour).UnixMilli(), ToMS: now.UnixMilli()}})
	if err != nil {
		t.Fatal(err)
	}
	var bytes uint64
	for _, p := range ts.Points {
		bytes += p.Bytes
	}
	if bytes != 1000 {
		t.Fatalf("backfilled history double counted: bytes=%d points=%d", bytes, len(ts.Points))
	}

	// A newly configured resolution is backfilled even when other tiers exist.
	if _, err := reopened.DB().Exec(`DELETE FROM agg_traffic WHERE res_ms = ?`, int64(5*time.Minute/time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err = Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, reopened, "agg_traffic"); got != 3 {
		t.Fatalf("missing tier backfill: rows=%d", got)
	}

	// A second open must not seed again.
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if got := countRows(t, again, "agg_traffic"); got != 3 {
		t.Fatalf("idempotent backfill: rows=%d", got)
	}
}

func TestFlowsSortBeforePagination(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 8760*time.Hour)
	now := time.Now().UTC()
	items := []model.Flow{flow(now.Add(-time.Minute), "10.0.0.1", "203.0.113.1", 50), flow(now.Add(-2*time.Minute), "10.0.0.2", "203.0.113.1", 10), flow(now.Add(-3*time.Minute), "10.0.0.3", "203.0.113.1", 30)}
	if err := st.InsertBatch(context.Background(), items); err != nil {
		t.Fatal(err)
	}
	q := FlowQuery{Range: Range{FromMS: now.Add(-time.Hour).UnixMilli(), ToMS: now.Add(time.Second).UnixMilli()}, Limit: 1, Offset: 1, Sort: "bytes", SortDir: "asc"}
	page, err := st.Flows(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 3 || len(page.Rows) != 1 || page.Rows[0].Bytes != 30 {
		t.Fatalf("sorted page: %+v", page)
	}
	q.Sort = "bytes DESC; DROP TABLE flows;--"
	if _, err := st.Flows(context.Background(), q); err != nil {
		t.Fatal(err)
	}
}

func TestTimeseriesCoarseSpanUsesOneWidth(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 8760*time.Hour)
	now := time.Now().UTC().Truncate(time.Hour)
	insert := func(res, bucket int64, bytes uint64) {
		t.Helper()
		if _, err := st.DB().Exec(`
			INSERT INTO agg_traffic (res_ms, bucket_ms, bytes, packets, flows, sampled_flows, unknown_flows)
			VALUES (?, ?, ?, 1, 1, 0, 0)`, res, bucket, bytes); err != nil {
			t.Fatal(err)
		}
	}
	insert(minuteMS, now.Add(-30*time.Minute).UnixMilli(), 1000)
	insert(5*minuteMS, now.Add(-10*24*time.Hour).UnixMilli(), 2000)

	// Ten days is beyond the one-minute tier retention, so every segment is
	// rendered at the five-minute width even though two tiers contribute.
	ts, err := st.Timeseries(context.Background(), Query{Range: Range{FromMS: now.Add(-11 * 24 * time.Hour).UnixMilli(), ToMS: now.UnixMilli()}})
	if err != nil {
		t.Fatal(err)
	}
	if ts.Resolution != "aggregate" || ts.BucketMS != 5*minuteMS || ts.AggregateBucketMS != 5*minuteMS {
		t.Fatalf("resolution=%s buckets=%d/%d", ts.Resolution, ts.BucketMS, ts.AggregateBucketMS)
	}
	var bytes uint64
	for _, p := range ts.Points {
		bytes += p.Bytes
	}
	if bytes != 3000 {
		t.Fatalf("bytes=%d want 3000", bytes)
	}
}
