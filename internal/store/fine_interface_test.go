package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

func TestFineInterfaceHistorySurvivesRawExpiry(t *testing.T) {
	st, _ := testStore(t, 7*24*time.Hour, 365*24*time.Hour)
	ctx := context.Background()
	now := time.Now().UTC()
	start := now.Truncate(time.Hour).Add(-8 * 24 * time.Hour)
	in := flow(start.Add(7*time.Minute), "10.0.0.1", "203.0.113.1", 400)
	out := in
	out.InputIf, out.OutputIf = 2, 1
	out.Bytes = 700
	if err := st.InsertBatch(ctx, []model.Flow{in, out}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SweepRetention(ctx, now); err != nil {
		t.Fatal(err)
	}
	if countRows(t, st, "flows") != 0 {
		t.Fatal("fixture not pruned")
	}
	got, err := st.InterfaceTimeseries(ctx, Range{FromMS: start.UnixMilli(), ToMS: start.Add(time.Hour).UnixMilli()}, in.Exporter, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Ingress) != 12 || len(got.Egress) != 12 || got.Ingress[1].DurationMS != 300000 || got.Ingress[1].Bytes != 400 || got.Egress[1].Bytes != 700 {
		t.Fatalf("wanted 5-minute directional history: %+v", got)
	}
}

func TestUpgradeBackfillsFineInterfacesAndReopenDoesNotDuplicate(t *testing.T) {
	st, cfg := testStore(t, 7*24*time.Hour, 365*24*time.Hour)
	ctx := context.Background()
	start := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	a := flow(start, "10.0.0.1", "203.0.113.1", 400)
	b := flow(start.Add(time.Minute), "10.0.0.1", "203.0.113.1", 600)
	if err := st.InsertBatch(ctx, []model.Flow{a, b}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().Exec(`DROP TABLE agg_interface_fine; DROP TABLE interface_fine_coverage; DELETE FROM schema_migrations WHERE version=6`); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		upgraded, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		got, err := upgraded.InterfaceTimeseries(ctx, Range{FromMS: start.UnixMilli(), ToMS: start.Add(2 * time.Minute).UnixMilli()}, a.Exporter, 1)
		if err != nil {
			upgraded.Close()
			t.Fatal(err)
		}
		if len(got.Ingress) != 2 || got.Ingress[0].Bytes != 400 || got.Ingress[1].Bytes != 600 {
			upgraded.Close()
			t.Fatalf("wrong migrated points: %+v", got)
		}
		if err := upgraded.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFineInterfaceFollowsDedupPromotion(t *testing.T) {
	st := pcStore(t)
	ctx := context.Background()
	end := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour + time.Minute)
	copies := pingRecords(end)
	for _, f := range []model.Flow{copies[2], copies[0]} {
		if err := st.InsertBatch(ctx, []model.Flow{f}); err != nil {
			t.Fatal(err)
		}
	}
	for _, width := range []int64{60000, 300000} {
		for index, want := range map[int]int{2: 84, 7: 0} {
			var got int
			if err := st.DB().QueryRow(`SELECT COALESCE(SUM(bytes),0) FROM agg_interface_fine WHERE res_ms=? AND if_index=? AND direction='ingress'`, width, index).Scan(&got); err != nil || got != want {
				t.Fatalf("width %d, ifIndex %d: got %d, want %d, err %v", width, index, got, want, err)
			}
		}
	}
}

func TestHostChartHourlyFallback(t *testing.T) {
	st, _ := testStore(t, 7*24*time.Hour, 365*24*time.Hour)
	ctx := context.Background()
	now := time.Now().UTC()
	start := now.Truncate(time.Hour).Add(-8 * 24 * time.Hour)
	a := flow(start.Add(time.Minute), "10.0.0.1", "203.0.113.1", 400)
	b := flow(start.Add(2*time.Minute), "203.0.113.1", "10.0.0.1", 700)
	if err := st.InsertBatch(ctx, []model.Flow{a, b}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SweepRetention(ctx, now); err != nil {
		t.Fatal(err)
	}
	q := Query{Range: Range{FromMS: start.UnixMilli(), ToMS: start.Add(time.Hour).UnixMilli()}, Filters: map[string]string{"src_host": "10.0.0.1"}}
	got, err := st.Timeseries(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Points) != 1 || got.Points[0].Bytes != 400 || got.Points[0].DurationMS != hourMS {
		t.Fatalf("wanted source-only hourly history: %+v", got)
	}
	q.Filters["src_host"] = "ip:10.0.0."
	got, err = st.Timeseries(ctx, q)
	if err != nil || len(got.Points) != 1 || got.Points[0].Bytes != 400 {
		t.Fatalf("address-only substring should use hourly history: %+v %v", got, err)
	}
	q.Filters["src_host"] = "10.0.0."
	got, err = st.Timeseries(ctx, q)
	if err != nil || len(got.Points) != 0 {
		t.Fatalf("bare substring also matches names and must remain raw-only: %+v %v", got, err)
	}
	q.Filters["src_host"] = "10.0.0.1"
	q.Filters["dst_host"] = "203.0.113.1"
	got, err = st.Timeseries(ctx, q)
	if err != nil || len(got.Points) != 0 {
		t.Fatalf("combined query must not use hourly totals: %+v %v", got, err)
	}
}
