package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

// The reviewer's repro, asserted rather than logged. Before the fix the stored
// row kept vlan 10 / sampled 0 while agg_vlan reported vlan 20 flows=1
// sampled=1 and vlan 10 flows=0 — the row and the aggregates described
// different records. Two records that differ in VLAN and sampling are different
// observations, so both are stored and each bucket matches its own row.
func TestIndependentReproIsConsistent(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	child := pingRecord(end, 7, "10.10.10.194", 0)
	child.SrcVLAN = 10
	child.Sampled = false
	parent := pingRecord(end, 2, "10.10.10.194", 0)
	parent.SrcVLAN = 20
	parent.Sampled = true

	if err := st.InsertBatch(context.Background(), []model.Flow{child}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertBatch(context.Background(), []model.Flow{parent}); err != nil {
		t.Fatal(err)
	}

	type row struct{ vlan, sampled int }
	var rows []row
	cur, err := st.DB().Query(`SELECT src_vlan, sampled FROM flows ORDER BY src_vlan`)
	if err != nil {
		t.Fatal(err)
	}
	for cur.Next() {
		var r row
		if err := cur.Scan(&r.vlan, &r.sampled); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	cur.Close()
	if len(rows) != 2 {
		t.Fatalf("stored %d rows, want 2: a VLAN-10 unsampled and a VLAN-20 sampled record are different frames", len(rows))
	}
	if rows[0] != (row{10, 0}) || rows[1] != (row{20, 1}) {
		t.Fatalf("stored rows = %+v, want [{10 0} {20 1}]", rows)
	}

	buckets := map[int]struct{ flows, sampled int }{}
	bq, err := st.DB().Query(`SELECT vlan, flows, sampled_flows FROM agg_vlan WHERE direction='source'`)
	if err != nil {
		t.Fatal(err)
	}
	for bq.Next() {
		var vlan, flows, sampled int
		if err := bq.Scan(&vlan, &flows, &sampled); err != nil {
			t.Fatal(err)
		}
		buckets[vlan] = struct{ flows, sampled int }{flows, sampled}
	}
	bq.Close()
	want := map[int]struct{ flows, sampled int }{
		10: {1, 0},
		20: {1, 1},
	}
	if len(buckets) != len(want) {
		t.Fatalf("agg_vlan source has %d buckets %+v, want %+v", len(buckets), buckets, want)
	}
	for vlan, w := range want {
		if got := buckets[vlan]; got != w {
			t.Fatalf("agg_vlan vlan=%d = %+v, want %+v", vlan, got, w)
		}
	}
	assertAggregatesMatchFlows(t, st, "independent repro")
}
