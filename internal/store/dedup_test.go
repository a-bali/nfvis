package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

// pingRecord is one NetFlow record for the same single ICMP echo, as OPNsense's
// ng_netflow exports it: once on the parent interface, once on the VLAN child
// interface, and once more after NAT/PAT rewrote the source.
//
// The values are the ones measured on the production exporter; the three
// records are the fixture every dedup assertion is checked against.
func pingRecord(end time.Time, inputIf uint32, src string, srcPort uint16) model.Flow {
	return model.Flow{
		ReceivedAtMS:   end.UnixMilli() + 20,
		StartMS:        end.UnixMilli() - 1000,
		EndMS:          end.UnixMilli(),
		TimeSource:     "exported",
		Version:        9,
		Exporter:       "192.0.2.10",
		ExporterIP:     "192.0.2.10",
		ExporterID:     0,
		InputIf:        inputIf,
		OutputIf:       1,
		Direction:      1,
		DirectionKnown: true,
		SrcAddr:        src,
		DstAddr:        "146.110.3.12",
		SrcPort:        srcPort,
		DstPort:        0, // ICMP echo: no ports
		Protocol:       1, // ICMP
		Bytes:          98,
		Packets:        1,
		SrcVLAN:        -1,
		DstVLAN:        -1,
		SrcVLANSource:  "unknown",
		DstVLANSource:  "unknown",
	}
}

// pingRecords returns the measured three-record export of one ping: parent
// (input_if 2), VLAN child (input_if 7), and the post-NAT copy.
func pingRecords(end time.Time) []model.Flow {
	return []model.Flow{
		pingRecord(end, 2, "10.10.10.194", 0),
		pingRecord(end, 7, "10.10.10.194", 0),
		pingRecord(end, 2, "192.168.0.2", 41234),
	}
}

func storedFlows(t *testing.T, st *Store, limit int) []FlowRecord {
	t.Helper()
	rows, err := st.DB().Query(`SELECT `+flowColumns+` FROM flows ORDER BY id LIMIT ?`, limit)
	if err != nil {
		t.Fatalf("query flows: %v", err)
	}
	defer rows.Close()
	var out []FlowRecord
	for rows.Next() {
		r, err := scanFlowRow(rows)
		if err != nil {
			t.Fatalf("scan flow: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// M1: the parent and VLAN-child copies of one frame are byte-identical apart
// from input_if/output_if, so they must collapse to a single row. The post-NAT
// copy has a different source address and belongs to M2, so M1 leaves it.
func TestParentChildDuplicatesCollapseToParent(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)
	if err := st.InsertBatch(context.Background(), pingRecords(end)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got := storedFlows(t, st, 10)
	if len(got) != 2 {
		t.Fatalf("stored %d rows, want 2 (parent + post-NAT): %+v", len(got), got)
	}
	// The surviving member of the duplicate group is the parent interface row.
	parent := got[0].Flow
	if parent.InputIf != 2 || parent.OutputIf != 1 {
		t.Fatalf("survivor interfaces = in=%d out=%d, want in=2 out=1", parent.InputIf, parent.OutputIf)
	}
	if parent.SrcAddr != "10.10.10.194" || parent.DstAddr != "146.110.3.12" {
		t.Fatalf("survivor endpoints = %s -> %s", parent.SrcAddr, parent.DstAddr)
	}
	if parent.Bytes != 98 || parent.Packets != 1 || parent.Protocol != 1 {
		t.Fatalf("survivor payload = %d bytes / %d pkts / proto %d", parent.Bytes, parent.Packets, parent.Protocol)
	}
	// The post-NAT record is untouched.
	if got[1].SrcAddr != "192.168.0.2" {
		t.Fatalf("second row src = %s, want the post-NAT 192.168.0.2", got[1].SrcAddr)
	}
}

// M1 is not batch-local: 10,389 duplicate groups in one measured hour straddle
// the 250 ms flush boundary, so a second batch must not re-insert a key the
// previous batch already stored.
func TestParentChildDuplicatesAcrossBatches(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)
	records := pingRecords(end)

	if err := st.InsertBatch(context.Background(), records[:1]); err != nil {
		t.Fatalf("insert first batch: %v", err)
	}
	if err := st.InsertBatch(context.Background(), records[1:2]); err != nil {
		t.Fatalf("insert second batch: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 1 {
		t.Fatalf("stored %d rows across batches, want 1", got)
	}
	// Arrival order must not decide the survivor: the child arriving first still
	// leaves the parent stored once both have been seen.
	got := storedFlows(t, st, 5)
	if len(got) != 1 || got[0].InputIf != 2 {
		t.Fatalf("survivor = %+v, want a single row with input_if 2", got)
	}
}

// A long-lived flow is re-exported with cumulative counters (packets 1, then
// 2, ...). Those records share every field except bytes/packets and must all be
// stored, or live connections lose their traffic.
func TestCumulativeReExportsAreNotCollapsed(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)
	first := pingRecord(end, 2, "10.10.10.194", 0)
	second := first
	second.Packets = 2
	second.Bytes = 196

	if err := st.InsertBatch(context.Background(), []model.Flow{first, second}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2 (cumulative re-exports are distinct)", got)
	}
}

// Deduping storage while still summing the dropped rows into the aggregates is
// the bug to avoid: every aggregate must equal the deduped totals.
func TestDedupedRowsDriveAggregates(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)
	if err := st.InsertBatch(context.Background(), pingRecords(end)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Two stored records: 98 bytes each, one packet each.
	var bytes, packets, flows int64
	if err := st.DB().QueryRow(`SELECT SUM(bytes), SUM(packets), SUM(flows) FROM agg_exporter`).Scan(&bytes, &packets, &flows); err != nil {
		t.Fatalf("read agg_exporter: %v", err)
	}
	if bytes != 196 || packets != 2 || flows != 2 {
		t.Fatalf("agg_exporter = %d bytes / %d packets / %d flows, want 196/2/2", bytes, packets, flows)
	}
	// The ingress interface aggregate must not carry the dropped child row.
	var childFlows int64
	if err := st.DB().QueryRow(`SELECT COALESCE(SUM(flows),0) FROM agg_interface WHERE if_index = 7`).Scan(&childFlows); err != nil {
		t.Fatalf("read agg_interface: %v", err)
	}
	if childFlows != 0 {
		t.Fatalf("dropped child row reached agg_interface: %d flows on if_index 7", childFlows)
	}
}

// Promotions must not leave a stale interface aggregate behind. When the VLAN

// child is stored by an earlier batch and the parent arrives later, the row is
// rewritten to the parent's interfaces — the aggregate written for the child
// must move with it, or the dashboard reports a traversal that no stored row
// supports.
func TestPromotionMovesInterfaceAggregate(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)
	records := pingRecords(end)

	// Child first, parent second, in separate batches.
	if err := st.InsertBatch(context.Background(), records[1:2]); err != nil {
		t.Fatalf("insert child: %v", err)
	}
	if err := st.InsertBatch(context.Background(), records[:1]); err != nil {
		t.Fatalf("insert parent: %v", err)
	}

	got := storedFlows(t, st, 5)
	if len(got) != 1 || got[0].InputIf != 2 {
		t.Fatalf("stored %d rows (first input_if %d), want one row on input_if 2", len(got), got[0].InputIf)
	}
	ingress := map[uint32]int64{}
	rows, err := st.DB().Query(`SELECT if_index, COALESCE(SUM(flows),0) FROM agg_interface WHERE direction='ingress' GROUP BY if_index`)
	if err != nil {
		t.Fatalf("read agg_interface: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ifIndex uint32
		var flows int64
		if err := rows.Scan(&ifIndex, &flows); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ingress[ifIndex] = flows
	}
	if ingress[7] != 0 {
		t.Fatalf("agg_interface still counts the replaced child interface: if_index 7 = %d flows", ingress[7])
	}
	if ingress[2] != 1 {
		t.Fatalf("agg_interface ingress for the parent = %d flows, want 1 (%v)", ingress[2], ingress)
	}
}

// A record with no usable identity carries a NULL key and must never be
// collapsed: rows written before the dedup migration have no key, and two such
// rows are genuinely distinct as far as the store can tell.
func TestRecordsWithoutIdentityAreAlwaysStored(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	empty := model.Flow{TimeSource: "received"}
	twice := []model.Flow{empty, empty}
	if err := st.InsertBatch(context.Background(), twice); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d keyless rows, want 2", got)
	}
}

// Two different exporters reporting the same payload are distinct observations,
// and a flow with no dedup identity (pre-existing rows) must never be collapsed.
func TestDedupKeyScopedToExporter(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)
	a := pingRecord(end, 2, "10.10.10.194", 0)
	b := a
	b.Exporter = "192.0.2.11"
	b.ExporterIP = "192.0.2.11"

	if err := st.InsertBatch(context.Background(), []model.Flow{a, b}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2 (one per exporter)", got)
	}
}
