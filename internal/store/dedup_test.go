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
// Field values are the measured production ones (nfcapd.202611201545, exporter
// 192.168.1.1): ICMP carries no L4 ports, so both are 0 — nfdump renders the
// ICMP type in that column, but the record itself has no port and a fixture that
// invented one would not match production.
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
		Bytes:          84,
		Packets:        1,
		SrcVLAN:        -1,
		DstVLAN:        -1,
		SrcVLANSource:  "unknown",
		DstVLANSource:  "unknown",
	}
}

// pingRecords returns the measured three-record export of one ping, all with the
// same start/end timestamp:
//
//	R1 parent, pre-NAT   in=2 out=1  10.10.10.194
//	R2 post-NAT          in=7 out=1  192.168.0.2
//	R3 child, pre-NAT    in=7 out=1  10.10.10.194
//
// R1 and R3 share a fingerprint (they differ only in interfaces); R2 differs in
// source address and so is M2's business, not M1's.
func pingRecords(end time.Time) []model.Flow {
	return []model.Flow{
		pingRecord(end, 2, "10.10.10.194", 0),
		pingRecord(end, 7, "192.168.0.2", 0),
		pingRecord(end, 7, "10.10.10.194", 0),
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
	if parent.Bytes != 84 || parent.Packets != 1 || parent.Protocol != 1 {
		t.Fatalf("survivor payload = %d bytes / %d pkts / proto %d", parent.Bytes, parent.Packets, parent.Protocol)
	}
	if got[1].SrcAddr != natWANSrc {
		t.Fatalf("second row src = %s, want the post-NAT %s", got[1].SrcAddr, natWANSrc)
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
	if err := st.InsertBatch(context.Background(), records[2:3]); err != nil {
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

	// Two stored records — the parent and the post-NAT copy — at 84 bytes each.
	var bytes, packets, flows int64
	if err := st.DB().QueryRow(`SELECT SUM(bytes), SUM(packets), SUM(flows) FROM agg_exporter`).Scan(&bytes, &packets, &flows); err != nil {
		t.Fatalf("read agg_exporter: %v", err)
	}
	if bytes != 168 || packets != 2 || flows != 2 {
		t.Fatalf("agg_exporter = %d bytes / %d packets / %d flows, want 168/2/2", bytes, packets, flows)
	}
	// The ingress interface aggregate must not carry the dropped child row.
	var childFlows int64
	if err := st.DB().QueryRow(`SELECT COALESCE(SUM(flows),0) FROM agg_interface WHERE if_index = 7`).Scan(&childFlows); err != nil {
		t.Fatalf("read agg_interface: %v", err)
	}
	// The post-NAT row is the only thing left on if_index 7; the dropped pre-NAT
	// child must not add to it.
	if childFlows != 1 {
		t.Fatalf("agg_interface if_index 7 = %d flows, want 1 (post-NAT only)", childFlows)
	}
}

// ifIndex 0 means "absent", not "the most parent interface". A duplicate group
// that contains a record with no input interface must never keep it as the
// survivor: doing so attributes the traffic to an unknown interface. This is the
// shape sre measured as `0+11`, `2+0+11`, `0+2+7`.
func TestZeroInputInterfaceNeverWins(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)

	// The ifIndex-0 copy arrives first so the survivor rule has to promote.
	unknown := pingRecord(end, 0, "10.10.10.194", 0)
	parent := pingRecord(end, 2, "10.10.10.194", 0)
	if err := st.InsertBatch(context.Background(), []model.Flow{unknown, parent}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got := storedFlows(t, st, 5)
	if len(got) != 1 {
		t.Fatalf("stored %d rows, want 1", len(got))
	}
	if got[0].InputIf != 2 {
		t.Fatalf("survivor input_if = %d, want 2 (ifIndex 0 is absent, not the parent)", got[0].InputIf)
	}

	// The unknown copy arriving second must not displace the parent either.
	st2, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	if err := st2.InsertBatch(context.Background(), []model.Flow{parent, unknown}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got2 := storedFlows(t, st2, 5)
	if len(got2) != 1 || got2[0].InputIf != 2 {
		t.Fatalf("survivor = %+v, want a single row on input_if 2", got2)
	}
}

// A group where every member has no input interface must still store exactly one
// row, and it must not panic or drop the flow.
func TestAllZeroInputInterfaceKeepsOneRow(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)
	a := pingRecord(end, 0, "10.10.10.194", 0)
	b := pingRecord(end, 0, "10.10.10.194", 0)
	b.OutputIf = 1

	if err := st.InsertBatch(context.Background(), []model.Flow{a, b}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 1 {
		t.Fatalf("stored %d rows, want 1", got)
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
	// The pre-NAT child (in=7) is stored first, the parent (in=2) second, in
	// separate batches: the stored row must be promoted to the parent's interfaces.
	child := pingRecord(end, 7, natInnerSrc, 0)
	parent := pingRecord(end, 2, natInnerSrc, 0)
	if err := st.InsertBatch(context.Background(), []model.Flow{child}); err != nil {
		t.Fatalf("insert child: %v", err)
	}
	if err := st.InsertBatch(context.Background(), []model.Flow{parent}); err != nil {
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
