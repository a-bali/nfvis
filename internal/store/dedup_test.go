package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
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
// source address and so is the NAT mechanism's business, not M1's.
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

// M1: with the parent/child link for interfaces 2 and 7 declared, the two
// copies of one frame collapse to a single row carrying the parent's interfaces.
// Without that declaration nothing collapses, which TestParentChildNeedsLinks
// covers.
func TestParentChildDuplicatesCollapseToParent(t *testing.T) {
	st := pcStore(t)
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
	assertAggregatesMatchFlows(t, st, "parent/child collapse")
}

// No declared link means nothing is provably a duplicate, so every record is
// stored. This is the lossless default and the reason parent_child cannot be
// switched on by itself.
func TestParentChildNeedsLinks(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)
	if err := st.InsertBatch(context.Background(), pingRecords(end)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 3 {
		t.Fatalf("stored %d rows with no links configured, want all 3", got)
	}
}

// M1 is not batch-local: 10,389 duplicate groups in one measured hour straddle
// the 250 ms flush boundary, so a second batch must not re-insert a key the
// previous batch already stored.
func TestParentChildDuplicatesAcrossBatches(t *testing.T) {
	st := pcStore(t)
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
	assertAggregatesMatchFlows(t, st, "parent/child across batches")
}

// A long-lived flow is re-exported with cumulative counters (packets 1, then
// 2, ...). Those records share every field except bytes/packets and must all be
// stored, or live connections lose their traffic.
func TestCumulativeReExportsAreNotCollapsed(t *testing.T) {
	st := pcStore(t)
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
	st := pcStore(t)
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
	assertAggregatesMatchFlows(t, st, "deduped aggregates")
}

// ifIndex 0 means "absent", not "the most parent interface". A record that
// carries no input interface is never collapsed into another row, in either
// arrival order: doing so would attribute or hide a traversal arbitrarily.
func TestZeroInputInterfaceIsNeverCollapsed(t *testing.T) {
	for _, order := range []string{"unknown-first", "parent-first"} {
		t.Run(order, func(t *testing.T) {
			st := pcStore(t)
			end := time.Now().UTC().Truncate(time.Second)
			unknown := pingRecord(end, 0, "10.10.10.194", 0)
			parent := pingRecord(end, 2, "10.10.10.194", 0)
			batch := []model.Flow{unknown, parent}
			if order == "parent-first" {
				batch = []model.Flow{parent, unknown}
			}
			if err := st.InsertBatch(context.Background(), batch); err != nil {
				t.Fatalf("insert: %v", err)
			}
			if got := countRows(t, st, "flows"); got != 2 {
				t.Fatalf("stored %d rows, want 2: an unrecorded interface cannot be proven a copy", got)
			}
			assertAggregatesMatchFlows(t, st, "zero interface preserved")
		})
	}
}

// A group of more than one stored copy is not collapsed: once two rows share a
// fingerprint there is no single provable parent, and collapsing into either
// would discard a traversal on a guess.
func TestMultiplicityAboveOneIsPreserved(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	parent := pingRecord(end, 2, "10.10.10.194", 0)
	child := pingRecord(end, 7, "10.10.10.194", 0)
	// A third copy on a second child interface, which is not a declared link.
	other := pingRecord(end, 9, "10.10.10.194", 0)

	if err := st.InsertBatch(context.Background(), []model.Flow{parent, child, other}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// parent+child collapse; the unlinked interface 9 record stays.
	got := storedFlows(t, st, 10)
	if len(got) != 2 {
		t.Fatalf("stored %d rows, want 2 (collapsed parent/child plus the unlinked copy)", len(got))
	}
	var ifIndexes []uint32
	for _, r := range got {
		ifIndexes = append(ifIndexes, r.InputIf)
	}
	if ifIndexes[0] != 2 || ifIndexes[1] != 9 {
		t.Fatalf("survivors on interfaces %v, want [2 9]", ifIndexes)
	}
	assertAggregatesMatchFlows(t, st, "multiplicity")
}

// Promotion moves the stored row's interface contribution: the aggregate written
// for the child must move with it, or the dashboard reports a traversal that no
// stored row supports.
func TestPromotionMovesInterfaceAggregate(t *testing.T) {
	st := pcStore(t)
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
	assertAggregatesMatchFlows(t, st, "promotion aggregates")
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

// Two different exporters reporting the same payload are distinct observations.
func TestDedupKeyScopedToExporter(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	a := pingRecord(end, 2, "10.10.10.194", 0)
	b := pingRecord(end, 7, "10.10.10.194", 0)
	b.Exporter = "192.0.2.11"
	b.ExporterIP = "192.0.2.11"

	if err := st.InsertBatch(context.Background(), []model.Flow{a, b}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2 (one per exporter)", got)
	}
}

// The configured link is directional and per exporter: the reverse pair and a
// different exporter are both preserved.
func TestParentChildLinkIsPerExporterAndDirectional(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	// Reversed: 2 arrives as a "child" of 7, which is not declared.
	reversed := []model.Flow{
		pingRecord(end, 7, "10.10.10.194", 0),
		pingRecord(end, 2, "10.10.10.194", 0),
	}
	if err := st.InsertBatch(context.Background(), reversed); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// The declared direction is parent 2, child 7, so this pair still collapses.
	if got := countRows(t, st, "flows"); got != 1 {
		t.Fatalf("declared pair collapsed to %d rows, want 1", got)
	}

	// A second exporter with the same interface numbers has no links.
	st2 := pcStore(t)
	other := pingRecord(end, 7, "10.10.10.194", 0)
	other.Exporter, other.ExporterIP = "192.0.2.11", "192.0.2.11"
	base := pingRecord(end, 2, "10.10.10.194", 0)
	base.Exporter, base.ExporterIP = "192.0.2.11", "192.0.2.11"
	if err := st2.InsertBatch(context.Background(), []model.Flow{base, other}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st2, "flows"); got != 2 {
		t.Fatalf("stored %d rows for the unlinked exporter, want 2", got)
	}
}

// A child copy that differs in its output interface is not a copy: the pair only
// shares the fields the fingerprint covers, and the path through the device
// differs.
func TestParentChildRequiresMatchingOutputInterface(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	parent := pingRecord(end, 2, "10.10.10.194", 0)
	child := pingRecord(end, 7, "10.10.10.194", 0)
	child.OutputIf = 3
	if err := st.InsertBatch(context.Background(), []model.Flow{parent, child}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2: differing output interfaces are different paths", got)
	}
}

// Config validation must reject a topology that cannot work rather than silently
// doing nothing.
func TestDedupConfigValidation(t *testing.T) {
	cases := map[string]func(*config.Config){
		"parent_child without links": func(c *config.Config) { c.Database.Dedup.ParentChild = true },
		"link with zero parent": func(c *config.Config) {
			c.Database.Dedup.ParentChildLinks = []config.ParentChildLink{{Exporter: "192.0.2.10", Parent: 0, Child: 7}}
		},
		"nat addresses without exporters": func(c *config.Config) {
			c.Database.Dedup.NAT.WANAddresses = []string{natWANSrc}
		},
		"nat exporter without interfaces": func(c *config.Config) {
			c.Database.Dedup.NAT.Exporters = []config.NATExporter{{Address: "192.0.2.10"}}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := config.Default()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatalf("configuration accepted, want a validation error")
			}
		})
	}
	// The defaults are valid and enable nothing.
	cfg := config.Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	if cfg.Database.Dedup.ParentChild || len(cfg.Database.Dedup.NAT.WANAddresses) > 0 {
		t.Fatal("defaults enable duplicate suppression, which cannot be safe without topology")
	}
}
