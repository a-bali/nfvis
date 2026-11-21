package store

import (
	"context"
	"testing"
	"time"

	"path/filepath"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// The measured M2 fixture: ng_netflow exports one forwarded packet twice from
// the same interface node. The post-NAT copy has the source address and source
// port rewritten by PAT, the same destination and counters, and an export
// timestamp up to a second away from its pre-NAT mate.
const (
	natWANSrc   = "192.168.0.2" // the OPNsense WAN address, after PAT
	natInnerSrc = "10.10.10.194"
)

// natStore opens a store with NAT dedup enabled for the measured topology: the
// firewall exports the pre-NAT view from input node 2 and the post-NAT view from
// input node 7. Both the WAN address and those interfaces are required — without
// the interface topology a counters match is not evidence of a NAT pair.
func natStore(t *testing.T) *Store {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.Database.Retention.Raw = config.Duration(24 * time.Hour)
	cfg.Database.Retention.Hourly = config.Duration(72 * time.Hour)
	cfg.Database.Dedup.NAT.WANAddresses = []string{natWANSrc}
	cfg.Database.Dedup.NAT.Exporters = []config.NATExporter{
		{Address: "192.0.2.10", PreInterface: 2, PostInterface: 7},
	}
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func preNATRecord(end time.Time) model.Flow {
	f := pingRecord(end, 2, natInnerSrc, 0)
	return f
}

// postNATRecord is the measured post-NAT view: same counters and destination,
// source rewritten to the WAN address, exported inside the window. It is on
// input node 7 in the production capture, which is why nothing may key on the
// interface when pairing the two views.
func postNATRecord(end time.Time) model.Flow {
	f := pingRecord(end.Add(700*time.Millisecond), 7, natWANSrc, 0)
	return f
}

// M2: the pre-NAT and post-NAT view of one packet collapse to the pre-NAT record,
// which carries the real source identity.
func TestPostNATCopyCollapsesToPreNAT(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	if err := st.InsertBatch(context.Background(), []model.Flow{preNATRecord(end), postNATRecord(end)}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got := storedFlows(t, st, 5)
	if len(got) != 1 {
		t.Fatalf("stored %d rows, want 1: %+v", len(got), got)
	}
	if got[0].SrcAddr != natInnerSrc || got[0].SrcPort != 0 {
		t.Fatalf("survivor = %s:%d, want the pre-NAT %s", got[0].SrcAddr, got[0].SrcPort, natInnerSrc)
	}
}

// Arrival order must not matter: the post-NAT copy may be exported and stored
// first, in which case the row has to be rebuilt when its mate arrives.
func TestPostNATCopyCollapsesWhenItArrivesFirst(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	// Separate batches, post-NAT first.
	if err := st.InsertBatch(context.Background(), []model.Flow{postNATRecord(end)}); err != nil {
		t.Fatalf("insert post-NAT: %v", err)
	}
	if err := st.InsertBatch(context.Background(), []model.Flow{preNATRecord(end)}); err != nil {
		t.Fatalf("insert pre-NAT: %v", err)
	}
	got := storedFlows(t, st, 5)
	if len(got) != 1 {
		t.Fatalf("stored %d rows, want 1", len(got))
	}
	if got[0].SrcAddr != natInnerSrc {
		t.Fatalf("survivor src = %s, want the pre-NAT %s", got[0].SrcAddr, natInnerSrc)
	}
	// Interfaces and counters follow the surviving record.
	if got[0].Bytes != 84 || got[0].Packets != 1 {
		t.Fatalf("survivor counters = %d/%d", got[0].Bytes, got[0].Packets)
	}
}

// The pre-NAT mate arrives beyond the ±2 s window described in the analysis:
// these are two different packets and both must be kept.
func TestNATPairOutsideWindowIsKept(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	mate := postNATRecord(end)
	mate.EndMS = preNATRecord(end).EndMS + 5000

	if err := st.InsertBatch(context.Background(), []model.Flow{preNATRecord(end), mate}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2 outside the NAT window", got)
	}
}

// Firewall-originated traffic — sourced from the WAN address with in==0 and no
// pre-NAT mate — is genuine traffic (the firewall's own DNS, NTP, ...). A rule
// keyed on "source == WAN" would delete it; the mate requirement must not.
func TestFirewallOriginatedWANTrafficSurvives(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	own := pingRecord(end, 0, natWANSrc, 5353)
	own.DstAddr = "192.168.1.1"
	own.DstPort = 53
	own.Protocol = 17
	own.Bytes = 74

	if err := st.InsertBatch(context.Background(), []model.Flow{own}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 1 {
		t.Fatalf("stored %d rows, the firewall's own WAN-sourced flow was dropped", got)
	}
	got := storedFlows(t, st, 2)
	if got[0].SrcAddr != natWANSrc || got[0].InputIf != 0 {
		t.Fatalf("stored row = %s in=%d, want the untouched firewall-sourced record", got[0].SrcAddr, got[0].InputIf)
	}
}

// The combined mechanism on the measured ping: with both the parent/child link
// and the NAT topology declared, three records in, one out, and the survivor is
// the pre-NAT parent.
func TestCombinedDedupLeavesOnePreNATParentRecord(t *testing.T) {
	st := combinedStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	records := append(pingRecords(end), postNATRecord(end))

	if err := st.InsertBatch(context.Background(), records); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got := storedFlows(t, st, 5)
	if len(got) != 1 {
		t.Fatalf("stored %d rows, want 1: %+v", len(got), got)
	}
	f := got[0].Flow
	if f.InputIf != 2 || f.OutputIf != 1 {
		t.Fatalf("survivor interfaces = in=%d out=%d, want in=2 out=1", f.InputIf, f.OutputIf)
	}
	if f.SrcAddr != natInnerSrc || f.DstAddr != "146.110.3.12" {
		t.Fatalf("survivor endpoints = %s -> %s", f.SrcAddr, f.DstAddr)
	}
	if f.Bytes != 84 || f.Packets != 1 || f.Protocol != 1 {
		t.Fatalf("survivor payload = %d bytes / %d packets / proto %d", f.Bytes, f.Packets, f.Protocol)
	}
	// Aggregates must count the one surviving traversal, exactly once.
	var bytes, packets, flows int64
	if err := st.DB().QueryRow(`SELECT SUM(bytes), SUM(packets), SUM(flows) FROM agg_exporter`).Scan(&bytes, &packets, &flows); err != nil {
		t.Fatalf("read agg_exporter: %v", err)
	}
	if bytes != 84 || packets != 1 || flows != 1 {
		t.Fatalf("agg_exporter = %d bytes / %d packets / %d flows, want 84/1/1", bytes, packets, flows)
	}
}

// A post-NAT row rebuilt in place must move its aggregate contribution too: the
// source-host aggregate written for the WAN address cannot stay behind.
func TestPostNATRebuildMovesHostAggregate(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	if err := st.InsertBatch(context.Background(), []model.Flow{postNATRecord(end)}); err != nil {
		t.Fatalf("insert post-NAT: %v", err)
	}
	if err := st.InsertBatch(context.Background(), []model.Flow{preNATRecord(end)}); err != nil {
		t.Fatalf("insert pre-NAT: %v", err)
	}
	var wanHosts int64
	if err := st.DB().QueryRow(`SELECT COALESCE(SUM(flows),0) FROM agg_host WHERE host = ? AND direction='source'`, natWANSrc).Scan(&wanHosts); err != nil {
		t.Fatalf("read agg_host: %v", err)
	}
	if wanHosts != 0 {
		t.Fatalf("agg_host still counts the replaced post-NAT source: %d flows for %s", wanHosts, natWANSrc)
	}
	var innerHosts int64
	if err := st.DB().QueryRow(`SELECT COALESCE(SUM(flows),0) FROM agg_host WHERE host = ? AND direction='source'`, natInnerSrc).Scan(&innerHosts); err != nil {
		t.Fatalf("read agg_host: %v", err)
	}
	if innerHosts != 1 {
		t.Fatalf("agg_host counts %d flows for the surviving source, want 1", innerHosts)
	}
}

// A pre-NAT record and its post-NAT mate can share an exported interval with a
// different packet; the counters are part of the identity, so a busy connection
// is not lost.
func TestNATDedupKeepsDistinctCounterValues(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	first := postNATRecord(end)
	second := first
	second.Packets = 2
	second.Bytes = 196

	if err := st.InsertBatch(context.Background(), []model.Flow{first, second}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2", got)
	}
}
