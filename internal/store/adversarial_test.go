package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

// Adversarial tests for the duplicate-suppression policy. The theme: any state
// the writer cannot prove is a pair must be stored, and every aggregate family
// must remain equal to the rows that exist.

// Two unrelated records with the same counters and destination, but on the same
// configured interface, are not a NAT pair: one of them cannot be both views.
func TestNATPairRequiresOppositeInterfaces(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	// Same interface, same everything: two identical records. Both stored.
	a := postNATRecord(end)
	b := a
	if err := st.InsertBatch(context.Background(), []model.Flow{a, b}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2", got)
	}
	assertAggregatesMatchFlows(t, st, "same-interface NAT records")
}

// A record on a configured interface whose mate would have to be on the other
// configured interface must not pair with a record on a third interface.
func TestNATPairIgnoresUnconfiguredInterfaces(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	// Pre-NAT view on interface 2 (configured pre side).
	pre := preNATRecord(end)
	// A LAN flow on interface 3 (not configured) whose source is the WAN address
	// would look like a mate if the interface were ignored.
	other := postNATRecord(end)
	other.InputIf = 3
	if err := st.InsertBatch(context.Background(), []model.Flow{pre, other}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2 (interface 3 is not part of the topology)", got)
	}
	assertAggregatesMatchFlows(t, st, "unconfigured NAT interface")
}

// Two candidate mates mean the pair is ambiguous: nothing is dropped, and both
// candidates survive alongside the new record.
func TestNATAmbiguousPairKeepsEverything(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	// Two post-NAT views with identical counters (e.g. two clients translated to
	// the same port in different moments but with the same byte count).
	mateA := postNATRecord(end)
	mateB := postNATRecord(end)
	mateB.SrcPort = 4321
	if err := st.InsertBatch(context.Background(), []model.Flow{mateA}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := st.InsertBatch(context.Background(), []model.Flow{mateB}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d post-NAT rows, want 2", got)
	}
	// The pre-NAT view now has two candidate mates: no pair is proven.
	pre := preNATRecord(end)
	if err := st.InsertBatch(context.Background(), []model.Flow{pre}); err != nil {
		t.Fatalf("insert pre: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 3 {
		t.Fatalf("stored %d rows, want 3: an ambiguous pair must not collapse", got)
	}
	assertAggregatesMatchFlows(t, st, "ambiguous NAT pair")
}

// A post-NAT record whose mate has a different output interface is not the same
// traversal.
func TestNATPairRequiresMatchingOutputInterface(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	pre := preNATRecord(end)
	post := postNATRecord(end)
	post.OutputIf = 2
	if err := st.InsertBatch(context.Background(), []model.Flow{pre, post}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2 (different egress paths)", got)
	}
}

// The firewall's own WAN-sourced traffic can collide on every counter with a
// forwarded flow. It is exported on interface 0, which is not the configured
// post-NAT interface, so it is never a candidate.
func TestNATIgnoresFirewallOwnTrafficOnUnlistedInterface(t *testing.T) {
	for _, order := range []string{"own-first", "lan-first"} {
		t.Run(order, func(t *testing.T) {
			st := natStore(t)
			end := time.Now().UTC().Truncate(time.Second)

			lan := pingRecord(end, 2, "10.10.10.194", 3535)
			lan.DstAddr, lan.DstPort = "8.8.8.8", 53
			lan.Protocol, lan.Bytes = 17, 74
			lan.OutputIf = 1

			own := lan
			own.SrcAddr = natWANSrc
			own.InputIf = 0 // firewall-originated
			own.SrcPort = 4321

			batch := []model.Flow{lan, own}
			if order == "own-first" {
				batch = []model.Flow{own, lan}
			}
			if err := st.InsertBatch(context.Background(), batch); err != nil {
				t.Fatalf("insert: %v", err)
			}
			if got := countRows(t, st, "flows"); got != 2 {
				t.Fatalf("stored %d rows, want 2: the firewall's own flow has no mate", got)
			}
			assertAggregatesMatchFlows(t, st, "firewall-own traffic")
		})
	}
}

// A cross-exporter match is never a pair: the topology is per exporter.
func TestNATPairIsScopedToItsExporter(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	pre := preNATRecord(end)
	post := postNATRecord(end)
	post.Exporter, post.ExporterIP = "192.0.2.11", "192.0.2.11"
	if err := st.InsertBatch(context.Background(), []model.Flow{pre, post}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2 (different exporters)", got)
	}
}

// A whole realistic hour: many pairs in both arrival orders, bystanders in
// several aggregate families, gaps in the timestamps. Every family must end up
// describing exactly the surviving rows.
func TestSteadyStateHourKeepsEveryAggregateExact(t *testing.T) {
	st := natStore(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)

	pending := make([]model.Flow, 0, 200)
	for i := 0; i < 60; i++ {
		end := hour.Add(time.Duration(i) * time.Second)
		post := postNATRecord(end)
		pre := preNATRecord(end)
		dst := fmt.Sprintf("198.51.%d.%d", i/250, i%250)
		post.DstAddr, pre.DstAddr = dst, dst
		post.SrcPort, pre.SrcPort = uint16(30000+i), uint16(30000+i)

		bystander := pingRecord(end, 2, fmt.Sprintf("10.0.%d.%d", i/250, i%250), uint16(1000+i))
		bystander.DstAddr = fmt.Sprintf("203.0.113.%d", i%250)
		bystander.Protocol = uint8(6 + i%3)

		// Alternate arrival order so both dedup paths run.
		var pair []model.Flow
		if i%2 == 0 {
			pair = []model.Flow{post, pre}
		} else {
			pair = []model.Flow{pre, post}
		}
		pending = append(pending, pair...)
		pending = append(pending, bystander)
		if len(pending) >= 9 {
			if err := st.InsertBatch(context.Background(), pending); err != nil {
				t.Fatalf("batch at %d: %v", i, err)
			}
			pending = pending[:0]
		}
	}
	if len(pending) > 0 {
		if err := st.InsertBatch(context.Background(), pending); err != nil {
			t.Fatalf("final batch: %v", err)
		}
	}
	// 60 pairs collapse to 60 rows, plus 60 bystanders.
	if got := countRows(t, st, "flows"); got != 120 {
		t.Fatalf("stored %d rows, want 120", got)
	}
	assertAggregatesMatchFlows(t, st, "steady-state hour")
}

// A deletion that empties a bucket entirely must not leave a negative or a zero
// row that later reads as a real bucket.
func TestReplacementLeavesNoNegativeAggregates(t *testing.T) {
	st := natStore(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	end := hour.Add(10 * time.Minute)

	// The post-NAT view is the only row for its hosts on the WAN side.
	post := postNATRecord(end)
	post.DstAddr = "198.51.100.33"
	insertOne(t, st, post)
	pre := preNATRecord(end)
	pre.DstAddr = post.DstAddr
	insertOne(t, st, pre)

	for _, table := range []string{"agg_exporter", "agg_interface", "agg_host", "agg_vlan", "agg_protocol", "agg_traffic"} {
		rows, err := st.DB().Query(fmt.Sprintf(`SELECT flows, bytes, packets, sampled_flows, unknown_flows FROM %s`, table))
		if err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		for rows.Next() {
			var flows, bytes, packets, sampled, unknown int64
			if err := rows.Scan(&flows, &bytes, &packets, &sampled, &unknown); err != nil {
				t.Fatalf("%s scan: %v", table, err)
			}
			if flows < 0 || bytes < 0 || packets < 0 || sampled < 0 || unknown < 0 {
				rows.Close()
				t.Fatalf("%s holds a negative aggregate: flows=%d bytes=%d packets=%d sampled=%d unknown=%d",
					table, flows, bytes, packets, sampled, unknown)
			}
		}
		rows.Close()
	}
	assertAggregatesMatchFlows(t, st, "replacement leaves no negatives")
}

// Reverse DNS must be attached to the row that holds the record's own addresses.
func TestInsertResultIdentifiesTheRowForEachRecord(t *testing.T) {
	st := combinedStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	parent := pingRecord(end, 2, "10.10.10.1", 3535)
	child := pingRecord(end, 7, "10.10.10.1", 3535)
	other := pingRecord(end, 2, "10.10.10.3", 3535)
	other.DstAddr = "198.51.100.3"

	res, err := st.InsertBatchResult(context.Background(), []model.Flow{parent, child, other})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if len(res.IDs) != 3 {
		t.Fatalf("ids = %d, want one per record", len(res.IDs))
	}
	if res.NewRows != 2 || res.Merged != 1 {
		t.Fatalf("NewRows=%d Merged=%d, want 2 and 1", res.NewRows, res.Merged)
	}
	for i, want := range []string{"10.10.10.1", "10.10.10.1", "10.10.10.3"} {
		if res.IDs[i] == 0 {
			t.Fatalf("position %d has no row id", i)
		}
		var src string
		if err := st.DB().QueryRow(`SELECT src_addr FROM flows WHERE id = ?`, res.IDs[i]).Scan(&src); err != nil {
			t.Fatalf("position %d id %d: %v", i, res.IDs[i], err)
		}
		if src != want {
			t.Fatalf("position %d id %d holds %s, want %s", i, res.IDs[i], src, want)
		}
	}
	// The merged child is attached to the parent's row, and that is the only row
	// for the frame.
	if res.IDs[0] != res.IDs[1] {
		t.Fatalf("child attached to %d but parent is %d", res.IDs[1], res.IDs[0])
	}
}

// A post-NAT view dropped in favour of a stored pre-NAT mate carries different
// addresses, so it must NOT be given the pre-NAT row's id: a lookup for the WAN
// address written onto the pre-NAT row would mislabel it.
func TestDroppedPostNATCarriesNoRowID(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	pre := preNATRecord(end)
	post := postNATRecord(end)
	res, err := st.InsertBatchResult(context.Background(), []model.Flow{pre, post})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if res.IDs[0] == 0 {
		t.Fatal("the surviving pre-NAT record has no row id")
	}
	if res.IDs[1] != 0 {
		t.Fatalf("the dropped post-NAT record was attached to row %d", res.IDs[1])
	}
}
