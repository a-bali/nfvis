package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

// A promoted duplicate must leave the stored row and the aggregates describing
// the same thing. Promotion rewrites the interface columns of the stored row, so
// any non-interface field that differs between the two records would make the
// row and the aggregate bucket disagree: the row keeps the child's VLAN and
// sampling flag while the aggregates move to the parent's.

// The reviewer's repro: child first with VLAN 10 and sampling off, parent second
// with VLAN 20 and sampling on.
func TestParentChildDoesNotCollapseDifferentVLAN(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	child := pingRecord(end, 7, "10.10.10.194", 3535)
	child.SrcVLAN = 10
	parent := pingRecord(end, 2, "10.10.10.194", 3535)
	parent.SrcVLAN = 20
	parent.Sampled = true

	insertOne(t, st, child)
	insertOne(t, st, parent)

	// Different VLAN tags: the records are not the same observed frame, so both
	// are stored rather than one being rewritten into the other.
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2 (different VLANs are different frames)", got)
	}
	assertAggregatesMatchFlows(t, st, "different VLAN")
}

// Sampling is part of the aggregate contribution, so a differing flag cannot be
// promoted away either.
func TestParentChildDoesNotCollapseDifferentSampling(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	child := pingRecord(end, 7, "10.10.10.194", 3535)
	child.Sampled = false
	parent := pingRecord(end, 2, "10.10.10.194", 3535)
	parent.Sampled = true
	parent.SamplingInterval = 100
	parent.SamplingSource = "header"

	insertOne(t, st, child)
	insertOne(t, st, parent)

	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2 (a sampled and an unsampled record are not the same frame)", got)
	}
	assertAggregatesMatchFlows(t, st, "different sampling")
}

// Every non-interface field that is stored must agree before one record may
// stand in for another: otherwise the survivor reports the child's value while
// the reader of the row believes it is the parent's record.
func TestParentChildDoesNotCollapseDifferingFields(t *testing.T) {
	cases := map[string]func(*model.Flow){
		"tcp flags": func(f *model.Flow) { f.TCPFlags = 0x12 },
		"src tos":   func(f *model.Flow) { f.SrcTOS = 0x28 },
		"dst tos":   func(f *model.Flow) { f.DstTOS = 0x04 },
		"src as":    func(f *model.Flow) { f.SrcAS = 64512 },
		"dst as":    func(f *model.Flow) { f.DstAS = 64513 },
		"dst vlan":  func(f *model.Flow) { f.DstVLAN = 30 },
		"dst vlan source": func(f *model.Flow) {
			f.DstVLANSource = "exported"
		},
		"dst addr":       func(f *model.Flow) { f.DstAddr = "146.110.3.13" },
		"dst port":       func(f *model.Flow) { f.DstPort = 443 },
		"protocol":       func(f *model.Flow) { f.Protocol = 6 },
		"direction":      func(f *model.Flow) { f.Direction = 2 },
		"sampling algo":  func(f *model.Flow) { f.SamplingAlgorithm = 1 },
		"sampling src":   func(f *model.Flow) { f.SamplingSource = "options" },
		"sampling intvl": func(f *model.Flow) { f.SamplingInterval = 512 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			st := pcStore(t)
			end := time.Now().UTC().Truncate(time.Second)
			// The byte/packet counts stay equal so the records remain candidates
			// for the same fingerprint; only the mutated field differs.
			child := pingRecord(end, 7, "10.10.10.194", 3535)
			parent := pingRecord(end, 2, "10.10.10.194", 3535)
			mutate(&parent)

			insertOne(t, st, child)
			insertOne(t, st, parent)

			if got := countRows(t, st, "flows"); got != 2 {
				t.Fatalf("stored %d rows, want 2: records differing in %s are not the same frame", got, name)
			}
			assertAggregatesMatchFlows(t, st, "differing "+name)
		})
	}
}

// Records with no exported window are bucketed by received time. Two copies
// received in different hours are different aggregate contributions, and the row
// itself cannot move because its received time is what places it.
func TestParentChildDoesNotCollapseAcrossHoursOnReceivedTimeFallback(t *testing.T) {
	st := pcStore(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-5 * time.Hour)

	child := pingRecord(hour.Add(50*time.Minute), 7, "10.10.10.194", 3535)
	child.StartMS, child.EndMS = 0, 0
	child.TimeSource = "received"
	parent := pingRecord(hour.Add(70*time.Minute), 2, "10.10.10.194", 3535)
	parent.StartMS, parent.EndMS = 0, 0
	parent.TimeSource = "received"

	insertOne(t, st, child)
	insertOne(t, st, parent)

	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2: the copies fall in different hours", got)
	}
	assertAggregatesMatchFlows(t, st, "received-time fallback across hours")
}

// Control: when the only difference is the interface node the record was
// reported on, the pair must still collapse — the equivalence check must not
// disable the feature.
func TestParentChildStillCollapsesEquivalentCopies(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	parent := pingRecord(end, 2, "10.10.10.194", 3535)
	parent.InputLabel = "re1"
	child := pingRecord(end, 7, "10.10.10.194", 3535)
	child.InputLabel = "re1_vlan20"

	insertOne(t, st, child)
	insertOne(t, st, parent)

	got := storedFlows(t, st, 5)
	if len(got) != 1 {
		t.Fatalf("stored %d rows, want 1 (equivalent copies must still collapse)", len(got))
	}
	if got[0].InputIf != 2 || got[0].InputLabel != "re1" {
		t.Fatalf("survivor = in=%d label=%q, want the parent's in=2 label=re1", got[0].InputIf, got[0].InputLabel)
	}
	if got[0].SrcVLAN != -1 || got[0].Sampled {
		t.Fatalf("survivor carries vlan=%d sampled=%v, want the shared values", got[0].SrcVLAN, got[0].Sampled)
	}
	assertAggregatesMatchFlows(t, st, "equivalent copies")
}

// Same-batch arrival must behave identically to the cross-batch case.
func TestParentChildDifferentVLANInOneBatchIsPreserved(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	child := pingRecord(end, 7, "10.10.10.194", 3535)
	child.SrcVLAN = 10
	parent := pingRecord(end, 2, "10.10.10.194", 3535)
	parent.SrcVLAN = 20
	if err := st.InsertBatch(context.Background(), []model.Flow{child, parent}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2", got)
	}
	assertAggregatesMatchFlows(t, st, "different VLAN in one batch")
}

// The same requirement under the combined configuration: a NAT mate whose VLANs
// differ is still replaced (the whole row is inserted), so only the parent/child
// promotion path needs the gate.
func TestCombinedDifferentVLANKeepsRowsAndAggregates(t *testing.T) {
	st := combinedStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	child := pingRecord(end, 7, natInnerSrc, 3535)
	child.SrcVLAN = 10
	parent := pingRecord(end, 2, natInnerSrc, 3535)
	parent.SrcVLAN = 20
	insertOne(t, st, child)
	insertOne(t, st, parent)
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2", got)
	}
	assertAggregatesMatchFlows(t, st, "combined different VLAN")
}
