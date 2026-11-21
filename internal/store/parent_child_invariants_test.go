package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/model"
)

// A three-node chain (or a duplicated export of one node) puts both a possible
// parent and a possible child in the same fingerprint group. No pair can be
// proven there, so nothing is collapsed.
func TestParentChildChainOfThreeIsNotCollapsed(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	// 2 -> 7 is declared. The third record is on interface 2 as well, which makes
	// the interface-7 record both a child of one row and a parent of nothing, but
	// the interface-2 group ambiguous.
	a := pingRecord(end, 2, "10.10.10.194", 3535)
	b := pingRecord(end, 7, "10.10.10.194", 3535)
	// A second row on the parent interface with the same payload: now the child
	// has two possible parents.
	c := pingRecord(end, 2, "10.10.10.194", 3535)

	if err := st.InsertBatch(context.Background(), []model.Flow{a, b, c}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// a+b collapse (a is the single parent seen when b arrives). c is a second
	// parent, so adding it must not collapse c into the same row: the group now
	// has two parents and the child is ambiguous.
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2 (one collapsed pair and the ambiguous second parent)", got)
	}
	assertAggregatesMatchFlows(t, st, "three-node chain")
}

// An exporter whose links exist but whose interface is not in them never
// collapses, in either order.
func TestParentChildUnlinkedChildIsNotCollapsed(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	parent := pingRecord(end, 2, "10.10.10.194", 3535)
	// Interface 9 shares nothing with the declared link 2->7.
	other := pingRecord(end, 9, "10.10.10.194", 3535)
	if err := st.InsertBatch(context.Background(), []model.Flow{other, parent}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("stored %d rows, want 2", got)
	}
	assertAggregatesMatchFlows(t, st, "unlinked interface")
}

// The reverse arrival order of the declared pair must collapse, and the survivor
// must be the parent in both cases.
func TestParentChildArrivalOrderDoesNotMatter(t *testing.T) {
	for _, order := range []string{"parent-first", "child-first"} {
		t.Run(order, func(t *testing.T) {
			st := pcStore(t)
			end := time.Now().UTC().Truncate(time.Second)
			parent := pingRecord(end, 2, "10.10.10.194", 3535)
			child := pingRecord(end, 7, "10.10.10.194", 3535)
			batch := []model.Flow{parent, child}
			if order == "child-first" {
				batch = []model.Flow{child, parent}
			}
			if err := st.InsertBatch(context.Background(), batch); err != nil {
				t.Fatalf("insert: %v", err)
			}
			got := storedFlows(t, st, 5)
			if len(got) != 1 {
				t.Fatalf("stored %d rows, want 1", len(got))
			}
			if got[0].InputIf != 2 || got[0].OutputIf != 1 {
				t.Fatalf("survivor interfaces in=%d out=%d, want 2/1", got[0].InputIf, got[0].OutputIf)
			}
			assertAggregatesMatchFlows(t, st, "arrival order "+order)
		})
	}
}
