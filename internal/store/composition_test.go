package store

import (
	"context"
	"testing"
	"time"

	"github.com/balia/nfvis/internal/model"
)

// Composition tests: NAT and parent/child act on the same measured packet, whose
// three exports are
//
//	R1 parent pre-NAT   in=2 out=1  src 10.10.10.194
//	R2 post-NAT         in=7 out=1  src 192.168.0.2
//	R3 child  pre-NAT   in=7 out=1  src 10.10.10.194
//
// Exactly one row must survive whichever order they arrive in, in one batch or in
// separate batches. Before the fix the orders that put R1 last left two rows:
// the NAT plan deleted R2 and then inserted R1 without noticing that R3 was its
// child copy.

// pingOrders returns every delivery order of the three measured records as
// indexes into pingRecords.
func pingOrders() map[string][]int {
	return map[string][]int{
		"R1,R2,R3": {0, 1, 2},
		"R1,R3,R2": {0, 2, 1},
		"R2,R1,R3": {1, 0, 2},
		"R2,R3,R1": {1, 2, 0},
		"R3,R1,R2": {2, 0, 1},
		"R3,R2,R1": {2, 1, 0},
	}
}

func orderedPing(end time.Time, order []int) []model.Flow {
	recs := pingRecords(end)
	out := make([]model.Flow, len(order))
	for i, idx := range order {
		out[i] = recs[idx]
	}
	return out
}

// assertOnePreNATParentRow checks the surviving row is the pre-NAT parent record.
func assertOnePreNATParentRow(t *testing.T, st *Store, context string) {
	t.Helper()
	var rows int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM flows`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		var detail []string
		rs, _ := st.DB().Query(`SELECT id, input_if, output_if, src_addr, bytes FROM flows ORDER BY id`)
		if rs != nil {
			defer rs.Close()
			for rs.Next() {
				var id, b int64
				var in, out uint32
				var src string
				_ = rs.Scan(&id, &in, &out, &src, &b)
				detail = append(detail, fmtRow(id, in, out, src, b))
			}
		}
		t.Fatalf("%s: one packet exported three ways survived as %d rows: %v", context, rows, detail)
	}
	var in, out uint32
	var src string
	var bytes, packets int64
	if err := st.DB().QueryRow(`SELECT input_if, output_if, src_addr, bytes, packets FROM flows`).
		Scan(&in, &out, &src, &bytes, &packets); err != nil {
		t.Fatal(err)
	}
	if in != 2 || out != 1 {
		t.Fatalf("%s: survivor interfaces in=%d out=%d, want 2/1", context, in, out)
	}
	if src != natInnerSrc {
		t.Fatalf("%s: survivor src=%s, want the pre-NAT %s", context, src, natInnerSrc)
	}
	if bytes != 84 || packets != 1 {
		t.Fatalf("%s: survivor counters %d/%d, want 84/1", context, bytes, packets)
	}
	assertAggregatesMatchFlows(t, st, context)
}

func fmtRow(id int64, in, out uint32, src string, bytes int64) string {
	return "{" + itoa(id) + " in=" + itoa(int64(in)) + " out=" + itoa(int64(out)) + " src=" + src + " bytes=" + itoa(bytes) + "}"
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf []byte
	for v > 0 {
		buf = append([]byte{byte('0' + v%10)}, buf...)
		v /= 10
	}
	if neg {
		return "-" + string(buf)
	}
	return string(buf)
}

// All six orders in one batch.
func TestCombinedDedupAllArrivalOrdersOneBatch(t *testing.T) {
	for name, order := range pingOrders() {
		t.Run(name, func(t *testing.T) {
			st := combinedStore(t)
			end := time.Now().UTC().Truncate(time.Second)
			if err := st.InsertBatch(context.Background(), orderedPing(end, order)); err != nil {
				t.Fatalf("insert: %v", err)
			}
			assertOnePreNATParentRow(t, st, "one batch "+name)
		})
	}
}

// All six orders with each record in its own batch: the real 250 ms flush shape,
// which is where the cross-batch memory has to hold.
func TestCombinedDedupAllArrivalOrdersSeparateBatches(t *testing.T) {
	for name, order := range pingOrders() {
		t.Run(name, func(t *testing.T) {
			st := combinedStore(t)
			end := time.Now().UTC().Truncate(time.Second)
			for _, f := range orderedPing(end, order) {
				insertOne(t, st, f)
			}
			assertOnePreNATParentRow(t, st, "separate batches "+name)
		})
	}
}

// Mixed: first record alone, the other two together. Covers the "one committed,
// two in flight" shape the flush boundary produces.
func TestCombinedDedupMixedBatchBoundaries(t *testing.T) {
	for name, order := range pingOrders() {
		for split := 1; split < len(order); split++ {
			name, order, split := name, order, split
			t.Run(name+"-split"+itoa(int64(split)), func(t *testing.T) {
				st := combinedStore(t)
				end := time.Now().UTC().Truncate(time.Second)
				recs := orderedPing(end, order)
				if err := st.InsertBatch(context.Background(), recs[:split]); err != nil {
					t.Fatalf("first batch: %v", err)
				}
				if err := st.InsertBatch(context.Background(), recs[split:]); err != nil {
					t.Fatalf("second batch: %v", err)
				}
				assertOnePreNATParentRow(t, st, "split at "+itoa(int64(split)))
			})
		}
	}
}

// With only NAT configured (no parent/child link), the R3-then-R1 shape is not a
// provable parent/child pair, so the two pre-NAT copies stay as two rows: a false
// negative, never a dropped record.
func TestNATOnlyKeepsUnlinkedPreNATCopies(t *testing.T) {
	st := natStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	recs := pingRecords(end)
	insertOne(t, st, recs[0]) // parent
	insertOne(t, st, recs[2]) // child: no link configured, cannot be proven a copy
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("rows = %d, want 2 without a parent/child link", got)
	}
	assertAggregatesMatchFlows(t, st, "NAT only, unlinked copies")
}

// A pre-NAT record that supersedes a post-NAT row and is itself the child copy of
// a stored parent must merge into the parent rather than add a row: the same
// composition in the opposite role assignment.
func TestCombinedChildSupersedesPostNATThenMerges(t *testing.T) {
	st := combinedStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	recs := pingRecords(end)
	// parent stored, then the post-NAT view (dropped as redundant), then the
	// child copy (must merge into the parent).
	insertOne(t, st, recs[0])
	insertOne(t, st, recs[1])
	insertOne(t, st, recs[2])
	assertOnePreNATParentRow(t, st, "parent, post-NAT, child")
}

// Two identical post-NAT candidates make the NAT pair ambiguous. Nothing is
// dropped by guesswork, and the parent/child merge still sees exactly one child.
func TestCombinedAmbiguousPostNATDoesNotDrop(t *testing.T) {
	st := combinedStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	recs := pingRecords(end)

	// Two post-NAT copies of the same packet (e.g. re-exported).
	post1 := recs[1]
	post2 := recs[1]
	post2.SrcPort = 4321
	insertOne(t, st, post1)
	insertOne(t, st, post2)
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("rows = %d, want 2 (both post-NAT copies kept while ambiguous)", got)
	}
	// The pre-NAT parent arrives: two candidate mates means no pair is proven, so
	// it is stored as its own row; nothing already stored is deleted.
	insertOne(t, st, recs[0])
	if got := countRows(t, st, "flows"); got != 3 {
		t.Fatalf("rows = %d, want 3 (ambiguous pair must not delete anything)", got)
	}
	assertAggregatesMatchFlows(t, st, "ambiguous post-NAT")
}

// Non-equivalent fields must still block the merge half of the composition: a
// parent whose VLAN differs from the stored child's is stored separately, and the
// post-NAT row it supersedes is still removed (that pairing is unaffected).
func TestCombinedNonEquivalentParentDoesNotMerge(t *testing.T) {
	st := combinedStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	recs := pingRecords(end)

	child := recs[2]
	child.SrcVLAN = 10
	insertOne(t, st, child)
	insertOne(t, st, recs[1]) // post-NAT

	parent := recs[0]
	parent.SrcVLAN = 20
	insertOne(t, st, parent)

	// child (vlan 10) and parent (vlan 20) are different observations; the
	// post-NAT row was superseded. Two rows, both real.
	if got := countRows(t, st, "flows"); got != 2 {
		t.Fatalf("rows = %d, want 2 (child + parent; post-NAT superseded)", got)
	}
	assertAggregatesMatchFlows(t, st, "non-equivalent parent")
}
