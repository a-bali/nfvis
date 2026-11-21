package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// These are the black-box regression tests for the four review findings. They
// use only APIs that exist before the fix, so they run — and fail — against the
// unfixed tree.

// ---------------------------------------------------------------------------
// Finding 1: reverse-order NAT replacement cannot re-derive an hour from raw
// rows. Unrelated aggregates in the same hour collide with the re-derivation,
// the transaction aborts, and the whole batch is lost.
// ---------------------------------------------------------------------------

func TestRegressionReverseNATWithBystandersInHour(t *testing.T) {
	st := natStore(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-6 * time.Hour)

	// A bystander that differs from the NAT pair in protocol, exporter, host and
	// VLAN: its aggregate rows belong to other groups of the same hour.
	bystanders := []model.Flow{
		func() model.Flow {
			f := pingRecord(hour.Add(2*time.Minute), 2, "10.0.0.61", 1234)
			f.Protocol = 17
			f.DstAddr, f.DstPort = "198.51.100.200", 53
			return f
		}(),
		func() model.Flow {
			f := pingRecord(hour.Add(3*time.Minute), 3, "10.0.0.62", 1235)
			f.Exporter, f.ExporterIP = "192.0.2.11", "192.0.2.11"
			f.SrcVLAN = 12
			return f
		}(),
		func() model.Flow {
			f := pingRecord(hour.Add(4*time.Minute), 5, "", 0)
			f.DstAddr = "203.0.113.250"
			f.Sampled = true
			return f
		}(),
	}
	for _, b := range bystanders {
		insertOne(t, st, b)
	}

	end := hour.Add(30 * time.Minute)
	post := postNATRecord(end)
	post.DstAddr = "198.51.100.42"
	insertOne(t, st, post)
	pre := preNATRecord(end)
	pre.DstAddr = "198.51.100.42"
	insertOne(t, st, pre)

	if n := countRows(t, st, "flows"); n != int64(len(bystanders))+1 {
		t.Fatalf("rows = %d, want %d (bystanders + the surviving pre-NAT view)", n, len(bystanders)+1)
	}
	assertAggregatesMatchFlows(t, st, "reverse NAT with bystanders")
}

// The same shape with the pair in one batch: the batch's own rows must not be
// counted twice when a replacement re-derives the hour.
func TestRegressionSameBatchReverseNATDoesNotDoubleCount(t *testing.T) {
	st := natStore(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-7 * time.Hour)
	end := hour.Add(20 * time.Minute)

	batch := []model.Flow{postNATRecord(end)}
	batch[0].DstAddr = "203.0.113.77"
	for i := 0; i < 20; i++ {
		f := pingRecord(end.Add(time.Duration(i)*time.Millisecond), 2, "10.10.10.194", uint16(2000+i))
		f.DstAddr = fmt.Sprintf("203.0.113.%d", 100+i)
		batch = append(batch, f)
	}
	pre := preNATRecord(end)
	pre.DstAddr = batch[0].DstAddr
	batch = append(batch, pre)

	if err := st.InsertBatch(context.Background(), batch); err != nil {
		t.Fatalf("insert: %v", err)
	}
	assertAggregatesMatchFlows(t, st, "same-batch reverse NAT")
}

// Aggregate history outlives raw rows, so re-deriving an hour from raw rows
// that retention has already pruned destroys aggregate totals permanently.
func TestRegressionReverseNATPreservesPrunedAggregates(t *testing.T) {
	st := natStoreWithRetention(t, time.Hour, 72*time.Hour)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)

	old := pingRecord(hour.Add(5*time.Minute), 2, "10.20.30.40", 1111)
	old.Bytes, old.Packets = 1000, 10
	insertOne(t, st, old)

	before := aggregateTotals(t, st)
	// Prune raw rows with a cutoff inside the hour; aggregates stay, as they do
	// under the shipped 30d raw / 365d hourly defaults.
	if _, err := st.SweepRetention(context.Background(), hour.Add(30*time.Minute).Add(time.Hour)); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n := countRows(t, st, "flows"); n != 0 {
		t.Fatalf("flows after prune = %d, want 0", n)
	}
	afterPrune := aggregateTotals(t, st)
	if fmt.Sprint(afterPrune) != fmt.Sprint(before) {
		t.Fatalf("pruning raw rows changed aggregates: %v -> %v", before, afterPrune)
	}

	end := hour.Add(40 * time.Minute)
	post := postNATRecord(end)
	post.DstAddr = "198.51.100.7"
	insertOne(t, st, post)
	pre := preNATRecord(end)
	pre.DstAddr = "198.51.100.7"
	insertOne(t, st, pre)

	// The surviving rows are: the pre-NAT view of the new pair. The pruned flow's
	// contribution must still be in the aggregates.
	var bytes int64
	if err := st.DB().QueryRow(`SELECT COALESCE(SUM(bytes),0) FROM agg_exporter`).Scan(&bytes); err != nil {
		t.Fatal(err)
	}
	if bytes < afterPrune["agg_exporter"][1] {
		t.Fatalf("replacement erased pruned aggregate history: %d -> %d bytes", afterPrune["agg_exporter"][1], bytes)
	}
}

// agg_traffic is an aggregate family like any other: a deleted row must be
// subtracted from it too.
func TestRegressionReverseNATKeepsTrafficTierConsistent(t *testing.T) {
	st := natStore(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-5 * time.Hour)
	end := hour.Add(15 * time.Minute)

	post := postNATRecord(end)
	post.DstAddr = "198.51.100.9"
	insertOne(t, st, post)
	pre := preNATRecord(end)
	pre.DstAddr = "198.51.100.9"
	insertOne(t, st, pre)

	assertAggregatesMatchFlows(t, st, "traffic tier after reverse NAT")
}

// ---------------------------------------------------------------------------
// Finding 2: a payload match alone is not evidence of a duplicate. Copies on the
// same interface, on an unconfigured interface, or from a different exporter
// are separate observations and must be preserved.
// ---------------------------------------------------------------------------

func TestRegressionSameInterfaceIdenticalRecordsArePreserved(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)

	a := pingRecord(end, 2, "10.10.10.194", 3535)
	b := a // byte-identical, same interface: two genuinely identical flows
	insertOne(t, st, a)
	insertOne(t, st, b)

	if n := countRows(t, st, "flows"); n != 2 {
		t.Fatalf("stored %d rows, want 2: identical records on the same interface are distinct observations", n)
	}
	assertAggregatesMatchFlows(t, st, "same-interface duplicates")
}

func TestRegressionUnconfiguredInterfacePairIsPreserved(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)

	a := pingRecord(end, 2, "10.10.10.194", 3535)
	b := pingRecord(end, 9, "10.10.10.194", 3535) // 9 is not a configured child of 2
	insertOne(t, st, a)
	insertOne(t, st, b)

	if n := countRows(t, st, "flows"); n != 2 {
		t.Fatalf("stored %d rows, want 2: an unconfigured interface pair is not a duplicate", n)
	}
}

func TestRegressionDifferentExporterPairIsPreserved(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)

	a := pingRecord(end, 2, "10.10.10.194", 3535)
	b := pingRecord(end, 7, "10.10.10.194", 3535)
	b.Exporter, b.ExporterIP = "192.0.2.11", "192.0.2.11"
	insertOne(t, st, a)
	insertOne(t, st, b)

	if n := countRows(t, st, "flows"); n != 2 {
		t.Fatalf("stored %d rows, want 2: records from different exporters are distinct", n)
	}
}

// ---------------------------------------------------------------------------
// Finding 3: a record's interface contribution must leave the bucket it was
// counted in when the row is rewritten, including the unknown (if_index 0)
// bucket, and every aggregate family must stay exact.
// ---------------------------------------------------------------------------

// An ifIndex-0 record is never treated as a copy: "interface not recorded" is
// not evidence that a record is the parent's copy, so both are stored. That is
// the conservative direction — it cannot delete a real traversal.
func TestRegressionZeroIndexRecordIsNeverCollapsed(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	unknown := pingRecord(end, 0, "10.10.10.194", 3535)
	parent := pingRecord(end, 2, "10.10.10.194", 3535)
	insertOne(t, st, unknown)
	insertOne(t, st, parent)

	if n := countRows(t, st, "flows"); n != 2 {
		t.Fatalf("stored %d rows, want 2: a record with no input interface cannot be proven to be a copy", n)
	}
	assertAggregatesMatchFlows(t, st, "zero-index records preserved")
}

// Promotion moves only the interface buckets, and it must move them exactly:
// the child's ingress bucket loses the row, the parent's gains it, and the
// shared egress bucket (here the unknown if_index 0, which the old code skipped)
// is untouched rather than double counted.
func TestRegressionPromotionMovesEveryInterfaceBucket(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)

	child := pingRecord(end, 7, "10.10.10.194", 3535)
	child.OutputIf = 0 // unknown egress: the bucket the old move skipped
	parent := pingRecord(end, 2, "10.10.10.194", 3535)
	parent.OutputIf = 0

	insertOne(t, st, child)
	// The child's contribution is committed; the parent arrives in a later batch
	// and rewrites the stored row in place.
	insertOne(t, st, parent)

	got := storedFlows(t, st, 5)
	if len(got) != 1 || got[0].InputIf != 2 {
		t.Fatalf("stored %d rows (first input_if %d), want one row on input_if 2", len(got), got[0].InputIf)
	}
	assertAggregatesMatchFlows(t, st, "promotion")

	var ingress7, unknownEgress int64
	if err := st.DB().QueryRow(`SELECT COALESCE(SUM(flows),0) FROM agg_interface WHERE if_index = 7`).Scan(&ingress7); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(`SELECT COALESCE(SUM(flows),0), COALESCE(SUM(bytes),0) FROM agg_interface WHERE if_index = 0 AND direction='egress'`).Scan(&unknownEgress, new(int64)); err != nil {
		t.Fatal(err)
	}
	if ingress7 != 0 {
		t.Fatalf("agg_interface still counts the replaced child interface: if_index 7 = %d flows", ingress7)
	}
	if unknownEgress != 1 {
		t.Fatalf("agg_interface egress if_index 0 = %d flows, want 1 (the surviving row's own egress)", unknownEgress)
	}
}

// The same promotion inside one batch must produce the same aggregates.
func TestRegressionPromotionInOneBatchIsExact(t *testing.T) {
	st := pcStore(t)
	end := time.Now().UTC().Truncate(time.Second)
	child := pingRecord(end, 7, "10.10.10.194", 3535)
	parent := pingRecord(end, 2, "10.10.10.194", 3535)
	if err := st.InsertBatch(context.Background(), []model.Flow{child, parent}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if n := countRows(t, st, "flows"); n != 1 {
		t.Fatalf("stored %d rows, want 1", n)
	}
	assertAggregatesMatchFlows(t, st, "promotion in one batch")
}

// ---------------------------------------------------------------------------
// Finding 4: the ids returned for a batch must identify the record they belong
// to, so reverse DNS is not attached to a different flow.
// ---------------------------------------------------------------------------

func TestRegressionBatchIDsIdentifyTheirRecord(t *testing.T) {
	st, _ := testStore(t, 24*time.Hour, 72*time.Hour)
	end := time.Now().UTC().Truncate(time.Second)

	a := pingRecord(end, 2, "10.10.10.1", 1000)
	a.DstAddr = "198.51.100.1"
	dup := a
	c := pingRecord(end, 2, "10.10.10.3", 3000)
	c.DstAddr = "198.51.100.3"

	ids, err := st.InsertBatchIDs(context.Background(), []model.Flow{a, dup, c})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	for i, id := range ids {
		if id == 0 {
			continue
		}
		var src string
		if err := st.DB().QueryRow(`SELECT src_addr FROM flows WHERE id = ?`, id).Scan(&src); err != nil {
			t.Fatalf("id %d from position %d is not a stored row: %v", id, i, err)
		}
		if want := []model.Flow{a, dup, c}[i].SrcAddr; src != want {
			t.Fatalf("position %d (src %s) returned id %d which holds src %s", i, want, id, src)
		}
	}
}

// natStoreWithRetention opens a store with a raw/hourly retention pair and NAT
// enabled. (Named distinctly from the reviewer fixtures that may be dropped in.)
func natStoreWithRetention(t *testing.T, raw, hourly time.Duration) *Store {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.Database.Retention.Raw = config.Duration(raw)
	cfg.Database.Retention.Hourly = config.Duration(hourly)
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

// pcStore opens a store with one configured parent/child link: exporter
// 192.0.2.10 reports input node 7 as the child of node 2.
func pcStore(t *testing.T) *Store {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.Database.Retention.Raw = config.Duration(24 * time.Hour)
	cfg.Database.Retention.Hourly = config.Duration(72 * time.Hour)
	cfg.Database.Dedup.ParentChild = true
	cfg.Database.Dedup.ParentChildLinks = []config.ParentChildLink{
		{Exporter: "192.0.2.10", Parent: 2, Child: 7},
	}
	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// combinedStore opens a store with both mechanisms configured for the measured
// topology: parent/child link 2->7 and NAT pre/post interfaces 2 and 7.
func combinedStore(t *testing.T) *Store {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.Database.Retention.Raw = config.Duration(24 * time.Hour)
	cfg.Database.Retention.Hourly = config.Duration(72 * time.Hour)
	cfg.Database.Dedup.ParentChild = true
	cfg.Database.Dedup.ParentChildLinks = []config.ParentChildLink{
		{Exporter: "192.0.2.10", Parent: 2, Child: 7},
	}
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
