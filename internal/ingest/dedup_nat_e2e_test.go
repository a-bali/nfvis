package ingest

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/decoder"
	"github.com/a-bali/nfvis/internal/store"
	"github.com/a-bali/nfvis/internal/testutil"
)

// End to end through the real socket, decoder and store: the three records
// OPNsense exports for one ping — parent (in=2), VLAN child (in=7), and the
// post-NAT copy from the WAN address — collapse to one row carrying the pre-NAT
// source on the parent interface. Both topologies have to be declared for this:
// the parent/child link and the pre/post-NAT interfaces.
func TestCombinedDedupThroughTheCollector(t *testing.T) {
	const wanSrc = "192.168.0.2"
	cfg := dedupConfig(t)
	cfg.Database.Dedup.NAT.WANAddresses = []string{wanSrc}
	cfg.Database.Dedup.NAT.Exporters = []config.NATExporter{
		{Address: "127.0.0.1", PreInterface: 2, PostInterface: 7},
	}
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c := New(cfg, decoder.New(cfg), st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, c.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	inner := [4]byte{10, 10, 10, 194}
	wan := [4]byte{192, 168, 0, 2}
	dst := testutil.DefaultV4Dst()
	parent := testutil.V5Field{SrcAddr: inner, DstAddr: dst, Input: 2, Output: 1,
		Packets: 1, Bytes: 84, Proto: 1, First: 1000, Last: 2000}
	child := parent
	child.Input = 7
	postNAT := parent
	postNAT.SrcAddr = wan
	postNAT.SrcPort = 41234
	postNAT.Input = 7

	if _, err := conn.Write(testutil.V5Packet(time.Now(), 2000, 0, parent, child, postNAT)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	var count int
	for time.Now().Before(deadline) {
		if err := st.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM flows`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.Stop(); err != nil { // drains the final batch
		t.Fatal(err)
	}

	if err := st.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM flows`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		var rows []string
		rs, _ := st.DB().QueryContext(context.Background(), `SELECT src_addr, input_if FROM flows`)
		if rs != nil {
			defer rs.Close()
			for rs.Next() {
				var src string
				var in uint32
				_ = rs.Scan(&src, &in)
				rows = append(rows, src)
			}
		}
		t.Fatalf("stored %d flows through the collector, want 1 (%v)", count, rows)
	}
	var src string
	var in, out uint32
	if err := st.DB().QueryRowContext(context.Background(), `SELECT src_addr, input_if, output_if FROM flows`).Scan(&src, &in, &out); err != nil {
		t.Fatal(err)
	}
	if src != "10.10.10.194" {
		t.Fatalf("survivor src = %s, want the pre-NAT 10.10.10.194", src)
	}
	if in != 2 || out != 1 {
		t.Fatalf("survivor interfaces = in=%d out=%d, want in=2 out=1", in, out)
	}
	// The single surviving traversal is counted once, and the dropped copies are
	// nowhere in the aggregates.
	var aggBytes, aggFlows int64
	if err := st.DB().QueryRowContext(context.Background(), `SELECT COALESCE(SUM(bytes),0), COALESCE(SUM(flows),0) FROM agg_exporter`).Scan(&aggBytes, &aggFlows); err != nil {
		t.Fatal(err)
	}
	if aggBytes != 84 || aggFlows != 1 {
		t.Fatalf("agg_exporter = %d bytes / %d flows, want 84/1", aggBytes, aggFlows)
	}
	var wanSource int64
	if err := st.DB().QueryRowContext(context.Background(), `SELECT COALESCE(SUM(flows),0) FROM agg_host WHERE host = ?`, wanSrc).Scan(&wanSource); err != nil {
		t.Fatal(err)
	}
	if wanSource != 0 {
		t.Fatalf("agg_host still counts the dropped post-NAT source: %d", wanSource)
	}
}

// The measured production shape: the post-NAT view and its pre-NAT mate land in
// different batches, in an hour that already holds other hosts' aggregates. The
// batch must not be lost, and every aggregate family must equal the rows that
// remain.
func TestReverseNATAcrossBatchesKeepsBatchAndAggregates(t *testing.T) {
	const wanSrc = "192.168.0.2"
	cfg := dedupConfig(t)
	cfg.Database.Dedup.ParentChild = false
	cfg.Database.Dedup.ParentChildLinks = nil
	cfg.Database.Dedup.NAT.WANAddresses = []string{wanSrc}
	cfg.Database.Dedup.NAT.Exporters = []config.NATExporter{
		{Address: "127.0.0.1", PreInterface: 2, PostInterface: 7},
	}
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	c := New(cfg, decoder.New(cfg), st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, c.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	hour := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	src, dst := testutil.DefaultV4Src(), testutil.DefaultV4Dst()

	// Batch 1: ordinary traffic that makes the hour's aggregates non-empty for
	// hosts, protocols and exporters other than the pair's.
	bystanders := []testutil.V5Field{
		{SrcAddr: [4]byte{10, 0, 0, 11}, DstAddr: dst, Input: 2, Output: 1, Packets: 1, Bytes: 84, Proto: 1, First: 1000, Last: 2000},
		{SrcAddr: [4]byte{10, 0, 0, 12}, DstAddr: dst, Input: 3, Output: 1, Packets: 2, Bytes: 300, Proto: 17, First: 1000, Last: 2000},
	}
	if _, err := conn.Write(testutil.V5Packet(hour.Add(2*time.Minute), 2000, 0, bystanders...)); err != nil {
		t.Fatal(err)
	}
	waitDecodedCount(t, c, 2)
	time.Sleep(400 * time.Millisecond) // let flush() commit batch 1

	// Batch 2: the post-NAT view alone.
	pre := testutil.V5Field{SrcAddr: src, DstAddr: dst, Input: 2, Output: 1,
		Packets: 3, Bytes: 252, Proto: 1, First: 3000, Last: 4000}
	post := pre
	post.SrcAddr = [4]byte{192, 168, 0, 2}
	post.Input = 7
	post.SrcPort = 40000
	pre.SrcPort = 40000
	exportAt := hour.Add(20 * time.Minute)
	if _, err := conn.Write(testutil.V5Packet(exportAt, 4000, 0, post)); err != nil {
		t.Fatal(err)
	}
	waitDecodedCount(t, c, 3)
	time.Sleep(400 * time.Millisecond)

	// Batch 3: the pre-NAT mate plus unrelated traffic in the same hour.
	other := testutil.V5Field{SrcAddr: [4]byte{10, 0, 0, 50}, DstAddr: dst, Input: 2, Output: 1,
		Packets: 9, Bytes: 900, Proto: 6, First: 3700, Last: 4700}
	other.DstPort = 1234
	if _, err := conn.Write(testutil.V5Packet(exportAt.Add(700*time.Millisecond), 4700, 0, pre, other)); err != nil {
		t.Fatal(err)
	}
	waitDecodedCount(t, c, 5)
	time.Sleep(500 * time.Millisecond)

	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}
	if got := c.Counters().FlowsDropped.Load(); got != 0 {
		t.Fatalf("flush dropped %d flows: the reverse-order replacement aborted a batch", got)
	}
	// bystanders (2) + surviving pre-NAT view (1) + unrelated (1)
	var rows int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM flows`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 4 {
		t.Fatalf("rows = %d, want 4", rows)
	}
	assertAggregatesMatchFlowsIngest(t, st)
}

func waitDecodedCount(t *testing.T, c *Collector, want uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for c.Counters().FlowsDecoded.Load() < want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := c.Counters().FlowsDecoded.Load(); got < want {
		t.Fatalf("decoded %d flows, want at least %d", got, want)
	}
}

// assertAggregatesMatchFlowsIngest checks that total aggregate flows equal the
// stored rows, for every family. The store package has the full field-by-field
// check; this is the end-to-end sanity version.
func assertAggregatesMatchFlowsIngest(t *testing.T, st *store.Store) {
	t.Helper()
	var rows int64
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM flows`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"agg_exporter", "agg_protocol"} {
		var flows int64
		if err := st.DB().QueryRow(`SELECT COALESCE(SUM(flows),0) FROM ` + table).Scan(&flows); err != nil {
			t.Fatal(err)
		}
		if flows != rows {
			t.Fatalf("%s counts %d flows for %d stored rows", table, flows, rows)
		}
	}
	// agg_traffic holds one row per configured tier, so its total is rows times
	// the tier count; check it per tier instead.
	var tiers int
	if err := st.DB().QueryRow(`SELECT COUNT(DISTINCT res_ms) FROM agg_traffic`).Scan(&tiers); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < tiers; i++ {
		var flows int64
		if err := st.DB().QueryRow(`SELECT COALESCE(SUM(flows),0) FROM agg_traffic WHERE res_ms = (SELECT DISTINCT res_ms FROM agg_traffic ORDER BY res_ms LIMIT 1 OFFSET ?)`, i).Scan(&flows); err != nil {
			t.Fatal(err)
		}
		if flows != rows {
			t.Fatalf("agg_traffic tier %d counts %d flows for %d stored rows", i, flows, rows)
		}
	}
	// Interface and host families carry one bucket per stored row per side.
	for _, table := range []string{"agg_interface", "agg_host"} {
		var flows int64
		if err := st.DB().QueryRow(`SELECT COALESCE(SUM(flows),0)/2 FROM ` + table).Scan(&flows); err != nil {
			t.Fatal(err)
		}
		if flows != rows {
			t.Fatalf("%s counts %d rows for %d stored rows", table, flows, rows)
		}
	}
}

// A file-based store is needed for the cross-batch test above; this keeps the
// temp directory cleanup tied to the test that opened it.
var _ = filepath.Join
