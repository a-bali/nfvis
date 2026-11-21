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
// source on the parent interface.
func TestCombinedDedupThroughTheCollector(t *testing.T) {
	const wanSrc = "192.168.0.2"
	cfg := config.Default()
	cfg.Listen.UDP = "127.0.0.1:0"
	cfg.Database.Path = filepath.Join(t.TempDir(), "test.db")
	cfg.DNS.Enabled = false
	cfg.Database.Dedup.NAT.WANAddresses = []string{wanSrc}
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
		Packets: 1, Bytes: 98, Proto: 1, First: 1000, Last: 2000}
	child := parent
	child.Input = 7
	postNAT := parent
	postNAT.SrcAddr = wan
	postNAT.SrcPort = 41234

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
	if aggBytes != 98 || aggFlows != 1 {
		t.Fatalf("agg_exporter = %d bytes / %d flows, want 98/1", aggBytes, aggFlows)
	}
	var wanSource int64
	if err := st.DB().QueryRowContext(context.Background(), `SELECT COALESCE(SUM(flows),0) FROM agg_host WHERE host = ?`, wanSrc).Scan(&wanSource); err != nil {
		t.Fatal(err)
	}
	if wanSource != 0 {
		t.Fatalf("agg_host still counts the dropped post-NAT source: %d", wanSource)
	}
}
