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

// End to end: one ICMP echo exported twice by OPNsense — once on the parent
// interface (input_if 2) and once on the VLAN child (input_if 7) — arrives as
// real NetFlow v5 datagrams on the UDP socket, is decoded, batched, and stored.
// Exactly one row must survive, and it must be the parent's.
func TestParentChildDuplicateCollapsesThroughTheCollector(t *testing.T) {
	cfg := config.Default()
	cfg.Listen.UDP = "127.0.0.1:0"
	cfg.Database.Path = filepath.Join(t.TempDir(), "test.db")
	cfg.DNS.Enabled = false
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

	// Same five-tuple, same counters and export window; only the interface
	// differs. First/last are exporter uptime milliseconds, so they define the
	// flow's start/end and must match for the copies to be identical.
	src, dst := testutil.DefaultV4Src(), testutil.DefaultV4Dst()
	parent := testutil.V5Field{
		SrcAddr: src, DstAddr: dst, Input: 2, Output: 1,
		Packets: 1, Bytes: 98, Proto: 1, First: 1000, Last: 2000,
	}
	child := parent
	child.Input = 7
	for _, records := range [][]testutil.V5Field{{parent, child}} {
		packet := testutil.V5Packet(time.Now(), 2000, 0, records...)
		if _, err := conn.Write(packet); err != nil {
			t.Fatal(err)
		}
	}

	var count int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := st.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM flows`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.Stop(); err != nil { // Stop drains the final batch.
		t.Fatal(err)
	}

	if err := st.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM flows`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		rows, _ := st.DB().QueryContext(context.Background(), `SELECT input_if, output_if, src_addr FROM flows`)
		var detail []string
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var in, out uint32
				var s string
				_ = rows.Scan(&in, &out, &s)
				detail = append(detail, s)
			}
		}
		t.Fatalf("stored %d flows through the collector, want 1 (%v)", count, detail)
	}
	var in, out uint32
	var addr string
	if err := st.DB().QueryRowContext(context.Background(), `SELECT input_if, output_if, src_addr FROM flows`).Scan(&in, &out, &addr); err != nil {
		t.Fatal(err)
	}
	if in != 2 || out != 1 {
		t.Fatalf("survivor interfaces = in=%d out=%d, want in=2 out=1", in, out)
	}
	// Interfaces are what distinguishes the copies; the payload must be intact.
	var packets, bytes int64
	if err := st.DB().QueryRowContext(context.Background(), `SELECT packets, bytes FROM flows`).Scan(&packets, &bytes); err != nil {
		t.Fatal(err)
	}
	if packets != 1 || bytes != 98 {
		t.Fatalf("survivor counters = %d packets / %d bytes, want 1/98", packets, bytes)
	}
	// One traversal, counted once.
	var aggFlows int64
	if err := st.DB().QueryRowContext(context.Background(), `SELECT COALESCE(SUM(flows),0) FROM agg_interface WHERE direction='ingress'`).Scan(&aggFlows); err != nil {
		t.Fatal(err)
	}
	if aggFlows != 1 {
		t.Fatalf("agg_interface ingress counted %d flows, want 1", aggFlows)
	}
}
