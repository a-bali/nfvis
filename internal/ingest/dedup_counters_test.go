package ingest

import (
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

// Duplicate suppression must be visible in the counters: records that were
// collapsed are not "stored", they are counted as duplicates, so the dashboard
// can tell a quiet collector from a deduplicating one.
func TestDuplicateCountersThroughTheCollector(t *testing.T) {
	cfg := dedupConfig(t)
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

	src, dst := testutil.DefaultV4Src(), testutil.DefaultV4Dst()
	parent := testutil.V5Field{SrcAddr: src, DstAddr: dst, Input: 2, Output: 1,
		Packets: 1, Bytes: 98, Proto: 1, First: 1000, Last: 2000}
	child := parent
	child.Input = 7
	if _, err := conn.Write(testutil.V5Packet(time.Now(), 2000, 0, parent, child)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for c.Counters().FlowsDecoded.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}
	if got := c.Counters().FlowsDecoded.Load(); got != 2 {
		t.Fatalf("flows_decoded = %d, want 2", got)
	}
	if got := c.Counters().FlowsStored.Load(); got != 1 {
		t.Fatalf("flows_stored = %d, want 1 (the collapsed copy is not stored)", got)
	}
	if got := c.Counters().FlowsDuplicates.Load(); got != 1 {
		t.Fatalf("flows_duplicates = %d, want 1", got)
	}
	if got := c.Counters().FlowsDropped.Load(); got != 0 {
		t.Fatalf("flows_dropped = %d, want 0 (a duplicate is not a drop)", got)
	}
}

// With nothing configured, both copies are stored and neither counter moves.
func TestDuplicateCountersStayZeroWithoutTopology(t *testing.T) {
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

	src, dst := testutil.DefaultV4Src(), testutil.DefaultV4Dst()
	parent := testutil.V5Field{SrcAddr: src, DstAddr: dst, Input: 2, Output: 1,
		Packets: 1, Bytes: 98, Proto: 1, First: 1000, Last: 2000}
	child := parent
	child.Input = 7
	if _, err := conn.Write(testutil.V5Packet(time.Now(), 2000, 0, parent, child)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for c.Counters().FlowsDecoded.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}
	if got := c.Counters().FlowsStored.Load(); got != 2 {
		t.Fatalf("flows_stored = %d, want 2", got)
	}
	if got := c.Counters().FlowsDuplicates.Load(); got != 0 {
		t.Fatalf("flows_duplicates = %d, want 0", got)
	}
}
