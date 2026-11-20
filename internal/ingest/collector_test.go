package ingest

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/decoder"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
	"github.com/a-bali/nfvis/internal/testutil"
)

func TestStopDrainsFinalBatch(t *testing.T) {
	cfg := config.Default()
	cfg.Listen.UDP = "127.0.0.1:0"
	cfg.Database.Path = filepath.Join(t.TempDir(), "test.db")
	cfg.Database.Flush = config.Duration(time.Hour)
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
	packet := testutil.V5Packet(time.Now(), 1000, 0, testutil.V5Field{First: 900, Last: 1000, Bytes: 100, Packets: 1})
	if _, err := conn.Write(packet); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for c.Counters().PacketsReceived.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.Counters().PacketsReceived.Load() == 0 {
		t.Fatal("packet not received")
	}
	if err := c.Stop(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := st.DB().QueryRowContext(context.Background(), "SELECT COUNT(*) FROM flows").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("stored %d flows; want 1", count)
	}
}

type orderedDecoder struct {
	first   chan struct{}
	release chan struct{}
	second  chan struct{}
}

func (d *orderedDecoder) Decode(_ netip.Addr, payload []byte, _ time.Time) ([]model.Flow, decoder.Stats, error) {
	if payload[0] == 1 {
		close(d.first)
		<-d.release
	} else {
		close(d.second)
	}
	return nil, decoder.Stats{}, nil
}
func (*orderedDecoder) ExpireTemplates(time.Time) int { return 0 }
func (*orderedDecoder) TemplateCount() int            { return 0 }

func TestSameExporterPacketsStayOrdered(t *testing.T) {
	cfg := config.Default()
	cfg.Listen.UDP = "127.0.0.1:0"
	cfg.Decoder.Workers = 2
	cfg.Database.Path = filepath.Join(t.TempDir(), "test.db")
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	d := &orderedDecoder{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	c := New(cfg, d, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, c.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	<-d.first
	if _, err = conn.Write([]byte{2}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for c.Counters().PacketsReceived.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.Counters().PacketsReceived.Load() < 2 {
		close(d.release)
		c.Stop()
		t.Fatal("second packet not received")
	}
	select {
	case <-d.second:
		close(d.release)
		c.Stop()
		t.Fatal("second packet decoded before first completed")
	default:
	}
	close(d.release)
	c.Stop()
	select {
	case <-d.second:
	default:
		t.Fatal("second packet not decoded")
	}
}

func TestRecentMetrics(t *testing.T) {
	var c Counters
	now := time.Now()
	c.recordReceived(now.Add(-2*time.Minute), 4)
	c.recordReceived(now.Add(-30*time.Minute), 6)
	c.recordReceived(now.Add(-2*time.Hour), 8)
	c.recordReceived(now.Add(-25*time.Hour), 20)
	c.recordReceived(now.Add(-3*time.Minute), 0)
	c.recordProcessing(4*time.Millisecond, 2)
	c.recordProcessing(12*time.Millisecond, 3)
	s := c.Snapshot()
	if s.Flows5Min != 4 || s.Flows1Hour != 10 || s.Flows24Hour != 18 {
		t.Fatalf("unexpected recent counts: %+v", s)
	}
	if s.LastFlowReceivedMS != now.Add(-2*time.Minute).UnixMilli() {
		t.Fatalf("last receipt = %d", s.LastFlowReceivedMS)
	}
	if s.ProcessingAvgMS == nil || *s.ProcessingAvgMS != 3 || s.ProcessingMedianMS == nil || s.ProcessingP90MS == nil {
		t.Fatalf("unexpected processing metrics: %+v", s)
	}
}
