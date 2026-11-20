package decoder

import (
	"net/netip"
	"testing"
	"time"

	"github.com/balia/nfvis/internal/testutil"
)

var benchExporter = netip.MustParseAddr("192.0.2.1")

func BenchmarkDecodeV5(b *testing.B) {
	cfg := testConfig()
	d := New(cfg)
	now := time.Now()
	records := make([]testutil.V5Field, 30)
	for i := range records {
		records[i] = testutil.V5Field{
			SrcAddr: testutil.DefaultV4Src(), DstAddr: testutil.DefaultV4Dst(),
			Packets: 3, Bytes: 300, Proto: 6,
		}
	}
	packet := testutil.V5Packet(now, 1000, 100, records...)
	b.SetBytes(int64(len(packet)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := d.Decode(benchExporter, packet, now); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeV9(b *testing.B) {
	cfg := testConfig()
	cfg.Decoder.MaxFlowsPerPacket = 1000
	d := New(cfg)
	now := time.Now()
	fields := standardV9Fields()
	tmpl := testutil.FlowSet(0, testutil.Template(256, fields...))
	records := make([][]byte, 30)
	for i := range records {
		records[i] = testutil.DataRecord(fields...)
	}
	packet := testutil.V9Packet(now, 1000, 42, 1, tmpl, testutil.FlowSet(256, records...))
	b.SetBytes(int64(len(packet)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := d.Decode(benchExporter, packet, now); err != nil {
			b.Fatal(err)
		}
	}
}
