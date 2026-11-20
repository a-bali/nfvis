package decoder

import (
	"net/netip"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/testutil"
)

func testConfig() config.Config {
	c := config.Default()
	c.Exporters = []config.Exporter{{
		Address: "192.0.2.1", Label: "core",
		Interfaces: map[uint32]string{1: "wan0", 2: "lan0"},
	}}
	c.VLANRules = []config.VLANRule{
		{CIDR: "10.0.0.0/8", VLAN: 10, Direction: "src"},
		{CIDR: "10.1.0.0/16", VLAN: 11, Direction: "src"},
		{CIDR: "2001:db8::/32", VLAN: 20, Direction: "dst"},
	}
	return c
}

func exporter(t *testing.T) netip.Addr {
	t.Helper()
	a, err := netip.ParseAddr("192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestV5DecodeNormalizesFieldsAndVLANs(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	d := New(testConfig())
	packet := testutil.V5Packet(now, 1000, 100, testutil.V5Field{
		SrcAddr: testutil.DefaultV4Src(), DstAddr: testutil.DefaultV4Dst(),
		Input: 1, Output: 2, Packets: 5, Bytes: 500,
		First: 900, Last: 950, SrcPort: 1234, DstPort: 443, Flags: 0x18, Proto: 6,
	})
	flows, stats, err := d.Decode(exporter(t), packet, now.Add(10*time.Millisecond))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stats.PacketsDecoded != 1 || stats.FlowsDecoded != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	f := flows[0]
	if f.SrcAddr != "10.1.2.3" || f.DstAddr != "203.0.113.5" {
		t.Fatalf("addresses: %+v", f)
	}
	if f.SrcVLAN != 11 || f.SrcVLANSource != "rule" {
		t.Fatalf("source VLAN should use longest prefix: %+v", f)
	}
	if f.DstVLAN != -1 || f.DstVLANSource != "unknown" {
		t.Fatalf("destination VLAN should be unknown: %+v", f)
	}
	if f.InputLabel != "wan0" || f.OutputLabel != "lan0" {
		t.Fatalf("interface labels missing: %+v", f)
	}
	if !f.Sampled || f.SamplingInterval != 100 || f.SamplingSource != "header" {
		t.Fatalf("sampling not preserved: %+v", f)
	}
	if f.TimeSource != "exported" || f.EndMS != now.UnixMilli()-50 || f.StartMS != now.UnixMilli()-100 {
		t.Fatalf("timestamps: nowMS=%d start=%d end=%d source=%s", now.UnixMilli(), f.StartMS, f.EndMS, f.TimeSource)
	}
}

func TestV5MalformedAndOversized(t *testing.T) {
	d := New(testConfig())
	_, stats, err := d.Decode(exporter(t), []byte{0, 5, 1}, time.Now())
	if stats.PacketsMalformed != 1 {
		t.Fatalf("short packet: err=%v stats=%+v", err, stats)
	}
	big := make([]byte, 70000)
	big[0], big[1] = 0, 5
	_, stats, err = d.Decode(exporter(t), big, time.Now())
	if err != nil || stats.PacketsOversized != 1 {
		t.Fatalf("oversized packet: err=%v stats=%+v", err, stats)
	}
	_, stats, _ = d.Decode(exporter(t), []byte{0, 11, 0}, time.Now())
	if stats.PacketsUnsupported != 1 {
		t.Fatalf("unsupported version not counted: %+v", stats)
	}
}

func standardV9Fields() []testutil.Field {
	return []testutil.Field{
		{Type: 8, Value: testutil.IPv4(10, 1, 2, 3)},
		{Type: 12, Value: testutil.IPv4(203, 0, 113, 5)},
		{Type: 1, Value: testutil.U32(900)},
		{Type: 2, Value: testutil.U32(9)},
		{Type: 4, Value: testutil.U8(6)},
		{Type: 10, Value: testutil.U16(1)},
		{Type: 14, Value: testutil.U16(2)},
		{Type: 7, Value: testutil.U16(1234)},
		{Type: 11, Value: testutil.U16(443)},
		{Type: 6, Value: testutil.U8(0x18)},
		{Type: 21, Value: testutil.U32(950)},
		{Type: 22, Value: testutil.U32(900)},
		{Type: 58, Value: testutil.U16(77)},
		{Type: 59, Value: testutil.U16(0)},
	}
}

func v9TemplateAndData(now time.Time, templateID uint16, fields []testutil.Field) []byte {
	tmpl := testutil.FlowSet(0, testutil.Template(templateID, fields...))
	data := testutil.FlowSet(templateID, testutil.DataRecord(fields...))
	return testutil.V9Packet(now, 1000, 42, 1, tmpl, data)
}

func TestV9TemplateIsolationAndExpiry(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	d := New(testConfig()).(*decoder)
	dd := d.v9
	fakeNow := now
	dd.store.now = func() time.Time { return fakeNow }
	fields := standardV9Fields()

	// Exporter A teaches template 256; exporter B uses the same ID without a
	// template and must not be able to decode it.
	flows, stats, err := d.Decode(exporter(t), v9TemplateAndData(now, 256, fields), now)
	if err != nil || len(flows) != 1 {
		t.Fatalf("first exporter: err=%v flows=%d", err, len(flows))
	}
	other, _ := netip.ParseAddr("192.0.2.2")
	dataOnly := testutil.V9Packet(now, 1000, 42, 1, testutil.FlowSet(256, testutil.DataRecord(fields...)))
	flows, stats, err = d.Decode(other, dataOnly, now)
	if err != nil {
		t.Fatalf("unexpected fatal error: %v", err)
	}
	if len(flows) != 0 || stats.UnknownTemplateSets == 0 {
		t.Fatalf("template isolation failed: flows=%d stats=%+v", len(flows), stats)
	}

	// Different source IDs are isolated too.
	sameIPOtherSource := testutil.V9Packet(now, 1000, 43, 1, testutil.FlowSet(256, testutil.DataRecord(fields...)))
	flows, _, _ = d.Decode(exporter(t), sameIPOtherSource, now)
	if len(flows) != 0 {
		t.Fatalf("source ID isolation failed: %d flows", len(flows))
	}

	fakeNow = now.Add(testConfig().Decoder.TemplateTTL.Duration() + time.Second)
	if removed := d.ExpireTemplates(fakeNow); removed != 1 {
		t.Fatalf("expected one expired template, got %d", removed)
	}
	flows, stats, _ = d.Decode(exporter(t), dataOnly, fakeNow)
	if len(flows) != 0 || stats.UnknownTemplateSets == 0 {
		t.Fatalf("expired template was used: flows=%d stats=%+v", len(flows), stats)
	}
}

func TestV9TemplateRefreshAndExportedVLAN(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	d := New(testConfig())
	fields := standardV9Fields()
	_, _, err := d.Decode(exporter(t), v9TemplateAndData(now, 257, fields), now)
	if err != nil {
		t.Fatal(err)
	}
	changed := append([]testutil.Field{}, fields...)
	changed[12] = testutil.Field{Type: 58, Value: testutil.U16(88)}
	flows, _, err := d.Decode(exporter(t), v9TemplateAndData(now, 257, changed), now)
	if err != nil || len(flows) != 1 {
		t.Fatalf("refresh decode: err=%v flows=%d", err, len(flows))
	}
	if flows[0].SrcVLAN != 88 || flows[0].SrcVLANSource != "exported" {
		t.Fatalf("exported VLAN must beat rules: %+v", flows[0])
	}
}

func TestV9MissingFieldsAndMalformed(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	d := New(testConfig())
	minimal := []testutil.Field{{Type: 1, Value: testutil.U32(10)}}
	flows, stats, err := d.Decode(exporter(t), v9TemplateAndData(now, 258, minimal), now)
	if err != nil || len(flows) != 1 {
		t.Fatalf("minimal record: err=%v flows=%d", err, len(flows))
	}
	if flows[0].TimeSource != "received" || flows[0].SrcAddr != "" || stats.TimestampFallbacks == 0 {
		t.Fatalf("missing fields should stay unknown: %+v stats=%+v", flows[0], stats)
	}
	_, stats, err = d.Decode(exporter(t), []byte{0, 9, 1, 2}, now)
	if err == nil || stats.PacketsMalformed == 0 {
		t.Fatalf("truncated v9 should be malformed: err=%v stats=%+v", err, stats)
	}
}

func TestV9SamplingOptions(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	d := New(testConfig())
	fields := standardV9Fields()
	optsTmpl := testutil.FlowSet(1, testutil.OptionsTemplate(300, 8, 4,
		testutil.Field{Type: 34, Value: testutil.U32(50)},
		testutil.Field{Type: 35, Value: testutil.U32(1)},
	))
	optsData := testutil.FlowSet(300, testutil.OptionsData(testutil.IPv4(192, 0, 2, 1),
		testutil.Field{Type: 34, Value: testutil.U32(50)},
		testutil.Field{Type: 35, Value: testutil.U32(1)},
	))
	first := testutil.V9Packet(now, 1000, 44, 1, optsTmpl, optsData)
	if _, stats, err := d.Decode(exporter(t), first, now); err != nil || stats.SamplingOptions == 0 {
		t.Fatalf("options: err=%v stats=%+v", err, stats)
	}
	second := testutil.V9Packet(now, 1000, 44, 2, testutil.FlowSet(256, testutil.DataRecord(fields...)))
	_, _, err := d.Decode(exporter(t), second, now)
	if err != nil {
		t.Fatalf("data packet needs no template after options decode: %v", err)
	}
	// The data set above intentionally has no template; test sampling on a
	// packet that carries its own data template.
	d2 := New(testConfig())
	all := testutil.V9Packet(now, 1000, 44, 3, optsTmpl, optsData,
		testutil.FlowSet(0, testutil.Template(256, fields...)), testutil.FlowSet(256, testutil.DataRecord(fields...)))
	flows, _, err := d2.Decode(exporter(t), all, now)
	if err != nil || len(flows) != 1 {
		t.Fatalf("sampled flow: err=%v flows=%d", err, len(flows))
	}
	if !flows[0].Sampled || flows[0].SamplingInterval != 50 || flows[0].SamplingSource != "options" {
		t.Fatalf("sampling options not applied: %+v", flows[0])
	}
}

func TestV9SequenceRegressionDoesNotClearTemplates(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	d := New(testConfig())
	fields := standardV9Fields()
	first := testutil.V9Packet(now, 1000, 42, 200000, testutil.FlowSet(0, testutil.Template(256, fields...)))
	if _, _, err := d.Decode(exporter(t), first, now); err != nil {
		t.Fatal(err)
	}
	second := testutil.V9Packet(now.Add(time.Second), 2000, 42, 1, testutil.FlowSet(256, testutil.DataRecord(fields...)))
	flows, stats, err := d.Decode(exporter(t), second, now.Add(time.Second))
	if err != nil || len(flows) != 1 || stats.ExporterReboots != 0 || stats.UnknownTemplateSets != 0 {
		t.Fatalf("sequence regression: flows=%d stats=%+v err=%v", len(flows), stats, err)
	}
}

func TestV9ExporterRebootClearsTemplates(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	d := New(testConfig())
	fields := standardV9Fields()
	if _, _, err := d.Decode(exporter(t), v9TemplateAndData(now, 256, fields), now); err != nil {
		t.Fatal(err)
	}
	// Uptime resets while wall-clock advances: header anchor moves backwards.
	later := now.Add(10 * time.Minute)
	reboot := testutil.V9Packet(later, 0, 42, 1, testutil.FlowSet(256, testutil.DataRecord(fields...)))
	flows, stats, err := d.Decode(exporter(t), reboot, later)
	if err != nil {
		t.Fatal(err)
	}
	if len(flows) != 0 || stats.ExporterReboots != 1 || stats.UnknownTemplateSets == 0 {
		t.Fatalf("reboot should clear templates: flows=%d stats=%+v", len(flows), stats)
	}
}

func TestClassifierIPv6LongestPrefix(t *testing.T) {
	c := NewClassifier(testConfig())
	sv, ss, dv, ds := c.Classify("10.1.2.3", "2001:db8:1::5", 0, 0)
	if sv != 11 || ss != "rule" || dv != 20 || ds != "rule" {
		t.Fatalf("LPM failed: %d/%s %d/%s", sv, ss, dv, ds)
	}
	sv, ss, _, ds = c.Classify("10.1.2.3", "2001:db9::1", 5, 0)
	if sv != 5 || ss != "exported" || ds != "unknown" {
		t.Fatalf("precedence failed: %d/%s %s", sv, ss, ds)
	}
}
