package store

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/a-bali/nfvis/internal/model"
)

// assertAggregatesMatchFlows reads every stored raw flow, recomputes each
// aggregate family from it, and compares that with what the aggregate tables
// hold. It is the black-box statement "the aggregates describe the rows that
// exist", which every dedup path must preserve.
//
// It is only valid while raw retention has not pruned any row of the range under
// test: aggregates outlive raw rows by design, so a pruned hour legitimately
// disagrees.
func assertAggregatesMatchFlows(t *testing.T, st *Store, context string) {
	t.Helper()
	rows, err := st.DB().Query(`SELECT exporter, end_ms, received_ms, input_if, input_label,
		output_if, output_label, src_addr, dst_addr, src_vlan, dst_vlan, protocol,
		bytes, packets, sampled FROM flows`)
	if err != nil {
		t.Fatalf("%s: read flows: %v", context, err)
	}
	defer rows.Close()
	var flows []model.Flow
	for rows.Next() {
		var f model.Flow
		if err := rows.Scan(&f.Exporter, &f.EndMS, &f.ReceivedAtMS, &f.InputIf, &f.InputLabel,
			&f.OutputIf, &f.OutputLabel, &f.SrcAddr, &f.DstAddr, &f.SrcVLAN, &f.DstVLAN,
			&f.Protocol, &f.Bytes, &f.Packets, &f.Sampled); err != nil {
			t.Fatalf("%s: scan flow: %v", context, err)
		}
		flows = append(flows, f)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: flow rows: %v", context, err)
	}

	want := newBatchAggregates(len(flows), st.tiers)
	for _, f := range flows {
		want.add(f)
	}
	got := readAggregates(t, st)

	diffAggregates(t, context, "agg_exporter", want.exporter, got.exporter)
	diffAggregates(t, context, "agg_interface", want.iface, got.iface)
	diffAggregates(t, context, "agg_host", want.host, got.host)
	diffAggregates(t, context, "agg_vlan", want.vlan, got.vlan)
	diffAggregates(t, context, "agg_protocol", want.protocol, got.protocol)
	diffAggregates(t, context, "agg_traffic", want.traffic, got.traffic)
}

func diffAggregates[K comparable](t *testing.T, context, table string, want, got map[K]*counters) {
	t.Helper()
	keys := make(map[K]struct{}, len(want)+len(got))
	for k := range want {
		keys[k] = struct{}{}
	}
	for k := range got {
		keys[k] = struct{}{}
	}
	var bad []string
	for k := range keys {
		w, g := want[k], got[k]
		if w == nil {
			w = &counters{}
		}
		if g == nil {
			g = &counters{}
		}
		if w.bytes != g.bytes || w.packets != g.packets || w.flows != g.flows ||
			w.sampled != g.sampled || w.unknown != g.unknown {
			bad = append(bad, fmt.Sprintf("%v: want %+v, got %+v", k, *w, *g))
		}
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		t.Fatalf("%s: %s disagrees with the stored rows (%d keys):\n%s",
			context, table, len(bad), joinLines(bad))
	}
}

func joinLines(lines []string) string {
	out := ""
	for _, l := range lines {
		out += "  " + l + "\n"
	}
	return out
}

// insertOne stores a single record and fails the test on error.
func insertOne(t *testing.T, st *Store, f model.Flow) {
	t.Helper()
	if err := st.InsertBatch(context.Background(), []model.Flow{f}); err != nil {
		t.Fatalf("insert %s -> %s: %v", f.SrcAddr, f.DstAddr, err)
	}
}

type aggregateSnapshot struct {
	exporter map[exporterKey]*counters
	iface    map[ifaceKey]*counters
	host     map[hostKey]*counters
	vlan     map[vlanKey]*counters
	protocol map[protocolKey]*counters
	traffic  map[trafficKey]*counters
}

func readAggregates(t *testing.T, st *Store) aggregateSnapshot {
	t.Helper()
	return aggregateSnapshot{
		exporter: readExporterAgg(t, st),
		iface:    readInterfaceAgg(t, st),
		host:     readHostAgg(t, st),
		vlan:     readVLANAgg(t, st),
		protocol: readProtocolAgg(t, st),
		traffic:  readTrafficAgg(t, st),
	}
}

func readExporterAgg(t *testing.T, st *Store) map[exporterKey]*counters {
	t.Helper()
	out := map[exporterKey]*counters{}
	rows, err := st.DB().Query(`SELECT hour_ms, exporter, bytes, packets, flows, sampled_flows, unknown_flows FROM agg_exporter`)
	if err != nil {
		t.Fatalf("query agg_exporter: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k exporterKey
		var c counters
		if err := rows.Scan(&k.hour, &k.exporter, &c.bytes, &c.packets, &c.flows, &c.sampled, &c.unknown); err != nil {
			t.Fatalf("scan agg_exporter: %v", err)
		}
		cc := c
		out[k] = &cc
	}
	return out
}

func readInterfaceAgg(t *testing.T, st *Store) map[ifaceKey]*counters {
	t.Helper()
	out := map[ifaceKey]*counters{}
	rows, err := st.DB().Query(`SELECT hour_ms, exporter, if_index, direction, bytes, packets, flows, sampled_flows, unknown_flows FROM agg_interface`)
	if err != nil {
		t.Fatalf("query agg_interface: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k ifaceKey
		var c counters
		if err := rows.Scan(&k.hour, &k.exporter, &k.ifIndex, &k.direction, &c.bytes, &c.packets, &c.flows, &c.sampled, &c.unknown); err != nil {
			t.Fatalf("scan agg_interface: %v", err)
		}
		cc := c
		out[k] = &cc
	}
	return out
}

func readHostAgg(t *testing.T, st *Store) map[hostKey]*counters {
	t.Helper()
	out := map[hostKey]*counters{}
	rows, err := st.DB().Query(`SELECT hour_ms, host, direction, bytes, packets, flows, sampled_flows, unknown_flows FROM agg_host`)
	if err != nil {
		t.Fatalf("query agg_host: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k hostKey
		var c counters
		if err := rows.Scan(&k.hour, &k.host, &k.direction, &c.bytes, &c.packets, &c.flows, &c.sampled, &c.unknown); err != nil {
			t.Fatalf("scan agg_host: %v", err)
		}
		cc := c
		out[k] = &cc
	}
	return out
}

func readVLANAgg(t *testing.T, st *Store) map[vlanKey]*counters {
	t.Helper()
	out := map[vlanKey]*counters{}
	rows, err := st.DB().Query(`SELECT hour_ms, vlan, direction, bytes, packets, flows, sampled_flows, unknown_flows FROM agg_vlan`)
	if err != nil {
		t.Fatalf("query agg_vlan: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k vlanKey
		var c counters
		if err := rows.Scan(&k.hour, &k.vlan, &k.direction, &c.bytes, &c.packets, &c.flows, &c.sampled, &c.unknown); err != nil {
			t.Fatalf("scan agg_vlan: %v", err)
		}
		cc := c
		out[k] = &cc
	}
	return out
}

func readProtocolAgg(t *testing.T, st *Store) map[protocolKey]*counters {
	t.Helper()
	out := map[protocolKey]*counters{}
	rows, err := st.DB().Query(`SELECT hour_ms, protocol, bytes, packets, flows, sampled_flows, unknown_flows FROM agg_protocol`)
	if err != nil {
		t.Fatalf("query agg_protocol: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k protocolKey
		var c counters
		if err := rows.Scan(&k.hour, &k.protocol, &c.bytes, &c.packets, &c.flows, &c.sampled, &c.unknown); err != nil {
			t.Fatalf("scan agg_protocol: %v", err)
		}
		cc := c
		out[k] = &cc
	}
	return out
}

func readTrafficAgg(t *testing.T, st *Store) map[trafficKey]*counters {
	t.Helper()
	out := map[trafficKey]*counters{}
	rows, err := st.DB().Query(`SELECT res_ms, bucket_ms, bytes, packets, flows, sampled_flows, unknown_flows FROM agg_traffic`)
	if err != nil {
		t.Fatalf("query agg_traffic: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k trafficKey
		var c counters
		if err := rows.Scan(&k.res, &k.bucket, &c.bytes, &c.packets, &c.flows, &c.sampled, &c.unknown); err != nil {
			t.Fatalf("scan agg_traffic: %v", err)
		}
		cc := c
		out[k] = &cc
	}
	return out
}

// aggregateTotals returns the summed flows/bytes of every aggregate table, for
// the "did anything shrink" assertions.
func aggregateTotals(t *testing.T, st *Store) map[string][2]int64 {
	t.Helper()
	out := map[string][2]int64{}
	for _, table := range []string{"agg_exporter", "agg_interface", "agg_host", "agg_vlan", "agg_protocol", "agg_traffic"} {
		var flows, bytes int64
		if err := st.DB().QueryRow(fmt.Sprintf(`SELECT COALESCE(SUM(flows),0), COALESCE(SUM(bytes),0) FROM %s`, table)).Scan(&flows, &bytes); err != nil {
			t.Fatalf("totals %s: %v", table, err)
		}
		out[table] = [2]int64{flows, bytes}
	}
	return out
}
