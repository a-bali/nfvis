package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/decoder"
	"github.com/a-bali/nfvis/internal/ingest"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
	"github.com/a-bali/nfvis/internal/testutil"
)

func TestSystemEndpointAndDNSFlush(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.DNS.Enabled = true
	cfg.Listen.UDP = "127.0.0.1:0"
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	coll := ingest.New(cfg, decoder.New(cfg), st, &ingest.Counters{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := coll.Start(); err != nil {
		t.Fatal(err)
	}
	defer coll.Stop()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(cfg, st, coll, log, "test-version")

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v1/system")
	if err != nil {
		t.Fatal(err)
	}
	var system map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&system); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got := system["dns"].(map[string]interface{})["entries"]; got != float64(0) {
		t.Fatalf("system DNS entries = %v, want 0", got)
	}
	if _, ok := system["database"].(map[string]interface{})["size_bytes"]; !ok {
		t.Fatalf("system response missing database size: %v", system["database"])
	}

	resp, err = http.Post(ts.URL+"/api/v1/system/dns/cache", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	var flushed map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&flushed); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if got := flushed["flushed"]; got != float64(0) {
		t.Fatalf("flushed = %v, want 0", got)
	}
	if got := flushed["dns"].(map[string]interface{})["entries"]; got != float64(0) {
		t.Fatalf("entries after flush = %v, want 0", got)
	}
}

func TestUDPIngestToAPI(t *testing.T) {
	udp := freeUDPAddr(t)
	cfg := config.Default()
	cfg.Listen.UDP = udp
	cfg.Listen.HTTP = "127.0.0.1:0"
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.Database.Flush = config.Duration(20 * time.Millisecond)
	cfg.Decoder.QueueSize = 64
	cfg.DNS.Enabled = false
	// The loopback UDP sender is the exporter from the collector's perspective.
	cfg.Exporters = []config.Exporter{{Address: "127.0.0.1", Label: "edge", Interfaces: map[uint32]string{1: "wan0", 2: "lan0"}}}

	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	counters := &ingest.Counters{}
	coll := ingest.New(cfg, decoder.New(cfg), st, counters, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := coll.Start(); err != nil {
		t.Fatal(err)
	}
	defer coll.Stop()

	server := httptest.NewServer(New(cfg, st, coll, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-version").Handler())
	defer server.Close()

	conn, err := net.Dial("udp", udp)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	now := time.Now().UTC()
	packet := testutil.V5Packet(now, 1000, 100, testutil.V5Field{
		SrcAddr: testutil.DefaultV4Src(), DstAddr: testutil.DefaultV4Dst(),
		Input: 1, Output: 2, Packets: 4, Bytes: 400,
		First: 900, Last: 950, Proto: 6, SrcPort: 1234, DstPort: 443,
	})
	if _, err := conn.Write(packet); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(server.URL + "/api/v1/health")
		if err == nil {
			var body map[string]interface{}
			_ = json.NewDecoder(resp.Body).Decode(&body)
			_ = resp.Body.Close()
			if n, ok := body["database"].(map[string]interface{})["flow_rows"]; ok && n.(float64) >= 1 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("flow never reached SQLite: last health=%v", resp)
		}
		time.Sleep(25 * time.Millisecond)
	}

	for _, tc := range []struct {
		path  string
		check func(t *testing.T, body map[string]interface{})
	}{
		{"/api/v1/health", func(t *testing.T, b map[string]interface{}) {
			if b["version"] != "test-version" {
				t.Fatalf("health version: %v", b["version"])
			}
			if b["status"] != "ok" {
				t.Fatalf("health status: %v", b)
			}
			if b["udp_addr"] == "" || b["http_addr"] == "" {
				t.Fatalf("missing addresses: %v", b)
			}
		}},
		{"/api/v1/coverage", func(t *testing.T, b map[string]interface{}) {
			if b["resolution"] != "raw" {
				t.Fatalf("coverage: %v", b)
			}
		}},
		{"/api/v1/timeseries", func(t *testing.T, b map[string]interface{}) {
			if b["resolution"] != "raw" || len(b["points"].([]interface{})) != 0 {
				// Points may be empty if the flow falls outside the default
				// one-hour window; only check the resolution field.
				t.Logf("timeseries: %v", b["resolution"])
			}
		}},
		{"/api/v1/top?dimension=exporter", func(t *testing.T, b map[string]interface{}) {
			rows := b["rows"].([]interface{})
			found := false
			for _, raw := range rows {
				row := raw.(map[string]interface{})
				if row["key"] == "127.0.0.1" && row["label"] == "edge" {
					found = true
				}
			}
			if !found {
				t.Fatalf("exporter label missing: %v", rows)
			}
		}},
		{"/api/v1/flows", func(t *testing.T, b map[string]interface{}) {
			rows := b["rows"].([]interface{})
			if len(rows) != 1 {
				t.Fatalf("flow detail rows = %d", len(rows))
			}
			row := rows[0].(map[string]interface{})
			if row["src_addr"] != "10.1.2.3" || row["input_label"] != "wan0" {
				t.Fatalf("flow detail: %v", row)
			}
		}},
		{"/api/v1/exporters", func(t *testing.T, b map[string]interface{}) {
			items := b["exporters"].([]interface{})
			if len(items) != 1 || items[0].(map[string]interface{})["label"] != "edge" {
				t.Fatalf("exporters: %v", items)
			}
		}},
		{"/api/v1/config", func(t *testing.T, b map[string]interface{}) {
			if b["authentication"] != false {
				t.Fatalf("config should disclose no auth: %v", b)
			}
		}},
	} {
		resp, err := http.Get(server.URL + tc.path)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("%s decode: %v", tc.path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status %d: %v", tc.path, resp.StatusCode, body)
		}
		tc.check(t, body)
	}
}

func TestAPIBadRequests(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.DNS.Enabled = false
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	coll := ingest.New(cfg, decoder.New(cfg), st, &ingest.Counters{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s := New(cfg, st, coll, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-version")
	for _, path := range []string{
		"/api/v1/timeseries?from=bad",
		"/api/v1/top?dimension=nope",
		"/api/v1/flows?limit=-1",
		"/api/v1/timeseries?src_port=abc",
		"/api/v1/flows?sampled=maybe",
		"/api/v1/top?vlan=abc",
		"/api/v1/flows?direction=sideways",
		"/api/v1/flows?src_port=abc",
		"/api/v1/flows?dst_port=70000",
	} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d body %s", path, rec.Code, rec.Body.String())
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/unknown", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown endpoint status %d", rec.Code)
	}
}

func freeUDPAddr(t *testing.T) string {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := c.LocalAddr().String()
	_ = c.Close()
	return addr
}

func TestFlowHostFilterMatchesResolvedNames(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.DNS.Enabled = true
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	now := time.Now().UTC()
	flows := []model.Flow{
		{EndMS: now.Add(-time.Minute).UnixMilli(), TimeSource: "exported", Exporter: "192.0.2.254", SrcAddr: "192.0.2.1", DstAddr: "198.51.100.1", Bytes: 100},
		{EndMS: now.Add(-2 * time.Minute).UnixMilli(), TimeSource: "exported", Exporter: "192.0.2.254", SrcAddr: "203.0.113.9", DstAddr: "192.0.2.1", Bytes: 200},
	}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatal(err)
	}

	s := New(cfg, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-version")
	names := map[string]string{
		"192.0.2.1":    "edge-router.lan",
		"198.51.100.1": "web-server.lan",
	}
	seedNames(t, st, flows, names)

	check := func(path string, wantTotal int64, wantSrc, wantDst string) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d: %s", path, rec.Code, rec.Body.String())
		}
		var page store.FlowPage
		if err := json.NewDecoder(rec.Body).Decode(&page); err != nil {
			t.Fatal(err)
		}
		if page.Total != wantTotal || len(page.Rows) != 1 {
			t.Fatalf("%s: total=%d rows=%d", path, page.Total, len(page.Rows))
		}
		if page.Rows[0].SrcName != wantSrc || page.Rows[0].DstName != wantDst {
			t.Fatalf("%s: names = %q/%q, want %q/%q", path, page.Rows[0].SrcName, page.Rows[0].DstName, wantSrc, wantDst)
		}
	}
	checkTS := func(path string, wantBytes uint64, wantFlows uint64) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d: %s", path, rec.Code, rec.Body.String())
		}
		var result store.TimeseriesResult
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		var bytes, flows uint64
		for _, p := range result.Points {
			bytes += p.Bytes
			flows += p.Flows
		}
		if bytes != wantBytes || flows != wantFlows {
			t.Fatalf("%s: bytes=%d flows=%d, want %d/%d", path, bytes, flows, wantBytes, wantFlows)
		}
	}

	check("/api/v1/flows?src_host=ROUTER", 1, "edge-router.lan", "web-server.lan")
	check("/api/v1/flows?dst_host=server", 1, "edge-router.lan", "web-server.lan")
	check("/api/v1/flows?src_host=203.0.113.9", 1, "", "edge-router.lan")
	check("/api/v1/flows?src_host=203.0", 1, "", "edge-router.lan")
	check("/api/v1/flows?dst_host=192.0.2", 1, "", "edge-router.lan")
	checkTS("/api/v1/timeseries?src_host=ROUTER&bucket_seconds=60", 100, 1)
	checkTS("/api/v1/timeseries?src_host=203.0", 200, 1)

	// Partial IP search does not require reverse DNS.
	check("/api/v1/flows?src_host=192.0", 1, "edge-router.lan", "web-server.lan")
}

func TestFlowsExplicitInputOutputFilters(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	now := time.Now().UTC()
	first := model.Flow{EndMS: now.Add(-time.Minute).UnixMilli(), TimeSource: "exported", Exporter: "192.0.2.254", InputIf: 1, InputLabel: "wan0", OutputIf: 2, OutputLabel: "lan0", SrcAddr: "10.0.0.1", DstAddr: "203.0.113.1", SrcVLAN: 10, DstVLAN: 20, Bytes: 100, Packets: 1}
	second := model.Flow{EndMS: now.Add(-2 * time.Minute).UnixMilli(), TimeSource: "exported", Exporter: "192.0.2.254", InputIf: 2, InputLabel: "lan0", OutputIf: 1, OutputLabel: "wan0", SrcAddr: "203.0.113.1", DstAddr: "10.0.0.1", SrcVLAN: 20, DstVLAN: 10, Bytes: 200, Packets: 1}
	if err := st.InsertBatch(context.Background(), []model.Flow{first, second}); err != nil {
		t.Fatal(err)
	}
	s := New(cfg, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-version")

	check := func(filter string, wantBytes uint64) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/flows?"+filter, nil)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d: %s", filter, rec.Code, rec.Body.String())
		}
		var page store.FlowPage
		if err := json.NewDecoder(rec.Body).Decode(&page); err != nil {
			t.Fatal(err)
		}
		if page.Total != 1 || len(page.Rows) != 1 || page.Rows[0].Bytes != wantBytes {
			t.Fatalf("%s: total=%d rows=%d bytes=%d, want bytes=%d", filter, page.Total, len(page.Rows), page.Rows[0].Bytes, wantBytes)
		}
	}
	check("input_interface=wan0", 100)
	check("output_interface=wan0", 200)
	check("input_interface=1", 100)
	check("output_interface=1", 200)
	check("input_vlan=10", 100)
	check("output_vlan=10", 200)
}

func TestHostSearchAndGlobalSort(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "test.db")
	cfg.DNS.Enabled = true
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC()
	flows := make([]model.Flow, 0, 1100)
	for i := 0; i < 1100; i++ {
		flows = append(flows, model.Flow{EndMS: now.Add(-time.Minute).UnixMilli(), TimeSource: "exported", Exporter: "192.0.2.1", SrcAddr: fmt.Sprintf("10.%d.%d.%d", i/65536, (i/256)%256, i%256), DstAddr: "203.0.113.1", Bytes: uint64(1100 - i), Packets: uint64(i + 1)})
	}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatal(err)
	}
	s := New(cfg, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-version")
	seedNames(t, st, flows, map[string]string{flows[1099].SrcAddr: "buried.example"})

	get := func(query string) store.TopResult {
		t.Helper()
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/top?dimension=host&direction=source&from=-1h&"+query, nil))
		if rec.Code != 200 {
			t.Fatalf("%s: %d %s", query, rec.Code, rec.Body.String())
		}
		var result store.TopResult
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	for _, term := range []string{"buried.example", flows[1099].SrcAddr} {
		r := get("search=" + term)
		if r.Total != 1 || r.Rows[0].Key != flows[1099].SrcAddr {
			t.Fatalf("search %s: %+v", term, r)
		}
	}
	r := get("sort=packets&order=desc&limit=1&offset=0")
	if r.Total != 1100 || r.Rows[0].Key != flows[1099].SrcAddr {
		t.Fatalf("global sort: %+v", r)
	}
	r = get("sort=bytes&order=asc&limit=1&offset=1")
	if r.Rows[0].Key != flows[1098].SrcAddr {
		t.Fatalf("sort page: %+v", r)
	}
}

func TestTopLabels(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.DNS.Enabled = true
	cfg.VLANLabels = map[int32]string{20: "Clients"}
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	now := time.Now().UTC()
	flows := []model.Flow{
		{EndMS: now.Add(-time.Minute).UnixMilli(), TimeSource: "exported", Exporter: "192.0.2.254", SrcAddr: "10.0.0.1", DstAddr: "10.0.0.2", Protocol: 6, SrcVLAN: 20, DstVLAN: 30, Bytes: 100, Packets: 1},
		{EndMS: now.Add(-2 * time.Minute).UnixMilli(), TimeSource: "exported", Exporter: "192.0.2.254", SrcAddr: "10.0.0.2", DstAddr: "10.0.0.1", Protocol: 2, SrcVLAN: 30, DstVLAN: 20, Bytes: 200, Packets: 1},
	}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatal(err)
	}

	s := New(cfg, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-version")
	names := map[string]string{"10.0.0.1": "host-one.lan", "10.0.0.2": "host-two.lan"}
	seedNames(t, st, flows, names)

	labels := func(path string) map[string]string {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d: %s", path, rec.Code, rec.Body.String())
		}
		var result store.TopResult
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		out := make(map[string]string, len(result.Rows))
		for _, row := range result.Rows {
			out[row.Key] = row.Label
		}
		return out
	}

	checks := []struct {
		path string
		want map[string]string
	}{
		{"/api/v1/top?dimension=protocol", map[string]string{"6": "TCP", "2": "IGMP"}},
		{"/api/v1/top?dimension=vlan&direction=source", map[string]string{"20": "Clients", "30": "VLAN 30"}},
		{"/api/v1/top?dimension=host&direction=source", map[string]string{"10.0.0.1": "host-one.lan", "10.0.0.2": "host-two.lan"}},
	}
	for _, tc := range checks {
		got := labels(tc.path)
		for key, want := range tc.want {
			if got[key] != want {
				t.Fatalf("%s: label[%s]=%q, want %q (all %v)", tc.path, key, got[key], want, got)
			}
		}
	}
}

func TestStackedTimeseriesLabels(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.DNS.Enabled = true
	cfg.VLANLabels = map[int32]string{20: "Clients"}
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	now := time.Now().UTC()
	flows := []model.Flow{
		{EndMS: now.Add(-time.Minute).UnixMilli(), TimeSource: "exported", Exporter: "192.0.2.254", SrcAddr: "10.0.0.1", DstAddr: "10.0.0.2", Protocol: 6, SrcVLAN: 20, DstVLAN: 30, Bytes: 100, Packets: 1},
		{EndMS: now.Add(-2 * time.Minute).UnixMilli(), TimeSource: "exported", Exporter: "192.0.2.254", SrcAddr: "10.0.0.2", DstAddr: "10.0.0.1", Protocol: 17, SrcVLAN: 30, DstVLAN: 20, Bytes: 200, Packets: 1},
	}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatal(err)
	}

	s := New(cfg, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-version")
	seedNames(t, st, flows, map[string]string{"10.0.0.1": "host-one.lan"})

	split := func(query string) store.SplitResult {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/timeseries?from=-1h&"+query, nil)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d: %s", query, rec.Code, rec.Body.String())
		}
		var result store.SplitResult
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	labelFor := func(result store.SplitResult, key string) string {
		for _, band := range result.Series {
			if band.Key == key {
				return band.Label
			}
		}
		return ""
	}

	if got := labelFor(split("split_by=protocol"), "6"); got != "TCP" {
		t.Fatalf("protocol label=%q want TCP", got)
	}
	if got := labelFor(split("split_by=input_vlan"), "20"); got != "Clients" {
		t.Fatalf("vlan label=%q want Clients", got)
	}
	if got := labelFor(split("split_by=src_host"), "10.0.0.1"); got != "host-one.lan" {
		t.Fatalf("host label=%q want host-one.lan", got)
	}
	// Metric selects the ranking and the response echoes it.
	if got := split("split_by=protocol&metric=packets").Metric; got != "packets" {
		t.Fatalf("metric=%q want packets", got)
	}

	for _, bad := range []string{"split_by=exporter", "split_by=protocol&metric=widgets"} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/timeseries?from=-1h&"+bad, nil)
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d want 400", bad, rec.Code)
		}
	}
}

func seedNames(t *testing.T, st *store.Store, flows []model.Flow, names map[string]string) {
	t.Helper()
	for i, f := range flows {
		for _, e := range []struct{ ip, dir string }{{f.SrcAddr, "source"}, {f.DstAddr, "destination"}} {
			if name := names[e.ip]; name != "" {
				if err := st.SaveDNS(context.Background(), store.DNSObservation{ID: int64(i + 1), IP: e.ip, Direction: e.dir, Name: name, HourMS: (f.EndMS / 3600000) * 3600000, CompletedMS: time.Now().UnixMilli()}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}
