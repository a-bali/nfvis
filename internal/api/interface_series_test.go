package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
)

func TestInterfaceTimeseries(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	end := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	start := end.Add(-3 * time.Hour)
	var flows []model.Flow
	for _, ts := range []time.Time{start.Add(time.Minute), start.Add(time.Hour), end.Add(-10 * time.Second)} {
		add := func(exporter string, in, out uint32, bytes, packets uint64) {
			flows = append(flows, model.Flow{Exporter: exporter, InputIf: in, OutputIf: out, EndMS: ts.UnixMilli(), ReceivedAtMS: ts.UnixMilli(), Bytes: bytes, Packets: packets})
		}
		add("192.0.2.1", 1, 2, 600, 6)
		add("192.0.2.1", 2, 1, 120, 2)
		add("192.0.2.1", 1, 1, 30, 1) // Both directions, not a net total.
		add("192.0.2.1", 2, 3, 5000, 5)
		add("192.0.2.2", 1, 2, 999, 9)
	}
	if err := st.InsertBatch(context.Background(), flows); err != nil {
		t.Fatal(err)
	}
	handler := New(cfg, st, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "test").Handler()
	query := url.Values{"from": {start.Format(time.RFC3339)}, "to": {end.Format(time.RFC3339)}, "exporter": {"192.0.2.1"}, "if_index": {"1"}}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/interface-timeseries?"+query.Encode(), nil))
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var result struct{ Ingress, Egress []store.Point }
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Ingress) == 0 || len(result.Ingress) != len(result.Egress) {
		t.Fatalf("unaligned results: %+v", result)
	}
	var in, out [3]uint64
	var hourly, minute bool
	for i, p := range result.Ingress {
		q := result.Egress[i]
		if p.BucketMS != q.BucketMS || p.DurationMS != q.DurationMS || p.DurationMS <= 0 {
			t.Fatalf("buckets differ: %+v %+v", p, q)
		}
		if i > 0 && result.Ingress[i-1].BucketMS >= p.BucketMS {
			t.Fatal("bucket order is not increasing")
		}
		hourly = hourly || p.DurationMS == 3600000
		minute = minute || p.DurationMS == 60000
		in[0] += p.Bytes
		in[1] += p.Packets
		in[2] += p.Flows
		out[0] += q.Bytes
		out[1] += q.Packets
		out[2] += q.Flows
	}
	if !hourly || !minute {
		t.Fatalf("want hourly and minute buckets; got %v %v", hourly, minute)
	}
	if in != [3]uint64{1890, 21, 6} || out != [3]uint64{450, 9, 6} {
		t.Fatalf("wrong direction totals: %v %v", in, out)
	}
	query.Set("if_index", "0")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/interface-timeseries?"+query.Encode(), nil))
	if rec.Code != 400 {
		t.Fatalf("zero ifIndex: status %d", rec.Code)
	}
}
