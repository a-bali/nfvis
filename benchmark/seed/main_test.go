package main

import (
	"context"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/store"
)

func TestNormalizeCurveFillsGaps(t *testing.T) {
	var samples [24]float64
	samples[0] = 10
	samples[1] = 20
	samples[2] = 40 // peak
	samples[23] = 5

	curve := normalizeCurve(samples)
	if curve[2] != 1 {
		t.Fatalf("peak hour = %v, want 1", curve[2])
	}
	for hour, v := range curve {
		if v <= 0 {
			t.Fatalf("hour %d has no traffic (%v); gaps must be interpolated", hour, v)
		}
		if v < 0.12 || v > 1 {
			t.Fatalf("hour %d = %v, want 0.12..1", hour, v)
		}
	}
	// Interpolated hours between 2 (peak) and 23 (5) must sit between them.
	if mid := curve[13]; mid <= curve[23] || mid >= 1 {
		t.Fatalf("hour 13 = %v, want strictly between %v and 1", mid, curve[23])
	}
}

func TestNormalizeCurveWrapsMidnight(t *testing.T) {
	var samples [24]float64
	samples[22] = 100
	samples[23] = 100

	curve := normalizeCurve(samples)
	// Hours after midnight were never sampled; they must inherit the shape
	// across midnight rather than collapsing to the floor.
	if curve[0] != 1 {
		t.Fatalf("hour 0 = %v, want the wrap-around peak", curve[0])
	}
}

func TestLoadPatternFallsBackWithoutDatabase(t *testing.T) {
	cfg := config.Default()
	pat := loadPattern(filepath.Join(t.TempDir(), "missing.db"), cfg)
	if pat.exporters.empty() || pat.protocols.empty() || pat.srcHosts.empty() || pat.dstHosts.empty() {
		t.Fatalf("fallback pattern is incomplete: %s", pat.describe())
	}
	if pat.bytesPerPacket <= 0 || pat.packetsPerFlow <= 0 {
		t.Fatalf("fallback ratios invalid: %v B/pkt, %v pkt/flow", pat.bytesPerPacket, pat.packetsPerFlow)
	}
	for hour, v := range pat.hourCurve {
		if v <= 0 {
			t.Fatalf("fallback hour %d density = %v", hour, v)
		}
	}
}

// TestSeedFillsRawAndEveryTier checks the invariants the dashboard relies on:
// recent hours have individual flows, and every resolution tier reports the
// same byte total, so no range double counts.
func TestSeedFillsRawAndEveryTier(t *testing.T) {
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "seed.db")
	cfg.Exporters = nil // exercise the built-in fallback pattern
	if len(cfg.Database.Resolution) == 0 {
		t.Fatal("expected configured resolution tiers")
	}

	ctx := context.Background()
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	pat := loadPattern("", cfg)
	now := time.Now().UTC().Truncate(time.Hour)
	total, err := runSeed(ctx, st, pat, cfg, seedOptions{
		days:          1,
		flowsPerHour:  600,
		batch:         500,
		seed:          7,
		weekendFactor: 1,
	}, now)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if total < 5000 {
		t.Fatalf("wrote %d flows for a day, want more", total)
	}

	var rows int64
	if err := st.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM flows`).Scan(&rows); err != nil {
		t.Fatalf("count flows: %v", err)
	}
	if rows != total {
		t.Fatalf("flow rows = %d, wrote %d", rows, total)
	}

	// The seeded day must land inside the last 24 hours and every tier must
	// agree on the total, otherwise long ranges would double count.
	var tiers int
	err = st.DB().QueryRowContext(ctx, `SELECT COUNT(DISTINCT res_ms) FROM agg_traffic`).Scan(&tiers)
	if err != nil {
		t.Fatalf("count tiers: %v", err)
	}
	if tiers < 3 {
		t.Fatalf("agg_traffic has %d tiers, want at least 3", tiers)
	}
	var minTotal, maxTotal int64
	if err := st.DB().QueryRowContext(ctx, `SELECT MIN(t), MAX(t) FROM (SELECT SUM(bytes) AS t FROM agg_traffic GROUP BY res_ms)`).Scan(&minTotal, &maxTotal); err != nil {
		t.Fatalf("read tier totals: %v", err)
	}
	if minTotal != maxTotal || minTotal <= 0 {
		t.Fatalf("tier totals differ: min %d max %d", minTotal, maxTotal)
	}

	// Aggregates must cover whole hours while raw covers the full last hour,
	// which is what makes bucket=1m usable for recent ranges.
	cov, err := st.Coverage(ctx)
	if err != nil {
		t.Fatalf("coverage: %v", err)
	}
	if cov.RawOldestMS <= 0 || cov.AggregateOldestMS <= 0 {
		t.Fatalf("coverage empty: %+v", cov)
	}
	if cov.RawNewestMS < cov.AggregateNewestMS {
		t.Fatalf("raw ends before aggregates: raw %d agg %d", cov.RawNewestMS, cov.AggregateNewestMS)
	}

	// Byte counts must be plausible: the pattern anchors them at ~500 B/packet.
	var bytesPerPacket float64
	if err := st.DB().QueryRowContext(ctx, `SELECT SUM(bytes) * 1.0 / SUM(packets) FROM flows`).Scan(&bytesPerPacket); err != nil {
		t.Fatalf("read byte ratio: %v", err)
	}
	if math.Abs(bytesPerPacket-500) > 100 {
		t.Fatalf("bytes per packet = %.0f, want close to 500", bytesPerPacket)
	}
}

func TestSeedIsDeterministic(t *testing.T) {
	cfg := config.Default()
	cfg.Exporters = nil
	pat := loadPattern("", cfg)
	now := time.Now().UTC().Truncate(time.Hour)
	opts := seedOptions{days: 1, flowsPerHour: 200, batch: 100, seed: 42, weekendFactor: 1}

	run := func() []string {
		dir := t.TempDir()
		cfg.Database.Path = filepath.Join(dir, "seed.db")
		st, err := store.Open(cfg)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		defer func() { _ = st.Close() }()
		if _, err := runSeed(context.Background(), st, pat, cfg, opts, now); err != nil {
			t.Fatalf("seed: %v", err)
		}
		rows, err := st.DB().Query(`SELECT src_addr, dst_addr, protocol, dst_port, bytes, packets, start_ms
			FROM flows ORDER BY id LIMIT 20`)
		if err != nil {
			t.Fatalf("read flows: %v", err)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var src, dst string
			var proto, port int
			var bytes, packets, startMS int64
			if err := rows.Scan(&src, &dst, &proto, &port, &bytes, &packets, &startMS); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, src+" "+dst+" "+string(rune(proto))+" "+time.UnixMilli(startMS).String())
		}
		return out
	}

	first, second := run(), run()
	if len(first) != len(second) {
		t.Fatalf("row counts differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("row %d differs:\n%s\n%s", i, first[i], second[i])
		}
	}
}
