// Command seed fills a scratch database with synthetic flow history shaped
// like an existing nfvis database.
//
// It reads a traffic pattern from a database that has been collecting real
// traffic - exporters, interfaces, protocols, hosts, destination ports and the
// hourly volume curve - and then writes flows for an arbitrary span through the
// normal store path. That means the raw table, the hourly aggregates and every
// chart-resolution tier end up populated exactly as if the flows had been
// ingested, and retention is applied afterwards. The result behaves like a
// collector that has been running for months: recent hours carry individual
// flows, older days only aggregates.
//
// The pattern is optional. Without a readable pattern database the generated
// data falls back to plausible homelab defaults.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/decoder"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
)

const hourMS = int64(time.Hour / time.Millisecond)

func main() {
	dbPath := flag.String("db", "data/seed.db", "database to create or extend")
	days := flag.Int("days", 60, "days of history to generate")
	perHour := flag.Float64("flows-per-hour", 1500, "flows in the busiest hour of the day; other hours scale with the daily curve")
	batchSize := flag.Int("batch", 20000, "flows per insert transaction")
	patternPath := flag.String("pattern-db", "data/nfvis.db", "database to read the traffic pattern from (optional)")
	configPath := flag.String("config", "config.example.yaml", "config supplying retention, exporter labels and VLAN rules")
	configOut := flag.String("config-out", "", "write a config for the seeded database to this path")
	seedValue := flag.Int64("seed", 20261119, "random seed; the same seed and options produce the same flows")
	weekend := flag.Float64("weekend-factor", 0.85, "traffic multiplier for Saturday and Sunday")
	prune := flag.Bool("prune", true, "apply retention after seeding, like a running collector would")
	reset := flag.Bool("reset", false, "delete the target database before seeding")
	flag.Parse()

	if *days < 1 {
		fatal("-days must be at least 1")
	}
	if *perHour <= 0 {
		fatal("-flows-per-hour must be positive")
	}
	if *batchSize < 1 {
		fatal("-batch must be positive")
	}
	if *weekend <= 0 {
		fatal("-weekend-factor must be positive")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal(err.Error())
	}
	cfg.Database.Path = *dbPath
	if *configOut != "" {
		// The seed database usually runs next to the real one, so give it its
		// own ports instead of fighting over 2055/8080.
		cfg.Listen.UDP = "127.0.0.1:2056"
		cfg.Listen.HTTP = "127.0.0.1:8081"
		cfg.DNS.Enabled = false
	}
	if err := cfg.Validate(); err != nil {
		fatal("configuration invalid: " + err.Error())
	}

	absTarget, err := filepath.Abs(*dbPath)
	if err != nil {
		fatal(err.Error())
	}
	if *patternPath != "" {
		if absPattern, err := filepath.Abs(*patternPath); err == nil && absPattern == absTarget {
			fatal("refusing to seed the pattern database; choose a different -db")
		}
	}

	// Patterns are read from a live database, so never take a write lock on it.
	if *patternPath != "" {
		fmt.Printf("reading pattern from %s\n", *patternPath)
	}
	pat := loadPattern(*patternPath, cfg)
	fmt.Printf("pattern: %s\n", pat.describe())

	ctx := context.Background()
	if *reset {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if err := os.Remove(absTarget + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
				fatal(fmt.Sprintf("reset %s: %v", absTarget+suffix, err))
			}
		}
		fmt.Printf("reset: removed %s\n", absTarget)
	}

	st, err := store.Open(cfg)
	if err != nil {
		fatal("open database: " + err.Error())
	}
	defer func() { _ = st.Close() }()

	opts := seedOptions{
		days:          *days,
		flowsPerHour:  *perHour,
		batch:         *batchSize,
		seed:          *seedValue,
		weekendFactor: *weekend,
	}
	started := time.Now()
	total, err := runSeed(ctx, st, pat, cfg, opts, time.Now())
	if err != nil {
		fatal("seed: " + err.Error())
	}
	fmt.Printf("wrote %s flows in %s (%.0f flows/s)\n",
		comma(total), time.Since(started).Round(time.Second), float64(total)/time.Since(started).Seconds())

	if *prune {
		pruned, err := st.SweepRetention(ctx, time.Now())
		if err != nil {
			fatal("apply retention: " + err.Error())
		}
		fmt.Printf("retention: removed %s raw flows, %s hourly rows, %s traffic rows\n",
			comma(pruned.RawDeleted), comma(pruned.AggregateDeleted), comma(pruned.TrafficDeleted))
	}

	if err := report(ctx, st, absTarget); err != nil {
		fatal("report: " + err.Error())
	}

	if *configOut != "" {
		data, err := yaml.Marshal(cfg)
		if err != nil {
			fatal("encode config: " + err.Error())
		}
		header := "# Generated by `make seed-db`; points at the seeded database.\n"
		if err := os.WriteFile(*configOut, append([]byte(header), data...), 0o644); err != nil {
			fatal("write config: " + err.Error())
		}
		fmt.Printf("config: %s\n", *configOut)
	}
}

type seedOptions struct {
	days          int
	flowsPerHour  float64
	batch         int
	seed          int64
	weekendFactor float64
}

// runSeed writes one hour of flows at a time. Each hour becomes a single insert
// transaction, which keeps the aggregate upserts meaningful (they are keyed by
// hour) without holding a huge write transaction open.
func runSeed(ctx context.Context, st *store.Store, pat *pattern, cfg config.Config, opt seedOptions, now time.Time) (int64, error) {
	s := &seeder{
		pat:    pat,
		rnd:    rand.New(rand.NewSource(opt.seed)),
		class:  decoder.NewClassifier(cfg),
		labels: interfaceLabels(cfg),
		seq:    map[string]uint32{},
	}

	// Stop at the current partial hour so the newest minutes carry flows, the
	// way a collector that is running right now would.
	now = now.UTC()
	end := now.Truncate(time.Hour)
	start := end.Add(-time.Duration(opt.days) * 24 * time.Hour)
	fmt.Printf("span: %s .. %s (%d day%s plus the current hour, ~%.0f flows/h)\n",
		start.Format("2006-01-02 15:04"), now.Format("2006-01-02 15:04"),
		opt.days, plural(opt.days), opt.flowsPerHour)

	var total int64
	progress := time.Now()
	for hour := start; !hour.After(end); hour = hour.Add(time.Hour) {
		count := s.flowsInHour(hour, opt)
		limit := hour.Add(time.Hour)
		if hour.Equal(end) {
			// Only the part of this hour that has already happened.
			count = int(math.Round(float64(count) * now.Sub(hour).Minutes() / 60))
			limit = now
		}
		if count <= 0 {
			continue
		}
		flows := make([]model.Flow, 0, min(count, opt.batch))
		for i := 0; i < count; i++ {
			flows = append(flows, s.flow(hour, limit))
			if len(flows) >= opt.batch {
				if err := st.InsertBatch(ctx, flows); err != nil {
					return total, err
				}
				total += int64(len(flows))
				flows = flows[:0]
			}
		}
		if len(flows) > 0 {
			if err := st.InsertBatch(ctx, flows); err != nil {
				return total, err
			}
			total += int64(len(flows))
		}
		if elapsed := time.Since(progress); elapsed >= 5*time.Second {
			progress = time.Now()
			rate := float64(total) / elapsed.Seconds()
			done := int(hour.Sub(start)/time.Hour) + 1
			left := float64(opt.days*24+1-done) * (float64(total) / float64(done))
			fmt.Printf("  through %s: %s flows, %.0f/s, eta %s\n",
				hour.Format("2006-01-02 15:04"), comma(total), rate,
				time.Duration(left/rate*float64(time.Second)).Round(time.Second))
		}
	}
	return total, nil
}

type seeder struct {
	pat    *pattern
	rnd    *rand.Rand
	class  *decoder.Classifier
	labels map[string]map[uint32]string
	seq    map[string]uint32
}

// flowsInHour scales the configured rate by the daily curve, the weekday, and
// a little noise so neighbouring days never look identical.
func (s *seeder) flowsInHour(hour time.Time, opt seedOptions) int {
	factor := s.pat.hourCurve[hour.Hour()]
	if wd := hour.Weekday(); wd == time.Saturday || wd == time.Sunday {
		factor *= opt.weekendFactor
	}
	// ±12% jitter, never below a sixth of the configured rate so quiet hours
	// still produce a chart line instead of a hole.
	noise := 0.88 + 0.24*s.rnd.Float64()
	count := int(math.Round(opt.flowsPerHour * factor * noise))
	if min := int(opt.flowsPerHour / 6); count < min {
		count = min
	}
	return count
}

// flow builds one record. Times land inside the hour, up to limit; durations
// are mostly short with a long tail, which is what makes the "flows" columns
// and the byte/packet ratio look like real traffic.
func (s *seeder) flow(hour, limit time.Time) model.Flow {
	r := s.rnd
	spanMS := limit.Sub(hour).Milliseconds()
	if spanMS < 1 {
		spanMS = int64(time.Hour / time.Millisecond)
	}
	endMS := hour.UnixMilli() + r.Int63n(spanMS)
	durationMS := int64(math.Exp(r.NormFloat64()*1.2)) * 1000
	if durationMS < 1000 {
		durationMS = 1000
	}
	startMS := endMS - durationMS

	exp := s.pat.pickExporter(r)
	inputIf := exp.ingress.pick(r)
	outputIf := exp.egress.pick(r)
	proto := s.pat.protocols.pick(r)
	src := s.pat.srcHosts.pick(r)
	dst := s.pat.dstHosts.pick(r)
	dstPort := s.pat.portFor(r, proto)
	srcPort := ephemeralPort(r)
	if r.Intn(10) == 0 {
		// Some flows are server-to-client, so the well-known port is on the
		// source side; otherwise every capture looks like a client request.
		srcPort, dstPort = dstPort, ephemeralPort(r)
	}

	// Flow sizes are log-normal around the pattern's averages. The jitter is
	// mean-preserving and the bulk tail is divided out of the base rate, so the
	// generated bytes-per-flow and packets-per-flow match the source data
	// instead of drifting upward.
	packets := s.pat.packetsPerFlow / (1 + bulkShare*(bulkFactor-1)) * mulLogNormal(r, 0.9)
	if r.Float64() < bulkShare {
		// Rare bulk transfer: much heavier than average, which is what makes
		// the top-talker and largest-flow views interesting.
		packets *= bulkFactor * (0.5 + r.Float64())
	}
	packetCount := int64(math.Round(packets))
	if packetCount < 1 {
		packetCount = 1
	}
	byteCount := int64(math.Round(float64(packetCount) * s.pat.bytesPerPacket * mulLogNormal(r, 0.35)))
	if byteCount < 40 {
		byteCount = 40
	}

	srcVLAN, srcSource, dstVLAN, dstSource := s.class.Classify(src, dst, 0, 0)
	if proto == 1 || proto == 58 {
		dstPort = 0
		if proto == 1 && r.Intn(4) != 0 {
			srcPort = 0
		}
	}

	exporter := exp.address
	s.seq[exporter]++
	return model.Flow{
		ReceivedAtMS:   endMS,
		StartMS:        startMS,
		EndMS:          endMS,
		TimeSource:     "exported",
		Version:        5,
		Exporter:       exporter,
		ExporterIP:     exporter,
		ExporterID:     uint32(r.Intn(4)),
		Sequence:       s.seq[exporter],
		InputIf:        inputIf,
		InputLabel:     s.labels[exporter][inputIf],
		OutputIf:       outputIf,
		OutputLabel:    s.labels[exporter][outputIf],
		SrcAddr:        src,
		DstAddr:        dst,
		SrcPort:        uint16(srcPort),
		DstPort:        uint16(dstPort),
		Protocol:       proto,
		TCPFlags:       tcpFlags(proto, r),
		Bytes:          uint64(byteCount),
		Packets:        uint64(packetCount),
		SrcVLAN:        srcVLAN,
		SrcVLANSource:  srcSource,
		DstVLAN:        dstVLAN,
		DstVLANSource:  dstSource,
		SamplingSource: "unknown",
	}
}

func tcpFlags(proto uint8, r *rand.Rand) uint8 {
	if proto != 6 {
		return 0
	}
	switch r.Intn(10) {
	case 0, 1, 2:
		return 0x02 // SYN
	case 3, 4, 5:
		return 0x10 // ACK
	case 6, 7, 8:
		return 0x18 // PSH+ACK
	default:
		return 0x11 // FIN+ACK
	}
}

const (
	bulkShare  = 0.01 // share of flows that carry a bulk transfer
	bulkFactor = 45   // average size multiplier for those flows
)

// mulLogNormal returns a positive multiplier with mean 1 and the given log
// standard deviation. Subtracting sigma^2/2 keeps the mean at 1 so the pattern
// averages survive the noise.
func mulLogNormal(r *rand.Rand, sigma float64) float64 {
	return math.Exp(r.NormFloat64()*sigma - sigma*sigma/2)
}

func ephemeralPort(r *rand.Rand) int {
	return 32768 + r.Intn(28000)
}

// interfaceLabels indexes the configured exporter interface names by address,
// so seeded rows carry the same labels the decoder would attach.
func interfaceLabels(cfg config.Config) map[string]map[uint32]string {
	out := make(map[string]map[uint32]string, len(cfg.Exporters))
	for _, exp := range cfg.Exporters {
		addr := strings.TrimSpace(exp.Address)
		ifaces := make(map[uint32]string, len(exp.Interfaces))
		for idx, label := range exp.Interfaces {
			ifaces[idx] = label
		}
		out[addr] = ifaces
	}
	return out
}

// report prints what the database now holds, which is the quickest way to spot
// a seed run that did not apply retention as expected.
func report(ctx context.Context, st *store.Store, path string) error {
	db := st.DB()
	fmt.Println("tables:")
	for _, table := range []string{"flows", "agg_exporter", "agg_interface", "agg_host", "agg_vlan", "agg_protocol"} {
		var n int64
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
			return fmt.Errorf("count %s: %w", table, err)
		}
		fmt.Printf("  %-14s %12s rows\n", table, comma(n))
	}
	rows, err := db.QueryContext(ctx, `SELECT res_ms, COUNT(*), SUM(bytes) FROM agg_traffic GROUP BY res_ms ORDER BY res_ms`)
	if err != nil {
		return fmt.Errorf("read agg_traffic: %w", err)
	}
	for rows.Next() {
		var res, n, bytes int64
		if err := rows.Scan(&res, &n, &bytes); err != nil {
			_ = rows.Close()
			return err
		}
		fmt.Printf("  agg_traffic    %12s rows  bucket %-7s  %s\n", comma(n), bucketLabel(res), humanBytes(bytes))
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}

	cov, err := st.Coverage(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("coverage: raw %s .. %s, aggregates %s .. %s, resolution %s\n",
		stamp(cov.RawOldestMS), stamp(cov.RawNewestMS),
		stamp(cov.AggregateOldestMS), stamp(cov.AggregateNewestMS), cov.Resolution)
	for _, tier := range cov.Tiers {
		fmt.Printf("  tier %-7s retention %-5s %s\n", bucketLabel(tier.BucketMS),
			time.Duration(tier.RetentionMS)*time.Millisecond, window(tier.OldestMS, tier.NewestMS))
	}

	if _, err := db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	for _, suffix := range []string{"", "-wal"} {
		if info, err := os.Stat(path + suffix); err == nil {
			fmt.Printf("size: %-14s %s\n", filepath.Base(path+suffix), humanBytes(info.Size()))
		}
	}
	return nil
}

func bucketLabel(ms int64) string {
	switch {
	case ms >= hourMS && ms%hourMS == 0:
		return fmt.Sprintf("%dh", ms/hourMS)
	case ms >= 60_000 && ms%60_000 == 0:
		return fmt.Sprintf("%dm", ms/60_000)
	default:
		return fmt.Sprintf("%dms", ms)
	}
}

func window(oldest, newest int64) string {
	if oldest <= 0 {
		return "empty"
	}
	return stamp(oldest) + " .. " + stamp(newest)
}

func stamp(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04")
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for value := n / unit; value >= unit && exp < 4; value /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func comma(n int64) string {
	s := fmt.Sprintf("%d", n)
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 && c != '-' {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "seed: "+msg)
	os.Exit(1)
}

// ---------------------------------------------------------------------------
// Pattern sampling
// ---------------------------------------------------------------------------

type weighted[T any] struct {
	value  T
	weight float64
}

type picker[T any] struct {
	items []weighted[T]
	total float64
}

func newPicker[T any](items []weighted[T]) *picker[T] {
	p := &picker[T]{}
	for _, item := range items {
		if item.weight <= 0 {
			continue
		}
		p.items = append(p.items, item)
		p.total += item.weight
	}
	return p
}

func (p *picker[T]) pick(r *rand.Rand) T {
	var zero T
	if p == nil || len(p.items) == 0 || p.total <= 0 {
		return zero
	}
	roll := r.Float64() * p.total
	for _, item := range p.items {
		roll -= item.weight
		if roll <= 0 {
			return item.value
		}
	}
	return p.items[len(p.items)-1].value
}

func (p *picker[T]) empty() bool { return p == nil || len(p.items) == 0 }

type exporterPattern struct {
	address string
	weight  float64
	ingress *picker[uint32]
	egress  *picker[uint32]
}

type pattern struct {
	exporters      *picker[*exporterPattern]
	protocols      *picker[uint8]
	srcHosts       *picker[string]
	dstHosts       *picker[string]
	ports          map[uint8]*picker[int]
	anyPort        *picker[int]
	hourCurve      [24]float64
	bytesPerPacket float64
	packetsPerFlow float64
	source         string
	detail         string
}

func (p *pattern) pickExporter(r *rand.Rand) *exporterPattern {
	if exp := p.exporters.pick(r); exp != nil {
		return exp
	}
	return fallbackExporters()[0]
}

func (p *pattern) portFor(r *rand.Rand, proto uint8) int {
	if set, ok := p.ports[proto]; ok && !set.empty() {
		return set.pick(r)
	}
	if port := p.anyPort.pick(r); port > 0 {
		return port
	}
	return 443
}

func (p *pattern) describe() string {
	desc := fmt.Sprintf("%s | exporters %d, protocols %d, hosts %d/%d, ports %d, %.0f B/pkt, %.1f pkt/flow",
		p.source, len(p.exporters.items), len(p.protocols.items),
		len(p.srcHosts.items), len(p.dstHosts.items), len(p.anyPort.items),
		p.bytesPerPacket, p.packetsPerFlow)
	if p.detail != "" {
		desc += " (" + p.detail + ")"
	}
	return desc
}

// loadPattern samples a database, falling back to built-in defaults for any
// section that is missing or empty so the command always produces usable data.
func loadPattern(path string, cfg config.Config) *pattern {
	pat := fallbackPattern(cfg)
	if path == "" {
		pat.source = "built-in defaults (no pattern database given)"
		return pat
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		pat.source = fmt.Sprintf("built-in defaults (%v)", err)
		return pat
	}
	if _, err := os.Stat(abs); err != nil {
		pat.source = fmt.Sprintf("built-in defaults (%s not found)", path)
		return pat
	}

	db, err := openReadOnly(abs)
	if err != nil {
		pat.source = fmt.Sprintf("built-in defaults (%v)", err)
		return pat
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var notes []string
	if exporters := loadExporters(ctx, db); len(exporters) > 0 {
		pat.exporters = newPicker(exporters)
		notes = append(notes, fmt.Sprintf("%d exporters", len(exporters)))
	}
	if protocols := loadProtocols(ctx, db); len(protocols) > 0 {
		pat.protocols = newPicker(protocols)
	}
	if hosts, err := loadHosts(ctx, db); err == nil {
		if !hosts.src.empty() {
			pat.srcHosts = hosts.src
		}
		if !hosts.dst.empty() {
			pat.dstHosts = hosts.dst
		}
	} else {
		log.Printf("seed: reading hosts from pattern: %v", err)
	}
	if ports, anyPort, err := loadPorts(ctx, db); err == nil {
		if anyPort != nil && !anyPort.empty() {
			pat.ports, pat.anyPort = ports, anyPort
		}
	} else {
		log.Printf("seed: reading ports from pattern: %v", err)
	}
	if curve, ok := loadHourCurve(ctx, db); ok {
		pat.hourCurve = curve
	}
	if bpp, ppf, ok := loadRatios(ctx, db); ok {
		pat.bytesPerPacket, pat.packetsPerFlow = bpp, ppf
	}
	pat.source = abs
	if len(notes) > 0 {
		pat.detail = strings.Join(notes, ", ")
	}
	return pat
}

func openReadOnly(path string) (*sql.DB, error) {
	// A live database keeps its -shm file, so a plain read-only connection
	// works and stays safe while the collector writes. A copy that was closed
	// (or taken with `.backup`) has no -shm, and SQLite refuses to open a WAL
	// database read-only without one, so retry there with immutable=1.
	db, err := openSQLite(path, false)
	if err == nil {
		return db, nil
	}
	immutable, immutableErr := openSQLite(path, true)
	if immutableErr != nil {
		return nil, err
	}
	return immutable, nil
}

func openSQLite(path string, immutable bool) (*sql.DB, error) {
	q := url.Values{}
	q.Add("mode", "ro")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "query_only(1)")
	if immutable {
		q.Add("immutable", "1")
	}
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: q.Encode()}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// loadExporters reads the exporter and interface mix. Interfaces are weighted
// by flow count per direction so the busiest interfaces stay busiest.
func loadExporters(ctx context.Context, db *sql.DB) []weighted[*exporterPattern] {
	type ifaceRow struct {
		ifIndex   uint32
		direction string
		flows     float64
	}
	byExporter := map[string]*exporterPattern{}
	order := []string{}

	rows, err := db.QueryContext(ctx, `
		SELECT exporter, SUM(flows) FROM agg_exporter
		GROUP BY exporter ORDER BY 2 DESC LIMIT 16`)
	if err == nil {
		for rows.Next() {
			var address string
			var flows float64
			if err := rows.Scan(&address, &flows); err != nil {
				break
			}
			if flows <= 0 || strings.TrimSpace(address) == "" {
				continue
			}
			if _, ok := byExporter[address]; !ok {
				byExporter[address] = &exporterPattern{address: address}
				order = append(order, address)
			}
			byExporter[address].weight += flows
		}
		_ = rows.Close()
	}

	if len(order) > 0 {
		ingress := map[string][]weighted[uint32]{}
		egress := map[string][]weighted[uint32]{}
		rows, err := db.QueryContext(ctx, `
			SELECT exporter, if_index, direction, SUM(flows) FROM agg_interface
			GROUP BY exporter, if_index, direction ORDER BY 4 DESC LIMIT 800`)
		if err == nil {
			for rows.Next() {
				var address, direction string
				var row ifaceRow
				if err := rows.Scan(&address, &row.ifIndex, &direction, &row.flows); err != nil {
					break
				}
				if row.flows <= 0 {
					continue
				}
				switch direction {
				case model.DirectionIngress:
					ingress[address] = append(ingress[address], weighted[uint32]{value: row.ifIndex, weight: row.flows})
				case model.DirectionEgress:
					egress[address] = append(egress[address], weighted[uint32]{value: row.ifIndex, weight: row.flows})
				}
			}
			_ = rows.Close()
		}
		for _, address := range order {
			exp := byExporter[address]
			exp.ingress = newPicker(ingress[address])
			exp.egress = newPicker(egress[address])
			if exp.ingress.empty() {
				exp.ingress = newPicker(defaultIngress)
			}
			if exp.egress.empty() {
				exp.egress = newPicker(defaultEgress)
			}
		}
	}

	out := make([]weighted[*exporterPattern], 0, len(order))
	for _, address := range order {
		exp := byExporter[address]
		out = append(out, weighted[*exporterPattern]{value: exp, weight: exp.weight})
	}
	return out
}

func loadProtocols(ctx context.Context, db *sql.DB) []weighted[uint8] {
	rows, err := db.QueryContext(ctx, `
		SELECT protocol, SUM(flows) FROM agg_protocol
		GROUP BY protocol ORDER BY 2 DESC LIMIT 16`)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var out []weighted[uint8]
	for rows.Next() {
		var protocol int
		var flows float64
		if err := rows.Scan(&protocol, &flows); err != nil {
			continue
		}
		if protocol < 0 || protocol > 255 || flows <= 0 {
			continue
		}
		out = append(out, weighted[uint8]{value: uint8(protocol), weight: flows})
	}
	return out
}

type hostSets struct {
	src *picker[string]
	dst *picker[string]
}

// loadHosts weights endpoints by bytes, keeping the heavy talkers heavy. The
// limit keeps memory bounded on databases with millions of hosts.
func loadHosts(ctx context.Context, db *sql.DB) (hostSets, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT host, direction, SUM(bytes) FROM agg_host
		WHERE host <> '' GROUP BY host, direction
		ORDER BY 3 DESC LIMIT 8000`)
	if err != nil {
		return hostSets{}, err
	}
	defer func() { _ = rows.Close() }()
	var src, dst []weighted[string]
	for rows.Next() {
		var host, direction string
		var bytes float64
		if err := rows.Scan(&host, &direction, &bytes); err != nil {
			return hostSets{}, err
		}
		if bytes <= 0 {
			continue
		}
		switch direction {
		case model.DirectionSource:
			src = append(src, weighted[string]{value: host, weight: bytes})
		case model.DirectionDest:
			dst = append(dst, weighted[string]{value: host, weight: bytes})
		}
	}
	if err := rows.Err(); err != nil {
		return hostSets{}, err
	}
	return hostSets{src: newPicker(src), dst: newPicker(dst)}, nil
}

// loadPorts keeps the destination-port mix per protocol, which is what makes
// the protocol and port filters agree with each other.
func loadPorts(ctx context.Context, db *sql.DB) (map[uint8]*picker[int], *picker[int], error) {
	rows, err := db.QueryContext(ctx, `
		SELECT protocol, dst_port, COUNT(*) FROM flows
		WHERE dst_port > 0 GROUP BY protocol, dst_port
		ORDER BY 3 DESC LIMIT 600`)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()

	byProto := map[uint8][]weighted[int]{}
	var all []weighted[int]
	for rows.Next() {
		var protocol, port int
		var n float64
		if err := rows.Scan(&protocol, &port, &n); err != nil {
			return nil, nil, err
		}
		if port <= 0 || port > 65535 || n <= 0 {
			continue
		}
		if protocol >= 0 && protocol <= 255 {
			byProto[uint8(protocol)] = append(byProto[uint8(protocol)], weighted[int]{value: port, weight: n})
		}
		all = append(all, weighted[int]{value: port, weight: n})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	out := make(map[uint8]*picker[int], len(byProto))
	for proto, items := range byProto {
		out[proto] = newPicker(items)
	}
	return out, newPicker(all), nil
}

// loadHourCurve turns the per-hour flow counts into a 0..1 daily shape. Hours
// the pattern never covered are interpolated so a short sample still yields a
// believable week.
func loadHourCurve(ctx context.Context, db *sql.DB) ([24]float64, bool) {
	rows, err := db.QueryContext(ctx, `
		SELECT CAST(strftime('%H', hour_ms/1000, 'unixepoch') AS INTEGER), SUM(flows)
		FROM agg_exporter GROUP BY 1`)
	if err != nil {
		return [24]float64{}, false
	}
	defer func() { _ = rows.Close() }()

	var samples [24]float64
	seen := 0
	for rows.Next() {
		var hour int
		var flows float64
		if err := rows.Scan(&hour, &flows); err != nil {
			continue
		}
		if hour < 0 || hour > 23 || flows <= 0 {
			continue
		}
		samples[hour] = flows
		seen++
	}
	if seen < 3 {
		return [24]float64{}, false
	}
	return normalizeCurve(samples), true
}

// normalizeCurve fills gaps by linear interpolation between the nearest known
// hours (wrapping around midnight), scales to the peak hour, and keeps a floor
// so quiet hours still render a line.
func normalizeCurve(samples [24]float64) [24]float64 {
	var out [24]float64
	known := make([]int, 0, 24)
	for hour, v := range samples {
		if v > 0 {
			known = append(known, hour)
		}
	}
	if len(known) == 0 {
		for i := range out {
			out[i] = 1
		}
		return out
	}
	sort.Ints(known)
	for hour := 0; hour < 24; hour++ {
		out[hour] = interpolate(samples, known, hour)
	}
	peak := 0.0
	for _, v := range out {
		if v > peak {
			peak = v
		}
	}
	if peak <= 0 {
		for i := range out {
			out[i] = 1
		}
		return out
	}
	for i, v := range out {
		scaled := v / peak
		if scaled < 0.12 {
			scaled = 0.12
		}
		out[i] = math.Round(scaled*1000) / 1000
	}
	return out
}

func interpolate(samples [24]float64, known []int, hour int) float64 {
	if samples[hour] > 0 {
		return samples[hour]
	}
	prev, next := known[len(known)-1]-24, known[0]+24
	for _, k := range known {
		if k < hour {
			prev = k
		}
		if k > hour && k < next {
			next = k
		}
	}
	before := sampleAt(samples, prev)
	after := sampleAt(samples, next)
	span := float64(next - prev)
	return before + (after-before)*(float64(hour-prev)/span)
}

// sampleAt resolves an hour that may sit outside 0..23 because interpolation
// works on an unwrapped day (midnight is both hour 24 and hour 0).
func sampleAt(samples [24]float64, hour int) float64 {
	return samples[((hour%24)+24)%24]
}

// loadRatios reads the average packet size and flow size, which anchor the
// generated byte and packet columns to the same units as the real data.
func loadRatios(ctx context.Context, db *sql.DB) (float64, float64, bool) {
	var bytes, packets, flows float64
	err := db.QueryRowContext(ctx, `SELECT SUM(bytes), SUM(packets), COUNT(*) FROM flows`).Scan(&bytes, &packets, &flows)
	if err != nil || bytes <= 0 || packets <= 0 || flows <= 0 {
		return 0, 0, false
	}
	bpp := bytes / packets
	ppf := packets / flows
	if bpp < 40 || bpp > 9000 || ppf < 1 {
		return 0, 0, false
	}
	return bpp, ppf, true
}

// ---------------------------------------------------------------------------
// Fallbacks
// ---------------------------------------------------------------------------

var (
	defaultIngress = []weighted[uint32]{
		{value: 2, weight: 45}, {value: 1, weight: 20}, {value: 7, weight: 12},
		{value: 10, weight: 8}, {value: 13, weight: 6}, {value: 0, weight: 5},
		{value: 14, weight: 2}, {value: 12, weight: 2},
	}
	defaultEgress = []weighted[uint32]{
		{value: 2, weight: 40}, {value: 1, weight: 22}, {value: 7, weight: 14},
		{value: 10, weight: 9}, {value: 13, weight: 7}, {value: 3, weight: 4},
		{value: 12, weight: 2}, {value: 11, weight: 2},
	}
	defaultProtocols = []weighted[uint8]{
		{value: 6, weight: 73}, {value: 17, weight: 23}, {value: 1, weight: 3}, {value: 2, weight: 1},
	}
	defaultPorts = []weighted[int]{
		{value: 443, weight: 55}, {value: 80, weight: 12}, {value: 53, weight: 10},
		{value: 22, weight: 6}, {value: 123, weight: 4}, {value: 8080, weight: 3},
		{value: 9000, weight: 2}, {value: 1900, weight: 2}, {value: 8443, weight: 2},
	}
)

func fallbackExporters() []*exporterPattern {
	addr := "10.10.10.1"
	return []*exporterPattern{{
		address: addr,
		weight:  1,
		ingress: newPicker(defaultIngress),
		egress:  newPicker(defaultEgress),
	}}
}

func fallbackPattern(cfg config.Config) *pattern {
	exps := fallbackExporters()
	weightedExps := make([]weighted[*exporterPattern], 0, len(exps))
	for _, exp := range exps {
		weightedExps = append(weightedExps, weighted[*exporterPattern]{value: exp, weight: exp.weight})
	}

	// Hosts default to a spread over the VLAN subnets the config knows about so
	// the VLAN column is not empty on a fresh checkout.
	var src, dst []weighted[string]
	for i := 0; i < 120; i++ {
		subnet := 2 + (i%7)*10
		host := fmt.Sprintf("10.10.%d.%d", subnet, 10+i%200)
		src = append(src, weighted[string]{value: host, weight: float64(120 - i)})
	}
	for i := 0; i < 400; i++ {
		host := fmt.Sprintf("10.10.%d.%d", 10+(i%5)*20, 2+i%250)
		dst = append(dst, weighted[string]{value: host, weight: float64(400 - i)})
	}
	for i := 0; i < 40; i++ {
		host := fmt.Sprintf("203.0.113.%d", i+1)
		dst = append(dst, weighted[string]{value: host, weight: float64(40 - i)})
		src = append(src, weighted[string]{value: host, weight: float64(40 - i)})
	}
	var curve [24]float64
	for hour := range curve {
		// Quiet nights, a quiet afternoon dip, busy evenings.
		switch {
		case hour >= 1 && hour <= 6:
			curve[hour] = 0.18
		case hour >= 7 && hour <= 8:
			curve[hour] = 0.55
		case hour >= 9 && hour <= 16:
			curve[hour] = 0.85
		case hour >= 17 && hour <= 22:
			curve[hour] = 1
		default:
			curve[hour] = 0.5
		}
	}

	return &pattern{
		exporters:      newPicker(weightedExps),
		protocols:      newPicker(defaultProtocols),
		srcHosts:       newPicker(src),
		dstHosts:       newPicker(dst),
		ports:          map[uint8]*picker[int]{6: newPicker(defaultPorts), 17: newPicker(defaultPorts)},
		anyPort:        newPicker(defaultPorts),
		hourCurve:      curve,
		bytesPerPacket: 500,
		packetsPerFlow: 36,
		detail:         "hardcoded homelab mix",
	}
}
