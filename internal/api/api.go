// Package api serves the JSON read API and embedded dashboard.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/ingest"
	"github.com/a-bali/nfvis/internal/store"
	"github.com/a-bali/nfvis/internal/webui"
)

// timeseriesTimeout bounds a single chart request. Filtered charts return
// partial points plus a limitation instead of failing when it expires.
const timeseriesTimeout = 15 * time.Second

// Server holds API dependencies.
type Server struct {
	cfg       config.Config
	store     *store.Store
	collector *ingest.Collector
	log       *slog.Logger
	version   string

	server *http.Server
}

// New creates an API server.
func New(cfg config.Config, st *store.Store, collector *ingest.Collector, log *slog.Logger, version string) *Server {

	a := &Server{cfg: cfg, store: st, collector: collector, log: log, version: version}
	a.server = &http.Server{Addr: cfg.Listen.HTTP, Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second}
	return a
}

// Handler returns the complete HTTP handler.
func (a *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", a.health)
	mux.HandleFunc("GET /api/v1/system", a.system)
	mux.HandleFunc("POST /api/v1/system/dns/cache", a.flushDNSCache)
	mux.HandleFunc("POST /api/v1/system/retention", a.runRetention)
	mux.HandleFunc("GET /api/v1/coverage", a.coverage)
	mux.HandleFunc("GET /api/v1/config", a.config)
	mux.HandleFunc("GET /api/v1/exporters", a.exporters)
	mux.HandleFunc("GET /api/v1/timeseries", a.timeseries)
	mux.HandleFunc("GET /api/v1/top", a.top)
	mux.HandleFunc("GET /api/v1/flows", a.flows)
	mux.Handle("/", a.static())
	return a.recoverMiddleware(mux)
}

// ListenAndServe starts the configured HTTP listener.
func (a *Server) ListenAndServe() error {
	a.log.Info("dashboard listening", "http", a.cfg.Listen.HTTP)
	return a.server.ListenAndServe()
}

// Shutdown gracefully stops the HTTP listener.
func (a *Server) Shutdown(ctx context.Context) error { return a.server.Shutdown(ctx) }

func (a *Server) recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				a.log.Error("HTTP handler panic", "panic", v, "path", r.URL.Path)
				writeError(w, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (a *Server) static() http.Handler {
	assets, err := webui.FS()
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "embedded UI unavailable", http.StatusInternalServerError)
		})
	}
	fileServer := http.FileServer(http.FS(assets))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			writeError(w, http.StatusNotFound, "not_found", "unknown API endpoint")
			return
		}
		if r.URL.Path != "/" {
			if _, err := fs.Stat(assets, strings.TrimPrefix(r.URL.Path, "/")); err != nil {
				r2 := r.Clone(r.Context())
				r2.URL.Path = "/"
				fileServer.ServeHTTP(w, r2)
				return
			}
		}
		w.Header().Set("Cache-Control", "no-cache")
		fileServer.ServeHTTP(w, r)
	})
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type errorResponse struct {
	Error errorBody `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: errorBody{Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (a *Server) parseRange(r *http.Request) (store.Range, error) {
	now := time.Now().UTC()
	to := now
	from := now.Add(-time.Hour)
	var err error
	if raw := r.URL.Query().Get("to"); raw != "" {
		to, err = parseTime(raw, now)
		if err != nil {
			return store.Range{}, fmt.Errorf("to: %w", err)
		}
	}
	if raw := r.URL.Query().Get("from"); raw != "" {
		from, err = parseTime(raw, now)
		if err != nil {
			return store.Range{}, fmt.Errorf("from: %w", err)
		}
	}
	if !from.Before(to) {
		return store.Range{}, fmt.Errorf("from must be before to")
	}
	if to.Sub(from) > 367*24*time.Hour {
		return store.Range{}, fmt.Errorf("range cannot exceed 367 days")
	}
	return store.Range{FromMS: from.UnixMilli(), ToMS: to.UnixMilli()}, nil
}

func parseTime(raw string, now time.Time) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "now" {
		return now, nil
	}
	if strings.HasPrefix(raw, "-") {
		d, err := time.ParseDuration(raw[1:])
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid relative time %q", raw)
		}
		return now.Add(-d), nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid RFC3339 time %q", raw)
	}
	return t.UTC(), nil
}

func filtersFrom(r *http.Request) map[string]string {
	filters := make(map[string]string)
	for _, key := range []string{"exporter", "host", "src_host", "dst_host", "interface", "vlan", "input_interface", "output_interface", "input_vlan", "output_vlan", "protocol", "sampled", "src_port", "dst_port"} {
		if v := strings.TrimSpace(r.URL.Query().Get(key)); v != "" {
			filters[key] = v
		}
	}
	return filters
}

func validateFilters(filters map[string]string) error {
	for _, key := range []string{"vlan", "input_vlan", "output_vlan"} {
		if v, ok := filters[key]; ok {
			if _, err := strconv.ParseInt(v, 10, 32); err != nil {
				return fmt.Errorf("%s must be an integer (-1 means unknown)", key)
			}
		}
	}
	if v, ok := filters["protocol"]; ok {
		n, err := strconv.ParseUint(v, 10, 8)
		if err != nil || n > 255 {
			return fmt.Errorf("protocol must be 0-255")
		}
	}
	for _, key := range []string{"src_port", "dst_port"} {
		if v, ok := filters[key]; ok {
			if _, err := strconv.ParseUint(v, 10, 16); err != nil {
				return fmt.Errorf("%s must be 0-65535", key)
			}
		}
	}
	for _, key := range []string{"interface", "input_interface", "output_interface"} {
		if v, ok := filters[key]; ok {
			if _, err := strconv.ParseUint(v, 10, 32); err != nil && strings.TrimSpace(v) == "" {
				return fmt.Errorf("%s must be an index or label", key)
			}
		}
	}
	if v, ok := filters["sampled"]; ok {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "false", "1", "0":
		default:
			return fmt.Errorf("sampled must be true or false")
		}
	}
	if v, ok := filters["direction"]; ok {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "ingress", "egress", "source", "destination":
		default:
			return fmt.Errorf("direction must be ingress, egress, source, or destination")
		}
	}
	return nil
}

func parseIntParam(r *http.Request, name string, fallback int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	return v, nil
}

type systemResponse struct {
	StartedAt     string          `json:"started_at"`
	UptimeSeconds float64         `json:"uptime_seconds"`
	Database      store.Stats     `json:"database"`
	DNS           ingest.DNSStats `json:"dns"`
	Ingest        ingest.Snapshot `json:"ingest"`
	Retention     struct {
		RawSeconds             float64 `json:"raw_seconds"`
		HourlySeconds          float64 `json:"hourly_seconds"`
		CleanupIntervalSeconds float64 `json:"cleanup_interval_seconds"`
	} `json:"retention"`
	Sweep ingest.RetentionState `json:"sweep"`
}

type healthResponse struct {
	Version       string          `json:"version"`
	Status        string          `json:"status"`
	StartedAt     string          `json:"started_at"`
	UptimeSeconds float64         `json:"uptime_seconds"`
	HTTPAddr      string          `json:"http_addr"`
	UDPAddr       string          `json:"udp_addr"`
	Counters      ingest.Snapshot `json:"counters"`
	Database      store.Stats     `json:"database"`
	Coverage      store.Coverage  `json:"coverage"`
	Warnings      []string        `json:"warnings"`
}

func (a *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stats, dbErr := a.store.Stats(ctx)
	cov, covErr := a.store.Coverage(ctx)
	counters := a.collector.Counters().Snapshot()
	warnings := []string{}
	status := "ok"
	if dbErr != nil || covErr != nil || !counters.Healthy {
		status = "degraded"
	}
	if counters.LastError != "" {
		warnings = append(warnings, counters.LastError)
	}
	if counters.UDPQueueDrops > 0 {
		warnings = append(warnings, "UDP packets were dropped because the decode queue was full")
	}
	if dbErr != nil {
		warnings = append(warnings, dbErr.Error())
	}
	udpAddr := ""
	if c := a.collector.Addr(); c != nil {
		udpAddr = c.String()
	}
	writeJSON(w, http.StatusOK, healthResponse{
		Version: a.version, Status: status, StartedAt: time.Now().Add(-a.collector.Uptime()).UTC().Format(time.RFC3339),
		UptimeSeconds: a.collector.Uptime().Seconds(), HTTPAddr: a.cfg.Listen.HTTP, UDPAddr: udpAddr,
		Counters: counters, Database: stats, Coverage: cov, Warnings: warnings,
	})
}

func (a *Server) dnsStats() ingest.DNSStats { return a.collector.DNSStats() }

func (a *Server) system(w http.ResponseWriter, r *http.Request) {
	stats, err := a.store.Stats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database", err.Error())
		return
	}
	var response systemResponse
	response.StartedAt = time.Now().Add(-a.collector.Uptime()).UTC().Format(time.RFC3339)
	response.UptimeSeconds = a.collector.Uptime().Seconds()
	response.Database = stats
	response.DNS = a.dnsStats()
	response.Ingest = a.collector.Counters().Snapshot()
	response.Retention.RawSeconds = time.Duration(a.cfg.Database.Retention.Raw).Seconds()
	response.Retention.HourlySeconds = time.Duration(a.cfg.Database.Retention.Hourly).Seconds()
	response.Retention.CleanupIntervalSeconds = time.Duration(a.cfg.Database.Retention.CleanupInterval).Seconds()
	response.Sweep = a.collector.RetentionStatus()
	writeJSON(w, http.StatusOK, response)
}

// retentionTriggerTimeout bounds how long a manual sweep request waits for the
// sweep to finish before the caller is told it is still running.
const retentionTriggerTimeout = 60 * time.Second

// runRetention runs a retention sweep now instead of waiting for the ticker.
func (a *Server) runRetention(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), retentionTriggerTimeout)
	defer cancel()
	state, err := a.collector.RunRetention(ctx)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		// The sweep keeps running; report the in-progress state rather than a
		// failure, because nothing went wrong on the server side.
		writeJSON(w, http.StatusAccepted, map[string]interface{}{"sweep": state})
		return
	}
	if err != nil {
		a.log.Error("manual retention sweep failed", "error", err)
		writeError(w, http.StatusInternalServerError, "retention", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"sweep": state})
}

func (a *Server) flushDNSCache(w http.ResponseWriter, _ *http.Request) {
	if !a.cfg.DNS.Enabled {
		writeError(w, http.StatusConflict, "dns_disabled", "reverse DNS is disabled")
		return
	}
	flushed := a.collector.FlushDNS()
	writeJSON(w, http.StatusOK, map[string]interface{}{"flushed": flushed, "dns": a.dnsStats()})
}

func (a *Server) coverage(w http.ResponseWriter, r *http.Request) {
	cov, err := a.store.Coverage(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, cov)
}

func (a *Server) config(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"listen":            a.cfg.Listen,
		"database":          map[string]interface{}{"path": a.cfg.Database.Path, "retention": a.cfg.Database.Retention},
		"decoder":           a.cfg.Decoder,
		"exporters":         a.cfg.Exporters,
		"vlan_rules":        a.cfg.VLANRules,
		"authentication":    false,
		"udp_authenticated": false,
		"security_note":     "UDP is unauthenticated. Restrict network access. HTTP has no built-in auth or TLS.",
	})
}

type exporterInfo struct {
	Address    string            `json:"address"`
	Label      string            `json:"label,omitempty"`
	Interfaces map[string]string `json:"interfaces"`
	Observed   bool              `json:"observed"`
}

func (a *Server) exporters(w http.ResponseWriter, r *http.Request) {
	// The hourly table carries every exporter seen in the last year and has a
	// handful of rows per exporter, so it answers this without scanning the raw
	// flows (which the dashboard polls every few seconds).
	observed, err := a.observedExporters(r.Context(), `SELECT DISTINCT exporter FROM agg_exporter`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database", err.Error())
		return
	}

	out := make([]exporterInfo, 0, len(a.cfg.Exporters)+len(observed))
	for _, cfg := range a.cfg.Exporters {
		addr := strings.TrimSpace(cfg.Address)
		ifaces := make(map[string]string, len(cfg.Interfaces))
		for k, v := range cfg.Interfaces {
			ifaces[strconv.FormatUint(uint64(k), 10)] = v
		}
		out = append(out, exporterInfo{Address: addr, Label: cfg.Label, Interfaces: ifaces, Observed: observed[addr]})
		delete(observed, addr)
	}
	for addr := range observed {
		out = append(out, exporterInfo{Address: addr, Interfaces: map[string]string{}, Observed: true})
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"exporters": out})
}

// observedExporters collects distinct exporter addresses, falling back to the
// raw flows when the aggregate tables are still empty.
func (a *Server) observedExporters(ctx context.Context, aggregateQuery string) (map[string]bool, error) {
	observed := make(map[string]bool)
	rows, err := a.store.DB().QueryContext(ctx, aggregateQuery)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			_ = rows.Close()
			return nil, err
		}
		observed[addr] = true
	}
	_ = rows.Close()
	if len(observed) == 0 {
		rows, err := a.store.DB().QueryContext(ctx, `SELECT DISTINCT exporter FROM flows`)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var addr string
			if err := rows.Scan(&addr); err != nil {
				_ = rows.Close()
				return nil, err
			}
			observed[addr] = true
		}
		_ = rows.Close()
	}
	return observed, nil
}

func (a *Server) timeseries(w http.ResponseWriter, r *http.Request) {
	tr, err := a.parseRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	bucket, err := parseIntParam(r, "bucket_seconds", 0)
	if err != nil || bucket < 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "bucket_seconds must be a non-negative integer")
		return
	}
	filters := filtersFrom(r)
	if err := validateFilters(filters); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	splitBy := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("split_by")))
	if splitBy != "" && !store.ValidSplitBy(splitBy) {
		writeError(w, http.StatusBadRequest, "bad_request",
			"split_by must be one of "+strings.Join(store.SplitByDimensions(), ", "))
		return
	}
	metric := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("metric")))
	switch metric {
	case "", "bytes", "packets", "flows":
	default:
		writeError(w, http.StatusBadRequest, "bad_request", "metric must be bytes, packets, or flows")
		return
	}
	// Filtered charts scan raw flows and can be expensive on wide ranges. The
	// store returns partial points plus a limitation when this budget expires.
	ctx, cancel := context.WithTimeout(r.Context(), timeseriesTimeout)
	defer cancel()
	q := store.Query{
		Range: tr, BucketMS: int64(bucket) * 1000, Filters: filters,
		SplitBy: splitBy, Metric: metric,
	}
	if err := a.resolveQueryHostTerms(ctx, &q); err != nil {
		a.log.Error("timeseries host query failed", "error", err)
		writeError(w, http.StatusInternalServerError, "database", "timeseries host query failed")
		return
	}
	if splitBy != "" {
		result, err := a.store.TimeseriesSplit(ctx, q)
		if err != nil {
			a.log.Error("stacked timeseries query failed", "error", err)
			writeError(w, http.StatusInternalServerError, "database", "stacked timeseries query failed")
			return
		}
		a.labelSplitSeries(ctx, &result)
		writeJSON(w, http.StatusOK, result)
		return
	}
	result, err := a.store.Timeseries(ctx, q)
	if err != nil {
		a.log.Error("timeseries query failed", "error", err)
		writeError(w, http.StatusInternalServerError, "database", "timeseries query failed")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *Server) top(w http.ResponseWriter, r *http.Request) {
	tr, err := a.parseRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	dimension := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("dimension")))
	if dimension == "" {
		dimension = "exporter"
	}
	if !store.ValidDimension(dimension) {
		writeError(w, http.StatusBadRequest, "bad_request", "dimension must be exporter, interface, host, vlan, or protocol")
		return
	}
	limit, err := parseIntParam(r, "limit", 20)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	offset, err := parseIntParam(r, "offset", 0)
	if err != nil || limit < 1 || limit > 1000 || offset < 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "limit must be 1-1000 and offset non-negative")
		return
	}
	filters := filtersFrom(r)
	if err := validateFilters(filters); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	sortKey, sortDir, err := parseSort(r, map[string]bool{"key": true, "label": true, "bytes": true, "packets": true, "flows": true, "share": true}, "bytes")
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	search := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search")))
	if search != "" && dimension != "host" {
		writeError(w, http.StatusBadRequest, "bad_request", "search is only supported for hosts")
		return
	}
	q := store.Query{
		Range: tr, Dimension: dimension, Direction: r.URL.Query().Get("direction"),
		Limit: limit, Offset: offset, Filters: filters, AllRows: true,
	}
	result, err := a.store.Top(r.Context(), q)
	if err != nil {
		a.log.Error("top query failed", "error", err)
		writeError(w, http.StatusInternalServerError, "database", "top query failed")
		return
	}
	a.applyLabels(&result, dimension)
	if dimension == "host" {
		direction := q.Direction
		if direction != "destination" {
			direction = "source"
		}
		names, err := a.store.HistoricalNames(r.Context(), tr, direction)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "database", "historical DNS query failed")
			return
		}
		for i := range result.Rows {
			found := names[result.Rows[i].Key]
			if len(found) > 0 {
				result.Rows[i].Label = strings.Join(found, ", ")
			}
		}
	}
	if search != "" {
		filtered := result.Rows[:0]
		for _, row := range result.Rows {
			if strings.Contains(strings.ToLower(row.Key), search) || strings.Contains(strings.ToLower(row.Label), search) {
				filtered = append(filtered, row)
			}
		}
		result.Rows = filtered
	}
	sort.Slice(result.Rows, func(i, j int) bool {
		a, b := result.Rows[i], result.Rows[j]
		cmp := 0
		switch sortKey {
		case "key":
			cmp = strings.Compare(strings.ToLower(a.Key), strings.ToLower(b.Key))
		case "label":
			cmp = strings.Compare(strings.ToLower(a.Label), strings.ToLower(b.Label))
		case "packets":
			cmp = compareUint(a.Packets, b.Packets)
		case "flows":
			cmp = compareUint(a.Flows, b.Flows)
		default:
			cmp = compareUint(a.Bytes, b.Bytes)
		}
		if cmp == 0 {
			return a.Key < b.Key
		}
		if sortDir == "desc" {
			return cmp > 0
		}
		return cmp < 0
	})
	result.Total = len(result.Rows)
	if offset >= len(result.Rows) {
		result.Rows = []store.TopRow{}
	} else {
		end := offset + limit
		if end > len(result.Rows) {
			end = len(result.Rows)
		}
		result.Rows = result.Rows[offset:end]
	}
	writeJSON(w, http.StatusOK, result)
}

var protocolNames = map[int32]string{
	0:   "IPv6 Hop-by-Hop",
	1:   "ICMP",
	2:   "IGMP",
	4:   "IPv4",
	6:   "TCP",
	17:  "UDP",
	41:  "IPv6",
	47:  "GRE",
	50:  "ESP",
	51:  "AH",
	58:  "ICMPv6",
	89:  "OSPF",
	132: "SCTP",
}

func protocolLabel(key string) string {
	v, err := strconv.ParseInt(strings.TrimSpace(key), 10, 32)
	if err != nil {
		return ""
	}
	return protocolNames[int32(v)]
}

func (a *Server) applyLabels(result *store.TopResult, dimension string) {
	byAddr := make(map[string]config.Exporter, len(a.cfg.Exporters))
	for _, e := range a.cfg.Exporters {
		byAddr[strings.TrimSpace(e.Address)] = e
	}
	for i := range result.Rows {
		row := &result.Rows[i]
		if dimension == "exporter" {
			if e, ok := byAddr[row.Key]; ok && e.Label != "" {
				row.Label = e.Label
			}
			continue
		}
		if dimension == "protocol" {
			row.Label = protocolLabel(row.Key)
			continue
		}
		if dimension == "vlan" {
			vlan, err := strconv.ParseInt(strings.TrimSpace(row.Key), 10, 32)
			if err == nil && vlan >= 1 && vlan <= 4094 {
				row.Label = a.cfg.VLANLabels[int32(vlan)]
				if row.Label == "" {
					row.Label = fmt.Sprintf("VLAN %d", vlan)
				}
			}
			continue
		}
		if dimension != "interface" {
			continue
		}
		idx, addr, ok := strings.Cut(row.Key, "@")
		if !ok {
			continue
		}
		if e, ok := byAddr[addr]; ok {
			if label := e.Interfaces[parseUint32(idx)]; label != "" {
				row.Label = label
			}
		}
	}
}

func (a *Server) labelSplitSeries(ctx context.Context, result *store.SplitResult) {
	for i := range result.Series {
		band := &result.Series[i]
		if band.Other {
			band.Label = "Other"
			continue
		}
		switch result.SplitBy {
		case "protocol":
			band.Label = protocolLabel(band.Key)
		case "input_vlan", "output_vlan":
			vlan, err := strconv.ParseInt(strings.TrimSpace(band.Key), 10, 32)
			if err != nil || vlan < 1 || vlan > 4094 {
				band.Label = "unknown"
				continue
			}
			band.Label = a.cfg.VLANLabels[int32(vlan)]
			if band.Label == "" {
				band.Label = fmt.Sprintf("VLAN %d", vlan)
			}
		}
	}
	if result.SplitBy != "src_host" && result.SplitBy != "dst_host" {
		return
	}
	direction := "source"
	if result.SplitBy == "dst_host" {
		direction = "destination"
	}
	names, err := a.store.HistoricalNames(ctx, result.Range, direction)
	if err != nil {
		return
	}
	for i := range result.Series {
		if found := names[result.Series[i].Key]; len(found) > 0 && !result.Series[i].Other {
			result.Series[i].Label = strings.Join(found, ", ")
		}
	}
}

func compareUint(a, b uint64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func parseSort(r *http.Request, allowed map[string]bool, defaultKey string) (string, string, error) {
	key := r.URL.Query().Get("sort")
	if key == "" {
		key = defaultKey
	}
	dir := r.URL.Query().Get("order")
	if dir == "" {
		dir = "desc"
	}
	if !allowed[key] || (dir != "asc" && dir != "desc") {
		return "", "", fmt.Errorf("invalid sort or order")
	}
	return key, dir, nil
}

func (a *Server) resolveQueryHostTerms(_ context.Context, _ *store.Query) error { return nil }

func ipLikeTerm(term string) bool {
	t := strings.TrimSpace(term)
	if t == "" {
		return false
	}
	for _, r := range t {
		if (r < '0' || r > '9') && r != '.' && r != ':' {
			return false
		}
	}
	return true
}

func parseUint32(s string) uint32 {
	v, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0
	}
	return uint32(v)
}

func (a *Server) flows(w http.ResponseWriter, r *http.Request) {
	tr, err := a.parseRange(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	limit, err := parseIntParam(r, "limit", 100)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	offset, err := parseIntParam(r, "offset", 0)
	if err != nil || limit < 1 || limit > 1000 || offset < 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "limit must be 1-1000 and offset non-negative")
		return
	}
	filters := filtersFrom(r)
	if dir := strings.TrimSpace(r.URL.Query().Get("direction")); dir != "" {
		filters["direction"] = dir
	}
	if err := validateFilters(filters); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	sortKey, sortDir, err := parseSort(r, map[string]bool{"time": true, "interface": true, "vlan": true, "source": true, "destination": true, "proto": true, "bytes": true, "packets": true}, "time")
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	q := store.FlowQuery{Range: tr, Limit: limit, Offset: offset, Filters: filters, Sort: sortKey, SortDir: sortDir}

	page, err := a.store.Flows(r.Context(), q)
	if err != nil {
		a.log.Error("flow query failed", "error", err)
		writeError(w, http.StatusInternalServerError, "database", "flow query failed")
		return
	}
	if !page.DetailAvailable {
		writeJSON(w, http.StatusGone, map[string]interface{}{
			"error":    errorBody{Code: "aggregate_only", Message: "Individual flows for this period are outside raw retention and may no longer be available."},
			"coverage": mustCoverage(r.Context(), a.store),
		})
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func mustCoverage(ctx context.Context, st *store.Store) store.Coverage {
	cov, _ := st.Coverage(ctx)
	return cov
}
