// Package ingest wires UDP reception, decoding, batching, and persistence.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net"
	"net/netip"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/decoder"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
)

// Counters holds collector health counters and bounded, synchronized statistics.
type Counters struct {
	PacketsReceived     atomic.Uint64
	PacketsDecoded      atomic.Uint64
	PacketsMalformed    atomic.Uint64
	PacketsUnsupported  atomic.Uint64
	PacketsOversized    atomic.Uint64
	BytesReceived       atomic.Uint64
	FlowsDecoded        atomic.Uint64
	FlowsStored         atomic.Uint64
	FlowsDuplicates     atomic.Uint64
	FlowsDropped        atomic.Uint64
	UnknownTemplateSets atomic.Uint64
	TemplateRefreshes   atomic.Uint64
	TemplateExpirations atomic.Uint64
	ExporterReboots     atomic.Uint64
	TimestampWraps      atomic.Uint64
	TimestampFallbacks  atomic.Uint64
	SamplingOptions     atomic.Uint64
	UnknownFields       atomic.Uint64
	UDPQueueDrops       atomic.Uint64
	DBErrors            atomic.Uint64
	DiskFullEvents      atomic.Uint64
	RetentionRawDeleted atomic.Uint64
	RetentionAggDeleted atomic.Uint64
	// RetentionTrafficDeleted counts agg_traffic rows removed by retention.
	RetentionTrafficDeleted atomic.Uint64
	TemplatesActive         atomic.Uint64
	LastSuccessMS           atomic.Int64
	LastFlowReceivedMS      atomic.Int64
	Healthy                 atomic.Bool
	lastError               atomic.Value
	metricsMu               sync.Mutex
	recentFlows             [1441]flowMinute
	processingSamples       []float64
	processingNext          int
}

type flowMinute struct {
	minute int64
	flows  uint64
}

// recordReceived tracks decoded flows by collector receipt time, independent of flow timestamps.
func (c *Counters) recordReceived(at time.Time, count uint64) {
	c.metricsMu.Lock()
	defer c.metricsMu.Unlock()
	if count == 0 {
		return
	}
	for previous := c.LastFlowReceivedMS.Load(); at.UnixMilli() > previous; previous = c.LastFlowReceivedMS.Load() {
		if c.LastFlowReceivedMS.CompareAndSwap(previous, at.UnixMilli()) {
			break
		}
	}
	minute := at.Unix() / 60
	slot := &c.recentFlows[minute%int64(len(c.recentFlows))]
	if slot.minute != minute {
		*slot = flowMinute{minute: minute}
	}
	slot.flows += count
}

// recordProcessing samples the amortized database insertion time per flow for each batch.
func (c *Counters) recordProcessing(elapsed time.Duration, count int) {
	if count == 0 {
		return
	}
	c.metricsMu.Lock()
	defer c.metricsMu.Unlock()
	v := float64(elapsed) / float64(time.Millisecond) / float64(count)
	if len(c.processingSamples) < 1024 {
		c.processingSamples = append(c.processingSamples, v)
	} else {
		c.processingSamples[c.processingNext] = v
		c.processingNext = (c.processingNext + 1) % len(c.processingSamples)
	}
}

// Snapshot is the JSON representation of Counters.
type Snapshot struct {
	PacketsReceived         uint64   `json:"packets_received"`
	PacketsDecoded          uint64   `json:"packets_decoded"`
	PacketsMalformed        uint64   `json:"packets_malformed"`
	PacketsUnsupported      uint64   `json:"packets_unsupported"`
	PacketsOversized        uint64   `json:"packets_oversized"`
	BytesReceived           uint64   `json:"bytes_received"`
	FlowsDecoded            uint64   `json:"flows_decoded"`
	FlowsStored             uint64   `json:"flows_stored"`
	FlowsDuplicates         uint64   `json:"flows_duplicates"`
	FlowsDropped            uint64   `json:"flows_dropped"`
	UnknownTemplateSets     uint64   `json:"unknown_template_sets"`
	TemplateRefreshes       uint64   `json:"template_refreshes"`
	TemplateExpirations     uint64   `json:"template_expirations"`
	ExporterReboots         uint64   `json:"exporter_reboots"`
	TimestampWraps          uint64   `json:"timestamp_wraps"`
	TimestampFallbacks      uint64   `json:"timestamp_fallbacks"`
	SamplingOptions         uint64   `json:"sampling_options"`
	UnknownFields           uint64   `json:"unknown_fields"`
	UDPQueueDrops           uint64   `json:"udp_queue_drops"`
	DBErrors                uint64   `json:"db_errors"`
	DiskFullEvents          uint64   `json:"disk_full_events"`
	RetentionRawDeleted     uint64   `json:"retention_raw_deleted"`
	RetentionAggDeleted     uint64   `json:"retention_aggregate_deleted"`
	RetentionTrafficDeleted uint64   `json:"retention_traffic_deleted"`
	TemplatesActive         uint64   `json:"templates_active"`
	LastSuccessMS           int64    `json:"last_successful_insert_ms"`
	LastFlowReceivedMS      int64    `json:"last_flow_received_ms"`
	Healthy                 bool     `json:"ingest_healthy"`
	LastError               string   `json:"last_error,omitempty"`
	Flows5Min               uint64   `json:"flows_5min"`
	Flows1Hour              uint64   `json:"flows_1hour"`
	Flows24Hour             uint64   `json:"flows_24hour"`
	ProcessingAvgMS         *float64 `json:"processing_avg_ms"`
	ProcessingMedianMS      *float64 `json:"processing_median_ms"`
	ProcessingP90MS         *float64 `json:"processing_p90_ms"`
}

// Snapshot copies counters for the API.
func (c *Counters) Snapshot() Snapshot {
	s := Snapshot{
		PacketsReceived: c.PacketsReceived.Load(), PacketsDecoded: c.PacketsDecoded.Load(),
		PacketsMalformed: c.PacketsMalformed.Load(), PacketsUnsupported: c.PacketsUnsupported.Load(),
		PacketsOversized: c.PacketsOversized.Load(), BytesReceived: c.BytesReceived.Load(),
		FlowsDecoded: c.FlowsDecoded.Load(), FlowsStored: c.FlowsStored.Load(),
		FlowsDuplicates: c.FlowsDuplicates.Load(),
		FlowsDropped:    c.FlowsDropped.Load(), UnknownTemplateSets: c.UnknownTemplateSets.Load(),
		TemplateRefreshes: c.TemplateRefreshes.Load(), TemplateExpirations: c.TemplateExpirations.Load(),
		ExporterReboots: c.ExporterReboots.Load(),
		TimestampWraps:  c.TimestampWraps.Load(), TimestampFallbacks: c.TimestampFallbacks.Load(),
		SamplingOptions: c.SamplingOptions.Load(), UnknownFields: c.UnknownFields.Load(),
		UDPQueueDrops: c.UDPQueueDrops.Load(),
		DBErrors:      c.DBErrors.Load(), DiskFullEvents: c.DiskFullEvents.Load(),
		RetentionRawDeleted: c.RetentionRawDeleted.Load(), RetentionAggDeleted: c.RetentionAggDeleted.Load(),
		RetentionTrafficDeleted: c.RetentionTrafficDeleted.Load(),
		TemplatesActive:         c.TemplatesActive.Load(), LastSuccessMS: c.LastSuccessMS.Load(), LastFlowReceivedMS: c.LastFlowReceivedMS.Load(),
		Healthy: c.Healthy.Load(),
	}
	c.metricsMu.Lock()
	nowMinute := time.Now().Unix() / 60
	for _, slot := range c.recentFlows {
		age := nowMinute - slot.minute
		if age >= 0 && age < 1440 {
			s.Flows24Hour += slot.flows
			if age < 60 {
				s.Flows1Hour += slot.flows
			}
			if age < 5 {
				s.Flows5Min += slot.flows
			}
		}
	}
	samples := append([]float64(nil), c.processingSamples...)
	c.metricsMu.Unlock()
	if len(samples) > 0 {
		sort.Float64s(samples)
		var sum float64
		for _, sample := range samples {
			sum += sample
		}
		avg, median, p90 := sum/float64(len(samples)), samples[(len(samples)-1)/2], samples[(len(samples)-1)*90/100]
		s.ProcessingAvgMS, s.ProcessingMedianMS, s.ProcessingP90MS = &avg, &median, &p90
	}
	if v := c.lastError.Load(); v != nil {
		s.LastError, _ = v.(string)
	}
	return s
}

func (c *Counters) setError(err error) {
	if err == nil {
		return
	}
	c.lastError.Store(err.Error())
	c.DBErrors.Add(1)
	c.Healthy.Store(false)
	if store.IsDiskFull(err) {
		c.DiskFullEvents.Add(1)
	}
}

// setSuccess records a successful write. It clears lastError: a transient lock
// or timeout that recovered must not keep being reported. DBErrors is a
// cumulative counter and still shows that something happened, so clearing the
// message loses no history, while leaving it set made a recovered instance
// report an error forever — the UI showed "database is locked" long after the
// last occurrence.
func (c *Counters) setSuccess() {
	c.lastError.Store("")
	c.LastSuccessMS.Store(time.Now().UnixMilli())
	c.Healthy.Store(true)
}

type packetJob struct {
	buf      []byte
	data     []byte
	exporter netip.Addr
	received time.Time
}

// Collector is a single-process NetFlow ingestion pipeline.
type Collector struct {
	cfg      config.Config
	dec      decoder.Decoder
	store    *store.Store
	log      *slog.Logger
	counters *Counters

	conn    *net.UDPConn
	jobs    []chan packetJob
	workers sync.WaitGroup
	flows   chan model.Flow
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	bufPool sync.Pool
	started time.Time
	dns     *dnsWorker

	// Retention schedule and last-run state, read by the system endpoint and
	// updated by the maintenance goroutine.
	retentionMu       sync.Mutex
	retention         RetentionState
	pendingRetention  *retentionRequest
	retentionQueue    chan *retentionRequest
	retentionDebounce time.Duration
}

// New creates a collector. Start opens the UDP socket.
func New(cfg config.Config, dec decoder.Decoder, st *store.Store, counters *Counters, log *slog.Logger) *Collector {
	if counters == nil {
		counters = &Counters{}
	}
	counters.Healthy.Store(true)
	jobs := make([]chan packetJob, cfg.Decoder.Workers)
	for i := range jobs {
		jobs[i] = make(chan packetJob, (cfg.Decoder.QueueSize+len(jobs)-1)/len(jobs))
	}
	return &Collector{
		cfg: cfg, dec: dec, store: st, log: log, counters: counters,
		jobs:    jobs,
		flows:   make(chan model.Flow, cfg.Decoder.QueueSize),
		bufPool: sync.Pool{New: func() interface{} { return make([]byte, 65535) }},
		// A manual sweep request never needs more than one slot; extra callers
		// wait on the same pending request.
		retentionQueue:    make(chan *retentionRequest, 1),
		retentionDebounce: retentionManualDebounce,
	}
}

// Counters returns the live counter set.
func (c *Collector) Counters() *Counters { return c.counters }

// Start begins listening and processing.
func (c *Collector) Start() error {
	addr, err := net.ResolveUDPAddr("udp", c.cfg.Listen.UDP)
	if err != nil {
		return fmt.Errorf("resolve UDP address: %w", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listen UDP: %w", err)
	}
	// A larger socket buffer absorbs short bursts while the decoder works.
	if err := conn.SetReadBuffer(4 << 20); err != nil {
		c.log.Debug("could not enlarge UDP receive buffer", "error", err)
	}
	c.conn = conn
	c.started = time.Now()
	c.initDNS()
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	c.wg.Add(1)
	go c.readLoop(ctx)
	for i := 0; i < c.cfg.Decoder.Workers; i++ {
		c.wg.Add(1)
		c.workers.Add(1)
		go c.decodeWorker(ctx, i)
	}
	c.wg.Add(1)
	go c.batchLoop(ctx)
	c.wg.Add(1)
	go c.maintenanceLoop(ctx)

	c.log.Info("netflow collector listening", "udp", conn.LocalAddr().String(), "workers", c.cfg.Decoder.Workers)
	return nil
}

// Stop drains queued flows, closes the socket, and waits for workers.
func (c *Collector) Stop() error {
	// Closing the socket first unblocks ReadFromUDP; readLoop then closes the
	// worker queues; Stop closes the flow channel after workers drain.
	if c.conn != nil {
		_ = c.conn.Close()
	}
	// Reception closes worker queues; workers drain in order, then the writer flushes.
	c.workers.Wait()
	close(c.flows)
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	if c.dns != nil {
		c.dns.cancel()
		close(c.dns.jobs)
		c.dns.wg.Wait()
	}
	c.log.Info("collector stopped", "flows_stored", c.counters.FlowsStored.Load())
	return nil
}

// Addr returns the bound UDP address, useful for tests using port 0.
func (c *Collector) Addr() *net.UDPAddr {
	if c.conn == nil {
		return nil
	}
	return c.conn.LocalAddr().(*net.UDPAddr)
}

func (c *Collector) readLoop(ctx context.Context) {
	defer c.wg.Done()
	defer func() {
		for _, jobs := range c.jobs {
			close(jobs)
		}
	}()
	for {
		buf := c.bufPool.Get().([]byte)
		n, peer, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			c.bufPool.Put(buf)
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			c.counters.setError(err)
			c.log.Warn("UDP read failed", "error", err)
			return
		}
		addr, ok := netip.AddrFromSlice(peer.IP)
		if !ok {
			c.bufPool.Put(buf)
			c.counters.PacketsMalformed.Add(1)
			continue
		}
		c.counters.PacketsReceived.Add(1)
		c.counters.BytesReceived.Add(uint64(n))
		job := packetJob{buf: buf, data: buf[:n], exporter: addr.Unmap(), received: time.Now().UTC()}
		// Hash by exporter so a template and its data retain arrival order.
		h := fnv.New32a()
		_, _ = h.Write(job.exporter.AsSlice())
		queue := c.jobs[int(h.Sum32())%len(c.jobs)]
		select {
		case queue <- job:
		case <-ctx.Done():
			c.bufPool.Put(buf)
			return
		default:
			c.bufPool.Put(buf)
			c.counters.UDPQueueDrops.Add(1)
		}
	}
}

func (c *Collector) decodeWorker(ctx context.Context, id int) {
	defer c.wg.Done()
	defer c.workers.Done()
	for job := range c.jobs[id] {
		flows, stats, err := c.dec.Decode(job.exporter, job.data, job.received)
		c.bufPool.Put(job.buf)
		c.recordStats(stats)
		c.counters.recordReceived(job.received, uint64(len(flows)))
		if err != nil {
			c.log.Debug("packet decode failed", "error", err, "exporter", job.exporter)
		}
		for _, flow := range flows {
			c.flows <- flow
		}
	}
}

func (c *Collector) recordStats(s decoder.Stats) {
	c.counters.PacketsDecoded.Add(s.PacketsDecoded)
	c.counters.PacketsMalformed.Add(s.PacketsMalformed)
	c.counters.PacketsUnsupported.Add(s.PacketsUnsupported)
	c.counters.PacketsOversized.Add(s.PacketsOversized)
	c.counters.FlowsDecoded.Add(s.FlowsDecoded)
	c.counters.UnknownTemplateSets.Add(s.UnknownTemplateSets)
	c.counters.TemplateRefreshes.Add(s.TemplateRefreshes)
	c.counters.TemplateExpirations.Add(s.TemplateExpirations)
	c.counters.ExporterReboots.Add(s.ExporterReboots)
	c.counters.TimestampWraps.Add(s.TimestampWraps)
	c.counters.TimestampFallbacks.Add(s.TimestampFallbacks)
	c.counters.SamplingOptions.Add(s.SamplingOptions)
	c.counters.UnknownFields.Add(s.UnknownFields)
}

func (c *Collector) batchLoop(ctx context.Context) {
	defer c.wg.Done()
	batch := make([]model.Flow, 0, c.cfg.Database.BatchSize)
	ticker := time.NewTicker(c.cfg.Database.Flush.Duration())
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		started := time.Now()
		// A shutdown must not cancel an in-flight or final database batch.
		flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := c.insertWithRetry(flushCtx, batch)
		cancel()
		c.counters.recordProcessing(time.Since(started), len(batch))
		if err != nil {
			c.counters.FlowsDropped.Add(uint64(len(batch)))
			c.log.Error("dropping flow batch after repeated database failures", "flows", len(batch), "error", err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case flow, ok := <-c.flows:
			if !ok {
				flush()
				return
			}
			batch = append(batch, flow)
			if len(batch) >= c.cfg.Database.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			// Drain anything already produced before shutting down.
			for {
				select {
				case flow, ok := <-c.flows:
					if !ok {
						flush()
						return
					}
					batch = append(batch, flow)
					if len(batch) >= c.cfg.Database.BatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

func (c *Collector) insertWithRetry(ctx context.Context, batch []model.Flow) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		var res store.InsertResult
		res, err = c.store.InsertBatchResult(ctx, batch)
		if err == nil {
			// A batch is not always fully written: duplicate suppression merges the
			// redundant copies. res.IDs is positional against batch, so the row a
			// record ended up in is known exactly — a merged record must be looked
			// up against the row that absorbed it, never against a neighbour's.
			c.queueDNS(res.IDs, batch)
			c.counters.FlowsStored.Add(uint64(res.NewRows))
			if res.Merged > 0 {
				c.counters.FlowsDuplicates.Add(uint64(res.Merged))
			}
			c.counters.setSuccess()
			return nil
		}
		if !store.IsBusy(err) {
			break
		}
		// A BEGIN that waited out busy_timeout is not transient contention: the
		// write lock was held for the entire timeout, so another attempt just pays
		// that full wait again. Retrying it turned one 5s stall into 15s (three
		// attempts, measured) with no better chance of success.
		if errors.Is(err, store.ErrLockWaitExhausted) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 50 * time.Millisecond):
		}
	}
	c.counters.setError(err)
	return err
}

func (c *Collector) maintenanceLoop(ctx context.Context) {
	defer c.wg.Done()
	interval := c.cfg.Database.Retention.CleanupInterval.Duration()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	// Mark the next scheduled run before the first sweep so the API can report
	// a countdown even while the startup sweep is still running.
	next := time.Now().Add(interval)
	c.retentionMu.Lock()
	c.retention.NextRunMS = next.UnixMilli()
	c.retentionMu.Unlock()
	c.runMaintenance(ctx, next)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			next = time.Now().Add(interval)
			c.runMaintenance(ctx, next)
		case req := <-c.retentionQueue:
			// A manual run replaces the pending schedule; the ticker keeps its
			// own cadence so a burst of manual runs cannot postpone the next
			// scheduled sweep.
			next = time.Now().Add(interval)
			c.runMaintenanceRequest(ctx, req, next)
		}
	}
}

// runMaintenance expires templates and sweeps retention, recording the outcome.
func (c *Collector) runMaintenance(ctx context.Context, next time.Time) {
	c.runMaintenanceRequest(ctx, nil, next)
}

func (c *Collector) runMaintenanceRequest(ctx context.Context, req *retentionRequest, next time.Time) {
	expired := c.dec.ExpireTemplates(time.Now())
	c.counters.TemplateExpirations.Add(uint64(expired))
	c.counters.TemplatesActive.Store(uint64(c.dec.TemplateCount()))

	started := time.Now()
	c.beginRetention(started, next)
	res, err := c.store.SweepRetention(ctx, started.UTC())
	finished := time.Now()
	if err != nil {
		c.log.Error("retention sweep failed", "error", err)
		c.counters.setError(err)
		c.endRetention(started, finished, 0, 0, 0, err)
		return
	}
	c.counters.RetentionRawDeleted.Add(uint64(res.RawDeleted))
	c.counters.RetentionAggDeleted.Add(uint64(res.AggregateDeleted))
	c.counters.RetentionTrafficDeleted.Add(uint64(res.TrafficDeleted))
	c.endRetention(started, finished, res.RawDeleted, res.AggregateDeleted, res.TrafficDeleted, nil)
	if res.RawDeleted > 0 || res.AggregateDeleted > 0 || res.TrafficDeleted > 0 {
		c.log.Debug("retention sweep removed rows",
			"raw", res.RawDeleted, "aggregate", res.AggregateDeleted,
			"traffic", res.TrafficDeleted, "elapsed", finished.Sub(started).String())
	}
}

// Uptime returns how long the collector has been running.
func (c *Collector) Uptime() time.Duration {
	if c.started.IsZero() {
		return 0
	}
	return time.Since(c.started)
}
