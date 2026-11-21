package ingest

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a-bali/nfvis/internal/config"
	dnsresolver "github.com/a-bali/nfvis/internal/dns"
	"github.com/a-bali/nfvis/internal/model"
	"github.com/a-bali/nfvis/internal/store"
)

type dnsJob struct {
	id   int64
	flow model.Flow
}

// DNSStats are process-local asynchronous enrichment diagnostics.
type DNSStats struct {
	Enabled         bool    `json:"enabled"`
	QueueDepth      int     `json:"queue_depth"`
	QueueCapacity   int     `json:"queue_capacity"`
	Dropped         uint64  `json:"dropped"`
	Completed       uint64  `json:"completed"`
	CacheHits       uint64  `json:"cache_hits"`
	CacheMisses     uint64  `json:"cache_misses"`
	Named           uint64  `json:"named"`
	Unresolved      uint64  `json:"unresolved"`
	Errors          uint64  `json:"errors"`
	AvgLagMS        float64 `json:"avg_lag_ms"`
	MaxLagMS        int64   `json:"max_lag_ms"`
	Entries         int     `json:"entries"`
	MaxEntries      int     `json:"max_entries"`
	TTLSeconds      int     `json:"ttl_seconds"`
	LookupTimeoutMS int     `json:"lookup_timeout_ms"`
	Server          string  `json:"server"`
}
type dnsWorker struct {
	resolver                                                         *dnsresolver.Resolver
	jobs                                                             chan dnsJob
	wg                                                               sync.WaitGroup
	dropped, completed, hits, misses, named, unresolved, errors, lag atomic.Uint64
	maxLag                                                           atomic.Int64
	timeout                                                          int
	server                                                           string
	ctx                                                              context.Context
	cancel                                                           context.CancelFunc
}

func (c *Collector) initDNS() {
	if !c.cfg.DNS.Enabled {
		return
	}
	server, err := config.DNSDialAddress(c.cfg.DNS.Server)
	if err != nil {
		// Validate() rejects this at startup; a Collector built directly in a
		// test must not silently fall back to the system resolver.
		c.log.Error("invalid dns.server, reverse DNS disabled", "server", c.cfg.DNS.Server, "error", err)
		return
	}
	d := &dnsWorker{resolver: dnsresolver.NewWithServer(c.cfg.DNS.CacheTTL.Duration(), c.cfg.DNS.CacheSize, c.cfg.DNS.LookupTimeout.Duration(), server), jobs: make(chan dnsJob, c.cfg.Decoder.QueueSize), timeout: int(c.cfg.DNS.LookupTimeout.Duration().Milliseconds()), server: server}
	d.ctx, d.cancel = context.WithCancel(context.Background())
	c.dns = d
	for i := 0; i < 8; i++ {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			for job := range d.jobs {
				if d.ctx.Err() != nil {
					d.dropped.Add(1)
					continue
				}
				for _, endpoint := range []struct{ ip, dir string }{{job.flow.SrcAddr, "source"}, {job.flow.DstAddr, "destination"}} {
					if endpoint.ip == "" {
						continue
					}
					name, hit := d.resolver.LookupWithCache(d.ctx, endpoint.ip)
					if d.ctx.Err() != nil {
						d.dropped.Add(1)
						break
					}
					completed := time.Now().UnixMilli()
					lag := completed - job.flow.ReceivedAtMS
					if lag < 0 {
						lag = 0
					}
					end := job.flow.EndMS
					if end <= 0 {
						end = job.flow.ReceivedAtMS
					}
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					err := c.store.SaveDNS(ctx, store.DNSObservation{ID: job.id, IP: endpoint.ip, Direction: endpoint.dir, Name: name, HourMS: (end / 3600000) * 3600000, CompletedMS: completed})
					cancel()
					if err != nil {
						d.errors.Add(1)
						c.log.Warn("DNS observation write failed", "error", err)
						continue
					}
					d.completed.Add(1)
					d.lag.Add(uint64(lag))
					for old := d.maxLag.Load(); lag > old; old = d.maxLag.Load() {
						if d.maxLag.CompareAndSwap(old, lag) {
							break
						}
					}
					if hit {
						d.hits.Add(1)
					} else {
						d.misses.Add(1)
					}
					if name == "" {
						d.unresolved.Add(1)
					} else {
						d.named.Add(1)
					}
				}
			}
		}()
	}
}

// queueDNS enqueues one lookup job per stored row, pairing each row id with the
// record that produced it. ids is positional against flows (see
// store.InsertResult), so a merged record's lookup is attached to the row that
// absorbed it instead of shifting every later record onto its neighbour's row.
// It returns the number of distinct rows that were enqueued.
func (c *Collector) queueDNS(ids []int64, flows []model.Flow) int {
	if c.dns == nil {
		return 0
	}
	if len(ids) != len(flows) {
		// A caller that cannot pair the two must not guess: a lookup written
		// against the wrong row persists a foreign hostname on it.
		c.log.Warn("DNS queue skipped: row ids do not align with the batch",
			"ids", len(ids), "flows", len(flows))
		return 0
	}
	queued := 0
	for i, id := range ids {
		if id == 0 {
			continue
		}
		queued++
		select {
		case c.dns.jobs <- dnsJob{id, flows[i]}:
		default:
			c.dns.dropped.Add(1)
		}
	}
	return queued
}
func (c *Collector) DNSStats() DNSStats {
	d := c.dns
	if d == nil {
		return DNSStats{}
	}
	s := d.resolver.Stats()
	n := d.completed.Load()
	avg := float64(0)
	if n > 0 {
		avg = float64(d.lag.Load()) / float64(n)
	}
	return DNSStats{Enabled: true, QueueDepth: len(d.jobs), QueueCapacity: cap(d.jobs), Dropped: d.dropped.Load(), Completed: n, CacheHits: d.hits.Load(), CacheMisses: d.misses.Load(), Named: d.named.Load(), Unresolved: d.unresolved.Load(), Errors: d.errors.Load(), AvgLagMS: avg, MaxLagMS: d.maxLag.Load(), Entries: s.Entries, MaxEntries: s.MaxEntries, TTLSeconds: s.TTLSeconds, LookupTimeoutMS: d.timeout, Server: d.server}
}
func (c *Collector) FlushDNS() int {
	if c.dns == nil {
		return 0
	}
	return c.dns.resolver.Flush()
}
