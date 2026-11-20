// Package dns provides a bounded reverse-DNS lookup cache.
package dns

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type cacheEntry struct {
	name    string
	expires time.Time
}

type inFlight struct {
	done chan struct{}
	name string
}

// Stats is a snapshot of the DNS cache.
type Stats struct {
	Entries    int `json:"entries"`
	MaxEntries int `json:"max_entries"`
	TTLSeconds int `json:"ttl_seconds"`
}

// LookupFunc performs a reverse lookup and returns PTR names.
type LookupFunc func(context.Context, string) ([]string, error)

// Resolver resolves IP addresses to names and caches both positive and
// negative results for a configured TTL.
type Resolver struct {
	mu       sync.Mutex
	entries  map[string]cacheEntry
	order    []string
	inFlight map[string]*inFlight

	ttl     time.Duration
	timeout time.Duration
	max     int
	now     func() time.Time
	lookup  func(context.Context, string) ([]string, error)
	hits    atomic.Uint64
	misses  atomic.Uint64
}

// New returns a resolver with the given cache TTL, entry cap, and lookup
// timeout. A nil or non-positive timeout uses two seconds.
func New(ttl time.Duration, max int, timeout time.Duration) *Resolver {
	return NewWithLookup(ttl, max, timeout, defaultLookup)
}

// NewWithLookup is New with an injectable lookup function, primarily for tests.
func NewWithLookup(ttl time.Duration, max int, timeout time.Duration, lookup LookupFunc) *Resolver {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	if max < 1 {
		max = 10000
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Resolver{
		entries:  make(map[string]cacheEntry),
		inFlight: make(map[string]*inFlight),
		ttl:      ttl,
		timeout:  timeout,
		max:      max,
		now:      time.Now,
		lookup:   lookup,
	}
}

func defaultLookup(ctx context.Context, ip string) ([]string, error) {
	return net.DefaultResolver.LookupAddr(ctx, ip)
}

// Stats returns the number of live entries after dropping expired ones.
func (r *Resolver) Stats() Stats {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	live := r.order[:0]
	for _, ip := range r.order {
		entry, ok := r.entries[ip]
		if !ok {
			continue
		}
		if now.Before(entry.expires) {
			live = append(live, ip)
			continue
		}
		delete(r.entries, ip)
	}
	r.order = live
	return Stats{Entries: len(r.entries), MaxEntries: r.max, TTLSeconds: int(r.ttl / time.Second)}
}

// Flush empties the cache and returns the number of entries removed.
func (r *Resolver) Flush() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.entries)
	r.entries = make(map[string]cacheEntry)
	r.order = nil
	return n
}

// Lookup returns a cached name when the last lookup is newer than the TTL.
// Failures are cached as empty names so an unreachable resolver is not hit on
// every request.
func (r *Resolver) Lookup(ctx context.Context, ip string) string {
	name, _ := r.LookupWithCache(ctx, ip)
	return name
}

// LookupWithCache reports whether a live cache entry served the request.
func (r *Resolver) LookupWithCache(ctx context.Context, ip string) (string, bool) {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return "", false
	}
	now := r.now()
	r.mu.Lock()
	if e, ok := r.entries[ip]; ok && now.Before(e.expires) {
		name := e.name
		r.mu.Unlock()
		r.hits.Add(1)
		return name, true
	}
	r.misses.Add(1)
	if call, ok := r.inFlight[ip]; ok {
		r.mu.Unlock()
		select {
		case <-call.done:
			return call.name, false
		case <-ctx.Done():
			return "", false
		}
	}
	call := &inFlight{done: make(chan struct{})}
	r.inFlight[ip] = call
	r.mu.Unlock()

	lookupCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	names, err := r.lookup(lookupCtx, ip)
	name := ""
	if err == nil {
		for _, candidate := range names {
			candidate = strings.TrimSuffix(strings.TrimSpace(candidate), ".")
			if candidate != "" {
				name = candidate
				break
			}
		}
	}

	r.mu.Lock()
	// A cancelled caller did not obtain a definitive negative DNS result.
	if ctx.Err() != nil {
		delete(r.inFlight, ip)
		close(call.done)
		r.mu.Unlock()
		return "", false
	}
	if _, existed := r.entries[ip]; !existed {
		r.order = append(r.order, ip)
	}
	r.entries[ip] = cacheEntry{name: name, expires: r.now().Add(r.ttl)}
	for len(r.order) > r.max {
		oldest := r.order[0]
		r.order = r.order[1:]
		if _, ok := r.entries[oldest]; ok {
			delete(r.entries, oldest)
		}
	}
	call.name = name
	delete(r.inFlight, ip)
	close(call.done)
	r.mu.Unlock()
	return name, false
}

// LookupMany resolves a set of addresses concurrently.
func (r *Resolver) LookupMany(ctx context.Context, ips []string) map[string]string {
	unique := make([]string, 0, len(ips))
	seen := make(map[string]bool, len(ips))
	for _, ip := range ips {
		ip = strings.TrimSpace(ip)
		if ip == "" || seen[ip] {
			continue
		}
		seen[ip] = true
		unique = append(unique, ip)
	}
	names := make([]string, len(unique))
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
loop:
	for i, ip := range unique {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		wg.Add(1)
		go func(i int, ip string) {
			defer wg.Done()
			defer func() { <-sem }()
			names[i] = r.Lookup(ctx, ip)
		}(i, ip)
	}
	wg.Wait()
	out := make(map[string]string, len(unique))
	for i, ip := range unique {
		out[ip] = names[i]
	}
	return out
}

// MatchLiteral returns addresses whose literal IP value contains term
// (case-insensitive). This also works when reverse DNS is disabled.
func MatchLiteral(ips []string, term string) []string {
	needle := strings.ToLower(strings.TrimSpace(term))
	if needle == "" {
		return nil
	}
	out := make([]string, 0)
	seen := make(map[string]bool, len(ips))
	for _, ip := range ips {
		if seen[ip] {
			continue
		}
		seen[ip] = true
		if strings.Contains(strings.ToLower(ip), needle) {
			out = append(out, ip)
		}
	}
	return out
}

// MatchIPs returns addresses whose literal IP value or resolved name contains
// term (case-insensitive).
func (r *Resolver) MatchIPs(ctx context.Context, ips []string, term string) []string {
	needle := strings.ToLower(strings.TrimSpace(term))
	if needle == "" {
		return nil
	}
	names := r.LookupMany(ctx, ips)
	out := MatchLiteral(ips, needle)
	seen := make(map[string]bool, len(out))
	for _, ip := range out {
		seen[ip] = true
	}
	for _, ip := range ips {
		if seen[ip] {
			continue
		}
		if name := names[ip]; name != "" && strings.Contains(strings.ToLower(name), needle) {
			out = append(out, ip)
			seen[ip] = true
		}
	}
	return out
}
