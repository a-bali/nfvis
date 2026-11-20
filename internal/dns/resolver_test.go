package dns

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestResolverUsesCachedResultUntilTTL(t *testing.T) {
	r := New(5*time.Minute, 10, time.Second)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return now }
	calls := 0
	r.lookup = func(context.Context, string) ([]string, error) {
		calls++
		if calls == 1 {
			return []string{"old.example."}, nil
		}
		return []string{"new.example."}, nil
	}

	if got := r.Lookup(context.Background(), "192.0.2.1"); got != "old.example" {
		t.Fatalf("first lookup = %q", got)
	}
	if got := r.Lookup(context.Background(), "192.0.2.1"); got != "old.example" {
		t.Fatalf("cached lookup = %q", got)
	}
	if calls != 1 {
		t.Fatalf("lookup calls = %d, want 1", calls)
	}

	now = now.Add(5 * time.Minute)
	if got := r.Lookup(context.Background(), "192.0.2.1"); got != "new.example" {
		t.Fatalf("lookup after TTL = %q", got)
	}
	if calls != 2 {
		t.Fatalf("lookup calls after TTL = %d, want 2", calls)
	}
}

func TestResolverStatsAndFlush(t *testing.T) {
	r := New(time.Minute, 10, time.Second)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return now }
	r.lookup = func(_ context.Context, ip string) ([]string, error) {
		return []string{ip + ".example."}, nil
	}

	r.Lookup(context.Background(), "192.0.2.1")
	r.Lookup(context.Background(), "192.0.2.2")
	if got := r.Stats(); got.Entries != 2 || got.MaxEntries != 10 || got.TTLSeconds != 60 {
		t.Fatalf("stats = %+v", got)
	}

	now = now.Add(2 * time.Minute)
	if got := r.Stats(); got.Entries != 0 {
		t.Fatalf("stats after TTL = %+v, want 0 entries", got)
	}

	r.Lookup(context.Background(), "192.0.2.3")
	if got := r.Flush(); got != 1 {
		t.Fatalf("Flush removed %d entries, want 1", got)
	}
	if got := r.Stats().Entries; got != 0 {
		t.Fatalf("entries after flush = %d, want 0", got)
	}
}

func TestResolverCachesFailuresAndMatchesNames(t *testing.T) {
	r := New(5*time.Minute, 10, time.Second)
	var calls atomic.Int32
	r.lookup = func(_ context.Context, ip string) ([]string, error) {
		calls.Add(1)
		if ip == "192.0.2.1" {
			return []string{"router.lan."}, nil
		}
		return nil, errors.New("nxdomain")
	}
	ips := []string{"192.0.2.1", "198.51.100.9", "203.0.113.7"}

	if got := r.MatchIPs(context.Background(), ips, "ROUTER"); len(got) != 1 || got[0] != "192.0.2.1" {
		t.Fatalf("name matches = %v", got)
	}
	if got := r.MatchIPs(context.Background(), ips, "203.0.113.7"); len(got) != 1 || got[0] != "203.0.113.7" {
		t.Fatalf("literal IP matches = %v", got)
	}
	if calls.Load() != 3 {
		t.Fatalf("lookup calls = %d, want 3", calls.Load())
	}
	if got := r.Lookup(context.Background(), "198.51.100.9"); got != "" {
		t.Fatalf("negative cache lookup = %q", got)
	}
	if calls.Load() != 3 {
		t.Fatalf("negative cache should not re-resolve: calls=%d", calls.Load())
	}
}

func TestMatchLiteralIPSubstring(t *testing.T) {
	ips := []string{"192.168.1.2", "10.10.10.1", "2001:db8::1"}
	got := MatchLiteral(ips, "10.10")
	if len(got) != 1 || got[0] != "10.10.10.1" {
		t.Fatalf("MatchLiteral = %v", got)
	}
	if got := MatchLiteral(ips, "DB8"); len(got) != 1 || got[0] != "2001:db8::1" {
		t.Fatalf("IPv6 substring = %v", got)
	}
}
