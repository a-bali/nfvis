package dns

import (
	"context"
	"testing"
	"time"

	"github.com/a-bali/nfvis/internal/testutil"
)

// A configured server must be the one queried: the answer comes from the stub
// even though the host resolver has no such name.
func TestServerLookupUsesConfiguredServer(t *testing.T) {
	srv, err := testutil.StartDNSServer("printer.lan")
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	r := NewWithServer(5*time.Minute, 10, 2*time.Second, srv.Addr)
	got := r.Lookup(context.Background(), "192.0.2.55")
	if got != "printer.lan" {
		t.Fatalf("lookup via configured server = %q, want printer.lan", got)
	}
	queries := srv.Queries()
	if len(queries) != 1 || queries[0] != "55.2.0.192.in-addr.arpa." {
		t.Fatalf("server queries = %v", queries)
	}
}

// An empty server keeps the host resolver, so New and NewWithServer agree.
func TestNewWithServerEmptyUsesHostResolver(t *testing.T) {
	withServer := NewWithServer(time.Minute, 10, time.Second, "")
	plain := New(time.Minute, 10, time.Second)
	if withServer.ttl != plain.ttl || withServer.max != plain.max || withServer.timeout != plain.timeout {
		t.Fatalf("empty server changed the resolver settings: %+v vs %+v", withServer, plain)
	}
}

// An unreachable configured server must fail the lookup rather than fall back
// to the host resolver, and the failure is cached like any other.
func TestServerLookupUnreachableServerFails(t *testing.T) {
	r := NewWithServer(5*time.Minute, 10, 500*time.Millisecond, "127.0.0.1:1")
	if got := r.Lookup(context.Background(), "192.0.2.77"); got != "" {
		t.Fatalf("lookup with unreachable server = %q, want empty", got)
	}
	if got := r.Stats().Entries; got != 1 {
		t.Fatalf("negative result should be cached: entries = %d", got)
	}
}
