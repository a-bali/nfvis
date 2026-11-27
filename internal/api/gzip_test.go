package api

import (
	"bytes"
	"compress/gzip"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/decoder"
	"github.com/a-bali/nfvis/internal/ingest"
	"github.com/a-bali/nfvis/internal/store"
)

func gzipServer(t *testing.T, body string, status int) *httptest.Server {
	t.Helper()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	})
	ts := httptest.NewServer(gzipMiddleware(h))
	t.Cleanup(ts.Close)
	return ts
}

// handlerTestServer builds the real API handler over an empty database, so a
// test can assert on middleware that Handler() installs rather than on the
// middleware in isolation.
func handlerTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	cfg.DNS.Enabled = false
	cfg.Listen.UDP = "127.0.0.1:0"
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	coll := ingest.New(cfg, decoder.New(cfg), st, &ingest.Counters{}, log)
	if err := coll.Start(); err != nil {
		t.Fatalf("start collector: %v", err)
	}
	t.Cleanup(func() { _ = coll.Stop() })
	ts := httptest.NewServer(New(cfg, st, coll, log, "test-version").Handler())
	t.Cleanup(ts.Close)
	return ts
}

func get(t *testing.T, url string, header map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, b
}

// TestGzipCompressesLargeBodies is the point of the middleware: the dashboard's
// Overview refresh transferred 113 KB per poll with no Content-Encoding at all.
func TestGzipCompressesLargeBodies(t *testing.T) {
	body := strings.Repeat(`{"bucket_ms":1795627500000,"bytes":123456789},`, 400)
	ts := gzipServer(t, body, http.StatusOK)

	resp, got := get(t, ts.URL, map[string]string{"Accept-Encoding": "gzip"})
	if enc := resp.Header.Get("Content-Encoding"); enc != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", enc)
	}
	zr, err := gzip.NewReader(bytes.NewReader(got))
	if err != nil {
		t.Fatalf("response is not valid gzip: %v", err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	if string(plain) != body {
		t.Fatalf("decompressed body differs: %d bytes vs %d", len(plain), len(body))
	}
	if len(got) >= len(body)/2 {
		t.Errorf("compressed %d bytes from %d: expected a substantial saving", len(got), len(body))
	}
}

// TestGzipSkippedWithoutAcceptEncoding is the correctness floor: a client that
// did not ask for gzip must receive plain bytes, whatever the body size.
func TestGzipSkippedWithoutAcceptEncoding(t *testing.T) {
	body := strings.Repeat("x", 8192)
	ts := gzipServer(t, body, http.StatusOK)

	resp, got := get(t, ts.URL, nil)
	if enc := resp.Header.Get("Content-Encoding"); enc != "" {
		t.Fatalf("Content-Encoding = %q for a client that did not negotiate", enc)
	}
	if string(got) != body {
		t.Fatalf("body altered: %d bytes, want %d", len(got), len(body))
	}
}

// TestGzipLeavesSmallBodiesAlone stops the middleware from adding framing to
// the hundreds-of-bytes replies (health, exporters) it would only inflate.
func TestGzipLeavesSmallBodiesAlone(t *testing.T) {
	body := `{"exporters":[]}`
	ts := gzipServer(t, body, http.StatusOK)

	resp, got := get(t, ts.URL, map[string]string{"Accept-Encoding": "gzip"})
	if enc := resp.Header.Get("Content-Encoding"); enc != "" {
		t.Fatalf("small body was compressed (Content-Encoding %q)", enc)
	}
	if string(got) != body {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

// TestGzipHonoursDeclaredContentLength covers a handler that sets Content-Length
// itself: a declared small body must not be compressed just because the writer
// had not flushed yet.
func TestGzipHonoursDeclaredContentLength(t *testing.T) {
	body := `{"status":"ok"}`
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "15")
		_, _ = io.WriteString(w, body)
	})
	ts := httptest.NewServer(gzipMiddleware(h))
	defer ts.Close()

	resp, got := get(t, ts.URL, map[string]string{"Accept-Encoding": "gzip"})
	if enc := resp.Header.Get("Content-Encoding"); enc != "" {
		t.Fatalf("declared-small body was compressed (Content-Encoding %q)", enc)
	}
	if string(got) != body {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

// TestGzipSetsVary is the cache-safety property. Without Vary: Accept-Encoding a
// shared cache may hand a gzipped body to a client that cannot decode it, and
// the symptom shows up as a garbled page behind a proxy, not as a test failure.
func TestGzipSetsVary(t *testing.T) {
	body := strings.Repeat("y", 4096)
	ts := gzipServer(t, body, http.StatusOK)

	resp, got := get(t, ts.URL, map[string]string{"Accept-Encoding": "gzip"})
	if got := resp.Header.Get("Vary"); !strings.Contains(strings.ToLower(got), "accept-encoding") {
		t.Fatalf("Vary = %q, want it to mention Accept-Encoding", got)
	}
	// A Content-Length may still be present, but only one that describes the
	// bytes actually sent. What must not survive is a length declared for the
	// *uncompressed* body: it would truncate the response or make the client
	// wait for bytes that never arrive.
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		if cl != strconv.Itoa(len(got)) {
			t.Fatalf("Content-Length = %q but %d bytes were sent", cl, len(got))
		}
	}
}

// TestGzipDropsHandlerContentLength is the specific hazard the previous test
// only implies: a handler that sets Content-Length and writes more than the
// compression threshold must not have that original length forwarded.
func TestGzipDropsHandlerContentLength(t *testing.T) {
	body := strings.Repeat(`{"k":"v"},`, 500)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = io.WriteString(w, body)
	})
	ts := httptest.NewServer(gzipMiddleware(h))
	defer ts.Close()

	resp, got := get(t, ts.URL, map[string]string{"Accept-Encoding": "gzip"})
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("large declared body was not compressed")
	}
	if cl := resp.Header.Get("Content-Length"); cl == strconv.Itoa(len(body)) {
		t.Fatalf("Content-Length still declares the uncompressed size %d", len(body))
	} else if cl != "" && cl != strconv.Itoa(len(got)) {
		t.Fatalf("Content-Length = %q but %d bytes were sent", cl, len(got))
	}
}

// TestHandlerServesCompressedAssets is the wiring test, and the only one that
// fails if the middleware is not installed on the real handler. The other tests
// call gzipMiddleware directly, so they pin its behaviour but would still pass
// on a server that never used it — the exact way a compression "fix" ships
// without compressing anything.
//
// It uses the embedded JS bundle: a ~786 KB body that is guaranteed to exist in
// any build, so the assertion is on a real served asset rather than a fixture.
func TestHandlerServesCompressedAssets(t *testing.T) {
	ts := handlerTestServer(t)

	index, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	page, err := io.ReadAll(index.Body)
	_ = index.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	asset := firstAssetPath(string(page))
	if asset == "" {
		t.Skip("embedded index.html names no /assets/ bundle")
	}

	req, err := http.NewRequest(http.MethodGet, ts.URL+asset, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if enc := resp.Header.Get("Content-Encoding"); enc != "gzip" {
		t.Fatalf("%s: Content-Encoding = %q, want gzip (middleware not wired into Handler)", asset, enc)
	}
	if got := resp.Header.Get("Cache-Control"); !strings.Contains(got, "immutable") {
		t.Fatalf("%s: Cache-Control = %q, want an immutable cached asset", asset, got)
	}
	if got := resp.Header.Get("Vary"); !strings.Contains(strings.ToLower(got), "accept-encoding") {
		t.Fatalf("%s: Vary = %q, want Accept-Encoding", asset, got)
	}
}

// TestHandlerKeepsIndexRevalidating is the other half of the caching contract:
// the document must never be cached immutably, or a deploy is invisible until
// the user clears their cache.
func TestHandlerKeepsIndexRevalidating(t *testing.T) {
	ts := handlerTestServer(t)

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if got := resp.Header.Get("Cache-Control"); strings.Contains(got, "immutable") {
		t.Fatalf("index Cache-Control = %q; the document must revalidate", got)
	}
}

func firstAssetPath(html string) string {
	const marker = "/assets/"
	i := strings.Index(html, marker)
	if i < 0 {
		return ""
	}
	rest := html[i:]
	if j := strings.IndexAny(rest, `"'`); j > 0 {
		return rest[:j]
	}
	return ""
}

func TestGzipPreservesStatus(t *testing.T) {
	body := strings.Repeat(`{"error":"boom"},`, 300)
	ts := gzipServer(t, body, http.StatusInternalServerError)

	resp, _ := get(t, ts.URL, map[string]string{"Accept-Encoding": "gzip"})
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
}
