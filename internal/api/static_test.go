package api

import (
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/decoder"
	"github.com/a-bali/nfvis/internal/ingest"
	"github.com/a-bali/nfvis/internal/store"
	"github.com/a-bali/nfvis/internal/webui"
)

func TestEmbeddedDashboardAssets(t *testing.T) {
	assets, err := webui.FS()
	if err != nil {
		t.Fatal(err)
	}
	index, err := fs.ReadFile(assets, "index.html")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(index), `id="root"`) {
		t.Fatalf("embedded index is still a placeholder: %s", index)
	}
	entries, err := fs.ReadDir(assets, "assets")
	if err != nil || len(entries) == 0 {
		t.Fatalf("no built dashboard assets: %v", err)
	}

	cfg := config.Default()
	cfg.Database.Path = filepath.Join(t.TempDir(), "nfvis.db")
	st, err := store.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	coll := ingest.New(cfg, decoder.New(cfg), st, &ingest.Counters{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s := New(cfg, st, coll, slog.New(slog.NewTextHandler(io.Discard, nil)), "test-version")

	for _, path := range []string{"/", "/assets/" + entries[0].Name(), "/some/client/route"} {
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d", path, rec.Code)
		}
		if rec.Body.Len() == 0 {
			t.Fatalf("%s: empty body", path)
		}
	}
}
