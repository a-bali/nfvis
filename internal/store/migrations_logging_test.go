package store

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/a-bali/nfvis/internal/config"
)

// syncBuffer is a bytes.Buffer safe for concurrent writes from the migrator.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs routes the default slog logger into a buffer for the duration of
// fn. It restores the previous logger even if fn fails.
func captureLogs(t *testing.T, fn func()) string {
	t.Helper()
	buf := &syncBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)
	fn()
	return buf.String()
}

// TestMigrationLogsProgress is the guard against silent migrations. Migrations
// run before the HTTP listener starts, so without these lines a five-minute
// index build is indistinguishable from a hung process: the pod is deaf on its
// health port and emits nothing. That ambiguity cost a production release.
//
// This test fails against the pre-fix code, which logged nothing at all.
func TestMigrationLogsProgress(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(dir, "migrate-log.db")

	out := captureLogs(t, func() {
		st, err := Open(cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})

	// A fresh database migrates from 0 to schemaVersion, so we expect the
	// opening line, one line per migration, and the completion summary.
	for _, want := range []string{
		"applying database migrations",
		"migration budget",
		"applying migration",
		"migration applied",
		"database migrations complete",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in migration logs:\n%s", want, out)
		}
	}
	// The completion line must carry the final schema and the elapsed time,
	// because "how long did it take" is the question the operator actually has.
	if !strings.Contains(out, "schema="+itoa(int64(schemaVersion))) {
		t.Errorf("completion line missing final schema %d:\n%s", schemaVersion, out)
	}
	if !strings.Contains(out, "elapsed=") {
		t.Errorf("no elapsed timing in migration logs:\n%s", out)
	}
	// Every migration must be named, so an overrun can be attributed to a version.
	for v := 1; v <= schemaVersion; v++ {
		if !strings.Contains(out, "version="+itoa(int64(v))) {
			t.Errorf("migration %d never logged:\n%s", v, out)
		}
	}
}

// TestMigrationLogsCurrentSchema verifies the already-migrated path still emits
// exactly one informative line rather than silence: an operator restarting an
// up-to-date instance should be able to see that no work was needed.
func TestMigrationLogsCurrentSchema(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Database.Path = filepath.Join(dir, "migrate-current.db")

	st, err := Open(cfg)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	out := captureLogs(t, func() {
		st, err := Open(cfg)
		if err != nil {
			t.Fatalf("second open: %v", err)
		}
		if err := st.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	})

	if !strings.Contains(out, "database schema is current") {
		t.Errorf("expected an up-to-date notice, got:\n%s", out)
	}
	if strings.Contains(out, "applying migration") {
		t.Errorf("up-to-date database should not apply migrations:\n%s", out)
	}
}
