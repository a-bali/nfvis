package config

import (
	"os"
	"path/filepath"
	"testing"
)

// writeConfig writes a YAML file into a temp dir and returns its path.
func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// TestLoadExplicitZeroSurvivesUnmarshalling is the crux of the pointer-based
// design, and until this test existed nothing exercised it: the unit tests built
// config structs by hand, so a regression in how the loader merges YAML over the
// pre-populated defaults would go unnoticed.
//
// Load starts from Default(), which already has MmapSizeMB = 1024. An explicit
// `mmap_size_mb: 0` in the file must replace that with 0 (disabling mmap), not be
// treated as "absent" and leave the default. That is only possible because the
// field is a *int: with a plain int, absent and 0 are indistinguishable.
func TestLoadExplicitZeroSurvivesUnmarshalling(t *testing.T) {
	path := writeConfig(t, `
listen:
  udp: "127.0.0.1:2055"
  http: "127.0.0.1:8080"
database:
  mmap_size_mb: 0
  cache_size_mb: 16
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Database.MmapSizeMB == nil {
		t.Fatal("MmapSizeMB is nil after loading an explicit 0; the field did not unmarshal")
	}
	if got := cfg.Database.EffectiveMmapSizeMB(); got != 0 {
		t.Errorf("EffectiveMmapSizeMB = %d, want 0 (mmap disabled)", got)
	}
	if got := cfg.Database.EffectiveCacheSizeMB(); got != 16 {
		t.Errorf("EffectiveCacheSizeMB = %d, want 16", got)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("mmap_size_mb: 0 must validate: %v", err)
	}
}

// TestLoadAbsentPragmasKeepDefaults is the counterpart: a file that says nothing
// must leave the upgrade-compatible defaults in place.
func TestLoadAbsentPragmasKeepDefaults(t *testing.T) {
	path := writeConfig(t, `
listen:
  udp: "127.0.0.1:2055"
  http: "127.0.0.1:8080"
database:
  batch_size: 500
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got, want := cfg.Database.EffectiveMmapSizeMB(), DefaultMmapSizeMB; got != want {
		t.Errorf("EffectiveMmapSizeMB = %d, want the default %d", got, want)
	}
	if got, want := cfg.Database.EffectiveCacheSizeMB(), DefaultCacheSizeMB; got != want {
		t.Errorf("EffectiveCacheSizeMB = %d, want the default %d", got, want)
	}
	if cfg.Database.BatchSize != 500 {
		t.Errorf("batch_size = %d, want 500 (the loader must still read known keys)", cfg.Database.BatchSize)
	}
}

// TestLoadRejectsOutOfRangePragmas checks that loading a bad value fails at the
// loader/validator boundary rather than being silently clamped by SQLite.
func TestLoadRejectsOutOfRangePragmas(t *testing.T) {
	// 8192 exceeds the driver's MAX_MMAP_SIZE of 0x7fff0000 (~2047 MiB), which
	// would otherwise be accepted and silently clamped.
	path := writeConfig(t, `
listen:
  udp: "127.0.0.1:2055"
  http: "127.0.0.1:8080"
database:
  mmap_size_mb: 8192
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("mmap_size_mb: 8192 must be rejected: the driver clamps anything above ~2047 MiB")
	}
}
