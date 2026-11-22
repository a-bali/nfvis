package config

import (
	"strings"
	"testing"
)

// TestPragmaValidation covers the bounds on the two new SQLite memory settings.
// They are validated at startup so a typo fails fast rather than surfacing as
// mysterious memory behaviour or a silently ignored setting.
func TestPragmaValidation(t *testing.T) {
	base := Default()
	base.Listen.UDP = "127.0.0.1:0"

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // substring; empty means valid
	}{
		{"defaults valid", func(c *Config) {}, ""},
		{"mmap 0 disables and is allowed", func(c *Config) { c.Database.MmapSizeMB = intPtr(0) }, ""},
		{"mmap 256 valid", func(c *Config) { c.Database.MmapSizeMB = intPtr(256) }, ""},
		{"mmap negative rejected", func(c *Config) { c.Database.MmapSizeMB = intPtr(-1) }, "mmap_size_mb"},
		{"mmap above ceiling rejected", func(c *Config) { c.Database.MmapSizeMB = intPtr(99999) }, "mmap_size_mb"},
		{"cache 16 valid", func(c *Config) { c.Database.CacheSizeMB = intPtr(16) }, ""},
		{"cache 0 rejected", func(c *Config) { c.Database.CacheSizeMB = intPtr(0) }, "cache_size_mb"},
		{"cache too large rejected", func(c *Config) { c.Database.CacheSizeMB = intPtr(99999) }, "cache_size_mb"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected an error mentioning %q, got none", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err, tc.wantErr)
			}
		})
	}
}

// TestPragmaDefaultsMaterialised checks that Default() supplies pointers rather
// than leaving them nil, so a caller that only reads Effective* gets the
// documented defaults and an explicit zero stays distinguishable from absence.
func TestPragmaDefaultsMaterialised(t *testing.T) {
	c := Default()
	if c.Database.MmapSizeMB == nil {
		t.Fatal("MmapSizeMB is nil in Default(); expected the default to be materialised")
	}
	if c.Database.CacheSizeMB == nil {
		t.Fatal("CacheSizeMB is nil in Default(); expected the default to be materialised")
	}
	if got, want := c.Database.EffectiveMmapSizeMB(), DefaultMmapSizeMB; got != want {
		t.Errorf("EffectiveMmapSizeMB = %d, want %d", got, want)
	}
	if got, want := c.Database.EffectiveCacheSizeMB(), DefaultCacheSizeMB; got != want {
		t.Errorf("EffectiveCacheSizeMB = %d, want %d", got, want)
	}

	// An explicit zero must survive as zero: disabling mmap is a real choice.
	c.Database.MmapSizeMB = intPtr(0)
	if got := c.Database.EffectiveMmapSizeMB(); got != 0 {
		t.Errorf("explicit mmap_size_mb=0 became %d; zero must disable mmap", got)
	}
}
