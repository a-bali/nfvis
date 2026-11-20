package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRejectUnknownConfigField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("listen:\n  htpp: 127.0.0.1:8081\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("unknown field was accepted")
	}
}

func TestRejectEquivalentConflictingPrefixes(t *testing.T) {
	cfg := Default()
	cfg.VLANRules = []VLANRule{{CIDR: "192.0.2.1/24", VLAN: 1}, {CIDR: "192.0.2.2/24", VLAN: 2}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("conflicting equivalent networks were accepted")
	}
}

func TestExampleMatchesDefaults(t *testing.T) {
	const path = "../../config.example.yaml"
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	// Empty YAML lists and nil defaults are equivalent.
	if len(got.Exporters) == 0 {
		got.Exporters = nil
	}
	if len(got.VLANRules) == 0 {
		got.VLANRules = nil
	}
	if want := Default(); !reflect.DeepEqual(got, want) {
		t.Fatalf("example differs from defaults:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestDefaultValidation(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatalf("defaults should validate: %v", err)
	}
	if c.Database.Retention.Raw.Duration() != 30*24*time.Hour {
		t.Fatalf("raw retention default: %v", c.Database.Retention.Raw.Duration())
	}
	if c.Database.Retention.Hourly.Duration() != 365*24*time.Hour {
		t.Fatalf("hourly retention default: %v", c.Database.Retention.Hourly.Duration())
	}
	if !c.IsLoopbackHTTP() {
		t.Fatal("HTTP should default to loopback")
	}
	if !c.DNS.Enabled || c.DNS.CacheTTL.Duration() != 5*time.Minute {
		t.Fatalf("DNS defaults: enabled=%v ttl=%v", c.DNS.Enabled, c.DNS.CacheTTL.Duration())
	}
}

func TestValidationRejectsConflictingDuplicateRules(t *testing.T) {
	c := Default()
	c.VLANRules = []VLANRule{
		{CIDR: "10.0.0.0/8", VLAN: 10, Direction: "src"},
		{CIDR: "10.0.0.0/8", VLAN: 11, Direction: "both"},
	}
	err := c.Validate()
	if err == nil || !strings.Contains(err.Error(), "conflicting VLAN") {
		t.Fatalf("expected conflicting VLAN error, got %v", err)
	}
	c.VLANRules[1].VLAN = 10
	if err := c.Validate(); err != nil {
		t.Fatalf("identical duplicate should be accepted: %v", err)
	}
}

func TestValidationRejectsBadAddressesAndRetention(t *testing.T) {
	c := Default()
	c.Listen.UDP = "not-an-address"
	if err := c.Validate(); err == nil {
		t.Fatal("bad UDP address accepted")
	}
	c = Default()
	c.Database.Retention.Hourly = Duration(24 * time.Hour)
	if err := c.Validate(); err == nil {
		t.Fatal("aggregate retention shorter than raw accepted")
	}
	c = Default()
	c.Listen.HTTP = ":8080"
	if err := c.Validate(); err == nil {
		t.Fatal("unspecified HTTP host accepted")
	}
	c = Default()
	c.DNS.CacheTTL = Duration(0)
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "dns.cache_ttl") {
		t.Fatalf("expected dns.cache_ttl error, got %v", err)
	}
	c = Default()
	c.VLANLabels = map[int32]string{4095: "bad"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "vlan_labels") {
		t.Fatalf("expected vlan_labels error, got %v", err)
	}
}

func TestResolutionTierDefaultsAndValidation(t *testing.T) {
	c := Default()
	tiers := c.Database.TrafficTiers()
	if len(tiers) != 3 {
		t.Fatalf("tiers=%+v", tiers)
	}
	wantBuckets := []time.Duration{time.Minute, 5 * time.Minute, time.Hour}
	for i, tier := range tiers {
		if tier.Bucket.Duration() != wantBuckets[i] {
			t.Fatalf("tier %d bucket=%v want %v", i, tier.Bucket.Duration(), wantBuckets[i])
		}
	}
	if tiers[2].Retention.Duration() != c.Database.Retention.Hourly.Duration() {
		t.Fatalf("hourly tier retention=%v want retention.hourly", tiers[2].Retention.Duration())
	}

	cases := []struct {
		name  string
		tiers []ResolutionTier
		want  string
	}{
		{"too fine", []ResolutionTier{{Bucket: Duration(30 * time.Second), Retention: Duration(time.Hour)}}, "bucket"},
		{"hourly tier", []ResolutionTier{{Bucket: Duration(time.Hour), Retention: Duration(48 * time.Hour)}}, "1h tier"},
		{"retention below bucket", []ResolutionTier{{Bucket: Duration(time.Minute), Retention: Duration(30 * time.Second)}}, "at least the bucket width"},
		{"retention not increasing", []ResolutionTier{
			{Bucket: Duration(time.Minute), Retention: Duration(24 * time.Hour)},
			{Bucket: Duration(5 * time.Minute), Retention: Duration(24 * time.Hour)},
		}, "increase"},
		{"beyond hourly retention", []ResolutionTier{{Bucket: Duration(time.Minute), Retention: Duration(9000 * time.Hour)}}, "cannot exceed retention.hourly"},
		{"sub-minute buckets", []ResolutionTier{{Bucket: Duration(90 * time.Second), Retention: Duration(time.Hour)}}, "whole number of minutes"},
	}
	for _, tc := range cases {
		c := Default()
		c.Database.Resolution = tc.tiers
		err := c.Validate()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: expected error containing %q, got %v", tc.name, tc.want, err)
		}
	}

	c = Default()
	c.Database.Resolution = []ResolutionTier{
		{Bucket: Duration(5 * time.Minute), Retention: Duration(72 * time.Hour)},
		{Bucket: Duration(time.Minute), Retention: Duration(24 * time.Hour)},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("out-of-order tiers should be normalized: %v", err)
	}
	tiers = c.Database.TrafficTiers()
	if tiers[0].Bucket.Duration() != time.Minute || tiers[1].Bucket.Duration() != 5*time.Minute {
		t.Fatalf("tiers not sorted: %+v", tiers)
	}
}

func TestDNSDialAddress(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"   ", ""},
		{"192.0.2.53", "192.0.2.53:53"},
		{"192.0.2.53:5353", "192.0.2.53:5353"},
		{"[fd00::1]:5353", "[fd00::1]:5353"},
		{"fd00::1", "[fd00::1]:53"},
		{" 10.0.0.1\t", "10.0.0.1:53"},
	}
	for _, c := range cases {
		got, err := DNSDialAddress(c.in)
		if err != nil {
			t.Fatalf("DNSDialAddress(%q) error: %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("DNSDialAddress(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"dns.example.com", "192.0.2.53:0", "192.0.2.53:99999", "not an ip", "[fd00::1"} {
		if got, err := DNSDialAddress(bad); err == nil {
			t.Fatalf("DNSDialAddress(%q) = %q, want an error", bad, got)
		}
	}
}

func TestValidationAcceptsDNSServerAndRejectsBadOnes(t *testing.T) {
	for _, server := range []string{"", "192.0.2.53", "192.0.2.53:5353", "[fd00::1]:5353"} {
		c := Default()
		c.DNS.Server = server
		if err := c.Validate(); err != nil {
			t.Fatalf("dns.server %q rejected: %v", server, err)
		}
	}
	c := Default()
	c.DNS.Server = "resolver.example.com"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "dns.server") {
		t.Fatalf("expected dns.server error, got %v", err)
	}
}

func TestDNSServerLoadsFromYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("dns:\n  server: 192.0.2.53:5353\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.DNS.Server != "192.0.2.53:5353" {
		t.Fatalf("dns.server = %q", c.DNS.Server)
	}
	if !c.DNS.Enabled {
		t.Fatal("setting dns.server must not disable reverse DNS")
	}
}
