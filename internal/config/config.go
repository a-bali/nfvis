// Package config loads and validates nfvis configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration accepts Go duration strings in YAML ("30s", "720h").
type Duration time.Duration

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("duration must be a scalar")
	}
	if node.Tag == "!!int" {
		n, err := strconv.ParseInt(node.Value, 10, 64)
		if err != nil {
			return err
		}
		*d = Duration(n)
		return nil
	}
	v, err := time.ParseDuration(strings.TrimSpace(node.Value))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}

// Duration returns the value as time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// Config is the complete on-disk configuration.
type Config struct {
	Listen     ListenConfig     `yaml:"listen"`
	Database   DatabaseConfig   `yaml:"database"`
	Decoder    DecoderConfig    `yaml:"decoder"`
	DNS        DNSConfig        `yaml:"dns"`
	Exporters  []Exporter       `yaml:"exporters"`
	VLANRules  []VLANRule       `yaml:"vlan_rules"`
	VLANLabels map[int32]string `yaml:"vlan_labels"`
	Logging    LoggingConfig    `yaml:"logging"`
}

// ListenConfig holds UDP and HTTP bind addresses.
type ListenConfig struct {
	UDP  string `yaml:"udp"`
	HTTP string `yaml:"http"`
}

// DatabaseConfig holds storage and retention settings.
type DatabaseConfig struct {
	Path       string           `yaml:"path"`
	BatchSize  int              `yaml:"batch_size"`
	Flush      Duration         `yaml:"flush_interval"`
	Retention  RetentionConfig  `yaml:"retention"`
	Resolution []ResolutionTier `yaml:"resolution"`
}

// ResolutionTier keeps traffic totals at a fixed bucket width for a bounded
// time window. Charts use the finest tier that covers each part of a range;
// the implicit hourly tier lives in Retention.Hourly.
type ResolutionTier struct {
	Bucket    Duration `yaml:"bucket"`
	Retention Duration `yaml:"retention"`
}

// TrafficTiers returns the chart-resolution tiers, finest first. The hourly
// tier is implicit and shares retention.hourly with the hourly aggregates.
func (d DatabaseConfig) TrafficTiers() []ResolutionTier {
	out := make([]ResolutionTier, 0, len(d.Resolution)+1)
	out = append(out, d.Resolution...)
	out = append(out, ResolutionTier{Bucket: Duration(time.Hour), Retention: d.Retention.Hourly})
	sort.SliceStable(out, func(i, j int) bool { return out[i].Bucket < out[j].Bucket })
	return out
}

// RetentionConfig controls independent raw and aggregate deletion.
type RetentionConfig struct {
	Raw             Duration `yaml:"raw"`
	Hourly          Duration `yaml:"hourly"`
	CleanupInterval Duration `yaml:"cleanup_interval"`
	CleanupBatch    int      `yaml:"cleanup_batch"`
}

// DecoderConfig controls the UDP ingest pipeline.
type DecoderConfig struct {
	Workers           int      `yaml:"workers"`
	QueueSize         int      `yaml:"queue_size"`
	TemplateTTL       Duration `yaml:"template_ttl"`
	MaxPacketBytes    int      `yaml:"max_packet_bytes"`
	MaxFlowsPerPacket int      `yaml:"max_flows_per_packet"`
}

// DNSConfig controls reverse lookup of flow endpoint addresses.
type DNSConfig struct {
	Enabled       bool     `yaml:"enabled"`
	CacheTTL      Duration `yaml:"cache_ttl"`
	CacheSize     int      `yaml:"cache_size"`
	LookupTimeout Duration `yaml:"lookup_timeout"`
}

// Exporter assigns labels to a source IP and its interface indexes.
type Exporter struct {
	Address    string            `yaml:"address"`
	Label      string            `yaml:"label"`
	Interfaces map[uint32]string `yaml:"interfaces"`
}

// VLANRule maps an endpoint CIDR to a VLAN tag.
type VLANRule struct {
	CIDR      string `yaml:"cidr"`
	VLAN      int32  `yaml:"vlan"`
	Direction string `yaml:"direction"`
}

// LoggingConfig holds the log level.
type LoggingConfig struct {
	Level string `yaml:"level"`
}

// Default returns configuration with MVP defaults.
func Default() Config {
	return Config{
		Listen: ListenConfig{UDP: "0.0.0.0:2055", HTTP: "127.0.0.1:8080"},
		Database: DatabaseConfig{
			Path:      "data/nfvis.db",
			BatchSize: 1000,
			Flush:     Duration(250 * time.Millisecond),
			Retention: RetentionConfig{
				Raw:             Duration(30 * 24 * time.Hour),
				Hourly:          Duration(365 * 24 * time.Hour),
				CleanupInterval: Duration(15 * time.Minute),
				CleanupBatch:    5000,
			},
			Resolution: []ResolutionTier{
				{Bucket: Duration(time.Minute), Retention: Duration(7 * 24 * time.Hour)},
				{Bucket: Duration(5 * time.Minute), Retention: Duration(30 * 24 * time.Hour)},
			},
		},
		Decoder: DecoderConfig{
			Workers:           4,
			QueueSize:         4096,
			TemplateTTL:       Duration(30 * time.Minute),
			MaxPacketBytes:    65535,
			MaxFlowsPerPacket: 10000,
		},
		DNS: DNSConfig{
			Enabled:       true,
			CacheTTL:      Duration(5 * time.Minute),
			CacheSize:     10000,
			LookupTimeout: Duration(2 * time.Second),
		},
		VLANLabels: map[int32]string{},
		Logging:    LoggingConfig{Level: "info"},
	}
}

// Load reads a YAML file over the defaults. A missing path returns defaults.
func Load(path string) (Config, error) {
	c := Default()
	if path == "" {
		return c, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return c, fmt.Errorf("read config: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return c, fmt.Errorf("parse config %s: %w", path, err)
	}
	return c, nil
}

// Validate checks all configuration values and returns a human-readable error
// describing the first problem found.
func (c Config) Validate() error {
	if _, err := net.ResolveUDPAddr("udp", c.Listen.UDP); err != nil {
		return fmt.Errorf("listen.udp: %w", err)
	}
	host, port, err := net.SplitHostPort(c.Listen.HTTP)
	if err != nil || port == "" {
		return fmt.Errorf("listen.http must be host:port")
	}
	if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("listen.http port must be numeric")
	}
	if host == "" {
		return fmt.Errorf("listen.http must include an explicit host (use 127.0.0.1)")
	}
	if strings.TrimSpace(c.Database.Path) == "" {
		return errors.New("database.path is required")
	}
	if c.Database.BatchSize < 1 || c.Database.BatchSize > 100000 {
		return errors.New("database.batch_size must be between 1 and 100000")
	}
	if c.Database.Flush.Duration() <= 0 || c.Database.Flush.Duration() > time.Minute {
		return errors.New("database.flush_interval must be between 0 and 1m")
	}
	r := c.Database.Retention
	if r.Raw.Duration() <= 0 || r.Hourly.Duration() <= 0 {
		return errors.New("retention.raw and retention.hourly must be positive")
	}
	if r.Hourly.Duration() < r.Raw.Duration() {
		return errors.New("retention.hourly must be at least retention.raw")
	}
	if r.CleanupInterval.Duration() <= 0 {
		return errors.New("retention.cleanup_interval must be positive")
	}
	if r.CleanupBatch < 1 || r.CleanupBatch > 1000000 {
		return errors.New("retention.cleanup_batch must be between 1 and 1000000")
	}
	tiers := c.Database.TrafficTiers()
	var prev ResolutionTier
	for i, tier := range tiers {
		bucket, retention := tier.Bucket.Duration(), tier.Retention.Duration()
		if bucket == time.Hour {
			if i != len(tiers)-1 {
				return errors.New("resolution must not contain a 1h tier; use retention.hourly")
			}
		} else if bucket < time.Minute || bucket > time.Hour {
			return fmt.Errorf("resolution[%d].bucket must be between 1m and 1h", i)
		} else if bucket%time.Minute != 0 {
			return fmt.Errorf("resolution[%d].bucket must be a whole number of minutes", i)
		}
		if retention < bucket {
			return fmt.Errorf("resolution[%d].retention must be at least the bucket width", i)
		}
		if retention > r.Hourly.Duration() {
			return errors.New("resolution retention cannot exceed retention.hourly")
		}
		if i > 0 {
			if bucket == prev.Bucket.Duration() {
				return fmt.Errorf("resolution[%d].bucket duplicates an earlier tier", i)
			}
			if retention <= prev.Retention.Duration() {
				return errors.New("resolution retentions must increase with the bucket width")
			}
		}
		prev = tier
	}
	if c.Decoder.Workers < 1 || c.Decoder.Workers > 256 {
		return errors.New("decoder.workers must be between 1 and 256")
	}
	if c.Decoder.QueueSize < 1 || c.Decoder.QueueSize > 1000000 {
		return errors.New("decoder.queue_size must be between 1 and 1000000")
	}
	if c.Decoder.TemplateTTL.Duration() <= 0 {
		return errors.New("decoder.template_ttl must be positive")
	}
	if c.Decoder.MaxPacketBytes < 256 || c.Decoder.MaxPacketBytes > 65535 {
		return errors.New("decoder.max_packet_bytes must be between 256 and 65535")
	}
	if c.Decoder.MaxFlowsPerPacket < 1 || c.Decoder.MaxFlowsPerPacket > 1000000 {
		return errors.New("decoder.max_flows_per_packet must be between 1 and 1000000")
	}
	if c.DNS.CacheTTL.Duration() <= 0 || c.DNS.CacheTTL.Duration() > 24*time.Hour {
		return errors.New("dns.cache_ttl must be between 0 and 24h")
	}
	if c.DNS.CacheSize < 1 || c.DNS.CacheSize > 1000000 {
		return errors.New("dns.cache_size must be between 1 and 1000000")
	}
	if c.DNS.LookupTimeout.Duration() < 100*time.Millisecond || c.DNS.LookupTimeout.Duration() > 30*time.Second {
		return errors.New("dns.lookup_timeout must be between 100ms and 30s")
	}
	switch strings.ToLower(strings.TrimSpace(c.Logging.Level)) {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("logging.level must be debug, info, warn, or error")
	}

	seenExporters := make(map[netip.Addr]bool)
	for i, e := range c.Exporters {
		addr, err := netip.ParseAddr(strings.TrimSpace(e.Address))
		if err != nil {
			return fmt.Errorf("exporters[%d].address: %w", i, err)
		}
		addr = addr.Unmap()
		if seenExporters[addr] {
			return fmt.Errorf("exporters[%d].address: duplicate exporter %s", i, addr)
		}
		seenExporters[addr] = true
		for idx := range e.Interfaces {
			if idx == 0 {
				return fmt.Errorf("exporters[%d].interfaces: index 0 is reserved for unknown", i)
			}
		}
	}

	for vlan := range c.VLANLabels {
		if vlan < 1 || vlan > 4094 {
			return fmt.Errorf("vlan_labels: VLAN %d must be between 1 and 4094", vlan)
		}
	}

	// Track every CIDR+direction combination. Identical rules with the same
	// VLAN are allowed; conflicting duplicates are not.
	seenRules := make(map[string]int32)
	for i, rule := range c.VLANRules {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(rule.CIDR))
		if err != nil {
			return fmt.Errorf("vlan_rules[%d].cidr: %w", i, err)
		}
		if rule.VLAN < 1 || rule.VLAN > 4094 {
			return fmt.Errorf("vlan_rules[%d].vlan must be between 1 and 4094", i)
		}
		switch NormalizeDirection(rule.Direction) {
		case "src", "dst", "both":
		default:
			return fmt.Errorf("vlan_rules[%d].direction must be src, dst, or both", i)
		}
		for _, dir := range RuleDirections(rule.Direction) {
			key := prefix.Masked().String() + "|" + dir
			if old, ok := seenRules[key]; ok && old != rule.VLAN {
				return fmt.Errorf("vlan_rules[%d]: conflicting VLAN %d and %d for %s %s", i, old, rule.VLAN, prefix, dir)
			}
			seenRules[key] = rule.VLAN
		}
	}
	return nil
}

// NormalizeDirection maps user-facing direction aliases to src or dst.
func NormalizeDirection(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "both", "src", "source":
		if strings.EqualFold(strings.TrimSpace(s), "source") {
			return "src"
		}
		return "both" // handled by RuleDirections
	case "dst", "destination":
		return "dst"
	default:
		return s
	}
}

// RuleDirections expands a rule direction into concrete endpoint directions.
func RuleDirections(s string) []string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "src", "source":
		return []string{"src"}
	case "dst", "destination":
		return []string{"dst"}
	default:
		return []string{"src", "dst"}
	}
}

// IsLoopbackHTTP reports whether the configured HTTP address binds only to
// loopback interfaces.
func (c Config) IsLoopbackHTTP() bool {
	host, _, err := net.SplitHostPort(c.Listen.HTTP)
	if err != nil {
		return false
	}
	ip, err := netip.ParseAddr(strings.Trim(host, "[]"))
	if err != nil {
		return false
	}
	return ip.IsLoopback()
}
