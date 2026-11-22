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
	// MigrationTimeout bounds how long schema migrations may run at startup.
	// A migration that rewrites or indexes a large existing table takes minutes
	// on a multi-GB database, so this cannot be a small fixed value: exceeding
	// it rolls the migration back, the process exits before serving, and a
	// crash-looping pod is a full outage. Zero uses DefaultMigrationTimeout.
	MigrationTimeout Duration `yaml:"migration_timeout"`
	// Dedup collapses records OPNsense's ng_netflow emits more than once for the
	// same packet.
	Dedup DedupConfig `yaml:"dedup"`
	// MmapSizeMB bounds how much of the database file SQLite maps into the
	// process address space. Touched pages are charged to the container as
	// file-backed memory, so this single value dominates what Kubernetes reports
	// for a large database: at the previous hard-coded 1024 MiB a 7 GiB database
	// kept ~1 GiB resident. Zero disables mmap, which costs 10-20% on wide scans
	// and saves the whole mapping. Absent uses DefaultMmapSizeMB.
	MmapSizeMB *int `yaml:"mmap_size_mb"`
	// CacheSizeMB is SQLite's own page cache, held as anonymous (unreclaimable)
	// memory rather than page cache. The previous hard-coded 128 MiB was 100+ MiB
	// of the process's heap on top of a working set that is otherwise reclaimable.
	// Absent uses DefaultCacheSizeMB.
	CacheSizeMB *int `yaml:"cache_size_mb"`
}

// Defaults for the SQLite memory pragmas. They reproduce the values that used to
// be hard-coded in store.Open, so an upgrade without explicit config changes
// nothing.
//
// Both are PER CONNECTION. store.Open sets SetMaxOpenConns(8), so a 128 MiB
// cache_size is up to eight of them — ~1 GiB of anonymous memory — and the mmap
// limit applies to each pooled connection separately. Measured on a 282 MB
// database with eight concurrent readers: cache 128 MiB gave 1933 MB RssAnon,
// cache 16 MiB gave 257 MB. That per-connection multiplication is why the
// default looks modest while the process still reports a gigabyte.
const (
	DefaultMmapSizeMB  = 1024
	DefaultCacheSizeMB = 128

	// maxMmapSizeMB is not a taste limit: the vendored modernc.org/sqlite is
	// built with MAX_MMAP_SIZE=0x7fff0000, so any request above ~2047 MiB is
	// silently clamped to that value rather than honoured. Accepting 8192 would
	// validate and then quietly apply 2047, which is worse than refusing.
	maxMmapSizeMB  = 2047
	maxCacheSizeMB = 4096
)

// DefaultMigrationTimeout is the ceiling for the whole migration run when
// database.migration_timeout is unset. Migrations are one-time work at startup;
// waiting is far cheaper than rolling back and crash-looping.
const DefaultMigrationTimeout = 30 * time.Minute

// EffectiveMigrationTimeout returns the configured timeout or the default.
func (d DatabaseConfig) EffectiveMigrationTimeout() time.Duration {
	if d.MigrationTimeout.Duration() > 0 {
		return d.MigrationTimeout.Duration()
	}
	return DefaultMigrationTimeout
}

// intPtr returns a pointer to v, for optional config fields where an explicit
// zero is meaningful and therefore distinct from absence.
func intPtr(v int) *int { return &v }

// EffectiveMmapSizeMB returns the configured mmap ceiling in MiB. An absent
// value takes the default; an explicit 0 disables mmap, which is a meaningful
// configuration rather than a missing one — hence the pointer.
func (d DatabaseConfig) EffectiveMmapSizeMB() int {
	if d.MmapSizeMB == nil {
		return DefaultMmapSizeMB
	}
	return *d.MmapSizeMB
}

// EffectiveCacheSizeMB returns the configured SQLite page-cache size in MiB, or
// the default when absent.
func (d DatabaseConfig) EffectiveCacheSizeMB() int {
	if d.CacheSizeMB == nil {
		return DefaultCacheSizeMB
	}
	return *d.CacheSizeMB
}

// DedupConfig controls duplicate record suppression.
//
// Both mechanisms are inert until the operator states the topology they are
// supposed to exploit: a payload-level match alone is not evidence that two
// records are the same packet. No topology configured therefore means every
// record is stored, which is the only lossless default.
type DedupConfig struct {
	// ParentChild collapses the parent-interface and VLAN-child copies of one
	// frame. A copy is only collapsed when the exporter and both interface
	// numbers appear in ParentChildLinks; ng_netflow exports one record per
	// interface node, but which node is a parent of which is site knowledge.
	ParentChild bool `yaml:"parent_child"`
	// ParentChildLinks lists the per-exporter interface pairs that are allowed
	// to collapse. Records on the same interface, on an interface that is not
	// part of a link, or on a pair belonging to another exporter are always
	// stored separately.
	ParentChildLinks []ParentChildLink `yaml:"parent_child_links"`
	// NAT suppresses the post-NAT copy of a forwarded packet when its pre-NAT
	// mate has been seen. It requires both WANAddresses and an Exporters entry
	// for the firewall that reports both views: without them there is no way to
	// tell a post-NAT copy from an unrelated record that merely shares its
	// counters. Off by default.
	NAT NATConfig `yaml:"nat"`
}

// ParentChildLink authorizes collapsing one interface node's copy of a frame
// into its parent's. The parent record survives: it carries the pre-NAT source
// identity that the measured export shows surviving.
type ParentChildLink struct {
	Exporter string `yaml:"exporter"`
	Parent   uint32 `yaml:"parent"`
	Child    uint32 `yaml:"child"`
}

// NATConfig configures pre-NAT/post-NAT duplicate suppression.
//
// The residual risk is documented in docs/operations.md: two unrelated flows
// that share protocol, destination, counters and a sub-second export window are
// indistinguishable from a NAT pair at the record level. This is why a pair is
// only acted on when exactly one candidate exists, and why the whole mechanism
// is opt-in per exporter.
type NATConfig struct {
	WANAddresses []string `yaml:"wan_addresses"`
	// Exporters names the firewall that reports both views and the input
	// interfaces each view is exported from. A record whose input interface is
	// not one of these is never paired.
	Exporters []NATExporter `yaml:"exporters"`
	// Window bounds the distance between the two views' export timestamps.
	Window Duration `yaml:"window"`
}

// NATExporter is one firewall's NAT topology.
type NATExporter struct {
	Address string `yaml:"address"`
	// PreInterface is the input interface the pre-NAT (surviving) view is
	// exported from.
	PreInterface uint32 `yaml:"pre_interface"`
	// PostInterface is the input interface the post-NAT (dropped) view is
	// exported from.
	PostInterface uint32 `yaml:"post_interface"`
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
	// Server is the resolver to send PTR queries to. Empty means the system
	// resolver from /etc/resolv.conf.
	Server string `yaml:"server"`
}

// defaultDNSPort is used when dns.server has no explicit port.
const defaultDNSPort = 53

// DNSDialAddress normalizes dns.server into a dialable "host:port" address.
// It accepts an IP literal with an optional port: 192.0.2.53, 192.0.2.53:5353,
// fd00::1, [fd00::1]:5353. An empty value means "use the system resolver" and
// normalizes to an empty string.
//
// A literal IP is required: resolving a hostname would need the very resolver
// being configured, and falling back to the system one would silently ignore
// the setting.
func DNSDialAddress(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", nil
	}
	if addrPort, err := netip.ParseAddrPort(s); err == nil {
		if addrPort.Port() == 0 {
			return "", fmt.Errorf("%q: port 0 is not a usable DNS port", raw)
		}
		return addrPort.String(), nil
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		s = s[1 : len(s)-1]
	}
	if addr, err := netip.ParseAddr(s); err == nil {
		return netip.AddrPortFrom(addr, defaultDNSPort).String(), nil
	}
	return "", fmt.Errorf("%q is not an IP address with an optional port (for example 192.0.2.53 or 192.0.2.53:5353)", raw)
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
				CleanupInterval: Duration(time.Hour),
				CleanupBatch:    5000,
			},
			Resolution: []ResolutionTier{
				{Bucket: Duration(time.Minute), Retention: Duration(7 * 24 * time.Hour)},
				{Bucket: Duration(5 * time.Minute), Retention: Duration(30 * 24 * time.Hour)},
			},
			MigrationTimeout: Duration(DefaultMigrationTimeout),
			MmapSizeMB:       intPtr(DefaultMmapSizeMB),
			CacheSizeMB:      intPtr(DefaultCacheSizeMB),
			Dedup: DedupConfig{
				// Both mechanisms need declared topology; off until the operator
				// supplies it, so the default is lossless.
				ParentChild: false,
				NAT:         NATConfig{Window: Duration(2 * time.Second)},
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
	if d := c.Database.MigrationTimeout.Duration(); d < 0 || (d > 0 && d < 5*time.Second) || d > 4*time.Hour {
		return errors.New("database.migration_timeout must be 0 (default) or between 5s and 4h")
	}
	if v := c.Database.MmapSizeMB; v != nil {
		if *v < 0 || *v > maxMmapSizeMB {
			return fmt.Errorf("database.mmap_size_mb must be between 0 (disabled) and %d", maxMmapSizeMB)
		}
	}
	if v := c.Database.CacheSizeMB; v != nil {
		if *v < 1 || *v > maxCacheSizeMB {
			return fmt.Errorf("database.cache_size_mb must be between 1 and %d", maxCacheSizeMB)
		}
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
	if _, err := DNSDialAddress(c.DNS.Server); err != nil {
		return fmt.Errorf("dns.server: %w", err)
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

	if err := c.Database.Dedup.Validate(); err != nil {
		return err
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

// Validate checks the dedup configuration. It runs at startup so a topology
// mistake is reported instead of silently disabling or misapplying suppression.
func (d DedupConfig) Validate() error {
	seenLinks := make(map[string]struct{}, len(d.ParentChildLinks))
	for i, l := range d.ParentChildLinks {
		if strings.TrimSpace(l.Exporter) == "" {
			return fmt.Errorf("database.dedup.parent_child_links[%d].exporter is required", i)
		}
		if l.Parent == 0 || l.Child == 0 {
			return fmt.Errorf("database.dedup.parent_child_links[%d]: parent and child must be non-zero interface indexes", i)
		}
		if l.Parent == l.Child {
			return fmt.Errorf("database.dedup.parent_child_links[%d]: parent and child are both %d", i, l.Parent)
		}
		key := fmt.Sprintf("%s|%d|%d", l.Exporter, l.Parent, l.Child)
		if _, ok := seenLinks[key]; ok {
			return fmt.Errorf("database.dedup.parent_child_links[%d]: duplicate link %s %d->%d", i, l.Exporter, l.Parent, l.Child)
		}
		seenLinks[key] = struct{}{}
	}
	if d.ParentChild && len(d.ParentChildLinks) == 0 {
		// Storing every record is the safe behaviour, but a switch that silently
		// does nothing is a setting mistake worth reporting at startup.
		return errors.New("database.dedup.parent_child requires at least one database.dedup.parent_child_links entry: without a declared interface link no two records can be proven to be copies, so every record is stored")
	}
	if len(d.NAT.WANAddresses) > 0 && len(d.NAT.Exporters) == 0 {
		return errors.New("database.dedup.nat.wan_addresses requires database.dedup.nat.exporters entries: without the firewall's pre/post-NAT input interfaces no pair can be proven, so NAT suppression stays off")
	}
	seenExporters := make(map[string]struct{}, len(d.NAT.Exporters))
	for i, e := range d.NAT.Exporters {
		if strings.TrimSpace(e.Address) == "" {
			return fmt.Errorf("database.dedup.nat.exporters[%d].address is required", i)
		}
		if _, ok := seenExporters[e.Address]; ok {
			return fmt.Errorf("database.dedup.nat.exporters[%d]: duplicate exporter %s", i, e.Address)
		}
		seenExporters[e.Address] = struct{}{}
		if e.PreInterface == 0 || e.PostInterface == 0 {
			return fmt.Errorf("database.dedup.nat.exporters[%d]: pre_interface and post_interface must be non-zero", i)
		}
		if e.PreInterface == e.PostInterface {
			return fmt.Errorf("database.dedup.nat.exporters[%d]: pre_interface and post_interface are both %d", i, e.PreInterface)
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
