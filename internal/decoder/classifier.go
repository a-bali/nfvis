package decoder

import (
	"net/netip"
	"sort"

	"github.com/a-bali/nfvis/internal/config"
)

type vlanRule struct {
	prefix netip.Prefix
	vlan   int32
}

// Classifier classifies source and destination endpoints with exported VLAN
// fields first and longest-prefix CIDR rules second.
type Classifier struct {
	src []vlanRule
	dst []vlanRule
}

// NewClassifier compiles configured VLAN rules. Rules must already be valid.
func NewClassifier(cfg config.Config) *Classifier {
	c := &Classifier{}
	for _, r := range cfg.VLANRules {
		p, err := netip.ParsePrefix(r.CIDR)
		if err != nil {
			continue
		}
		p = p.Masked()
		rule := vlanRule{prefix: p, vlan: r.VLAN}
		for _, dir := range config.RuleDirections(r.Direction) {
			if dir == "src" {
				c.src = append(c.src, rule)
			} else {
				c.dst = append(c.dst, rule)
			}
		}
	}
	sort.SliceStable(c.src, func(i, j int) bool { return c.src[i].prefix.Bits() > c.src[j].prefix.Bits() })
	sort.SliceStable(c.dst, func(i, j int) bool { return c.dst[i].prefix.Bits() > c.dst[j].prefix.Bits() })
	return c
}

// Classify returns VLAN tags and their provenance for the two endpoints.
func (c *Classifier) Classify(src, dst string, exportedSrcVLAN, exportedDstVLAN uint32) (int32, string, int32, string) {
	sv, ss := classifyOne(c.src, src, exportedSrcVLAN)
	dv, ds := classifyOne(c.dst, dst, exportedDstVLAN)
	return sv, ss, dv, ds
}

func classifyOne(rules []vlanRule, addr string, exported uint32) (int32, string) {
	if exported != 0 && exported <= 4094 {
		return int32(exported), "exported"
	}
	a, err := netip.ParseAddr(addr)
	if err != nil {
		return -1, "unknown"
	}
	a = a.Unmap()
	for _, r := range rules {
		if r.prefix.Contains(a) {
			return r.vlan, "rule"
		}
	}
	return -1, "unknown"
}

// UnknownVLAN is stored when no exported VLAN or matching rule exists.
const UnknownVLAN int32 = -1
