package store

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/a-bali/nfvis/internal/config"
	"github.com/a-bali/nfvis/internal/model"
)

// dedupKeyBytes is the width of the stored payload fingerprint. 16 bytes (a
// truncated SHA-256) collides with probability ~1e-19 across the ~10^7 rows a
// day of homelab traffic holds, and costs half what storing the nine-field text
// key would.
const dedupKeyBytes = 16

// flowDedupKey returns the identity of a flow's payload, or nil when the record
// carries no usable identity (all-zero timestamps), in which case it is never
// matched against anything.
//
// The key deliberately covers the payload and the export interval but not the
// interfaces: two records that differ only in input_if/output_if may be the
// parent and child copies of one frame. The key alone does not authorize a
// collapse — that requires a configured interface link (see pcState). Callers
// must treat key equality as "worth a lookup", never as "the same packet".
//
// bytes and packets are part of the key on purpose. OPNsense re-exports a live
// connection with cumulative counters at each 60 s active timeout, so the same
// 5-tuple appears repeatedly with packets 1, 2, 3… Including the counters keeps
// those records distinct; dropping them would silently merge a live connection
// into its first packet.
func flowDedupKey(f *model.Flow) []byte {
	if f.StartMS <= 0 && f.EndMS <= 0 && f.ReceivedAtMS <= 0 {
		return nil
	}
	h := sha256.New()
	var num [8]byte
	writeInt := func(v int64) {
		binary.BigEndian.PutUint64(num[:], uint64(v))
		_, _ = h.Write(num[:])
	}
	writeStr := func(s string) {
		binary.BigEndian.PutUint64(num[:], uint64(len(s)))
		_, _ = h.Write(num[:])
		_, _ = h.Write([]byte(s))
	}

	writeStr(f.Exporter)
	writeInt(f.StartMS)
	writeInt(f.EndMS)
	writeStr(f.SrcAddr)
	writeInt(int64(f.SrcPort))
	writeStr(f.DstAddr)
	writeInt(int64(f.DstPort))
	writeInt(int64(f.Protocol))
	writeInt(int64(f.Bytes))
	writeInt(int64(f.Packets))

	sum := h.Sum(nil)
	return sum[:dedupKeyBytes]
}

// relation is how two interface numbers relate under the configured topology.
type relation int

const (
	// relationNone: no configured link joins the two interfaces.
	relationNone relation = iota
	// relationParent: the first interface is the parent, the second its child.
	relationParent
	// relationChild: the first interface is the child, the second its parent.
	relationChild
)

// pcState is the resolved parent/child topology: which interface pairs of which
// exporter are allowed to collapse into one row.
//
// ng_netflow exports one record per interface node, so a tagged frame is
// reported on the physical parent and again on its VLAN child. Which node is a
// parent of which is site knowledge, and inference is unsafe: two records that
// differ only in interface can equally be two genuine traversals (a loop, a
// bridge, a monitor port). The policy therefore collapses only pairs the
// operator declared, per exporter, and never collapses anything else.
type pcState struct {
	links map[string]map[[2]uint32]struct{}
}

func newPCState(enabled bool, links []config.ParentChildLink) *pcState {
	if !enabled || len(links) == 0 {
		return nil
	}
	p := &pcState{links: make(map[string]map[[2]uint32]struct{}, len(links))}
	for _, l := range links {
		if l.Parent == 0 || l.Child == 0 || l.Parent == l.Child || l.Exporter == "" {
			continue
		}
		pair := [2]uint32{l.Parent, l.Child}
		if p.links[l.Exporter] == nil {
			p.links[l.Exporter] = make(map[[2]uint32]struct{})
		}
		p.links[l.Exporter][pair] = struct{}{}
	}
	if len(p.links) == 0 {
		return nil
	}
	return p
}

// hasExporter reports whether any link is configured for exporter. An exporter
// with no links never participates, so its records are always stored.
func (p *pcState) hasExporter(exporter string) bool {
	if p == nil {
		return false
	}
	_, ok := p.links[exporter]
	return ok
}

// relation reports how a and b relate under this exporter's links.
func (p *pcState) relation(exporter string, a, b uint32) relation {
	if p == nil || a == 0 || b == 0 || a == b {
		// ifIndex 0 means "not recorded", not "the outermost interface".
		return relationNone
	}
	for pair := range p.links[exporter] {
		switch {
		case pair[0] == a && pair[1] == b:
			return relationParent
		case pair[0] == b && pair[1] == a:
			return relationChild
		}
	}
	return relationNone
}
