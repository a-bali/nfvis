package decoder

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/netsampler/goflow2/v3/decoders/netflow"
)

type templateKey struct {
	router   string
	version  uint16
	domainID uint32
	template uint16
}

type cachedTemplate struct {
	value    interface{}
	lastSeen time.Time
}

// templateStore implements netflow.TemplateStore. Templates are isolated by
// exporter address and source ID and expire after a configurable idle period.
type templateStore struct {
	mu        sync.Mutex
	templates map[templateKey]cachedTemplate
	ttl       time.Duration
	now       func() time.Time
	refreshes uint64
	expired   uint64
}

func newTemplateStore(ttl time.Duration) *templateStore {
	return &templateStore{
		templates: make(map[templateKey]cachedTemplate),
		ttl:       ttl,
		now:       time.Now,
	}
}

func (s *templateStore) key(ctx netflow.FlowContext, version uint16, obsDomainID uint32, templateID uint16) templateKey {
	return templateKey{router: ctx.RouterKey, version: version, domainID: obsDomainID, template: templateID}
}

// AddTemplate stores or refreshes a template.
func (s *templateStore) AddTemplate(ctx netflow.FlowContext, version uint16, obsDomainID uint32, templateID uint16, template interface{}) (netflow.TemplateStatus, error) {
	if version != 9 || templateID < 256 {
		return netflow.TemplateUnchanged, fmt.Errorf("unsupported template version %d or id %d", version, templateID)
	}
	key := s.key(ctx, version, obsDomainID, templateID)
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if old, ok := s.templates[key]; ok {
		if reflect.DeepEqual(old.value, template) {
			old.lastSeen = now
			s.templates[key] = old
			return netflow.TemplateUnchanged, nil
		}
		s.refreshes++
	}
	s.templates[key] = cachedTemplate{value: template, lastSeen: now}
	return netflow.TemplateAdded, nil
}

// GetTemplate returns a live template, reporting not-found for expired ones.
func (s *templateStore) GetTemplate(ctx netflow.FlowContext, version uint16, obsDomainID uint32, templateID uint16) (interface{}, error) {
	key := s.key(ctx, version, obsDomainID, templateID)
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.templates[key]
	if !ok {
		return nil, netflow.ErrorTemplateNotFound
	}
	now := s.now()
	if now.Sub(entry.lastSeen) > s.ttl {
		delete(s.templates, key)
		s.expired++
		return nil, netflow.ErrorTemplateNotFound
	}
	entry.lastSeen = now
	s.templates[key] = entry
	return entry.value, nil
}

// RemoveTemplate deletes a specific template.
func (s *templateStore) RemoveTemplate(ctx netflow.FlowContext, version uint16, obsDomainID uint32, templateID uint16) (interface{}, bool, error) {
	key := s.key(ctx, version, obsDomainID, templateID)
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.templates[key]
	if !ok {
		return nil, false, nil
	}
	delete(s.templates, key)
	return entry.value, true, nil
}

// expire removes all templates idle since now-ttl.
func (s *templateStore) expire(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key, entry := range s.templates {
		if now.Sub(entry.lastSeen) > s.ttl {
			delete(s.templates, key)
			removed++
		}
	}
	s.expired += uint64(removed)
	return removed
}

func (s *templateStore) clearRouter(router string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key := range s.templates {
		if key.router == router {
			delete(s.templates, key)
			removed++
		}
	}
	return removed
}

func (s *templateStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.templates)
}

func (s *templateStore) refreshCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshes
}

func (s *templateStore) expiredCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expired
}
