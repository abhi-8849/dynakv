package storage

import (
	"context"
	"sync"

	"github.com/abhi-8849/dynakv/internal/vclock"
)

// Memory is an Engine that keeps everything in RAM and loses it on exit.
type Memory struct {
	mu   sync.RWMutex
	data map[string][]Versioned
}

// NewMemory returns an empty in-memory engine.
func NewMemory() *Memory {
	return &Memory{data: map[string][]Versioned{}}
}

// Put implements Engine.
func (m *Memory) Put(_ context.Context, key []byte, v Versioned) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.data[string(key)]
	kept := make([]Versioned, 0, len(cur)+1) // a fresh slice keeps slices already handed to readers unchanged
	for _, e := range cur {
		switch v.Clock.Compare(e.Clock) {
		case vclock.Before, vclock.Equal:
			return nil // v is stale or a duplicate
		case vclock.After:
			continue // v supersedes e
		}
		kept = append(kept, e)
	}
	m.data[string(key)] = append(kept, v)
	return nil
}

// Get implements Engine.
func (m *Memory) Get(_ context.Context, key []byte) ([]Versioned, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	vs := m.data[string(key)]
	if len(vs) == 0 {
		return nil, ErrNotFound
	}
	return vs, nil
}

// Close implements Engine.
func (m *Memory) Close() error { return nil }
