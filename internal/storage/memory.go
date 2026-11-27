package storage

import (
	"bytes"
	"context"
	"sort"
	"sync"
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
	// Merge builds a fresh slice, so slices already handed to readers stay unchanged.
	if next, changed := Merge(m.data[string(key)], v); changed {
		m.data[string(key)] = next
	}
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

// Scan implements Engine. fn runs without the lock held, so it may call Put and Remove.
func (m *Memory) Scan(ctx context.Context, prefix []byte, fn func(key []byte, vs []Versioned) error) error {
	m.mu.RLock()
	snap := map[string][]Versioned{}
	keys := []string{}
	for k, vs := range m.data {
		if bytes.HasPrefix([]byte(k), prefix) {
			snap[k] = vs
			keys = append(keys, k)
		}
	}
	m.mu.RUnlock()
	sort.Strings(keys)
	for _, k := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn([]byte(k), snap[k]); err != nil {
			return err
		}
	}
	return nil
}

// Remove implements Engine.
func (m *Memory) Remove(_ context.Context, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
	return nil
}

// Close implements Engine.
func (m *Memory) Close() error { return nil }
