package lsm

import (
	"sort"

	"github.com/abhi-8849/dynakv/internal/storage"
)

// memtable holds the newest records in memory until they are flushed to an SSTable. A record is a key's full version list; an empty
// list marks a removed key. It is sorted when flushed.
type memtable struct {
	m    map[string][]storage.Versioned
	size int64
}

func newMemtable() *memtable { return &memtable{m: map[string][]storage.Versioned{}} }

func (t *memtable) get(key []byte) ([]storage.Versioned, bool) {
	vs, ok := t.m[string(key)]
	return vs, ok
}

// set replaces the record for key and keeps the size estimate current.
func (t *memtable) set(key []byte, vs []storage.Versioned) {
	if old, ok := t.m[string(key)]; ok {
		t.size -= recordSize(key, old)
	}
	t.m[string(key)] = vs
	t.size += recordSize(key, vs)
}

// sortedKeys returns the keys in byte order.
func (t *memtable) sortedKeys() []string {
	keys := make([]string, 0, len(t.m))
	for k := range t.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// recordSize approximates a record's memory use: the payload plus a fixed cost per version for clocks and bookkeeping.
func recordSize(key []byte, vs []storage.Versioned) int64 {
	n := int64(len(key)) + 32
	for _, v := range vs {
		n += int64(len(v.Value)) + 96
	}
	return n
}
