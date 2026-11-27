package storage

import (
	"context"
	"errors"
	"time"

	"github.com/abhi-8849/dynakv/internal/vclock"
)

// ErrNotFound is returned by Engine.Get when the key has no stored version.
var ErrNotFound = errors.New("key not found")

// Versioned is one stored version of a key. Value is shared with readers and must not be modified.
type Versioned struct {
	// Value is the stored bytes; empty for a tombstone.
	Value []byte
	// Clock orders this version against the other versions of the key.
	Clock vclock.Clock
	// Tombstone marks the key as deleted at this version.
	Tombstone bool
	// TS is when the coordinator accepted the write; compaction uses it to age tombstones.
	TS time.Time
}

// Engine stores the versions of each key on one node.
type Engine interface {
	// Put stores v, drops stored versions v dominates, and ignores v when a stored version already covers it.
	Put(ctx context.Context, key []byte, v Versioned) error
	// Get returns the versions of key, or ErrNotFound.
	Get(ctx context.Context, key []byte) ([]Versioned, error)
	// Scan calls fn for each key with the given prefix in byte order; it exists for the hint prefix only.
	Scan(ctx context.Context, prefix []byte, fn func(key []byte, vs []Versioned) error) error
	// Remove deletes key and all its versions; it exists for replayed and expired hints only.
	Remove(ctx context.Context, key []byte) error
	// Close releases the engine's resources.
	Close() error
}

// Merge returns cur with v applied: versions v supersedes are dropped and v is appended. changed is false when a stored version
// already covers v, so replays and duplicate writes leave the key untouched. cur is not modified.
func Merge(cur []Versioned, v Versioned) (next []Versioned, changed bool) {
	next = make([]Versioned, 0, len(cur)+1)
	for _, e := range cur {
		switch v.Clock.Compare(e.Clock) {
		case vclock.Before, vclock.Equal:
			return cur, false
		case vclock.After:
			continue
		}
		next = append(next, e)
	}
	return append(next, v), true
}
