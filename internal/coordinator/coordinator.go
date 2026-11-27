package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/abhi-8849/dynakv/internal/storage"
	"github.com/abhi-8849/dynakv/internal/vclock"
)

var (
	// ErrNotFound means the key has no live version.
	ErrNotFound = errors.New("coordinator: key not found")
	// ErrUnavailable means fewer replicas than the requested R or W could take part.
	ErrUnavailable = errors.New("coordinator: not enough replicas")
	// ErrInvalidContext means the context token was not produced by a previous Get.
	ErrInvalidContext = errors.New("coordinator: invalid context")
	// ErrInvalidConfig means N, R or W are out of range.
	ErrInvalidConfig = errors.New("coordinator: invalid config")
)

// Clock supplies wall time so tests and callers can control it.
type Clock interface {
	Now() time.Time
}

// Config holds the coordinator's identity and default quorum sizes.
type Config struct {
	// ID is this node's id; it names the vector clock entry the node increments.
	ID string
	// N is the replication factor; R and W are the default read and write quorums.
	N, R, W int
}

// Coordinator serves reads and writes for the keys this node coordinates.
type Coordinator struct {
	cfg   Config
	store storage.Engine
	clock Clock
}

// New returns a coordinator over the node's local store.
func New(cfg Config, store storage.Engine, clock Clock) (*Coordinator, error) {
	if cfg.ID == "" || cfg.N < 1 || cfg.R < 1 || cfg.R > cfg.N || cfg.W < 1 || cfg.W > cfg.N {
		return nil, fmt.Errorf("%w: id=%q n=%d r=%d w=%d", ErrInvalidConfig, cfg.ID, cfg.N, cfg.R, cfg.W)
	}
	return &Coordinator{cfg: cfg, store: store, clock: clock}, nil
}

// Get returns the live versions of key and the merged context to pass to the next write. r of 0 means the default.
func (c *Coordinator) Get(ctx context.Context, key []byte, r int) ([]storage.Versioned, []byte, error) {
	if err := c.reachable(r, c.cfg.R); err != nil {
		return nil, nil, err
	}
	vs, err := c.local(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	live := vs[:0:0]
	for _, v := range vs {
		if !v.Tombstone {
			live = append(live, v)
		}
	}
	if len(live) == 0 {
		return nil, nil, ErrNotFound
	}
	return live, encodeContext(mergeAll(vs)), nil
}

// Put stores value as a new version of key. vctx is the context from a previous Get, or empty. w of 0 means the default.
func (c *Coordinator) Put(ctx context.Context, key, value, vctx []byte, w int) error {
	return c.write(ctx, key, storage.Versioned{Value: value}, vctx, w)
}

// Delete writes a tombstone version of key through the normal write path.
func (c *Coordinator) Delete(ctx context.Context, key, vctx []byte, w int) error {
	return c.write(ctx, key, storage.Versioned{Tombstone: true}, vctx, w)
}

func (c *Coordinator) write(ctx context.Context, key []byte, v storage.Versioned, vctx []byte, w int) error {
	if err := c.reachable(w, c.cfg.W); err != nil {
		return err
	}
	base, err := decodeContext(vctx)
	if err != nil {
		return err
	}
	stored, err := c.local(ctx, key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	// The counter must exceed every counter this node already stored, even if the client context is older.
	var floor uint64
	for _, s := range stored {
		floor = max(floor, s.Clock[c.cfg.ID].Counter)
	}
	now := c.clock.Now()
	v.Clock = base.Increment(c.cfg.ID, floor, now.UnixNano())
	v.TS = now
	if err := c.store.Put(ctx, key, v); err != nil {
		return fmt.Errorf("coordinator: write local: %w", err)
	}
	return nil
}

// local reads the node's own versions, mapping the engine's not-found to ErrNotFound.
func (c *Coordinator) local(ctx context.Context, key []byte) ([]storage.Versioned, error) {
	vs, err := c.store.Get(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("coordinator: read local: %w", err)
	}
	return vs, nil
}

// reachable checks the quorum size against the replicas this coordinator can reach.
func (c *Coordinator) reachable(requested, def int) error {
	need := def
	if requested > 0 {
		need = requested
	}
	// TODO: count the live replicas of the key's preference list once replication exists.
	if replicas := 1; need > replicas {
		return fmt.Errorf("%w: need %d, have %d", ErrUnavailable, need, replicas)
	}
	return nil
}

func mergeAll(vs []storage.Versioned) vclock.Clock {
	cs := make([]vclock.Clock, len(vs))
	for i, v := range vs {
		cs[i] = v.Clock
	}
	return vclock.Merge(cs...)
}
