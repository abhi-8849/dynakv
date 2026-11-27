package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/abhi-8849/dynakv/internal/ring"
	"github.com/abhi-8849/dynakv/internal/storage"
	"github.com/abhi-8849/dynakv/internal/vclock"
)

var (
	// ErrNotFound means the key has no live version.
	ErrNotFound = errors.New("coordinator: key not found")
	// ErrUnavailable means fewer replicas than the requested R or W answered in time.
	ErrUnavailable = errors.New("coordinator: not enough replicas")
	// ErrInvalidContext means the context token was not produced by a previous Get.
	ErrInvalidContext = errors.New("coordinator: invalid context")
	// ErrInvalidConfig means N, R, W or the timeout are out of range.
	ErrInvalidConfig = errors.New("coordinator: invalid config")
)

// Clock supplies wall time so callers can control it.
type Clock interface {
	Now() time.Time
}

// Cluster gives the coordinator the current ring.
type Cluster interface {
	Ring() *ring.Ring
	Alive(id string) bool
}

// Replicas reaches the other nodes' storage. A missing key is an empty result, not an error.
type Replicas interface {
	// Put stores v on to; a non-empty hintFor makes to keep it as a hint for that down node instead.
	Put(ctx context.Context, to ring.Node, key []byte, v storage.Versioned, hintFor string) error
	Get(ctx context.Context, to ring.Node, key []byte) ([]storage.Versioned, error)
}

// Hinter keeps a write for a down node until it returns.
type Hinter interface {
	Store(ctx context.Context, target string, key []byte, v storage.Versioned) error
}

// Deps are the collaborators a coordinator needs.
type Deps struct {
	Store    storage.Engine
	Clock    Clock
	Cluster  Cluster
	Replicas Replicas
	Hints    Hinter
}

// Config holds the coordinator's identity, default quorum sizes and the per-request replica timeout.
type Config struct {
	// ID is this node's id; it names the vector clock entry the node increments.
	ID string
	// N is the replication factor; R and W are the default read and write quorums.
	N, R, W int
	// Timeout bounds how long a request waits for its replicas.
	Timeout time.Duration
	// Log receives read repair events; nil discards them.
	Log *slog.Logger
}

// Coordinator serves reads and writes by fanning each request out to the key's N owners.
type Coordinator struct {
	cfg      Config
	store    storage.Engine
	clock    Clock
	cluster  Cluster
	replicas Replicas
	hints    Hinter
	// seq is the highest counter this node has issued; counters never repeat even for concurrent writes to one key.
	seq atomic.Uint64
	log *slog.Logger
}

// New returns a coordinator over the node's local store.
func New(cfg Config, d Deps) (*Coordinator, error) {
	if cfg.ID == "" || cfg.N < 1 || cfg.R < 1 || cfg.R > cfg.N || cfg.W < 1 || cfg.W > cfg.N || cfg.Timeout <= 0 {
		return nil, fmt.Errorf("%w: id=%q n=%d r=%d w=%d timeout=%s", ErrInvalidConfig, cfg.ID, cfg.N, cfg.R, cfg.W, cfg.Timeout)
	}
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Coordinator{cfg: cfg, store: d.Store, clock: d.Clock, cluster: d.Cluster, replicas: d.Replicas, hints: d.Hints, log: log}, nil
}

// Get returns the live versions of key and the merged context to pass to the next write. r of 0 means the default.
func (c *Coordinator) Get(ctx context.Context, key []byte, r int) ([]storage.Versioned, []byte, error) {
	owners, need, err := c.plan(key, r, c.cfg.R)
	if err != nil {
		return nil, nil, err
	}
	replies, err := fanout(ctx, c.cfg.Timeout, plain(owners), need, func(ctx context.Context, t target) ([]storage.Versioned, error) {
		return c.getFrom(ctx, t.Node, key)
	})
	if err != nil {
		return nil, nil, err
	}
	var all []storage.Versioned
	for _, rep := range replies {
		all = append(all, rep.val...)
	}
	vs := reconcile(all)
	c.repairStale(ctx, key, vs, replies)
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
	owners, need, err := c.plan(key, w, c.cfg.W)
	if err != nil {
		return err
	}
	base, err := decodeContext(vctx)
	if err != nil {
		return err
	}
	now := c.clock.Now()
	v.Clock = c.stamp(base, c.localFloor(ctx, key), now.UnixNano())
	v.TS = now
	_, err = fanout(ctx, c.cfg.Timeout, c.writeTargets(key, owners), need, func(ctx context.Context, t target) (struct{}, error) {
		return struct{}{}, c.putTarget(ctx, t, key, v)
	})
	return err
}

// stamp returns base with this node's counter advanced past floor and past every counter the node issued before.
func (c *Coordinator) stamp(base vclock.Clock, floor uint64, now int64) vclock.Clock {
	for {
		cur := c.seq.Load()
		clock := base.Increment(c.cfg.ID, max(floor, cur), now)
		if c.seq.CompareAndSwap(cur, clock[c.cfg.ID].Counter) {
			return clock
		}
	}
}

// localFloor is the highest counter of this node already stored for key, which survives restarts of a durable engine.
func (c *Coordinator) localFloor(ctx context.Context, key []byte) uint64 {
	var floor uint64
	stored, _ := c.store.Get(ctx, key) // a missing key leaves the floor at 0
	for _, s := range stored {
		floor = max(floor, s.Clock[c.cfg.ID].Counter)
	}
	return floor
}

// plan returns the key's owners and how many of them must answer.
func (c *Coordinator) plan(key []byte, requested, def int) ([]ring.Node, int, error) {
	need := def
	if requested > 0 {
		need = requested
	}
	r := c.cluster.Ring()
	if r == nil {
		return nil, 0, fmt.Errorf("%w: no ring yet", ErrUnavailable)
	}
	owners := r.PreferenceList(key, c.cfg.N)
	if need > len(owners) {
		return nil, 0, fmt.Errorf("%w: need %d, cluster has %d owners", ErrUnavailable, need, len(owners))
	}
	return owners, need, nil
}

func (c *Coordinator) putTo(ctx context.Context, n ring.Node, key []byte, v storage.Versioned) error {
	if n.ID == c.cfg.ID {
		return c.store.Put(ctx, key, v)
	}
	return c.replicas.Put(ctx, n, key, v, "")
}

// putTarget writes v to t, or stores it as a hint when t is a stand-in.
func (c *Coordinator) putTarget(ctx context.Context, t target, key []byte, v storage.Versioned) error {
	switch {
	case t.hintFor == "":
		return c.putTo(ctx, t.Node, key, v)
	case t.ID == c.cfg.ID:
		return c.hints.Store(ctx, t.hintFor, key, v)
	default:
		return c.replicas.Put(ctx, t.Node, key, v, t.hintFor)
	}
}

func (c *Coordinator) getFrom(ctx context.Context, n ring.Node, key []byte) ([]storage.Versioned, error) {
	if n.ID != c.cfg.ID {
		return c.replicas.Get(ctx, n, key)
	}
	vs, err := c.store.Get(ctx, key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	return vs, err
}
