package lsm

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/abhi-8849/dynakv/internal/storage"
)

// walSyncInterval is the group-commit period: writers arriving within it share one fsync.
const walSyncInterval = 10 * time.Millisecond

// Clock supplies wall time so callers can control it.
type Clock interface {
	Now() time.Time
}

// Options configure an Engine.
type Options struct {
	// Dir holds the WAL, the SSTables and the manifest.
	Dir string
	// MemtableSize is the approximate byte size at which the memtable is flushed to an SSTable.
	MemtableSize int64
	// TombstoneGrace is how long a tombstone version is kept; compaction drops older ones.
	TombstoneGrace time.Duration
	Clock          Clock
	// Log receives flush and compaction events; nil discards them.
	Log *slog.Logger
}

// Engine is a log-structured merge tree: writes go to a WAL and a memtable, full memtables become immutable SSTables, and
// size-tiered compaction merges tables. Each record holds a key's complete version list, so the newest record for a key wins.
type Engine struct {
	opts Options
	log  *slog.Logger

	mu      sync.RWMutex
	mem     *memtable
	wal     *wal
	walPath string
	tables  []*table // oldest first
	nextID  uint64

	closeOnce sync.Once
	stop      chan struct{}
	done      chan struct{}
}

var _ storage.Engine = (*Engine)(nil)

// Open loads the tables listed in the manifest, replays the WAL into a memtable and flushes it, so the engine starts with an empty
// memtable and a fresh log.
func Open(opts Options) (*Engine, error) {
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("lsm: create dir: %w", err)
	}
	e := &Engine{opts: opts, log: opts.Log, mem: newMemtable(), stop: make(chan struct{}), done: make(chan struct{})}
	if e.log == nil {
		e.log = slog.New(slog.DiscardHandler)
	}
	oldWALs, err := e.recover()
	if err != nil {
		return nil, err
	}
	if err := e.startWAL(); err != nil {
		return nil, err
	}
	if err := e.flush(); err != nil {
		return nil, err
	}
	for _, p := range oldWALs {
		os.Remove(p)
	}
	// syncLoop is started here and stopped by Close, which closes stop and waits on done; the loop closes done when it returns.
	go e.syncLoop()
	return e, nil
}

// recover opens the manifest's tables, removes files no manifest lists, and replays every WAL into the memtable.
func (e *Engine) recover() ([]string, error) {
	names, err := readManifest(e.opts.Dir)
	if err != nil {
		return nil, err
	}
	live := map[string]bool{}
	for _, n := range names {
		t, err := openTable(filepath.Join(e.opts.Dir, n))
		if err != nil {
			return nil, err
		}
		e.tables = append(e.tables, t)
		live[n] = true
	}
	files, err := os.ReadDir(e.opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("lsm: list dir: %w", err)
	}
	var wals []string
	for _, f := range files {
		name, path := f.Name(), filepath.Join(e.opts.Dir, f.Name())
		if id, err := strconv.ParseUint(strings.TrimSuffix(name, filepath.Ext(name)), 10, 64); err == nil {
			e.nextID = max(e.nextID, id+1)
		}
		switch filepath.Ext(name) {
		case ".sst":
			if !live[name] {
				os.Remove(path)
			}
		case ".tmp":
			os.Remove(path)
		case ".wal":
			wals = append(wals, path)
		}
	}
	sort.Strings(wals)
	for _, p := range wals {
		if err := replayWAL(p, e.mem.set); err != nil {
			return nil, err
		}
	}
	return wals, nil
}

func (e *Engine) allocPath(ext string) string {
	id := e.nextID
	e.nextID++
	return filepath.Join(e.opts.Dir, fmt.Sprintf("%06d%s", id, ext))
}

// startWAL opens a new log and makes it current.
func (e *Engine) startWAL() error {
	path := e.allocPath(".wal")
	w, err := createWAL(path)
	if err != nil {
		return err
	}
	e.wal, e.walPath = w, path
	return nil
}

// syncLoop commits the current WAL every walSyncInterval until Close.
func (e *Engine) syncLoop() {
	defer close(e.done)
	t := time.NewTicker(walSyncInterval)
	defer t.Stop()
	for {
		select {
		case <-e.stop: // Close was called
			return
		case <-t.C: // time for the next group commit
			e.mu.RLock()
			w := e.wal
			e.mu.RUnlock()
			if err := w.commit(); err != nil {
				e.log.Error("wal commit failed", "err", err)
			}
		}
	}
}

// Put implements storage.Engine. It returns once the write is on disk.
func (e *Engine) Put(ctx context.Context, key []byte, v storage.Versioned) error {
	e.mu.Lock()
	cur, err := e.lookup(key)
	if err != nil {
		e.mu.Unlock()
		return err
	}
	next, changed := storage.Merge(cur, v)
	if !changed {
		e.mu.Unlock()
		return nil
	}
	ch, err := e.apply(key, next)
	e.mu.Unlock()
	return waitCommit(ctx, ch, err)
}

// Remove implements storage.Engine. It writes an empty record that hides older ones until compaction drops it.
func (e *Engine) Remove(ctx context.Context, key []byte) error {
	e.mu.Lock()
	cur, err := e.lookup(key)
	if err != nil || len(cur) == 0 {
		e.mu.Unlock()
		return err
	}
	ch, err := e.apply(key, nil)
	e.mu.Unlock()
	return waitCommit(ctx, ch, err)
}

// Get implements storage.Engine.
func (e *Engine) Get(_ context.Context, key []byte) ([]storage.Versioned, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	vs, err := e.lookup(key)
	if err != nil {
		return nil, err
	}
	if len(vs) == 0 {
		return nil, storage.ErrNotFound
	}
	return vs, nil
}

// apply logs a record and puts it in the memtable, flushing when the memtable is full. The caller holds e.mu.
func (e *Engine) apply(key []byte, vs []storage.Versioned) (<-chan error, error) {
	ch, err := e.wal.append(encodeRecord(key, vs))
	if err != nil {
		return nil, err
	}
	e.mem.set(key, vs)
	if e.mem.size >= e.opts.MemtableSize {
		if err := e.flush(); err != nil {
			return nil, err
		}
	}
	return ch, nil
}

// lookup returns the newest record for key: the memtable first, then the tables from newest to oldest. An empty result means the
// key is absent. The caller holds e.mu.
func (e *Engine) lookup(key []byte) ([]storage.Versioned, error) {
	if vs, ok := e.mem.get(key); ok {
		return vs, nil
	}
	for i := len(e.tables) - 1; i >= 0; i-- {
		vs, found, err := e.tables[i].get(key)
		if err != nil {
			return nil, err
		}
		if found {
			return vs, nil
		}
	}
	return nil, nil
}

func waitCommit(ctx context.Context, ch <-chan error, err error) error {
	if err != nil {
		return err
	}
	select {
	case err := <-ch: // the syncer or a flush committed the log
		return err
	case <-ctx.Done(): // the caller gave up; the write still lands
		return ctx.Err()
	}
}

// Scan implements storage.Engine. It works on a snapshot, so fn may call Put and Remove.
func (e *Engine) Scan(ctx context.Context, prefix []byte, fn func(key []byte, vs []storage.Versioned) error) error {
	type rec struct {
		key []byte
		vs  []storage.Versioned
	}
	recs, err := func() ([]rec, error) {
		e.mu.RLock()
		defer e.mu.RUnlock()
		keys, err := e.keysWithPrefix(prefix)
		if err != nil {
			return nil, err
		}
		var out []rec
		for _, k := range keys {
			vs, err := e.lookup([]byte(k))
			if err != nil {
				return nil, err
			}
			if len(vs) > 0 {
				out = append(out, rec{[]byte(k), vs})
			}
		}
		return out, nil
	}()
	if err != nil {
		return err
	}
	for _, r := range recs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(r.key, r.vs); err != nil {
			return err
		}
	}
	return nil
}

// keysWithPrefix returns the sorted keys with the prefix from the memtable and every table. The caller holds e.mu.
func (e *Engine) keysWithPrefix(prefix []byte) ([]string, error) {
	set := map[string]struct{}{}
	for k := range e.mem.m {
		if bytes.HasPrefix([]byte(k), prefix) {
			set[k] = struct{}{}
		}
	}
	for _, t := range e.tables {
		it := t.iter(prefix)
		for ; it.ok && bytes.HasPrefix(it.key, prefix); it.next() {
			set[string(it.key)] = struct{}{}
		}
		if it.err != nil {
			return nil, it.err
		}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

// Close implements storage.Engine: it stops the syncer, flushes the memtable and closes every file.
func (e *Engine) Close() error {
	var err error
	e.closeOnce.Do(func() {
		close(e.stop)
		<-e.done
		e.mu.Lock()
		defer e.mu.Unlock()
		err = e.flush()
		if cerr := e.wal.close(); err == nil {
			err = cerr
		}
		for _, t := range e.tables {
			if cerr := t.close(); err == nil {
				err = cerr
			}
		}
	})
	return err
}
