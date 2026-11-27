package hints

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/abhi-8849/dynakv/internal/storage"
)

// Prefix is the reserved key prefix under which hints live in the engine; clients may not use it for their own keys.
const Prefix = "\x00hint/"

// Clock supplies wall time so callers can control it.
type Clock interface {
	Now() time.Time
}

// Spool keeps writes meant for a down node, in the node's own storage engine so a durable engine keeps them across restarts.
type Spool struct {
	engine storage.Engine
	clock  Clock
	ttl    time.Duration
	log    *slog.Logger
}

// New returns a spool whose hints expire ttl after they were written.
func New(engine storage.Engine, clock Clock, ttl time.Duration, log *slog.Logger) *Spool {
	return &Spool{engine: engine, clock: clock, ttl: ttl, log: log}
}

// key is Prefix + target + 0x00 + the data key, so one Scan prefix selects all hints for a target.
func key(target string, dataKey []byte) []byte {
	return append([]byte(Prefix+target+"\x00"), dataKey...)
}

// Store keeps v for target. Several hints for the same key collapse the way versions do.
func (s *Spool) Store(ctx context.Context, target string, dataKey []byte, v storage.Versioned) error {
	if err := s.engine.Put(ctx, key(target, dataKey), v); err != nil {
		return fmt.Errorf("hints: store: %w", err)
	}
	s.log.Info("hint stored", "target", target, "key", string(dataKey))
	return nil
}

// Replay sends every hint held for target and removes each one that send accepted. It stops at the first send error so the
// remaining hints are tried again later; sending a version twice is harmless.
func (s *Spool) Replay(ctx context.Context, target string, send func(key []byte, v storage.Versioned) error) (int, error) {
	prefix := key(target, nil)
	sent := 0
	err := s.engine.Scan(ctx, prefix, func(hk []byte, vs []storage.Versioned) error {
		dataKey := bytes.TrimPrefix(hk, prefix)
		for _, v := range vs {
			if err := send(dataKey, v); err != nil {
				return err
			}
		}
		sent++
		return s.engine.Remove(ctx, hk)
	})
	return sent, err
}

// Sweep removes hints older than the ttl, for live and dead targets alike, and returns how many it removed.
func (s *Spool) Sweep(ctx context.Context) (int, error) {
	expired := 0
	cutoff := s.clock.Now().Add(-s.ttl)
	err := s.engine.Scan(ctx, []byte(Prefix), func(hk []byte, vs []storage.Versioned) error {
		for _, v := range vs {
			if v.TS.After(cutoff) {
				return nil // a version still inside the ttl keeps the whole hint
			}
		}
		expired++
		return s.engine.Remove(ctx, hk)
	})
	return expired, err
}
