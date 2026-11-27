package hints

import (
	"context"
	"log/slog"
	"time"

	"github.com/abhi-8849/dynakv/internal/membership"
	"github.com/abhi-8849/dynakv/internal/ring"
	"github.com/abhi-8849/dynakv/internal/storage"
)

// Cluster lists the members and their liveness.
type Cluster interface {
	Members() []membership.Member
}

// Sender delivers a held write to the node it was meant for.
type Sender interface {
	ReplayHint(ctx context.Context, to ring.Node, key []byte, v storage.Versioned) error
}

// Replayer hands hints back to their targets once gossip reports them alive, and expires old hints.
type Replayer struct {
	Spool    *Spool
	Cluster  Cluster
	Sender   Sender
	Self     string
	Interval time.Duration
	Log      *slog.Logger
}

// Run blocks until ctx is cancelled. The node starts it in its own goroutine and cancels ctx to stop it; it sends and receives no
// channel values and only reads the ticker.
func (r *Replayer) Run(ctx context.Context) {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done(): // the node is shutting down
			return
		case <-t.C: // time for the next sweep and replay pass
			r.pass(ctx)
		}
	}
}

func (r *Replayer) pass(ctx context.Context) {
	if n, err := r.Spool.Sweep(ctx); err != nil {
		r.Log.Warn("hint sweep failed", "err", err)
	} else if n > 0 {
		r.Log.Info("hints expired", "count", n)
	}
	for _, m := range r.Cluster.Members() {
		if m.ID == r.Self || !m.Alive {
			continue
		}
		n, err := r.Spool.Replay(ctx, m.ID, func(key []byte, v storage.Versioned) error {
			return r.Sender.ReplayHint(ctx, m.Node, key, v)
		})
		if n > 0 {
			r.Log.Info("hints replayed", "target", m.ID, "count", n)
		}
		if err != nil {
			r.Log.Warn("hint replay stopped", "target", m.ID, "err", err)
		}
	}
}
