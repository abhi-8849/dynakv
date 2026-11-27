package coordinator

import (
	"context"

	"github.com/abhi-8849/dynakv/internal/ring"
	"github.com/abhi-8849/dynakv/internal/storage"
	"github.com/abhi-8849/dynakv/internal/vclock"
)

// repairStale writes the reconciled versions to every replica that answered without them, so the replicas converge without a
// separate sync. It returns at once: Get starts one goroutine that is the only sender to those replicas, owns the work, and ends
// when every write finished or the timeout passed. Nothing waits for it and no channel is involved.
func (c *Coordinator) repairStale(ctx context.Context, key []byte, merged []storage.Versioned, replies []result[[]storage.Versioned]) {
	type fix struct {
		node ring.Node
		vs   []storage.Versioned
	}
	var fixes []fix
	for _, rep := range replies {
		if missing := missingFrom(merged, rep.val); len(missing) > 0 {
			fixes = append(fixes, fix{rep.target.Node, missing})
		}
	}
	if len(fixes) == 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.Timeout)
		defer cancel()
		for _, f := range fixes {
			for _, v := range f.vs {
				if err := c.putTo(ctx, f.node, key, v); err != nil {
					c.log.Warn("read repair failed", "key", string(key), "node", f.node.ID, "err", err)
					break
				}
			}
			c.log.Info("read repair", "key", string(key), "node", f.node.ID, "versions", len(f.vs))
		}
	}()
}

// missingFrom returns the versions in merged that have no equal or newer counterpart in have.
func missingFrom(merged, have []storage.Versioned) []storage.Versioned {
	var out []storage.Versioned
	for _, m := range merged {
		covered := false
		for _, h := range have {
			if o := h.Clock.Compare(m.Clock); o == vclock.After || o == vclock.Equal {
				covered = true
				break
			}
		}
		if !covered {
			out = append(out, m)
		}
	}
	return out
}
