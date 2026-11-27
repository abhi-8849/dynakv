package client

import (
	"context"
	"time"

	"github.com/abhi-8849/dynakv/internal/ring"
)

// refreshInterval is how often the client re-reads cluster membership from a seed.
const refreshInterval = 10 * time.Second

// view is an immutable snapshot of the cluster the client routes with.
type view struct {
	ring  *ring.Ring
	alive map[string]bool
}

// refresh reads Ring.Status from the first reachable seed and replaces the routing view.
func (c *Client) refresh(ctx context.Context) error {
	members, err := c.status(ctx)
	if err != nil {
		return err
	}
	nodes := make([]ring.Node, len(members))
	alive := make(map[string]bool, len(members))
	for i, m := range members {
		nodes[i] = ring.Node{ID: m.ID, Addr: m.Addr, Zone: m.Zone}
		alive[m.ID] = m.Alive
	}
	c.view.Store(&view{ring: ring.New(nodes, ring.DefaultVnodes), alive: alive})
	return nil
}

// refreshLoop keeps the view fresh until ctx is cancelled. Dial starts it; Close cancels ctx and waits on done, which the loop closes
// on exit. It sends nothing and receives only the ticker; a failed refresh keeps the previous view.
func (c *Client) refreshLoop(ctx context.Context) {
	defer close(c.done)
	t := time.NewTicker(refreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done(): // Close was called
			return
		case <-t.C: // time to re-read membership
			_ = c.refresh(ctx)
		}
	}
}

// candidates lists the addresses to try for key: its first live owner, then the seeds. Without routing it is the first seed only.
func (c *Client) candidates(key []byte) []string {
	if c.direct {
		return c.seeds[:1]
	}
	var addrs []string
	if v := c.view.Load(); v != nil {
		for _, n := range v.ring.PreferenceList(key, ring.DefaultReplicas) {
			if v.alive[n.ID] {
				addrs = append(addrs, n.Addr)
				break
			}
		}
	}
	for _, s := range c.seeds {
		if len(addrs) == 0 || s != addrs[0] {
			addrs = append(addrs, s)
		}
	}
	return addrs
}
