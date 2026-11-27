package coordinator

import "github.com/abhi-8849/dynakv/internal/ring"

func plain(nodes []ring.Node) []target {
	out := make([]target, len(nodes))
	for i, n := range nodes {
		out[i] = target{Node: n}
	}
	return out
}

// writeTargets applies the sloppy quorum: an owner that gossip marks down is replaced by the next healthy node on the ring walk,
// which holds the write as a hint for it. With no healthy node left the owner itself is tried and will fail the quorum.
func (c *Coordinator) writeTargets(key []byte, owners []ring.Node) []target {
	out := plain(owners)
	used := make(map[string]bool, len(owners))
	var walk []ring.Node
	for _, o := range owners {
		used[o.ID] = true
	}
	for i, o := range owners {
		if c.cluster.Alive(o.ID) {
			continue
		}
		if walk == nil {
			walk = c.cluster.Ring().Walk(key)
		}
		for _, n := range walk {
			if !used[n.ID] && c.cluster.Alive(n.ID) {
				used[n.ID] = true
				out[i] = target{Node: n, hintFor: o.ID}
				break
			}
		}
	}
	return out
}
