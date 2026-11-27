package ring

import (
	"cmp"
	"slices"
	"strconv"

	"github.com/cespare/xxhash/v2"
)

// Defaults shared by nodes and clients; a client that routes with other values only loses the shortcut, because nodes forward
// requests they do not own.
const (
	DefaultVnodes   = 256
	DefaultReplicas = 3
)

// Node is a cluster member that can own keys.
type Node struct {
	// ID is the unique node name; it seeds the node's virtual node positions.
	ID string
	// Addr is the node's gRPC host:port.
	Addr string
	// Zone is the failure domain; replicas are spread across zones when possible.
	Zone string
}

type vnode struct {
	hash uint64
	node int // index into Ring.nodes
}

// Ring places keys on nodes with consistent hashing. It is immutable: a membership change builds a new Ring and the owner swaps
// it in, so readers never need a lock.
type Ring struct {
	nodes  []Node
	vnodes []vnode // sorted by hash
}

// New builds a ring where each node owns vnodes positions. Many positions per node spread load evenly and keep key movement small
// when a node joins or leaves.
func New(nodes []Node, vnodes int) *Ring {
	r := &Ring{nodes: slices.Clone(nodes)}
	for i, n := range r.nodes {
		for v := range vnodes {
			r.vnodes = append(r.vnodes, vnode{hash: xxhash.Sum64String(n.ID + "#" + strconv.Itoa(v)), node: i})
		}
	}
	slices.SortFunc(r.vnodes, func(a, b vnode) int {
		// Equal hashes are broken by node id so every member builds the same ring.
		return cmp.Or(cmp.Compare(a.hash, b.hash), cmp.Compare(r.nodes[a.node].ID, r.nodes[b.node].ID))
	})
	return r
}

// PreferenceList returns up to n distinct nodes that own key, in preference order. It walks clockwise from the key's position and
// takes a node only if its zone is not used yet; nodes in a used zone are held back and fill the list only after every zone is
// covered. The result is shorter than n when the ring has fewer nodes.
func (r *Ring) PreferenceList(key []byte, n int) []Node {
	if len(r.vnodes) == 0 || n <= 0 {
		return nil
	}
	start, _ := slices.BinarySearchFunc(r.vnodes, xxhash.Sum64(key), func(v vnode, h uint64) int { return cmp.Compare(v.hash, h) })
	seen := make(map[int]struct{}, n)
	usedZones := make(map[string]struct{}, n)
	var picked []Node
	var held []Node
	for i := 0; i < len(r.vnodes) && len(picked) < n && len(seen) < len(r.nodes); i++ {
		idx := r.vnodes[(start+i)%len(r.vnodes)].node
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		node := r.nodes[idx]
		if _, used := usedZones[node.Zone]; used {
			held = append(held, node)
			continue
		}
		usedZones[node.Zone] = struct{}{}
		picked = append(picked, node)
	}
	picked = append(picked, held...)
	return picked[:min(n, len(picked))]
}
