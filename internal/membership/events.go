package membership

import (
	"github.com/hashicorp/memberlist"
	"google.golang.org/protobuf/proto"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/ring"
)

// delegate publishes this node's metadata (gRPC address and zone) through gossip and carries no other data.
type delegate struct{ meta []byte }

func (d delegate) NodeMeta(int) []byte           { return d.meta }
func (delegate) NotifyMsg([]byte)                {}
func (delegate) GetBroadcasts(int, int) [][]byte { return nil }
func (delegate) LocalState(bool) []byte          { return nil }
func (delegate) MergeRemoteState([]byte, bool)   {}

// events receives join, leave and update callbacks from memberlist. Callbacks must not block.
type events struct{ m *Membership }

func (e events) NotifyJoin(n *memberlist.Node)   { e.m.upsert(n) }
func (e events) NotifyUpdate(n *memberlist.Node) { e.m.upsert(n) }
func (e events) NotifyLeave(n *memberlist.Node)  { e.m.markDead(n.Name) }

// upsert records a live member and rebuilds the ring when the set of nodes or their placement changed.
func (m *Membership) upsert(n *memberlist.Node) {
	var meta kvpb.Member
	if err := proto.Unmarshal(n.Meta, &meta); err != nil {
		m.log.Warn("ignoring member with bad metadata", "id", n.Name, "err", err)
		return
	}
	node := ring.Node{ID: n.Name, Addr: meta.GetAddr(), Zone: meta.GetZone()}
	m.mu.Lock()
	defer m.mu.Unlock()
	old, known := m.members[node.ID]
	m.members[node.ID] = Member{Node: node, Alive: true}
	if !known || old.Node != node {
		m.rebuild()
	}
	if !known || !old.Alive {
		m.log.Info("member alive", "id", node.ID, "addr", node.Addr, "zone", node.Zone)
	}
}

// markDead keeps the member in the ring and only flags it, so key ownership does not move.
func (m *Membership) markDead(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mem, ok := m.members[id]; ok && mem.Alive {
		mem.Alive = false
		m.members[id] = mem
		m.log.Warn("member dead", "id", id)
	}
}

// rebuild swaps in a ring built from all known members. The caller holds m.mu.
func (m *Membership) rebuild() {
	nodes := make([]ring.Node, 0, len(m.members))
	for _, mem := range m.members {
		nodes = append(nodes, mem.Node)
	}
	m.current.Store(ring.New(nodes, m.vnodes))
}
