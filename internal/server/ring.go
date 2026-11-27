package server

import (
	"context"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/membership"
	"github.com/abhi-8849/dynakv/internal/ring"
)

// Cluster is what the Ring service needs to know about the cluster.
type Cluster interface {
	Members() []membership.Member
	Ring() *ring.Ring
}

// Ring implements the Ring gRPC service: cluster status and key placement.
type Ring struct {
	kvpb.UnimplementedRingServer
	cluster Cluster
	n       int
}

// NewRing returns a Ring service; n is the replication factor used for owner lists.
func NewRing(cluster Cluster, n int) *Ring { return &Ring{cluster: cluster, n: n} }

// Status implements kvpb.RingServer.
func (s *Ring) Status(context.Context, *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	members := s.cluster.Members()
	out := make([]*kvpb.Member, len(members))
	for i, m := range members {
		out[i] = toMember(m.Node, m.Alive)
	}
	return &kvpb.StatusResponse{Members: out}, nil
}

// Owners implements kvpb.RingServer.
func (s *Ring) Owners(_ context.Context, req *kvpb.OwnersRequest) (*kvpb.OwnersResponse, error) {
	if err := validate(req.GetKey(), nil, 0); err != nil {
		return nil, err
	}
	alive := map[string]bool{}
	for _, m := range s.cluster.Members() {
		alive[m.ID] = m.Alive
	}
	var out []*kvpb.Member
	if r := s.cluster.Ring(); r != nil {
		for _, n := range r.PreferenceList(req.GetKey(), s.n) {
			out = append(out, toMember(n, alive[n.ID]))
		}
	}
	return &kvpb.OwnersResponse{Owners: out}, nil
}

func toMember(n ring.Node, alive bool) *kvpb.Member {
	return &kvpb.Member{Id: n.ID, Addr: n.Addr, Zone: n.Zone, Alive: alive}
}
