package client

import (
	"context"

	"github.com/abhi-8849/dynakv/api/kvpb"
)

// Member is a cluster node as reported by the Ring service.
type Member struct {
	ID    string
	Addr  string
	Zone  string
	Alive bool
}

// Status lists every known node and whether it is alive.
func (c *Client) Status(ctx context.Context) ([]Member, error) {
	resp, err := c.ring.Status(ctx, &kvpb.StatusRequest{})
	if err != nil {
		return nil, convert(err)
	}
	return fromProto(resp.GetMembers()), nil
}

// Owners returns the nodes that own key, in preference order.
func (c *Client) Owners(ctx context.Context, key []byte) ([]Member, error) {
	resp, err := c.ring.Owners(ctx, &kvpb.OwnersRequest{Key: key})
	if err != nil {
		return nil, convert(err)
	}
	return fromProto(resp.GetOwners()), nil
}

func fromProto(ms []*kvpb.Member) []Member {
	out := make([]Member, len(ms))
	for i, m := range ms {
		out[i] = Member{ID: m.GetId(), Addr: m.GetAddr(), Zone: m.GetZone(), Alive: m.GetAlive()}
	}
	return out
}
