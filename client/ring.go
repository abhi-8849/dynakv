package client

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	return c.status(ctx)
}

// Owners returns the nodes that own key, in preference order.
func (c *Client) Owners(ctx context.Context, key []byte) ([]Member, error) {
	var resp *kvpb.OwnersResponse
	err := c.anySeed(func(r kvpb.RingClient) (err error) {
		resp, err = r.Owners(ctx, &kvpb.OwnersRequest{Key: key})
		return err
	})
	if err != nil {
		return nil, convert(err)
	}
	return fromProto(resp.GetOwners()), nil
}

func (c *Client) status(ctx context.Context) ([]Member, error) {
	var resp *kvpb.StatusResponse
	err := c.anySeed(func(r kvpb.RingClient) (err error) {
		resp, err = r.Status(ctx, &kvpb.StatusRequest{})
		return err
	})
	if err != nil {
		return nil, convert(err)
	}
	return fromProto(resp.GetMembers()), nil
}

// anySeed runs call against the seeds in order until one is reachable.
func (c *Client) anySeed(call func(kvpb.RingClient) error) error {
	var err error
	for _, addr := range c.seeds {
		conn, derr := c.pool.get(addr)
		if derr != nil {
			err = derr
			continue
		}
		if err = call(kvpb.NewRingClient(conn)); status.Code(err) != codes.Unavailable {
			return err
		}
		if c.direct {
			break
		}
	}
	return err
}

func fromProto(ms []*kvpb.Member) []Member {
	out := make([]Member, len(ms))
	for i, m := range ms {
		out[i] = Member{ID: m.GetId(), Addr: m.GetAddr(), Zone: m.GetZone(), Alive: m.GetAlive()}
	}
	return out
}
