package client

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/abhi-8849/dynakv/api/kvpb"
)

// Client talks to a dynakv cluster over gRPC.
type Client struct {
	conn *grpc.ClientConn
	kv   kvpb.KVClient
}

// Dial connects to the first seed. The connection is established on first use.
func Dial(_ context.Context, seeds []string) (*Client, error) {
	if len(seeds) == 0 {
		return nil, errors.New("client: no seeds")
	}
	// The MVP has no authentication; nodes and clients share a private network.
	conn, err := grpc.NewClient(seeds[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", seeds[0], err)
	}
	return &Client{conn: conn, kv: kvpb.NewKVClient(conn)}, nil
}

// Close releases the connection.
func (c *Client) Close() error { return c.conn.Close() }

// Get returns every concurrent value of key (more than one means siblings) and the context to pass to the next write.
func (c *Client) Get(ctx context.Context, key []byte, opts ...ReadOpt) (values [][]byte, vctx []byte, err error) {
	var o readOpts
	for _, opt := range opts {
		opt(&o)
	}
	resp, err := c.kv.Get(ctx, &kvpb.GetRequest{Key: key, R: int32(o.r)})
	if err != nil {
		return nil, nil, convert(err)
	}
	for _, v := range resp.GetVersions() {
		values = append(values, v.GetValue())
	}
	return values, resp.GetContext(), nil
}

// Put stores value under key.
func (c *Client) Put(ctx context.Context, key, value []byte, opts ...WriteOpt) error {
	o := applyWrite(opts)
	_, err := c.kv.Put(ctx, &kvpb.PutRequest{Key: key, Value: value, Context: o.vctx, W: int32(o.w)})
	return convert(err)
}

// Delete removes key by writing a tombstone.
func (c *Client) Delete(ctx context.Context, key []byte, opts ...WriteOpt) error {
	o := applyWrite(opts)
	_, err := c.kv.Delete(ctx, &kvpb.DeleteRequest{Key: key, Context: o.vctx, W: int32(o.w)})
	return convert(err)
}

func applyWrite(opts []WriteOpt) writeOpts {
	var o writeOpts
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
