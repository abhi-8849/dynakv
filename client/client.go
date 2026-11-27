package client

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/abhi-8849/dynakv/api/kvpb"
)

// Client talks to a dynakv cluster over gRPC. It sends each request to the key's first live owner and falls back to a seed.
type Client struct {
	seeds  []string
	direct bool
	pool   *pool
	view   atomic.Pointer[view]
	cancel context.CancelFunc
	done   chan struct{}
}

// DialOpt customizes Dial.
type DialOpt func(*Client)

// WithoutRouting sends every request to the first seed and never reads the ring, which targets one chosen node.
func WithoutRouting() DialOpt { return func(c *Client) { c.direct = true } }

// Dial connects to the cluster through seeds, reads its membership and keeps it fresh until Close.
func Dial(ctx context.Context, seeds []string, opts ...DialOpt) (*Client, error) {
	if len(seeds) == 0 {
		return nil, errors.New("client: no seeds")
	}
	c := &Client{seeds: seeds, pool: newPool(), done: make(chan struct{})}
	for _, o := range opts {
		o(c)
	}
	if c.direct {
		close(c.done)
		return c, nil
	}
	if err := c.refresh(ctx); err != nil {
		c.pool.close()
		return nil, fmt.Errorf("client: no seed reachable: %w", err)
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go c.refreshLoop(loopCtx)
	return c, nil
}

// Close stops the refresh loop and releases the connections.
func (c *Client) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	<-c.done
	c.pool.close()
	return nil
}

// Get returns every concurrent value of key (more than one means siblings) and the context to pass to the next write.
func (c *Client) Get(ctx context.Context, key []byte, opts ...ReadOpt) (values [][]byte, vctx []byte, err error) {
	var o readOpts
	for _, opt := range opts {
		opt(&o)
	}
	var resp *kvpb.GetResponse
	err = c.route(key, func(kv kvpb.KVClient) (err error) {
		resp, err = kv.Get(ctx, &kvpb.GetRequest{Key: key, R: int32(o.r)})
		return err
	})
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
	return convert(c.route(key, func(kv kvpb.KVClient) error {
		_, err := kv.Put(ctx, &kvpb.PutRequest{Key: key, Value: value, Context: o.vctx, W: int32(o.w)})
		return err
	}))
}

// Delete removes key by writing a tombstone.
func (c *Client) Delete(ctx context.Context, key []byte, opts ...WriteOpt) error {
	o := applyWrite(opts)
	return convert(c.route(key, func(kv kvpb.KVClient) error {
		_, err := kv.Delete(ctx, &kvpb.DeleteRequest{Key: key, Context: o.vctx, W: int32(o.w)})
		return err
	}))
}

// route runs call against each candidate node until one answers with anything but Unavailable. A retry after Unavailable can leave
// an extra version of the key behind, as with any retried write.
func (c *Client) route(key []byte, call func(kvpb.KVClient) error) error {
	var err error
	for _, addr := range c.candidates(key) {
		conn, derr := c.pool.get(addr)
		if derr != nil {
			err = derr
			continue
		}
		if err = call(kvpb.NewKVClient(conn)); status.Code(err) != codes.Unavailable {
			return err
		}
	}
	return err
}

func applyWrite(opts []WriteOpt) writeOpts {
	var o writeOpts
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
