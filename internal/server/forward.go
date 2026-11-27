package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/ring"
	"github.com/abhi-8849/dynakv/internal/storage"
)

// forwardedKey marks a request that was already forwarded, so the receiver coordinates it instead of forwarding again.
const forwardedKey = "x-dynakv-forwarded"

// Forwarder sends requests for keys this node does not own to a live owner. A node that owns the key coordinates it itself, which is
// also how a client reaches a chosen replica directly.
type Forwarder struct {
	next    Coordinator
	self    string
	n       int
	cluster Cluster
	peers   *Peers
}

// NewForwarder wraps next, the local coordinator, with forwarding for keys outside this node's preference lists.
func NewForwarder(next Coordinator, self string, n int, cluster Cluster, peers *Peers) *Forwarder {
	return &Forwarder{next: next, self: self, n: n, cluster: cluster, peers: peers}
}

// Get implements Coordinator.
func (f *Forwarder) Get(ctx context.Context, key []byte, r int) ([]storage.Versioned, []byte, error) {
	targets, local := f.route(ctx, key)
	if local {
		return f.next.Get(ctx, key, r)
	}
	var resp *kvpb.GetResponse
	err := f.forward(ctx, targets, func(ctx context.Context, kv kvpb.KVClient) (err error) {
		resp, err = kv.Get(ctx, &kvpb.GetRequest{Key: key, R: int32(r)})
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	vs := make([]storage.Versioned, len(resp.GetVersions()))
	for i, v := range resp.GetVersions() {
		vs[i] = storage.FromProto(v)
	}
	return vs, resp.GetContext(), nil
}

// Put implements Coordinator.
func (f *Forwarder) Put(ctx context.Context, key, value, vctx []byte, w int) error {
	targets, local := f.route(ctx, key)
	if local {
		return f.next.Put(ctx, key, value, vctx, w)
	}
	return f.forward(ctx, targets, func(ctx context.Context, kv kvpb.KVClient) error {
		_, err := kv.Put(ctx, &kvpb.PutRequest{Key: key, Value: value, Context: vctx, W: int32(w)})
		return err
	})
}

// Delete implements Coordinator.
func (f *Forwarder) Delete(ctx context.Context, key, vctx []byte, w int) error {
	targets, local := f.route(ctx, key)
	if local {
		return f.next.Delete(ctx, key, vctx, w)
	}
	return f.forward(ctx, targets, func(ctx context.Context, kv kvpb.KVClient) error {
		_, err := kv.Delete(ctx, &kvpb.DeleteRequest{Key: key, Context: vctx, W: int32(w)})
		return err
	})
}

// route reports whether this node coordinates key, and otherwise the live owners to forward to in preference order.
func (f *Forwarder) route(ctx context.Context, key []byte) (targets []ring.Node, local bool) {
	if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get(forwardedKey)) > 0 {
		return nil, true
	}
	r := f.cluster.Ring()
	if r == nil {
		return nil, true // the coordinator reports the missing ring
	}
	for _, n := range r.PreferenceList(key, f.n) {
		if n.ID == f.self {
			return nil, true
		}
		if f.cluster.Alive(n.ID) {
			targets = append(targets, n)
		}
	}
	return targets, false
}

// forward tries each target in order and moves on only when a target cannot be reached or reports itself unavailable.
func (f *Forwarder) forward(ctx context.Context, targets []ring.Node, call func(context.Context, kvpb.KVClient) error) error {
	err := status.Error(codes.Unavailable, "no live owner for the key")
	ctx = metadata.AppendToOutgoingContext(ctx, forwardedKey, "1")
	for _, t := range targets {
		kv, derr := f.peers.kv(t.Addr)
		if derr != nil {
			err = derr
			continue
		}
		if err = call(ctx, kv); status.Code(err) != codes.Unavailable {
			return err
		}
	}
	return err
}
