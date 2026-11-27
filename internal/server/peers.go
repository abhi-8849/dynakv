package server

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/ring"
	"github.com/abhi-8849/dynakv/internal/storage"
)

// Peers holds one lazily dialed gRPC connection per peer address and reaches the peers' Replica and KV services.
type Peers struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

// NewPeers returns an empty connection pool.
func NewPeers() *Peers { return &Peers{conns: map[string]*grpc.ClientConn{}} }

func (p *Peers) conn(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	// Nodes talk on a private network without authentication.
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("server: dial peer %s: %w", addr, err)
	}
	p.conns[addr] = c
	return c, nil
}

func (p *Peers) kv(addr string) (kvpb.KVClient, error) {
	c, err := p.conn(addr)
	if err != nil {
		return nil, err
	}
	return kvpb.NewKVClient(c), nil
}

// Put stores v on peer to through its Replica service; a non-empty hintFor makes the peer keep it as a hint for that node.
func (p *Peers) Put(ctx context.Context, to ring.Node, key []byte, v storage.Versioned, hintFor string) error {
	c, err := p.conn(to.Addr)
	if err != nil {
		return err
	}
	_, err = kvpb.NewReplicaClient(c).ReplicaPut(ctx, &kvpb.ReplicaPutRequest{Key: key, Version: storage.ToProto(v), HintFor: hintFor})
	return err
}

// ReplayHint delivers a held write to the node it was meant for.
func (p *Peers) ReplayHint(ctx context.Context, to ring.Node, key []byte, v storage.Versioned) error {
	c, err := p.conn(to.Addr)
	if err != nil {
		return err
	}
	_, err = kvpb.NewReplicaClient(c).ReplayHint(ctx, &kvpb.ReplayHintRequest{Key: key, Version: storage.ToProto(v)})
	return err
}

// Get returns the versions peer to holds for key; a peer without the key returns none.
func (p *Peers) Get(ctx context.Context, to ring.Node, key []byte) ([]storage.Versioned, error) {
	c, err := p.conn(to.Addr)
	if err != nil {
		return nil, err
	}
	resp, err := kvpb.NewReplicaClient(c).ReplicaGet(ctx, &kvpb.ReplicaGetRequest{Key: key})
	if err != nil {
		return nil, err
	}
	out := make([]storage.Versioned, len(resp.GetVersions()))
	for i, v := range resp.GetVersions() {
		out[i] = storage.FromProto(v)
	}
	return out, nil
}

// Close closes every pooled connection.
func (p *Peers) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var first error
	for addr, c := range p.conns {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
		delete(p.conns, addr)
	}
	return first
}
