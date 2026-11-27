package client

import (
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// pool keeps one lazily dialed gRPC connection per node address.
type pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newPool() *pool { return &pool{conns: map[string]*grpc.ClientConn{}} }

func (p *pool) get(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	// The MVP has no authentication; nodes and clients share a private network.
	c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", addr, err)
	}
	p.conns[addr] = c
	return c, nil
}

func (p *pool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, c := range p.conns {
		_ = c.Close()
		delete(p.conns, addr)
	}
}
