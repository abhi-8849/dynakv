// Command dynakv runs one node of the key-value store.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/coordinator"
	"github.com/abhi-8849/dynakv/internal/membership"
	"github.com/abhi-8849/dynakv/internal/metrics"
	"github.com/abhi-8849/dynakv/internal/ring"
	"github.com/abhi-8849/dynakv/internal/server"
	"github.com/abhi-8849/dynakv/internal/storage"
)

// shutdownTimeout bounds how long leaving gossip and closing the metrics listener may take.
const shutdownTimeout = 5 * time.Second

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

type flags struct {
	id, addr, gossipAddr, metricsAddr, zone, join string
	vnodes, n, r, w                               int
	timeout                                       time.Duration
}

func parseFlags(args []string) (flags, error) {
	host, _ := os.Hostname()
	var f flags
	fs := flag.NewFlagSet("dynakv", flag.ContinueOnError)
	fs.StringVar(&f.id, "id", host, "node id, unique in the cluster")
	fs.StringVar(&f.addr, "addr", ":7000", "gRPC listen address; peers dial it, with localhost standing in for an empty host")
	fs.StringVar(&f.gossipAddr, "gossip-addr", ":7946", "memberlist gossip listen address")
	fs.StringVar(&f.metricsAddr, "metrics-addr", ":9100", "Prometheus /metrics listen address")
	fs.StringVar(&f.join, "join", "", "comma-separated gossip addresses of existing members; empty for the first node")
	fs.StringVar(&f.zone, "zone", "default", "failure domain of this node")
	fs.IntVar(&f.vnodes, "vnodes", ring.DefaultVnodes, "virtual nodes per node on the ring")
	fs.DurationVar(&f.timeout, "timeout", 100*time.Millisecond, "how long a request waits for its replicas")
	fs.IntVar(&f.n, "n", ring.DefaultReplicas, "replication factor")
	fs.IntVar(&f.r, "r", 2, "default read quorum")
	fs.IntVar(&f.w, "w", 2, "default write quorum")
	return f, fs.Parse(args)
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	f, err := parseFlags(os.Args[1:])
	if err == nil {
		var n *node
		if n, err = newNode(f, log); err == nil {
			err = n.run(ctx)
		}
	}
	if err != nil {
		log.Error("node failed", "err", err)
		os.Exit(1)
	}
}

// node is one running dynakv process.
type node struct {
	log        *slog.Logger
	store      storage.Engine
	mem        *membership.Membership
	peers      *server.Peers
	grpcSrv    *grpc.Server
	grpcLis    net.Listener
	metricsSrv *http.Server
	metricsLis net.Listener
}

// newNode opens the listeners, joins the cluster and wires the services. Nothing is served until run.
func newNode(f flags, log *slog.Logger) (*node, error) {
	n := &node{log: log, store: storage.NewMemory(), peers: server.NewPeers()}
	var err error
	if n.grpcLis, err = net.Listen("tcp", f.addr); err != nil {
		return nil, fmt.Errorf("listen %s: %w", f.addr, err)
	}
	if n.metricsLis, err = net.Listen("tcp", f.metricsAddr); err != nil {
		return nil, fmt.Errorf("listen %s: %w", f.metricsAddr, err)
	}
	var seeds []string
	if f.join != "" {
		seeds = strings.Split(f.join, ",")
	}
	n.mem, err = membership.Start(membership.Config{
		Self:       ring.Node{ID: f.id, Addr: advertise(f.addr), Zone: f.zone},
		GossipAddr: f.gossipAddr, Seeds: seeds, Vnodes: f.vnodes, Log: log,
	})
	if err != nil {
		return nil, err
	}
	if err := n.wire(f); err != nil {
		return nil, err
	}
	log.Info("node started", "id", f.id, "addr", f.addr, "gossip_addr", f.gossipAddr, "zone", f.zone)
	return n, nil
}

// wire builds the coordinator and registers the gRPC services and the metrics endpoint.
func (n *node) wire(f flags) error {
	cfg := coordinator.Config{ID: f.id, N: f.n, R: f.r, W: f.w, Timeout: f.timeout, Log: n.log}
	coord, err := coordinator.New(cfg, n.store, wallClock{}, n.mem, n.peers)
	if err != nil {
		return err
	}
	m := metrics.New()
	n.grpcSrv = grpc.NewServer(grpc.ChainUnaryInterceptor(server.Instrument(m)))
	kvpb.RegisterKVServer(n.grpcSrv, server.NewKV(server.NewForwarder(coord, f.id, f.n, n.mem, n.peers)))
	kvpb.RegisterReplicaServer(n.grpcSrv, server.NewReplica(n.store))
	kvpb.RegisterRingServer(n.grpcSrv, server.NewRing(n.mem, f.n))
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	n.metricsSrv = &http.Server{Handler: mux, ReadHeaderTimeout: shutdownTimeout}
	return nil
}

// advertise returns the gRPC address peers should dial; an empty or wildcard host becomes localhost.
func advertise(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "localhost"
	}
	return net.JoinHostPort(host, port)
}

// run serves until ctx is cancelled or a server fails, then leaves the cluster and drains.
func (n *node) run(ctx context.Context) error {
	defer n.store.Close()
	defer n.peers.Close()
	// run starts both serve goroutines and receives their results. Each sends exactly once when its server stops: Serve returns
	// after GracefulStop (nil) or on failure, and the HTTP server returns http.ErrServerClosed after Shutdown. errc is buffered for
	// both so a sender never blocks. It is never closed; run reads both results before returning.
	errc := make(chan error, 2)
	go func() { errc <- n.grpcSrv.Serve(n.grpcLis) }()
	go func() { errc <- n.metricsSrv.Serve(n.metricsLis) }()

	received := 0
	var runErr error
	select {
	case <-ctx.Done(): // SIGINT or SIGTERM: begin shutdown
	case runErr = <-errc: // a server stopped on its own, which is a failure
		received++
	}
	n.log.Info("node stopping")
	if err := n.mem.Leave(shutdownTimeout); err != nil {
		n.log.Warn("leave gossip", "err", err)
	}
	n.grpcSrv.GracefulStop() // stops accepting and waits for in-flight RPCs
	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	_ = n.metricsSrv.Shutdown(shutCtx)
	for ; received < 2; received++ {
		if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) && runErr == nil {
			runErr = err
		}
	}
	n.log.Info("node stopped")
	return runErr
}
