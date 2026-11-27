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
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/coordinator"
	"github.com/abhi-8849/dynakv/internal/metrics"
	"github.com/abhi-8849/dynakv/internal/server"
	"github.com/abhi-8849/dynakv/internal/storage"
)

// shutdownTimeout bounds how long the metrics listener may take to close.
const shutdownTimeout = 5 * time.Second

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

type flags struct {
	id          string
	addr        string
	metricsAddr string
	n, r, w     int
}

func parseFlags(args []string) (flags, error) {
	host, _ := os.Hostname()
	var f flags
	fs := flag.NewFlagSet("dynakv", flag.ContinueOnError)
	fs.StringVar(&f.id, "id", host, "node id, unique in the cluster")
	fs.StringVar(&f.addr, "addr", ":7000", "gRPC listen address")
	fs.StringVar(&f.metricsAddr, "metrics-addr", ":9100", "Prometheus /metrics listen address")
	fs.IntVar(&f.n, "n", 3, "replication factor")
	fs.IntVar(&f.r, "r", 2, "default read quorum")
	fs.IntVar(&f.w, "w", 2, "default write quorum")
	return f, fs.Parse(args)
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, log, os.Args[1:]); err != nil {
		log.Error("node failed", "err", err)
		os.Exit(1)
	}
}

// run starts the gRPC and metrics listeners and blocks until ctx is cancelled or one of them fails.
func run(ctx context.Context, log *slog.Logger, args []string) error {
	f, err := parseFlags(args)
	if err != nil {
		return err
	}
	store := storage.NewMemory()
	defer store.Close()
	coord, err := coordinator.New(coordinator.Config{ID: f.id, N: f.n, R: f.r, W: f.w}, store, wallClock{})
	if err != nil {
		return err
	}
	m := metrics.New()
	grpcSrv := grpc.NewServer(grpc.ChainUnaryInterceptor(server.Instrument(m)))
	kvpb.RegisterKVServer(grpcSrv, server.NewKV(coord))
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	metricsSrv := &http.Server{Handler: mux, ReadHeaderTimeout: shutdownTimeout}

	grpcLis, err := net.Listen("tcp", f.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", f.addr, err)
	}
	metricsLis, err := net.Listen("tcp", f.metricsAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", f.metricsAddr, err)
	}
	log.Info("node started", "id", f.id, "addr", f.addr, "metrics_addr", f.metricsAddr)

	// run starts both serve goroutines and receives their results. Each sends exactly once when its server stops: Serve returns
	// after GracefulStop (nil) or on failure, and the HTTP server returns http.ErrServerClosed after Shutdown. errc is buffered for
	// both so a sender never blocks. It is never closed; run reads both results before returning.
	errc := make(chan error, 2)
	go func() { errc <- grpcSrv.Serve(grpcLis) }()
	go func() { errc <- metricsSrv.Serve(metricsLis) }()

	received := 0
	var runErr error
	select {
	case <-ctx.Done(): // SIGINT or SIGTERM: begin shutdown
	case runErr = <-errc: // a server stopped on its own, which is a failure
		received++
	}
	log.Info("node stopping")
	grpcSrv.GracefulStop() // stops accepting and waits for in-flight RPCs
	shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	_ = metricsSrv.Shutdown(shutCtx)
	for ; received < 2; received++ {
		if err := <-errc; err != nil && !errors.Is(err, http.ErrServerClosed) && runErr == nil {
			runErr = err
		}
	}
	log.Info("node stopped")
	return runErr
}
