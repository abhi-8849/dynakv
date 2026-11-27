package server

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/abhi-8849/dynakv/internal/metrics"
)

// Instrument returns a unary interceptor that records every request in m by operation and gRPC code.
func Instrument(m *metrics.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := next(ctx, req)
		op := strings.ToLower(info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:])
		m.ObserveRequest(op, status.Code(err).String(), time.Since(start))
		return resp, err
	}
}
