package server

import (
	"bytes"
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/coordinator"
	"github.com/abhi-8849/dynakv/internal/hints"
	"github.com/abhi-8849/dynakv/internal/storage"
)

// Request size limits from the design; larger requests are rejected before they reach storage.
const (
	MaxKeyBytes   = 256
	MaxValueBytes = 1 << 20
)

// Coordinator is what the KV service needs from the coordinator.
type Coordinator interface {
	Get(ctx context.Context, key []byte, r int) ([]storage.Versioned, []byte, error)
	Put(ctx context.Context, key, value, vctx []byte, w int) error
	Delete(ctx context.Context, key, vctx []byte, w int) error
}

// KV implements the client-facing KV gRPC service.
type KV struct {
	kvpb.UnimplementedKVServer
	coord Coordinator
}

// NewKV returns a KV service backed by coord.
func NewKV(coord Coordinator) *KV { return &KV{coord: coord} }

// Get implements kvpb.KVServer.
func (s *KV) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	if err := validate(req.GetKey(), nil, req.GetR()); err != nil {
		return nil, err
	}
	vs, vctx, err := s.coord.Get(ctx, req.GetKey(), int(req.GetR()))
	if err != nil {
		return nil, toStatus(err)
	}
	out := make([]*kvpb.Version, len(vs))
	for i, v := range vs {
		out[i] = storage.ToProto(v)
	}
	return &kvpb.GetResponse{Versions: out, Context: vctx}, nil
}

// Put implements kvpb.KVServer.
func (s *KV) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	if err := validate(req.GetKey(), req.GetValue(), req.GetW()); err != nil {
		return nil, err
	}
	if err := s.coord.Put(ctx, req.GetKey(), req.GetValue(), req.GetContext(), int(req.GetW())); err != nil {
		return nil, toStatus(err)
	}
	return &kvpb.PutResponse{}, nil
}

// Delete implements kvpb.KVServer.
func (s *KV) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	if err := validate(req.GetKey(), nil, req.GetW()); err != nil {
		return nil, err
	}
	if err := s.coord.Delete(ctx, req.GetKey(), req.GetContext(), int(req.GetW())); err != nil {
		return nil, toStatus(err)
	}
	return &kvpb.DeleteResponse{}, nil
}

// validate enforces the size limits and rejects a negative quorum size.
func validate(key, value []byte, quorum int32) error {
	switch {
	case len(key) == 0 || len(key) > MaxKeyBytes:
		return status.Errorf(codes.InvalidArgument, "key must be 1 to %d bytes", MaxKeyBytes)
	case bytes.HasPrefix(key, []byte(hints.Prefix)):
		return status.Error(codes.InvalidArgument, "key uses a reserved prefix")
	case len(value) > MaxValueBytes:
		return status.Errorf(codes.InvalidArgument, "value must be at most %d bytes", MaxValueBytes)
	case quorum < 0:
		return status.Error(codes.InvalidArgument, "r and w must not be negative")
	}
	return nil
}

// toStatus maps coordinator errors to the gRPC codes of the API.
func toStatus(err error) error {
	if s, ok := status.FromError(err); ok {
		return s.Err() // already a status, for example from a forwarded request
	}
	switch {
	case errors.Is(err, coordinator.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, coordinator.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, coordinator.ErrInvalidContext):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
