package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/storage"
)

// Replica implements the node-to-node Replica gRPC service over the node's local store.
type Replica struct {
	kvpb.UnimplementedReplicaServer
	store storage.Engine
}

// NewReplica returns a Replica service backed by store.
func NewReplica(store storage.Engine) *Replica { return &Replica{store: store} }

// ReplicaPut implements kvpb.ReplicaServer.
func (s *Replica) ReplicaPut(ctx context.Context, req *kvpb.ReplicaPutRequest) (*kvpb.Ack, error) {
	if err := validate(req.GetKey(), nil, 0); err != nil {
		return nil, err
	}
	if req.GetVersion() == nil {
		return nil, status.Error(codes.InvalidArgument, "version is required")
	}
	if err := s.store.Put(ctx, req.GetKey(), storage.FromProto(req.GetVersion())); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &kvpb.Ack{}, nil
}

// ReplicaGet implements kvpb.ReplicaServer.
func (s *Replica) ReplicaGet(ctx context.Context, req *kvpb.ReplicaGetRequest) (*kvpb.ReplicaGetResponse, error) {
	if err := validate(req.GetKey(), nil, 0); err != nil {
		return nil, err
	}
	vs, err := s.store.Get(ctx, req.GetKey())
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := make([]*kvpb.Version, len(vs))
	for i, v := range vs {
		out[i] = storage.ToProto(v)
	}
	return &kvpb.ReplicaGetResponse{Versions: out}, nil
}
