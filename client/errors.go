package client

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrNotFound means the key does not exist or was deleted.
	ErrNotFound = errors.New("key not found")
	// ErrUnavailable means too few replicas answered to meet R or W.
	ErrUnavailable = errors.New("not enough replicas")
	// ErrInvalidArgument means the server rejected the request, for example an oversized key or value.
	ErrInvalidArgument = errors.New("invalid argument")
)

// convert maps a gRPC status to the package's sentinel errors and leaves other errors wrapped as they are.
func convert(err error) error {
	if err == nil {
		return nil
	}
	s := status.Convert(err)
	switch s.Code() {
	case codes.NotFound:
		return ErrNotFound
	case codes.Unavailable:
		return fmt.Errorf("%w: %s", ErrUnavailable, s.Message())
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %s", ErrInvalidArgument, s.Message())
	}
	return err
}
