package coordinator

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/storage"
	"github.com/abhi-8849/dynakv/internal/vclock"
)

// encodeContext turns a merged clock into the opaque token handed to clients. It reuses the Version message with only the clock set,
// so no separate format exists.
func encodeContext(c vclock.Clock) []byte {
	b, err := proto.Marshal(&kvpb.Version{Clock: storage.ClockToProto(c)})
	if err != nil {
		return nil // marshaling a map of plain integers cannot fail
	}
	return b
}

// decodeContext parses a token from encodeContext. An empty token is an empty clock, which makes the write a blind write.
func decodeContext(b []byte) (vclock.Clock, error) {
	if len(b) == 0 {
		return vclock.Clock{}, nil
	}
	var v kvpb.Version
	if err := proto.Unmarshal(b, &v); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidContext, err)
	}
	return storage.ClockFromProto(v.GetClock()), nil
}
