package lsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/storage"
)

// ErrCorrupt means a record, block or file failed its length or checksum checks.
var ErrCorrupt = errors.New("lsm: corrupt data")

// encodeRecord serializes a key with its full list of versions. WAL entries and SSTable entries share this layout:
// uvarint key length, key, uvarint version count, then per version a uvarint length and a kvpb.Version message.
func encodeRecord(key []byte, vs []storage.Versioned) []byte {
	b := binary.AppendUvarint(nil, uint64(len(key)))
	b = append(b, key...)
	b = binary.AppendUvarint(b, uint64(len(vs)))
	for _, v := range vs {
		pb, err := proto.Marshal(storage.ToProto(v))
		if err != nil {
			continue // marshaling a message of plain fields cannot fail
		}
		b = binary.AppendUvarint(b, uint64(len(pb)))
		b = append(b, pb...)
	}
	return b
}

// recordKey returns the key of an encoded record without decoding its versions. The result aliases b.
func recordKey(b []byte) ([]byte, error) {
	klen, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b)-n) < klen {
		return nil, fmt.Errorf("%w: record key", ErrCorrupt)
	}
	return b[n : n+int(klen)], nil
}

// decodeRecord parses a record produced by encodeRecord.
func decodeRecord(b []byte) ([]byte, []storage.Versioned, error) {
	key, err := recordKey(b)
	if err != nil {
		return nil, nil, err
	}
	klen, n := binary.Uvarint(b)
	b = b[n+int(klen):]
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, nil, fmt.Errorf("%w: version count", ErrCorrupt)
	}
	b = b[n:]
	vs := make([]storage.Versioned, 0, count)
	for range count {
		l, n := binary.Uvarint(b)
		if n <= 0 || uint64(len(b)-n) < l {
			return nil, nil, fmt.Errorf("%w: version", ErrCorrupt)
		}
		var p kvpb.Version
		if err := proto.Unmarshal(b[n:n+int(l)], &p); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		vs = append(vs, storage.FromProto(&p))
		b = b[n+int(l):]
	}
	return slices.Clone(key), vs, nil
}
