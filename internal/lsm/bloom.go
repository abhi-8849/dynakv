package lsm

import "github.com/cespare/xxhash/v2"

// bloomBitsPerKey gives roughly a 1% false positive rate with bloomHashes probes.
const (
	bloomBitsPerKey = 10
	bloomHashes     = 7
)

// bloom is a Bloom filter over the keys of one SSTable; a negative answer lets a lookup skip the file.
type bloom []byte

// buildBloom sets the bits for each key hash (xxhash of the key).
func buildBloom(hashes []uint64) bloom {
	bits := max(64, len(hashes)*bloomBitsPerKey)
	b := make(bloom, (bits+7)/8)
	for _, h := range hashes {
		b.add(h)
	}
	return b
}

// probes yields the bit positions for a key hash using double hashing.
func (b bloom) probes(h uint64, fn func(pos uint32)) {
	m := uint32(len(b) * 8)
	h1, h2 := uint32(h), uint32(h>>32)|1
	for i := range uint32(bloomHashes) {
		fn((h1 + i*h2) % m)
	}
}

func (b bloom) add(h uint64) {
	b.probes(h, func(pos uint32) { b[pos/8] |= 1 << (pos % 8) })
}

// mayContain reports false only when the key is certainly absent.
func (b bloom) mayContain(key []byte) bool {
	if len(b) == 0 {
		return true
	}
	present := true
	b.probes(xxhash.Sum64(key), func(pos uint32) {
		if b[pos/8]&(1<<(pos%8)) == 0 {
			present = false
		}
	})
	return present
}
