package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/cespare/xxhash/v2"

	"github.com/abhi-8849/dynakv/internal/storage"
)

// SSTable file layout: sorted data blocks, a Bloom filter, a block index and a fixed footer.
//
//	block   = entries... + CRC-32 of the entries; entry = uvarint length + record (see encodeRecord)
//	index   = per block: uvarint first-key length, first key, uvarint offset, uvarint length (the CRC included)
//	footer  = bloom offset, bloom length, index offset, index length, entry count, magic; each a little-endian uint64
const (
	blockTarget = 4096
	footerSize  = 48
	tableMagic  = 0x64796e616b765354
)

type indexEntry struct {
	first  []byte
	off, n int64
}

// tableWriter builds an SSTable from entries added in key order.
type tableWriter struct {
	path   string
	f      *os.File
	w      *bufio.Writer
	off    int64
	block  []byte
	first  []byte
	index  []indexEntry
	hashes []uint64
}

func newTableWriter(path string) (*tableWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("lsm: create table: %w", err)
	}
	return &tableWriter{path: path, f: f, w: bufio.NewWriter(f)}, nil
}

func (tw *tableWriter) add(key []byte, vs []storage.Versioned) error {
	rec := encodeRecord(key, vs)
	if tw.first == nil {
		tw.first = slices.Clone(key)
	}
	tw.block = binary.AppendUvarint(tw.block, uint64(len(rec)))
	tw.block = append(tw.block, rec...)
	tw.hashes = append(tw.hashes, xxhash.Sum64(key))
	if len(tw.block) >= blockTarget {
		return tw.flushBlock()
	}
	return nil
}

func (tw *tableWriter) flushBlock() error {
	if len(tw.block) == 0 {
		return nil
	}
	b := binary.LittleEndian.AppendUint32(tw.block, crc32.ChecksumIEEE(tw.block))
	if _, err := tw.w.Write(b); err != nil {
		return err
	}
	tw.index = append(tw.index, indexEntry{first: tw.first, off: tw.off, n: int64(len(b))})
	tw.off += int64(len(b))
	tw.block, tw.first = tw.block[:0], nil
	return nil
}

// finish writes the trailer, syncs and closes the file, and returns its size. A writer with no entries is the caller's to abort.
func (tw *tableWriter) finish() (int64, error) {
	if err := tw.flushBlock(); err != nil {
		return 0, err
	}
	bloomOff := tw.off
	bl := buildBloom(tw.hashes)
	var idx []byte
	for _, e := range tw.index {
		idx = binary.AppendUvarint(idx, uint64(len(e.first)))
		idx = append(idx, e.first...)
		idx = binary.AppendUvarint(idx, uint64(e.off))
		idx = binary.AppendUvarint(idx, uint64(e.n))
	}
	footer := make([]byte, 0, footerSize)
	for _, v := range []uint64{uint64(bloomOff), uint64(len(bl)), uint64(bloomOff) + uint64(len(bl)), uint64(len(idx)), uint64(len(tw.hashes)), tableMagic} {
		footer = binary.LittleEndian.AppendUint64(footer, v)
	}
	for _, part := range [][]byte{bl, idx, footer} {
		if _, err := tw.w.Write(part); err != nil {
			return 0, err
		}
	}
	if err := tw.w.Flush(); err != nil {
		return 0, err
	}
	if err := tw.f.Sync(); err != nil {
		return 0, err
	}
	size := bloomOff + int64(len(bl)+len(idx)+len(footer))
	return size, tw.f.Close()
}

// abort discards a half-written table.
func (tw *tableWriter) abort() {
	tw.f.Close()
	os.Remove(tw.path)
}

// table is an open SSTable. Its index and Bloom filter stay in memory; data blocks are read on demand.
type table struct {
	name  string
	path  string
	f     *os.File
	size  int64
	count int
	index []indexEntry
	bloom bloom
}

func openTable(path string) (*table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("lsm: open table: %w", err)
	}
	t := &table{name: filepath.Base(path), path: path, f: f}
	if err := t.load(); err != nil {
		f.Close()
		return nil, fmt.Errorf("lsm: load %s: %w", path, err)
	}
	return t, nil
}

func (t *table) load() error {
	st, err := t.f.Stat()
	if err != nil {
		return err
	}
	t.size = st.Size()
	if t.size < footerSize {
		return ErrCorrupt
	}
	footer := make([]byte, footerSize)
	if _, err := t.f.ReadAt(footer, t.size-footerSize); err != nil {
		return err
	}
	var f [6]uint64
	for i := range f {
		f[i] = binary.LittleEndian.Uint64(footer[i*8:])
	}
	if f[5] != tableMagic {
		return ErrCorrupt
	}
	t.count = int(f[4])
	t.bloom = make(bloom, f[1])
	if _, err := t.f.ReadAt(t.bloom, int64(f[0])); err != nil {
		return err
	}
	idx := make([]byte, f[3])
	if _, err := t.f.ReadAt(idx, int64(f[2])); err != nil {
		return err
	}
	return t.parseIndex(idx)
}

func (t *table) parseIndex(b []byte) error {
	for len(b) > 0 {
		klen, n := binary.Uvarint(b)
		if n <= 0 || uint64(len(b)-n) < klen {
			return ErrCorrupt
		}
		first := slices.Clone(b[n : n+int(klen)])
		b = b[n+int(klen):]
		off, n1 := binary.Uvarint(b)
		if n1 <= 0 {
			return ErrCorrupt
		}
		ln, n2 := binary.Uvarint(b[n1:])
		if n2 <= 0 {
			return ErrCorrupt
		}
		b = b[n1+n2:]
		t.index = append(t.index, indexEntry{first: first, off: int64(off), n: int64(ln)})
	}
	return nil
}

// readBlock returns the entries of block i after verifying its checksum.
func (t *table) readBlock(i int) ([]byte, error) {
	e := t.index[i]
	b := make([]byte, e.n)
	if _, err := t.f.ReadAt(b, e.off); err != nil {
		return nil, err
	}
	body, sum := b[:len(b)-4], binary.LittleEndian.Uint32(b[len(b)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return nil, fmt.Errorf("%w: block checksum in %s", ErrCorrupt, t.name)
	}
	return body, nil
}

// blockFor returns the index of the block that may hold key, or -1 when key sorts before the first block.
func (t *table) blockFor(key []byte) int {
	return sort.Search(len(t.index), func(i int) bool { return bytes.Compare(t.index[i].first, key) > 0 }) - 1
}

// get returns the record stored for key. found is true even for an empty record, which marks a removed key.
func (t *table) get(key []byte) (vs []storage.Versioned, found bool, err error) {
	if !t.bloom.mayContain(key) {
		return nil, false, nil
	}
	bi := t.blockFor(key)
	if bi < 0 {
		return nil, false, nil
	}
	body, err := t.readBlock(bi)
	if err != nil {
		return nil, false, err
	}
	for len(body) > 0 {
		l, n := binary.Uvarint(body)
		if n <= 0 || uint64(len(body)-n) < l {
			return nil, false, ErrCorrupt
		}
		rec := body[n : n+int(l)]
		body = body[n+int(l):]
		k, err := recordKey(rec)
		if err != nil {
			return nil, false, err
		}
		switch bytes.Compare(k, key) {
		case 0:
			_, vs, err := decodeRecord(rec)
			return vs, err == nil, err
		case 1:
			return nil, false, nil
		}
	}
	return nil, false, nil
}

func (t *table) close() error { return t.f.Close() }

// iterator walks a table's entries in key order, starting at the first key not below prefix.
type iterator struct {
	t      *table
	prefix []byte
	bi     int
	body   []byte
	key    []byte
	vs     []storage.Versioned
	ok     bool
	err    error
}

// iter returns an iterator already positioned on its first entry; check ok before reading key and vs.
func (t *table) iter(prefix []byte) *iterator {
	it := &iterator{t: t, prefix: prefix, bi: max(t.blockFor(prefix), 0)}
	it.next()
	return it
}

// next advances to the following entry and updates ok and err.
func (it *iterator) next() {
	it.ok = false
	for {
		if len(it.body) == 0 {
			if it.bi >= len(it.t.index) {
				return
			}
			body, err := it.t.readBlock(it.bi)
			if err != nil {
				it.err = err
				return
			}
			it.body, it.bi = body, it.bi+1
		}
		l, n := binary.Uvarint(it.body)
		if n <= 0 || uint64(len(it.body)-n) < l {
			it.err = ErrCorrupt
			return
		}
		rec := it.body[n : n+int(l)]
		it.body = it.body[n+int(l):]
		key, vs, err := decodeRecord(rec)
		if err != nil {
			it.err = err
			return
		}
		if bytes.Compare(key, it.prefix) < 0 {
			continue
		}
		it.key, it.vs, it.ok = key, vs, true
		return
	}
}
