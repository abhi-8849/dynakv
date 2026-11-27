package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/abhi-8849/dynakv/internal/storage"
)

// ErrClosed is returned for writes after the engine or its log was closed.
var ErrClosed = errors.New("lsm: closed")

// maxWALRecord rejects absurd lengths read from a damaged log instead of allocating them.
const maxWALRecord = 64 << 20

// wal is the write-ahead log. append buffers a record and returns a channel that receives the commit result; commit writes and
// fsyncs everything buffered so far, which lets many writers share one fsync.
type wal struct {
	mu      sync.Mutex
	f       *os.File
	buf     *bufio.Writer
	waiters []chan error // each is buffered with capacity 1 and receives exactly one result from commit
	closed  bool
}

func createWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("lsm: create wal: %w", err)
	}
	return &wal{f: f, buf: bufio.NewWriter(f)}, nil
}

// append frames rec as length, CRC-32 and payload. The caller waits on the returned channel for durability.
func (w *wal) append(rec []byte) (<-chan error, error) {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[:4], uint32(len(rec)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(rec))
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrClosed
	}
	if _, err := w.buf.Write(hdr[:]); err != nil {
		return nil, err
	}
	if _, err := w.buf.Write(rec); err != nil {
		return nil, err
	}
	ch := make(chan error, 1)
	w.waiters = append(w.waiters, ch)
	return ch, nil
}

// commit flushes and fsyncs, then tells every waiting writer the outcome. It does nothing when no writer is waiting.
func (w *wal) commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || len(w.waiters) == 0 {
		return nil
	}
	err := w.buf.Flush()
	if err == nil {
		err = w.f.Sync()
	}
	for _, ch := range w.waiters {
		ch <- err
	}
	w.waiters = nil
	return err
}

// close commits what is buffered and closes the file.
func (w *wal) close() error {
	err := w.commit()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return err
	}
	w.closed = true
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// replayWAL feeds every intact record of the log at path to fn. A torn or corrupt tail, left by a crash mid-write, ends the replay
// without an error.
func replayWAL(path string, fn func(key []byte, vs []storage.Versioned)) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("lsm: open wal: %w", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil
		}
		n := binary.LittleEndian.Uint32(hdr[:4])
		if n > maxWALRecord {
			return nil
		}
		rec := make([]byte, n)
		if _, err := io.ReadFull(r, rec); err != nil || crc32.ChecksumIEEE(rec) != binary.LittleEndian.Uint32(hdr[4:]) {
			return nil
		}
		key, vs, err := decodeRecord(rec)
		if err != nil {
			return nil
		}
		fn(key, vs)
	}
}
