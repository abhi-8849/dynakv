package lsm

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/abhi-8849/dynakv/internal/storage"
)

// minRun is how many similar-sized neighbouring tables trigger a merge.
const minRun = 4

// flush writes the memtable as a new SSTable, records it in the manifest, switches to a fresh WAL and compacts. The caller holds
// e.mu. The manifest is updated before the old WAL is deleted, so a crash in between only replays records that are already stored.
func (e *Engine) flush() error {
	if len(e.mem.m) == 0 {
		return nil
	}
	path := e.allocPath(".sst")
	tw, err := newTableWriter(path)
	if err != nil {
		return err
	}
	for _, k := range e.mem.sortedKeys() {
		if err := tw.add([]byte(k), e.mem.m[k]); err != nil {
			tw.abort()
			return err
		}
	}
	if _, err := tw.finish(); err != nil {
		tw.abort()
		return fmt.Errorf("lsm: write table: %w", err)
	}
	t, err := openTable(path)
	if err != nil {
		return err
	}
	if err := writeManifest(e.opts.Dir, tableNames(append(slices.Clone(e.tables), t))); err != nil {
		t.close()
		return err
	}
	e.tables = append(e.tables, t)
	old, oldPath := e.wal, e.walPath
	if err := e.startWAL(); err != nil {
		return err
	}
	if err := old.close(); err != nil {
		return err
	}
	os.Remove(oldPath)
	e.log.Info("memtable flushed", "table", t.name, "keys", len(e.mem.m), "tables", len(e.tables))
	e.mem = newMemtable()
	return e.compact()
}

func tableNames(ts []*table) []string {
	names := make([]string, len(ts))
	for i, t := range ts {
		names[i] = t.name
	}
	return names
}

// compact merges runs of similar-sized neighbouring tables until none is left.
func (e *Engine) compact() error {
	for {
		sizes := make([]int64, len(e.tables))
		for i, t := range e.tables {
			sizes[i] = t.size
		}
		start, ok := pickRun(sizes)
		if !ok {
			return nil
		}
		if err := e.mergeRun(start, start+minRun); err != nil {
			return err
		}
	}
}

// pickRun finds minRun neighbouring tables whose largest is at most twice the smallest. Only neighbours merge, because the table
// order is the age order that decides which record for a key is newest.
func pickRun(sizes []int64) (int, bool) {
	for i := 0; i+minRun <= len(sizes); i++ {
		w := sizes[i : i+minRun]
		if slices.Max(w) <= 2*slices.Min(w) {
			return i, true
		}
	}
	return 0, false
}

// mergeRun replaces tables[start:end] with one table holding the newest record of each key. Expired tombstone versions are dropped;
// a record left empty is dropped only when the run includes the oldest table, since otherwise it must keep hiding older records.
func (e *Engine) mergeRun(start, end int) error {
	run := e.tables[start:end]
	its := make([]*iterator, len(run))
	for i, t := range run {
		its[i] = t.iter(nil)
	}
	path := e.allocPath(".sst")
	tw, err := newTableWriter(path)
	if err != nil {
		return err
	}
	cutoff := e.opts.Clock.Now().Add(-e.opts.TombstoneGrace)
	written := 0
	for {
		key, vs, ok := popNewest(its)
		if !ok {
			break
		}
		if vs = dropExpired(vs, cutoff); len(vs) == 0 && start == 0 {
			continue
		}
		if err := tw.add(key, vs); err != nil {
			tw.abort()
			return err
		}
		written++
	}
	for _, it := range its {
		if it.err != nil {
			tw.abort()
			return it.err
		}
	}
	return e.installMerged(tw, path, written, start, end)
}

// installMerged swaps the merged table in, updates the manifest, then deletes the inputs.
func (e *Engine) installMerged(tw *tableWriter, path string, written, start, end int) error {
	next := slices.Clone(e.tables[:start])
	var merged *table
	if written > 0 {
		if _, err := tw.finish(); err != nil {
			tw.abort()
			return fmt.Errorf("lsm: write merged table: %w", err)
		}
		var err error
		if merged, err = openTable(path); err != nil {
			return err
		}
		next = append(next, merged)
	} else {
		tw.abort()
	}
	next = append(next, e.tables[end:]...)
	if err := writeManifest(e.opts.Dir, tableNames(next)); err != nil {
		return err
	}
	old := e.tables[start:end]
	e.tables = next
	for _, t := range old {
		t.close()
		os.Remove(t.path)
	}
	e.log.Info("compaction finished", "inputs", len(old), "kept_keys", written, "tables", len(e.tables))
	return nil
}

// popNewest returns the smallest key among the iterators with the record from the newest table (the highest index), and advances
// every iterator that was on that key.
func popNewest(its []*iterator) (key []byte, vs []storage.Versioned, ok bool) {
	best := -1
	for i, it := range its {
		if it.ok && (best < 0 || bytes.Compare(it.key, its[best].key) <= 0) {
			best = i
		}
	}
	if best < 0 {
		return nil, nil, false
	}
	key, vs = its[best].key, its[best].vs
	for _, it := range its {
		if it.ok && bytes.Equal(it.key, key) {
			it.next()
		}
	}
	return key, vs, true
}

// dropExpired removes tombstone versions written before cutoff.
func dropExpired(vs []storage.Versioned, cutoff time.Time) []storage.Versioned {
	kept := vs[:0:0]
	for _, v := range vs {
		if !(v.Tombstone && v.TS.Before(cutoff)) {
			kept = append(kept, v)
		}
	}
	return kept
}
