package vclock

import (
	"cmp"
	"slices"
)

// MaxEntries bounds clock size so it cannot grow with every coordinator a key has ever seen.
const MaxEntries = 10

// Order is the causal relation between two clocks.
type Order int

const (
	// Equal means both clocks record the same history.
	Equal Order = iota
	// Before means the receiver is an ancestor of the argument.
	Before
	// After means the receiver descends from the argument.
	After
	// Concurrent means neither clock descends from the other, so both versions must be kept as siblings.
	Concurrent
)

// Entry is one node's slot in a clock.
type Entry struct {
	// Counter is the number of writes the node has coordinated for the key.
	Counter uint64
	// TS is the unix time in nanoseconds of the node's last write; it only decides which entry to drop, never causal order.
	TS int64
}

// Clock maps a node id to its entry. A missing node counts as counter 0.
type Clock map[string]Entry

// Compare reports how c relates to o. Only counters decide the result.
func (c Clock) Compare(o Clock) Order {
	var less, greater bool
	for id, e := range c {
		switch {
		case e.Counter > o[id].Counter:
			greater = true
		case e.Counter < o[id].Counter:
			less = true
		}
	}
	for id, e := range o {
		if _, ok := c[id]; !ok && e.Counter > 0 {
			less = true
		}
	}
	switch {
	case less && greater:
		return Concurrent
	case less:
		return Before
	case greater:
		return After
	default:
		return Equal
	}
}

// Merge returns a new clock holding, per node, the entry with the highest counter. The inputs are not modified.
func Merge(cs ...Clock) Clock {
	m := Clock{}
	for _, c := range cs {
		for id, e := range c {
			if cur, ok := m[id]; !ok || e.Counter > cur.Counter || (e.Counter == cur.Counter && e.TS > cur.TS) {
				m[id] = e
			}
		}
	}
	return m
}

// Increment returns a copy of c with node's counter set to max(c's counter, floor)+1, so a coordinator never reuses a counter
// it already stored even when the client context is stale. When the result exceeds MaxEntries, the entries with the oldest TS are dropped.
func (c Clock) Increment(node string, floor uint64, now int64) Clock {
	n := Merge(c)
	n[node] = Entry{Counter: max(n[node].Counter, floor) + 1, TS: now}
	for len(n) > MaxEntries {
		delete(n, oldest(n))
	}
	return n
}

// oldest returns the id with the smallest TS in n, breaking ties by id so the choice is deterministic.
func oldest(n Clock) string {
	ids := make([]string, 0, len(n))
	for id := range n {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return slices.MinFunc(ids, func(a, b string) int { return cmp.Compare(n[a].TS, n[b].TS) })
}
