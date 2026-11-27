package coordinator

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/abhi-8849/dynakv/internal/storage"
	"github.com/abhi-8849/dynakv/internal/vclock"
)

// reconcile drops versions that another version supersedes and collapses duplicates; what remains are the siblings, oldest first so
// the order does not depend on which replica answered first.
func reconcile(vs []storage.Versioned) []storage.Versioned {
	var out []storage.Versioned
next:
	for _, v := range vs {
		for _, o := range vs {
			if v.Clock.Compare(o.Clock) == vclock.Before {
				continue next
			}
		}
		for _, e := range out {
			if v.Clock.Compare(e.Clock) == vclock.Equal {
				continue next
			}
		}
		out = append(out, v)
	}
	slices.SortFunc(out, func(a, b storage.Versioned) int {
		return cmp.Or(a.TS.Compare(b.TS), bytes.Compare(a.Value, b.Value))
	})
	return out
}

func mergeAll(vs []storage.Versioned) vclock.Clock {
	cs := make([]vclock.Clock, len(vs))
	for i, v := range vs {
		cs[i] = v.Clock
	}
	return vclock.Merge(cs...)
}
