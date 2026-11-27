package storage

import (
	"time"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/vclock"
)

// ClockToProto converts a clock to its wire form.
func ClockToProto(c vclock.Clock) map[string]*kvpb.ClockStamp {
	out := make(map[string]*kvpb.ClockStamp, len(c))
	for id, e := range c {
		out[id] = &kvpb.ClockStamp{Counter: e.Counter, Ts: e.TS}
	}
	return out
}

// ClockFromProto converts a wire clock back to a clock.
func ClockFromProto(m map[string]*kvpb.ClockStamp) vclock.Clock {
	out := make(vclock.Clock, len(m))
	for id, s := range m {
		out[id] = vclock.Entry{Counter: s.GetCounter(), TS: s.GetTs()}
	}
	return out
}

// ToProto converts a version to its wire form.
func ToProto(v Versioned) *kvpb.Version {
	return &kvpb.Version{Value: v.Value, Clock: ClockToProto(v.Clock), Tombstone: v.Tombstone, Ts: v.TS.UnixNano()}
}

// FromProto converts a wire version back to a Versioned.
func FromProto(p *kvpb.Version) Versioned {
	return Versioned{Value: p.GetValue(), Clock: ClockFromProto(p.GetClock()), Tombstone: p.GetTombstone(), TS: time.Unix(0, p.GetTs())}
}
