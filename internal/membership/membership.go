package membership

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
	"google.golang.org/protobuf/proto"

	"github.com/abhi-8849/dynakv/api/kvpb"
	"github.com/abhi-8849/dynakv/internal/ring"
)

// ErrJoin means none of the seed nodes could be reached.
var ErrJoin = errors.New("membership: join failed")

// Config describes this node and how it finds the cluster.
type Config struct {
	// Self is this node; Addr is the gRPC address peers dial and Zone its failure domain.
	Self ring.Node
	// GossipAddr is the host:port memberlist binds; an empty host binds all interfaces.
	GossipAddr string
	// Seeds are gossip addresses of existing members; empty for the first node.
	Seeds []string
	// Vnodes is the number of ring positions per node.
	Vnodes int
	Log    *slog.Logger
}

// Member is a node known through gossip.
type Member struct {
	ring.Node
	// Alive is false once gossip reports the node dead or departed.
	Alive bool
}

// Membership tracks the cluster through memberlist gossip and keeps the ring in step with it.
type Membership struct {
	log    *slog.Logger
	vnodes int
	list   *memberlist.Memberlist
	// current is replaced, never modified, on every membership change, so readers need no lock.
	current atomic.Pointer[ring.Ring]

	mu      sync.Mutex
	members map[string]Member
}

// Start binds the gossip port and joins the seeds. Dead members stay in the ring: their keys keep their owners, and hinted handoff
// covers them while they are down.
func Start(cfg Config) (*Membership, error) {
	meta, err := proto.Marshal(&kvpb.Member{Id: cfg.Self.ID, Addr: cfg.Self.Addr, Zone: cfg.Self.Zone})
	if err != nil {
		return nil, fmt.Errorf("membership: encode metadata: %w", err)
	}
	m := &Membership{log: cfg.Log, vnodes: cfg.Vnodes, members: map[string]Member{}}
	mc, err := memberlistConfig(cfg, m, meta)
	if err != nil {
		return nil, err
	}
	if m.list, err = memberlist.Create(mc); err != nil {
		return nil, fmt.Errorf("membership: create: %w", err)
	}
	if len(cfg.Seeds) > 0 {
		if _, err := m.list.Join(cfg.Seeds); err != nil {
			_ = m.list.Shutdown()
			return nil, fmt.Errorf("%w: %v: %w", ErrJoin, cfg.Seeds, err)
		}
	}
	return m, nil
}

func memberlistConfig(cfg Config, m *Membership, meta []byte) (*memberlist.Config, error) {
	host, portStr, err := net.SplitHostPort(cfg.GossipAddr)
	if err != nil {
		return nil, fmt.Errorf("membership: gossip address: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("membership: gossip port: %w", err)
	}
	mc := memberlist.DefaultLANConfig()
	mc.Name = cfg.Self.ID
	if host != "" {
		mc.BindAddr = host
	}
	mc.BindPort = port
	mc.Delegate = delegate{meta: meta}
	mc.Events = events{m: m}
	mc.LogOutput = io.Discard // membership changes are logged by events
	return mc, nil
}

// Members returns every known node ordered by id, including dead ones.
func (m *Membership) Members() []Member {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Member, 0, len(m.members))
	for _, mem := range m.members {
		out = append(out, mem)
	}
	slices.SortFunc(out, func(a, b Member) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// Alive reports whether gossip currently considers id alive.
func (m *Membership) Alive(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.members[id].Alive
}

// Ring returns the current ring. It is immutable and safe to use after later membership changes.
func (m *Membership) Ring() *ring.Ring { return m.current.Load() }

// Leave tells the cluster this node is going away, waiting at most timeout, then stops gossiping.
func (m *Membership) Leave(timeout time.Duration) error {
	err := m.list.Leave(timeout)
	if serr := m.list.Shutdown(); err == nil {
		err = serr
	}
	return err
}
