// Package sim is a seed-locked in-memory network for torturing Raft: message
// drops, delivery delays, reply reordering, partitions, node crashes, and
// per-node clock skew are all driven by one seeded RNG, so a failing fault
// schedule can be replayed by seed.
//
// Modeling choices (matching how real RPC stacks fail):
//   - A dropped reply still executes the request on the receiver — callers
//     must tolerate "it happened but I never heard back" (the reason Raft
//     RPCs are idempotent).
//   - A false return means only "no reply", never "not executed".
//   - Partitions are checked on both the request and the reply path, so a
//     partition that forms mid-RPC eats the reply.
package sim

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Service is anything that can be attached to the network. Method names are
// namespaced ("Raft.RequestVote", "KV.Get", ...); handlers run synchronously
// in the caller's RPC goroutine.
type Service interface {
	Dispatch(method string, args, reply any)
}

// GroupAny lets an endpoint (typically a client) reach every partition group.
const GroupAny = -1

// Network is the hub all endpoints attach to.
type Network struct {
	mu       sync.Mutex
	rng      *rand.Rand
	handlers map[int]Service
	enabled  map[int]bool
	group    map[int]int

	reliable       bool
	longReordering bool

	rpcCount  atomic.Int64
	byteCount atomic.Int64
}

// NewNetwork creates a reliable network whose randomness is derived from seed.
func NewNetwork(seed int64) *Network {
	return &Network{
		rng:      rand.New(rand.NewSource(seed)),
		handlers: make(map[int]Service),
		enabled:  make(map[int]bool),
		group:    make(map[int]int),
		reliable: true,
	}
}

// Register attaches (or re-attaches, after a crash) a service as endpoint id.
func (n *Network) Register(id int, svc Service) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = svc
	if _, ok := n.enabled[id]; !ok {
		n.enabled[id] = true
	}
	if _, ok := n.group[id]; !ok {
		n.group[id] = 0
	}
}

// Deregister detaches an endpoint, modeling a crashed process: in-flight
// requests to it fail, and its side of any in-flight reply is dropped.
func (n *Network) Deregister(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

// SetEnabled connects or disconnects an endpoint from the network entirely.
func (n *Network) SetEnabled(id int, on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.enabled[id] = on
}

// Enabled reports whether an endpoint is connected.
func (n *Network) Enabled(id int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.enabled[id]
}

// SetGroup places an endpoint in a partition group; endpoints in different
// groups cannot exchange messages (GroupAny reaches all groups).
func (n *Network) SetGroup(id, group int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group[id] = group
}

// HealPartitions puts every endpoint back in one group.
func (n *Network) HealPartitions() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.group {
		n.group[id] = 0
	}
}

// SetReliable toggles message drops and random delivery delays.
func (n *Network) SetReliable(on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = on
}

// SetLongReordering makes some replies arrive much later than others,
// exercising stale-reply handling.
func (n *Network) SetLongReordering(on bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.longReordering = on
}

// RPCCount returns the total number of RPCs attempted.
func (n *Network) RPCCount() int64 { return n.rpcCount.Load() }

// ByteCount returns the approximate payload bytes moved.
func (n *Network) ByteCount() int64 { return n.byteCount.Load() }

func (n *Network) reachable(a, b int) bool {
	if !n.enabled[a] || !n.enabled[b] {
		return false
	}
	ga, gb := n.group[a], n.group[b]
	return ga == gb || ga == GroupAny || gb == GroupAny
}

// randDur returns a duration in [0, max) drawn from the seeded RNG.
func (n *Network) randDur(max time.Duration) time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return time.Duration(n.rng.Int63n(int64(max)))
}

// randFloat draws from the seeded RNG.
func (n *Network) randFloat() float64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rng.Float64()
}

// Call performs an RPC from endpoint `from` to endpoint `to`. It returns
// false on any simulated network failure; the request may or may not have
// executed (exactly like a real timeout).
func (n *Network) Call(from, to int, method string, args, reply any, size int) bool {
	n.rpcCount.Add(1)
	n.byteCount.Add(int64(size))

	n.mu.Lock()
	ok := n.reachable(from, to)
	reliable := n.reliable
	n.mu.Unlock()
	if !ok {
		// Model an RPC timeout against an unreachable host.
		time.Sleep(n.randDur(80 * time.Millisecond))
		return false
	}
	if !reliable {
		time.Sleep(n.randDur(25 * time.Millisecond)) // delivery delay
		if n.randFloat() < 0.10 {
			return false // request lost in flight
		}
	}

	n.mu.Lock()
	h := n.handlers[to]
	ok = n.reachable(from, to)
	n.mu.Unlock()
	if h == nil || !ok {
		return false
	}
	h.Dispatch(method, args, reply)

	// Reply path: the partition/crash state may have changed while the
	// handler ran.
	n.mu.Lock()
	ok = n.reachable(from, to) && n.handlers[to] != nil
	reliable = n.reliable
	longReordering := n.longReordering
	n.mu.Unlock()
	if !ok {
		return false
	}
	if !reliable && n.randFloat() < 0.10 {
		return false // reply lost; request already executed
	}
	if longReordering && n.randFloat() < 0.30 {
		time.Sleep(200*time.Millisecond + n.randDur(1200*time.Millisecond))
	}
	return true
}
