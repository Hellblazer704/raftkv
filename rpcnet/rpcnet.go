// Package rpcnet is the production transport: Go net/rpc over TCP, with
// lazy dialing, per-call timeouts, and connection teardown on failure. The
// same method names used on the simulated network ("KV.Get", ...) are the
// registered net/rpc service methods, so the clerk and servers are
// transport-agnostic.
package rpcnet

import (
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/Hellblazer704/raftkv/kv"
	"github.com/Hellblazer704/raftkv/raft"
)

// KVService is the net/rpc receiver for client-facing methods.
type KVService struct {
	S *kv.Server
}

func (k *KVService) Get(args *kv.GetArgs, reply *kv.GetReply) error { return k.S.Get(args, reply) }
func (k *KVService) PutAppend(args *kv.PutAppendArgs, reply *kv.PutAppendReply) error {
	return k.S.PutAppend(args, reply)
}
func (k *KVService) Cas(args *kv.CasArgs, reply *kv.CasReply) error { return k.S.Cas(args, reply) }

// StatusArgs/StatusReply expose node health to operator tooling (CLI).
type StatusArgs struct{}

type StatusReply struct {
	Raft     raft.Stats
	Counters kv.Counters
}

func (k *KVService) Status(_ *StatusArgs, reply *StatusReply) error {
	reply.Raft = k.S.Raft().Stats()
	reply.Counters = k.S.Counters()
	return nil
}

// RaftService is the net/rpc receiver for consensus RPCs.
type RaftService struct {
	RF *raft.Raft
}

func (r *RaftService) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	r.RF.HandleRequestVote(args, reply)
	return nil
}

func (r *RaftService) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	r.RF.HandleAppendEntries(args, reply)
	return nil
}

func (r *RaftService) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	r.RF.HandleInstallSnapshot(args, reply)
	return nil
}

// Serve registers both services on a fresh rpc.Server and accepts
// connections until the listener closes.
func Serve(lis net.Listener, s *kv.Server) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("KV", &KVService{S: s}); err != nil {
		panic(err)
	}
	if err := srv.RegisterName("Raft", &RaftService{RF: s.Raft()}); err != nil {
		panic(err)
	}
	for {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		go srv.ServeConn(conn)
	}
}

// pool manages one lazily-dialed rpc.Client per address, dropped on any
// call failure so the next call redials.
type pool struct {
	mu      sync.Mutex
	clients map[string]*rpc.Client
	timeout time.Duration
}

func newPool(timeout time.Duration) *pool {
	return &pool{clients: make(map[string]*rpc.Client), timeout: timeout}
}

func (p *pool) get(addr string) *rpc.Client {
	p.mu.Lock()
	c := p.clients[addr]
	p.mu.Unlock()
	if c != nil {
		return c
	}
	conn, err := net.DialTimeout("tcp", addr, p.timeout)
	if err != nil {
		return nil
	}
	c = rpc.NewClient(conn)
	p.mu.Lock()
	if existing := p.clients[addr]; existing != nil {
		p.mu.Unlock()
		c.Close()
		return existing
	}
	p.clients[addr] = c
	p.mu.Unlock()
	return c
}

func (p *pool) drop(addr string, c *rpc.Client) {
	p.mu.Lock()
	if p.clients[addr] == c {
		delete(p.clients, addr)
	}
	p.mu.Unlock()
	c.Close()
}

// call performs one RPC with a timeout; false means no usable reply (the
// request may still have executed — standard RPC ambiguity).
func (p *pool) call(addr, method string, args, reply any) bool {
	c := p.get(addr)
	if c == nil {
		return false
	}
	done := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case res := <-done.Done:
		if res.Error != nil {
			p.drop(addr, c)
			return false
		}
		return true
	case <-time.After(p.timeout):
		p.drop(addr, c) // orphan the in-flight call; redial next time
		return false
	}
}

// RaftTransport implements raft.Transport over TCP; peers[i] is node i's
// address.
type RaftTransport struct {
	peers []string
	pool  *pool
}

func NewRaftTransport(peers []string, timeout time.Duration) *RaftTransport {
	return &RaftTransport{peers: peers, pool: newPool(timeout)}
}

func (t *RaftTransport) RequestVote(peer int, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) bool {
	return t.pool.call(t.peers[peer], "Raft.RequestVote", args, reply)
}

func (t *RaftTransport) AppendEntries(peer int, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) bool {
	return t.pool.call(t.peers[peer], "Raft.AppendEntries", args, reply)
}

func (t *RaftTransport) InstallSnapshot(peer int, args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) bool {
	return t.pool.call(t.peers[peer], "Raft.InstallSnapshot", args, reply)
}

// KVTransport implements kv.Transport (the clerk side) over TCP.
type KVTransport struct {
	addrs []string
	pool  *pool
}

func NewKVTransport(addrs []string, timeout time.Duration) *KVTransport {
	return &KVTransport{addrs: addrs, pool: newPool(timeout)}
}

func (t *KVTransport) Call(server int, method string, args, reply any) bool {
	return t.pool.call(t.addrs[server], method, args, reply)
}

// Addr returns server i's address (operator tooling).
func (t *KVTransport) Addr(i int) string { return t.addrs[i] }

// N returns the number of servers.
func (t *KVTransport) N() int { return len(t.addrs) }
