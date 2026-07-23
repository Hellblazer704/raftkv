package rpcnet_test

import (
	"net"
	"testing"
	"time"

	"github.com/Hellblazer704/raftkv/kv"
	"github.com/Hellblazer704/raftkv/raft"
	"github.com/Hellblazer704/raftkv/rpcnet"
)

// TestRealTCPCluster boots three replicas on real localhost TCP sockets and
// exercises the full production path: clerk ops, CAS, status.
func TestRealTCPCluster(t *testing.T) {
	const n = 3
	var (
		listeners []net.Listener
		addrs     []string
	)
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, lis)
		addrs = append(addrs, lis.Addr().String())
	}

	servers := make([]*kv.Server, n)
	for i := 0; i < n; i++ {
		cfg := raft.Config{
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  50 * time.Millisecond,
			Seed:               int64(i + 1),
		}
		servers[i] = kv.NewServer(i, n, rpcnet.NewRaftTransport(addrs, time.Second),
			raft.NewMemoryStorage(), 0, cfg)
		go rpcnet.Serve(listeners[i], servers[i])
	}
	defer func() {
		for i := 0; i < n; i++ {
			listeners[i].Close()
			servers[i].Kill()
		}
	}()

	clerk := kv.NewClerk(rpcnet.NewKVTransport(addrs, time.Second), n)
	clerk.Put("k", "v1")
	if v := clerk.Get("k"); v != "v1" {
		t.Fatalf("get = %q", v)
	}
	clerk.Append("k", "+v2")
	if v := clerk.Get("k"); v != "v1+v2" {
		t.Fatalf("get = %q", v)
	}
	if ok, old := clerk.Cas("k", "v1+v2", "v3"); !ok || old != "v1+v2" {
		t.Fatalf("cas = %v %q", ok, old)
	}

	// Status must be answerable on every node.
	tr := rpcnet.NewKVTransport(addrs, time.Second)
	leaders := 0
	for i := 0; i < n; i++ {
		reply := &rpcnet.StatusReply{}
		if !tr.Call(i, "KV.Status", &rpcnet.StatusArgs{}, reply) {
			t.Fatalf("node %d status unreachable", i)
		}
		if reply.Raft.State == "leader" {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders", leaders)
	}
}
