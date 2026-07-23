package kv

import "github.com/Hellblazer704/raftkv/sim"

// SimService exposes a Server (KV RPCs + its Raft node) as one endpoint on
// the simulated network.
type SimService struct {
	S *Server
}

func (ss SimService) Dispatch(method string, args, reply any) {
	switch method {
	case "KV.Get":
		_ = ss.S.Get(args.(*GetArgs), reply.(*GetReply))
	case "KV.PutAppend":
		_ = ss.S.PutAppend(args.(*PutAppendArgs), reply.(*PutAppendReply))
	default:
		(&sim.RaftService{RF: ss.S.rf}).Dispatch(method, args, reply)
	}
}

// SimTransport is the clerk-side Transport over the simulated network.
type SimTransport struct {
	Net  *sim.Network
	From int
}

func (t SimTransport) Call(server int, method string, args, reply any) bool {
	return t.Net.Call(t.From, server, method, args, reply, 96)
}
