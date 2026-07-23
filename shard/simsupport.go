package shard

import (
	"strconv"

	"github.com/Hellblazer704/raftkv/sim"
)

// Server names on the simulated network are endpoint ids in decimal, so no
// separate name registry is needed.

// SimNameTransport implements Transport over the simulated network.
type SimNameTransport struct {
	Net  *sim.Network
	From int
}

func (t SimNameTransport) Call(server string, method string, args, reply any) bool {
	id, err := strconv.Atoi(server)
	if err != nil {
		return false
	}
	return t.Net.Call(t.From, id, method, args, reply, 128)
}

// CtrlerService exposes a controller replica as one sim endpoint.
type CtrlerService struct {
	C *Ctrler
}

func (cs CtrlerService) Dispatch(method string, args, reply any) {
	switch method {
	case "Ctrler.Join":
		_ = cs.C.Join(args.(*JoinArgs), reply.(*CtrlerReply))
	case "Ctrler.Leave":
		_ = cs.C.Leave(args.(*LeaveArgs), reply.(*CtrlerReply))
	case "Ctrler.Move":
		_ = cs.C.Move(args.(*MoveArgs), reply.(*CtrlerReply))
	case "Ctrler.Query":
		_ = cs.C.Query(args.(*QueryArgs), reply.(*CtrlerReply))
	default:
		(&sim.RaftService{RF: cs.C.Raft()}).Dispatch(method, args, reply)
	}
}

// GroupService exposes a shard-group replica as one sim endpoint.
type GroupService struct {
	G *Group
}

func (gs GroupService) Dispatch(method string, args, reply any) {
	switch method {
	case "KV.Get":
		_ = gs.G.Get(args.(*GetArgs), reply.(*GetReply))
	case "KV.PutAppend":
		_ = gs.G.PutAppend(args.(*PutAppendArgs), reply.(*PutAppendReply))
	case "Shard.Pull":
		_ = gs.G.Pull(args.(*PullArgs), reply.(*PullReply))
	case "Shard.Installed":
		_ = gs.G.Installed(args.(*InstalledArgs), reply.(*InstalledReply))
	default:
		(&sim.RaftService{RF: gs.G.Raft()}).Dispatch(method, args, reply)
	}
}
