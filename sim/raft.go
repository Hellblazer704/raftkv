package sim

import "github.com/Hellblazer704/raftkv/raft"

// RaftTransport adapts the simulated network to raft.Transport for one node.
type RaftTransport struct {
	Net  *Network
	From int
}

func (t *RaftTransport) RequestVote(peer int, args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) bool {
	return t.Net.Call(t.From, peer, "Raft.RequestVote", args, reply, 32)
}

func (t *RaftTransport) AppendEntries(peer int, args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) bool {
	size := 48
	for _, e := range args.Entries {
		size += len(e.Command) + 16
	}
	return t.Net.Call(t.From, peer, "Raft.AppendEntries", args, reply, size)
}

func (t *RaftTransport) InstallSnapshot(peer int, args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) bool {
	return t.Net.Call(t.From, peer, "Raft.InstallSnapshot", args, reply, 32+len(args.Data))
}

// RaftService exposes a Raft node's RPC handlers to the network. Services
// layered on Raft (e.g. the KV server) embed this so one endpoint serves both
// its own methods and Raft's.
type RaftService struct {
	RF *raft.Raft
}

func (s *RaftService) Dispatch(method string, args, reply any) {
	switch method {
	case "Raft.RequestVote":
		s.RF.HandleRequestVote(args.(*raft.RequestVoteArgs), reply.(*raft.RequestVoteReply))
	case "Raft.AppendEntries":
		s.RF.HandleAppendEntries(args.(*raft.AppendEntriesArgs), reply.(*raft.AppendEntriesReply))
	case "Raft.InstallSnapshot":
		s.RF.HandleInstallSnapshot(args.(*raft.InstallSnapshotArgs), reply.(*raft.InstallSnapshotReply))
	default:
		panic("sim: unknown raft method " + method)
	}
}
