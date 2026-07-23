// raftkvd is one raftkv replica: Raft + KV service over TCP (net/rpc), WAL
// persistence on disk, Prometheus metrics, structured logs on stdout.
//
//	raftkvd -id 0 -peers node0:7000,node1:7000,node2:7000 \
//	        -listen :7000 -data /var/lib/raftkv -metrics :9100
//
// The -peers list is every node's address in id order (including this
// node's own entry); one TCP port serves both consensus and client RPCs.
package main

import (
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Hellblazer704/raftkv/kv"
	"github.com/Hellblazer704/raftkv/metrics"
	"github.com/Hellblazer704/raftkv/raft"
	"github.com/Hellblazer704/raftkv/raft/wal"
	"github.com/Hellblazer704/raftkv/rpcnet"
)

func main() {
	var (
		id          = flag.Int("id", 0, "this node's index into -peers")
		peers       = flag.String("peers", "", "comma-separated node addresses, id order")
		listen      = flag.String("listen", ":7000", "address to serve raft+client RPCs")
		dataDir     = flag.String("data", "data", "WAL/snapshot directory")
		metricsAddr = flag.String("metrics", ":9100", "Prometheus /metrics address (empty disables)")
		snapAt      = flag.Int("snapshot-threshold", 8<<20, "un-compacted log bytes that trigger a snapshot")
		debug       = flag.Bool("debug", false, "verbose consensus logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	peerList := strings.Split(*peers, ",")
	if len(peerList) < 1 || *id < 0 || *id >= len(peerList) {
		logger.Error("bad -id/-peers", "id", *id, "peers", peerList)
		os.Exit(2)
	}

	store, err := wal.Open(*dataDir)
	if err != nil {
		logger.Error("cannot open WAL", "dir", *dataDir, "err", err)
		os.Exit(1)
	}

	transport := rpcnet.NewRaftTransport(peerList, 2*time.Second)
	cfg := raft.Config{
		ElectionTimeoutMin: 300 * time.Millisecond,
		ElectionTimeoutMax: 600 * time.Millisecond,
		HeartbeatInterval:  100 * time.Millisecond,
		Logger:             logger,
	}
	server := kv.NewServer(*id, len(peerList), transport, store, *snapAt, cfg)

	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler(server, *id))
		go func() {
			if err := http.ListenAndServe(*metricsAddr, mux); err != nil {
				logger.Error("metrics server failed", "err", err)
			}
		}()
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("cannot listen", "addr", *listen, "err", err)
		os.Exit(1)
	}
	go rpcnet.Serve(lis, server)
	logger.Info("raftkvd up", "id", *id, "listen", *listen, "peers", peerList, "data", *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	logger.Info("shutting down")
	lis.Close()
	server.Kill()
}
