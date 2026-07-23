// raftkv-cli is the operator/client tool.
//
//	raftkv-cli -servers host0:7000,host1:7000,host2:7000 <command>
//
//	get <key>
//	put <key> <value>
//	append <key> <value>
//	cas <key> <expect> <new>       atomic compare-and-swap
//	txn incr <key> [delta]         atomic increment built on a CAS retry loop
//	watch <key> [-interval 500ms]  poll the key, print each change
//	status                         per-node consensus state and op counters
//
// txn note: raftkv's transactional primitive is single-key CAS; `txn incr`
// is an atomic read-modify-write built on it. There are no multi-key
// transactions — that would need either a lock service or deterministic
// multi-shard commit on top of the log.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Hellblazer704/raftkv/kv"
	"github.com/Hellblazer704/raftkv/rpcnet"
)

func main() {
	servers := flag.String("servers", "localhost:7001,localhost:7002,localhost:7003,localhost:7004,localhost:7005",
		"comma-separated KV server addresses")
	interval := flag.Duration("interval", 500*time.Millisecond, "watch poll interval")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}

	addrs := strings.Split(*servers, ",")
	transport := rpcnet.NewKVTransport(addrs, 2*time.Second)
	clerk := kv.NewClerk(transport, len(addrs))

	switch args[0] {
	case "get":
		need(args, 2)
		fmt.Println(clerk.Get(args[1]))
	case "put":
		need(args, 3)
		clerk.Put(args[1], args[2])
		fmt.Println("OK")
	case "append":
		need(args, 3)
		clerk.Append(args[1], args[2])
		fmt.Println("OK")
	case "cas":
		need(args, 4)
		ok, old := clerk.Cas(args[1], args[2], args[3])
		if ok {
			fmt.Println("SWAPPED")
		} else {
			fmt.Printf("FAILED old=%q\n", old)
		}
	case "txn":
		need(args, 3)
		if args[1] != "incr" {
			usage()
		}
		delta := 1
		if len(args) > 3 {
			d, err := strconv.Atoi(args[3])
			if err != nil {
				usage()
			}
			delta = d
		}
		fmt.Println(incr(clerk, args[2], delta))
	case "watch":
		need(args, 2)
		watch(clerk, args[1], *interval)
	case "status":
		status(transport)
	default:
		usage()
	}
}

// incr atomically adds delta to an integer key via a CAS loop; safe under
// any number of concurrent writers.
func incr(clerk *kv.Clerk, key string, delta int) int {
	for {
		cur := clerk.Get(key)
		n := 0
		if cur != "" {
			v, err := strconv.Atoi(cur)
			if err != nil {
				fmt.Fprintf(os.Stderr, "key %q holds non-integer %q\n", key, cur)
				os.Exit(1)
			}
			n = v
		}
		if ok, _ := clerk.Cas(key, cur, strconv.Itoa(n+delta)); ok {
			return n + delta
		}
	}
}

func watch(clerk *kv.Clerk, key string, interval time.Duration) {
	last := clerk.Get(key)
	fmt.Printf("%s %s = %q\n", time.Now().Format(time.TimeOnly), key, last)
	for {
		time.Sleep(interval)
		cur := clerk.Get(key)
		if cur != last {
			fmt.Printf("%s %s = %q\n", time.Now().Format(time.TimeOnly), key, cur)
			last = cur
		}
	}
}

func status(transport *rpcnet.KVTransport) {
	fmt.Printf("%-22s %-10s %5s %8s %8s %9s %10s %8s\n",
		"server", "state", "term", "commit", "applied", "logBytes", "leaseReads", "puts")
	for i := 0; i < transport.N(); i++ {
		reply := &rpcnet.StatusReply{}
		if !transport.Call(i, "KV.Status", &rpcnet.StatusArgs{}, reply) {
			fmt.Printf("%-22s unreachable\n", transport.Addr(i))
			continue
		}
		fmt.Printf("%-22s %-10s %5d %8d %8d %9d %10d %8d\n",
			transport.Addr(i), reply.Raft.State, reply.Raft.Term,
			reply.Raft.CommitIndex, reply.Raft.LastApplied, reply.Raft.LogBytes,
			reply.Counters.LeaseReads, reply.Counters.Puts)
	}
}

func need(args []string, n int) {
	if len(args) < n {
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: raftkv-cli [-servers a,b,c] <command>
  get <key>
  put <key> <value>
  append <key> <value>
  cas <key> <expect> <new>
  txn incr <key> [delta]
  watch <key> [-interval 500ms]
  status`)
	os.Exit(2)
}
