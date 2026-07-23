// Package metrics exposes a node's consensus and service state as
// Prometheus metrics. One collector per node; gauges read raft.Stats and
// counters read the KV server's op counters at scrape time, so nothing on
// the hot path touches Prometheus.
package metrics

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Hellblazer704/raftkv/kv"
)

type collector struct {
	s    *kv.Server
	node string

	term, isLeader, commitIndex, lastApplied, firstIndex, lastIndex, logBytes *prometheus.Desc
	ops                                                                      *prometheus.Desc
}

// NewCollector builds a Prometheus collector for one server.
func NewCollector(s *kv.Server, nodeID int) prometheus.Collector {
	node := strconv.Itoa(nodeID)
	labels := prometheus.Labels{"node": node}
	gauge := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("raftkv_"+name, help, nil, labels)
	}
	return &collector{
		s:           s,
		node:        node,
		term:        gauge("raft_term", "Current Raft term."),
		isLeader:    gauge("raft_is_leader", "1 if this node believes it is leader."),
		commitIndex: gauge("raft_commit_index", "Highest committed log index."),
		lastApplied: gauge("raft_last_applied", "Highest applied log index."),
		firstIndex:  gauge("raft_first_index", "Snapshot boundary (compacted through this index)."),
		lastIndex:   gauge("raft_last_index", "Highest log index."),
		logBytes:    gauge("raft_log_bytes", "Approximate size of the un-compacted log."),
		ops: prometheus.NewDesc("raftkv_ops_total",
			"Client operations observed, by op and disposition.",
			[]string{"op"}, labels),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.term
	ch <- c.isLeader
	ch <- c.commitIndex
	ch <- c.lastApplied
	ch <- c.firstIndex
	ch <- c.lastIndex
	ch <- c.logBytes
	ch <- c.ops
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	st := c.s.Raft().Stats()
	g := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
	}
	g(c.term, float64(st.Term))
	leader := 0.0
	if st.State == "leader" {
		leader = 1
	}
	g(c.isLeader, leader)
	g(c.commitIndex, float64(st.CommitIndex))
	g(c.lastApplied, float64(st.LastApplied))
	g(c.firstIndex, float64(st.FirstIndex))
	g(c.lastIndex, float64(st.LastIndex))
	g(c.logBytes, float64(st.LogBytes))

	ops := c.s.Counters()
	cnt := func(op string, v int64) {
		ch <- prometheus.MustNewConstMetric(c.ops, prometheus.CounterValue, float64(v), op)
	}
	cnt("get", ops.Gets)
	cnt("get_lease", ops.LeaseReads)
	cnt("put", ops.Puts)
	cnt("append", ops.Appends)
	cnt("cas", ops.Cas)
	cnt("wrong_leader", ops.WrongLeader)
	cnt("timeout", ops.Timeouts)
}

// Handler returns an http.Handler serving /metrics for one node.
func Handler(s *kv.Server, nodeID int) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(s, nodeID))
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
