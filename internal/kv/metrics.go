package kv

import (
	"strconv"

	"github.com/salesstair-01/001/internal/metrics"
)

var (
	commitSeconds = metrics.Default.NewHistogram("s2z_kv_commit_seconds",
		"Commit latency at the owner, from the commit request to its durable WAL PUT.", metrics.LatencyBuckets)
	putsTotal = metrics.Default.NewCounter("s2z_kv_puts_total",
		"Object-store PUTs by partition (WAL segments, SSTs, manifests).", "partition")
	takeoverSeconds = metrics.Default.NewHistogram("s2z_kv_takeover_seconds",
		"Partition takeover time; cache is warm when every checkpoint file came from the local cache.",
		metrics.LatencyBuckets, "cache")
)

// RegisterMetrics adds the node's gauges to r.
func (n *Node) RegisterMetrics(r *metrics.Registry) {
	r.NewGaugeFunc("s2z_kv_partitions", "Partitions this node serves.", func() []metrics.Sample {
		return []metrics.Sample{{Value: float64(len(n.Partitions()))}}
	})
}

func pidLabel(pid uint64) string { return strconv.FormatUint(pid, 10) }
