package directory

import "github.com/salesstair-01/001/internal/metrics"

// Wake reasons: first_use (never placed), suspended (released when idle) and
// node_lost (its node stopped answering).
var (
	wakesTotal = metrics.Default.NewCounter("s2z_dir_wakes_total",
		"Partition wakes by reason.", "reason")
	wakeSeconds = metrics.Default.NewHistogram("s2z_dir_wake_seconds",
		"Time to place a partition and have its node take it over, by reason.",
		metrics.LatencyBuckets, "reason")
	spawnsTotal = metrics.Default.NewCounter("s2z_dir_spawns_total",
		"KV processes started to place a partition.")
)
