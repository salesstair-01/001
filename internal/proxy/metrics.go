package proxy

import "github.com/salesstair-01/001/internal/metrics"

var (
	coldStartSeconds = metrics.Default.NewHistogram("s2z_proxy_cold_start_seconds",
		"Latency of statements that had to open a query-engine connection, from the statement to its "+
			"result: query-engine start (engine=started), session replay and any KV wake.", metrics.LatencyBuckets, "engine")
	queriesTotal = metrics.Default.NewCounter("s2z_proxy_queries_total",
		"Statements forwarded to query engines, by tenant.", "tenant")
)

// RegisterMetrics adds the proxy's gauges to r.
func (p *Proxy) RegisterMetrics(r *metrics.Registry) {
	r.NewGaugeFunc("s2z_proxy_sessions", "Client sessions.", func() []metrics.Sample {
		p.mu.Lock()
		defer p.mu.Unlock()
		return []metrics.Sample{{Value: float64(len(p.sessions))}}
	})
	r.NewGaugeFunc("s2z_proxy_backends", "Sessions holding a query-engine connection.", func() []metrics.Sample {
		return []metrics.Sample{{Value: float64(p.Backends())}}
	})
	r.NewGaugeFunc("s2z_proxy_engines", "Query engines running.", func() []metrics.Sample {
		return []metrics.Sample{{Value: float64(p.cfg.Pool.Running())}}
	})
}
