// Package metrics is a small Prometheus text-format registry: counters,
// histograms and gauges with labels, served at /metrics. It has no
// dependencies, so every binary can expose one.
package metrics

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Registry holds metric families in registration order.
type Registry struct {
	mu       sync.Mutex
	families []family
	names    map[string]bool
}

type family interface {
	write(b *strings.Builder)
}

// Default is the registry the binaries serve.
var Default = &Registry{}

func (r *Registry) add(name string, f family) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.names == nil {
		r.names = map[string]bool{}
	}
	if r.names[name] {
		panic("metrics: duplicate " + name)
	}
	r.names[name] = true
	r.families = append(r.families, f)
}

// Text renders every family in the Prometheus text format.
func (r *Registry) Text() string {
	r.mu.Lock()
	fs := append([]family(nil), r.families...)
	r.mu.Unlock()
	var b strings.Builder
	for _, f := range fs {
		f.write(&b)
	}
	return b.String()
}

// ServeHTTP serves Text.
func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(r.Text()))
}

// Serve listens on addr and serves /metrics until the process exits; an
// empty addr does nothing.
func Serve(addr string) error {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", Default)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	select {
	case err := <-errc:
		return err
	case <-time.After(50 * time.Millisecond):
		return nil
	}
}

func header(b *strings.Builder, name, help, typ string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

func labelString(names, values []string, extra ...string) string {
	var parts []string
	for i, n := range names {
		parts = append(parts, fmt.Sprintf("%s=%q", n, values[i]))
	}
	for i := 0; i+1 < len(extra); i += 2 {
		parts = append(parts, fmt.Sprintf("%s=%q", extra[i], extra[i+1]))
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case v == math.Trunc(v) && math.Abs(v) < 1e15:
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}

// ---- counters -----------------------------------------------------------------

// CounterVec is a counter family with labels.
type CounterVec struct {
	name, help string
	labels     []string
	mu         sync.Mutex
	vals       map[string]*counterVal
}

type counterVal struct {
	labels []string
	v      float64
}

// NewCounter registers a counter family.
func (r *Registry) NewCounter(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, labels: labels, vals: map[string]*counterVal{}}
	r.add(name, c)
	return c
}

// Add adds d to the counter with these label values.
func (c *CounterVec) Add(d float64, labelValues ...string) {
	k := strings.Join(labelValues, "\xff")
	c.mu.Lock()
	v := c.vals[k]
	if v == nil {
		v = &counterVal{labels: append([]string(nil), labelValues...)}
		c.vals[k] = v
	}
	v.v += d
	c.mu.Unlock()
}

// Inc adds 1.
func (c *CounterVec) Inc(labelValues ...string) { c.Add(1, labelValues...) }

// Value returns the counter with these label values.
func (c *CounterVec) Value(labelValues ...string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if v := c.vals[strings.Join(labelValues, "\xff")]; v != nil {
		return v.v
	}
	return 0
}

func (c *CounterVec) write(b *strings.Builder) {
	header(b, c.name, c.help, "counter")
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range sortedKeys(c.vals) {
		v := c.vals[k]
		fmt.Fprintf(b, "%s%s %s\n", c.name, labelString(c.labels, v.labels), formatFloat(v.v))
	}
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// ---- histograms ---------------------------------------------------------------

// LatencyBuckets suits durations from a millisecond to a minute, in seconds.
var LatencyBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

// HistogramVec is a histogram family with labels.
type HistogramVec struct {
	name, help string
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	vals       map[string]*histVal
}

type histVal struct {
	labels []string
	counts []uint64 // per bucket, not cumulative
	count  uint64
	sum    float64
}

// NewHistogram registers a histogram family.
func (r *Registry) NewHistogram(name, help string, buckets []float64, labels ...string) *HistogramVec {
	h := &HistogramVec{name: name, help: help, labels: labels, buckets: buckets, vals: map[string]*histVal{}}
	r.add(name, h)
	return h
}

// Observe records v.
func (h *HistogramVec) Observe(v float64, labelValues ...string) {
	k := strings.Join(labelValues, "\xff")
	h.mu.Lock()
	hv := h.vals[k]
	if hv == nil {
		hv = &histVal{labels: append([]string(nil), labelValues...), counts: make([]uint64, len(h.buckets))}
		h.vals[k] = hv
	}
	for i, ub := range h.buckets {
		if v <= ub {
			hv.counts[i]++
			break
		}
	}
	hv.count++
	hv.sum += v
	h.mu.Unlock()
}

// ObserveSince records the seconds since start.
func (h *HistogramVec) ObserveSince(start time.Time, labelValues ...string) {
	h.Observe(time.Since(start).Seconds(), labelValues...)
}

// Count returns how many values were observed with these label values.
func (h *HistogramVec) Count(labelValues ...string) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hv := h.vals[strings.Join(labelValues, "\xff")]; hv != nil {
		return hv.count
	}
	return 0
}

func (h *HistogramVec) write(b *strings.Builder) {
	header(b, h.name, h.help, "histogram")
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range sortedKeys(h.vals) {
		hv := h.vals[k]
		var cum uint64
		for i, ub := range h.buckets {
			cum += hv.counts[i]
			fmt.Fprintf(b, "%s_bucket%s %d\n", h.name, labelString(h.labels, hv.labels, "le", formatFloat(ub)), cum)
		}
		fmt.Fprintf(b, "%s_bucket%s %d\n", h.name, labelString(h.labels, hv.labels, "le", "+Inf"), hv.count)
		fmt.Fprintf(b, "%s_sum%s %g\n", h.name, labelString(h.labels, hv.labels), hv.sum)
		fmt.Fprintf(b, "%s_count%s %d\n", h.name, labelString(h.labels, hv.labels), hv.count)
	}
}

// ---- gauges -------------------------------------------------------------------

// Sample is one gauge value and its label values.
type Sample struct {
	Labels []string
	Value  float64
}

type gaugeFunc struct {
	name, help string
	labels     []string
	typ        string
	fn         func() []Sample
}

// NewGaugeFunc registers a gauge family whose samples fn returns at scrape time.
func (r *Registry) NewGaugeFunc(name, help string, fn func() []Sample, labels ...string) {
	r.add(name, &gaugeFunc{name: name, help: help, labels: labels, typ: "gauge", fn: fn})
}

// NewCounterFunc registers a counter family read from fn at scrape time.
func (r *Registry) NewCounterFunc(name, help string, fn func() []Sample, labels ...string) {
	r.add(name, &gaugeFunc{name: name, help: help, labels: labels, typ: "counter", fn: fn})
}

func (g *gaugeFunc) write(b *strings.Builder) {
	header(b, g.name, g.help, g.typ)
	for _, s := range g.fn() {
		fmt.Fprintf(b, "%s%s %s\n", g.name, labelString(g.labels, s.Labels), formatFloat(s.Value))
	}
}
