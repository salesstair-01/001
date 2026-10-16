package metrics

import (
	"strings"
	"testing"
)

func TestText(t *testing.T) {
	r := &Registry{}
	c := r.NewCounter("c_total", "a counter", "k")
	c.Inc("a")
	c.Add(2, "a")
	c.Inc(`b"q`)
	h := r.NewHistogram("h_seconds", "a histogram", []float64{0.1, 1})
	h.Observe(0.05)
	h.Observe(0.5)
	h.Observe(5)
	r.NewGaugeFunc("g", "a gauge", func() []Sample { return []Sample{{Value: 1.5}} })
	got := r.Text()
	for _, want := range []string{
		"# TYPE c_total counter\n",
		`c_total{k="a"} 3` + "\n",
		`c_total{k="b\"q"} 1` + "\n",
		`h_seconds_bucket{le="0.1"} 1` + "\n",
		`h_seconds_bucket{le="1"} 2` + "\n",
		`h_seconds_bucket{le="+Inf"} 3` + "\n",
		"h_seconds_count 3\n",
		"h_seconds_sum 5.55\n",
		"g 1.5\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in\n%s", want, got)
		}
	}
	if c.Value("a") != 3 || h.Count() != 3 {
		t.Fatal("Value/Count")
	}
}
