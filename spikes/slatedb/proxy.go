package main

import (
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// proxy sits between SlateDB and the store. SlateDB talks S3 from Rust, so
// the owner's Go fault wrapper cannot reach it; at the HTTP level the spike
// can still add latency (to model S3), inject failures, and count requests.
// The Host header is passed through unchanged, so SigV4 signatures, made for
// the proxy's address, still verify at the store.
type proxy struct {
	addr     string
	latency  time.Duration
	failProb float64 // answer 503 before forwarding
	dropProb float64 // forward, then drop the response (a lost acknowledgement)

	mu  sync.Mutex
	rng *rand.Rand

	gets, puts, other atomic.Int64
	bytesIn, bytesOut atomic.Int64
	failed            atomic.Int64
}

func startProxy(target string, latency time.Duration, failProb, dropProb float64, seed int64) (*proxy, error) {
	u, _ := url.Parse("http://" + target)
	rp := httputil.NewSingleHostReverseProxy(u)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &proxy{addr: ln.Addr().String(), latency: latency, failProb: failProb, dropProb: dropProb, rng: rand.New(rand.NewSource(seed))}
	go http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			p.gets.Add(1)
		case http.MethodPut:
			p.puts.Add(1)
		default:
			p.other.Add(1)
		}
		if r.ContentLength > 0 {
			p.bytesOut.Add(r.ContentLength)
		}
		if p.latency > 0 {
			time.Sleep(p.latency)
		}
		p.mu.Lock()
		fail, drop := p.rng.Float64() < p.failProb, p.rng.Float64() < p.dropProb
		p.mu.Unlock()
		if fail {
			p.failed.Add(1)
			http.Error(w, "injected", http.StatusServiceUnavailable)
			return
		}
		if drop {
			// Let the request land, then cut the connection before answering.
			p.failed.Add(1)
			rec := &discardWriter{h: http.Header{}}
			rp.ServeHTTP(rec, r)
			if hj, ok := w.(http.Hijacker); ok {
				if c, _, err := hj.Hijack(); err == nil {
					c.Close()
					return
				}
			}
			http.Error(w, "dropped", http.StatusBadGateway)
			return
		}
		rp.ServeHTTP(&countingWriter{ResponseWriter: w, n: &p.bytesIn}, r)
	}))
	return p, nil
}

func (p *proxy) reset() {
	for _, c := range []*atomic.Int64{&p.gets, &p.puts, &p.other, &p.bytesIn, &p.bytesOut, &p.failed} {
		c.Store(0)
	}
}

type countingWriter struct {
	http.ResponseWriter
	n *atomic.Int64
}

func (c *countingWriter) Write(b []byte) (int, error) {
	c.n.Add(int64(len(b)))
	return c.ResponseWriter.Write(b)
}

type discardWriter struct{ h http.Header }

func (d *discardWriter) Header() http.Header         { return d.h }
func (d *discardWriter) Write(b []byte) (int, error) { return io.Discard.Write(b) }
func (d *discardWriter) WriteHeader(int)             {}
