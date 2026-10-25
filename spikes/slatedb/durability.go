package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func key(i int) []byte { return []byte(fmt.Sprintf("k%09d", i)) }
func val(i int) []byte { return []byte(fmt.Sprintf("v%09d-%s", i, strings.Repeat("x", 100))) }

// fence: a second writer must fence the first. The first writer's later
// writes must fail and never become visible.
func fence() {
	ctx := context.Background()
	c := s3FromEnv()
	must(c.ensureBucket(ctx))
	store, err := c.objectStore("")
	must(err)
	defer store.Destroy()
	path := fmt.Sprintf("fence-%d", time.Now().UnixNano())

	a, err := openDB(store, path, nil)
	must(err)
	putDurable(a, "a1")
	b, err := openDB(store, path, nil)
	must(err)
	fmt.Println("writer B opened the same path")

	// A does not know yet. Its next write must not become durable.
	var aErr error
	if h, err := a.Put([]byte("a2"), []byte("from A after B opened")); err != nil {
		aErr = err
	} else {
		aErr = h.AwaitDurable()
		h.Destroy()
	}
	st := a.Status()
	fmt.Printf("A's write after fencing: err=%v, close reason=%v\n", aErr, closeReason(st))
	putDurable(b, "b1")
	if v, _ := b.Get([]byte("a2")); v != nil {
		must(errors.New("B sees A's write made after B opened"))
	}
	must(b.Shutdown())
	b.Destroy()
	_ = a.Shutdown()
	a.Destroy()

	c2, err := openDB(store, path, nil)
	must(err)
	got := map[string]bool{}
	for _, k := range []string{"a1", "a2", "b1"} {
		v, err := c2.Get([]byte(k))
		must(err)
		got[k] = v != nil
	}
	must(c2.Shutdown())
	c2.Destroy()
	fmt.Printf("after reopen: a1=%v a2=%v b1=%v\n", got["a1"], got["a2"], got["b1"])
	if aErr == nil || !got["a1"] || got["a2"] || !got["b1"] {
		must(errors.New("fencing: FAIL"))
	}
	fmt.Println("fence: ok (A's write after fencing failed and is not visible)")
}

func closeReason(st slatedb.DbStatus) string {
	if st.CloseReason == nil {
		return "open"
	}
	return fmt.Sprint(*st.CloseReason)
}

func putDurable(db *slatedb.Db, k string) {
	h, err := db.Put([]byte(k), []byte("v-"+k))
	must(err)
	must(h.AwaitDurable())
	h.Destroy()
}

// writer is the child process of crash: it writes keys from -from upwards
// with 8 concurrent committers and prints "ack N" once key N is durable.
func writer(args []string) {
	fs := flag.NewFlagSet("writer", flag.ExitOnError)
	path := fs.String("path", "", "database path")
	from := fs.Int("from", 0, "first key")
	endpoint := fs.String("endpoint", "", "store address (the proxy)")
	must(fs.Parse(args))
	c := s3FromEnv()
	store, err := c.objectStore(*endpoint)
	must(err)
	db, err := openDB(store, *path, map[string]string{"flush_interval": `"10ms"`})
	must(err)
	out := bufio.NewWriter(os.Stdout)
	var mu sync.Mutex
	next := *from
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				i := next
				next++
				mu.Unlock()
				h, err := db.Put(key(i), val(i))
				if err != nil {
					continue
				}
				if err := h.AwaitDurable(); err == nil {
					mu.Lock()
					fmt.Fprintf(out, "ack %d\n", i)
					out.Flush()
					mu.Unlock()
				}
				h.Destroy()
			}
		}()
	}
	wg.Wait()
}

// crash: kill -9 the writer at random moments, restart it, and check that
// every acknowledged key survived, through a proxy that can inject faults.
func crash(args []string) {
	fs := flag.NewFlagSet("crash", flag.ExitOnError)
	kills := fs.Int("kills", 5, "kill -9s")
	failProb := fs.Float64("fail", 0, "probability of an injected 503")
	dropProb := fs.Float64("drop", 0, "probability of a request landing with its response lost")
	seed := fs.Int64("seed", time.Now().UnixNano(), "seed")
	must(fs.Parse(args))
	ctx := context.Background()
	c := s3FromEnv()
	must(c.ensureBucket(ctx))
	p, err := startProxy(c.endpoint, 0, *failProb, *dropProb, *seed)
	must(err)
	path := fmt.Sprintf("crash-%d", time.Now().UnixNano())
	rng := rand.New(rand.NewSource(*seed))
	acked := map[int]bool{}
	next := 0
	for k := 0; k <= *kills; k++ {
		cmd := exec.Command(os.Args[0], "writer", "-path", path, "-from", strconv.Itoa(next), "-endpoint", p.addr)
		cmd.Stderr = os.Stderr
		stdout, err := cmd.StdoutPipe()
		must(err)
		must(cmd.Start())
		done := make(chan struct{})
		go func() {
			sc := bufio.NewScanner(stdout)
			for sc.Scan() {
				var i int
				if _, err := fmt.Sscanf(sc.Text(), "ack %d", &i); err == nil {
					acked[i] = true
					if i >= next {
						next = i + 1
					}
				}
			}
			close(done)
		}()
		time.Sleep(time.Duration(500+rng.Intn(1500)) * time.Millisecond)
		if k < *kills {
			_ = cmd.Process.Signal(syscall.SIGKILL)
			fmt.Printf("kill -9 writer #%d (acknowledged so far: %d)\n", k+1, len(acked))
		} else {
			_ = cmd.Process.Signal(syscall.SIGKILL)
		}
		<-done
		_ = cmd.Wait()
		next += 1000 // the next writer starts past anything in flight
	}

	store, err := c.objectStore("")
	must(err)
	db, err := openDB(store, path, nil)
	must(err)
	lost := 0
	for i := range acked {
		v, err := db.Get(key(i))
		must(err)
		if v == nil || !bytes.Equal(*v, val(i)) {
			lost++
		}
	}
	must(db.Shutdown())
	db.Destroy()
	fmt.Printf("acknowledged=%d lost=%d injected faults=%d\n", len(acked), lost, p.failed.Load())
	if lost > 0 {
		must(errors.New("crash: FAIL, acknowledged writes lost"))
	}
	fmt.Println("crash: ok")
}
