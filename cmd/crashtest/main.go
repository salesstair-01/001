// Command crashtest is the slice-1 crash script: it runs a write loop through
// sqld while killing kvd with SIGKILL at random moments, restarts it, and
// finally checks that every row the client saw acknowledged is still there.
//
// It needs a running MinIO, and kvd and sqld binaries (see scripts/crash-test.sh).
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	kvdBin := flag.String("kvd", "./bin/kvd", "kvd binary")
	sqldBin := flag.String("sqld", "./bin/sqld", "sqld binary")
	kills := flag.Int("kills", 5, "how many times to kill -9 kvd")
	writers := flag.Int("writers", 4, "concurrent writer connections")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	workDir := flag.String("work", "", "working directory (default: a temp dir)")
	partFlag := flag.Uint64("partition", 0, "tenant partition (default: a fresh one per run)")
	flag.Parse()

	rng := rand.New(rand.NewSource(*seed))
	runTag := time.Now().Unix() % (1 << 19) // keeps ids unique when a tenant is reused
	fmt.Printf("crashtest seed=%d\n", *seed)
	dir := *workDir
	if dir == "" {
		dir, _ = os.MkdirTemp("", "crashtest")
	}
	partition := fmt.Sprint(time.Now().UnixNano()%(1<<40) + 1000) // a fresh tenant per run
	if *partFlag != 0 {
		partition = fmt.Sprint(*partFlag)
	}
	kvAddr, sqlAddr := freeAddr(), freeAddr()

	startKV := func(n int) *exec.Cmd {
		cmd := exec.Command(*kvdBin, "-listen", kvAddr, "-data", filepath.Join(dir, fmt.Sprint("kv", n)),
			"-owner", fmt.Sprint("crash-", n))
		cmd.Stdout, cmd.Stderr = logFile(dir, fmt.Sprint("kvd", n)), logFile(dir, fmt.Sprint("kvd", n))
		must(cmd.Start())
		return cmd
	}
	kvd := startKV(0)
	sqld := exec.Command(*sqldBin, "-listen", sqlAddr, "-kv", kvAddr, "-partition", partition, "-create-db", "crash")
	sqld.Stdout, sqld.Stderr = logFile(dir, "sqld"), logFile(dir, "sqld")
	must(sqld.Start())
	defer sqld.Process.Kill()

	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/crash", sqlAddr))
	must(err)
	db.SetMaxOpenConns(*writers + 2)
	waitFor(func() error {
		_, err := db.Exec("CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY, w INT, v VARCHAR(64))")
		return err
	})

	var mu sync.Mutex
	acked := map[int64]string{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var attempts, failures int
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := int64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := int64(w)<<40 | int64(runTag)<<20 | i
				v := fmt.Sprintf("w%d-%d", w, i)
				_, err := db.Exec("INSERT INTO t VALUES (?, ?, ?)", id, w, v)
				mu.Lock()
				attempts++
				if err == nil {
					acked[id] = v
				} else {
					failures++
				}
				mu.Unlock()
				if err != nil {
					time.Sleep(20 * time.Millisecond)
				}
			}
		}(w)
	}

	for k := 1; k <= *kills; k++ {
		time.Sleep(time.Duration(300+rng.Intn(1500)) * time.Millisecond)
		mu.Lock()
		n := len(acked)
		mu.Unlock()
		fmt.Printf("kill -9 kvd #%d (acknowledged so far: %d)\n", k, n)
		must(kvd.Process.Signal(syscall.SIGKILL))
		_ = kvd.Wait()
		time.Sleep(time.Duration(rng.Intn(500)) * time.Millisecond)
		kvd = startKV(k)
	}
	time.Sleep(time.Second)
	close(stop)
	wg.Wait()

	// Verify through a fresh KV process.
	must(kvd.Process.Signal(syscall.SIGKILL))
	_ = kvd.Wait()
	kvd = startKV(*kills + 1)
	var found map[int64]string
	waitFor(func() error {
		found = map[int64]string{}
		rows, err := db.Query("SELECT id, v FROM t")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			var v string
			if err := rows.Scan(&id, &v); err != nil {
				return err
			}
			found[id] = v
		}
		return rows.Err()
	})
	lost := 0
	for id, v := range acked {
		if found[id] != v {
			lost++
			if lost <= 10 {
				fmt.Printf("LOST acknowledged row id=%d v=%q (found %q)\n", id, v, found[id])
			}
		}
	}
	// Stop the last kvd and sqld (os.Exit below skips deferred calls).
	_ = kvd.Process.Kill()
	_ = kvd.Wait()
	_ = sqld.Process.Kill()
	fmt.Printf("attempts=%d acknowledged=%d failed=%d rows=%d lost=%d\n", attempts, len(acked), failures, len(found), lost)
	if lost > 0 || len(acked) == 0 {
		fmt.Println("FAIL")
		os.Exit(1)
	}
	fmt.Println("PASS: every acknowledged row survived", *kills, "kill -9s")
}

func freeAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	defer l.Close()
	return l.Addr().String()
}

func logFile(dir, name string) *os.File {
	f, err := os.OpenFile(filepath.Join(dir, name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	must(err)
	return f
}

func waitFor(f func() error) {
	var err error
	for i := 0; i < 300; i++ {
		if err = f(); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	must(err)
}

func must(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(2)
	}
}
