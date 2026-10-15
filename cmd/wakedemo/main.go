// Command wakedemo runs the slice-6 scenario with real processes: dird
// starts a dedicated kvd for the tenant on demand, sqld routes through the
// directory, the kvd exits after the idle time, and the next query wakes a
// new one.
//
// It needs a running MinIO and the dird, kvd and sqld binaries.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salesstair-01/001/internal/directory"
	"github.com/salesstair-01/001/internal/kvrpc"
)

func main() {
	bin := flag.String("bin", "./bin", "directory with dird, kvd and sqld")
	idle := flag.Duration("idle", 5*time.Second, "idle time before the tenant's kvd suspends and exits (the v1 default is 5m)")
	rounds := flag.Int("rounds", 3, "sleep/wake rounds")
	flag.Parse()

	dir, _ := os.MkdirTemp("", "wakedemo")
	dirAddr, sqlAddr := freeAddr(), freeAddr()
	partition := fmt.Sprint(time.Now().UnixNano()%(1<<40) + 1000)

	dird := start(dir, "dird", filepath.Join(*bin, "dird"), "-listen", dirAddr, "-db", filepath.Join(dir, "dir.db"),
		"-spawn-kvd", filepath.Join(*bin, "kvd"), "-spawn-data", filepath.Join(dir, "kv"),
		"-spawn-args", "-idle-suspend "+idle.String())
	defer dird.Process.Kill()
	sqld := start(dir, "sqld", filepath.Join(*bin, "sqld"), "-listen", sqlAddr, "-directory", dirAddr, "-partition", partition, "-create-db", "demo")
	defer sqld.Process.Kill()

	cc, err := grpc.NewClient(dirAddr, append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	must(err)
	dc := directory.NewClient(cc)
	var pid uint64
	fmt.Sscan(partition, &pid)

	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/demo", sqlAddr))
	must(err)
	db.SetConnMaxIdleTime(time.Hour)
	waitFor("sqld", 30*time.Second, func() bool {
		_, err := db.Exec("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY, v TEXT)")
		return err == nil
	})
	for i := 0; i < 100; i++ {
		_, err := db.Exec("INSERT INTO t VALUES (?, 'hello')", i)
		must(err)
	}
	fmt.Printf("tenant %s: 100 rows written; kvd processes: %d\n", partition, kvdCount(dir))

	for r := 1; r <= *rounds; r++ {
		start := time.Now()
		waitFor("the idle kvd to exit", *idle+60*time.Second, func() bool {
			route, err := dc.Peek(context.Background(), pid)
			return err == nil && route.Node == "" && kvdCount(dir) == 0
		})
		fmt.Printf("round %d: kvd suspended and exited %v after the last query (idle time %v); kvd processes: %d\n",
			r, time.Since(start).Round(100*time.Millisecond), *idle, kvdCount(dir))
		q := time.Now()
		var n int
		must(db.QueryRow("SELECT COUNT(*) FROM t").Scan(&n))
		fmt.Printf("round %d: next query woke the tenant and returned %d rows in %v; kvd processes: %d\n",
			r, n, time.Since(q).Round(time.Millisecond), kvdCount(dir))
		if n != 100 {
			must(fmt.Errorf("expected 100 rows, got %d", n))
		}
	}
	fmt.Println("PASS")
}

// kvdCount counts this demo's spawned kvd processes.
func kvdCount(dir string) int {
	out, _ := exec.Command("pgrep", "-f", filepath.Join(dir, "kv")+"/").Output()
	return len(strings.Fields(string(out)))
}

func start(dir, name, path string, args ...string) *exec.Cmd {
	f, err := os.Create(filepath.Join(dir, name+".log"))
	must(err)
	cmd := exec.Command(path, args...)
	cmd.Stdout, cmd.Stderr = f, f
	must(cmd.Start())
	return cmd
}

func freeAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	defer l.Close()
	return l.Addr().String()
}

func waitFor(what string, timeout time.Duration, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			must(fmt.Errorf("timed out waiting for %s", what))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func must(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}
