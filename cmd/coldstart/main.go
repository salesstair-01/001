// Command coldstart measures slice 7's promise with real processes: an app
// keeps a connection pool open to proxyd while every compute process (query
// engines and KV owners) scales to zero, and each round times the first query
// that has to wake them all. It reports p50 and p99.
//
// It needs a running MinIO and the dird, kvd, sqld and proxyd binaries.
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	bin := flag.String("bin", "./bin", "directory with dird, kvd, sqld and proxyd")
	rounds := flag.Int("rounds", 20, "cold starts to measure")
	warm := flag.Int("warm", 0, "warm query engines kept by proxyd (0 = every layer at zero)")
	flag.Parse()

	dir, _ := os.MkdirTemp("", "coldstart")
	dirAddr, proxyAddr := freeAddr(), freeAddr()
	partition := uint64(time.Now().UnixNano()%(1<<40) + 1000)
	tenants, _ := json.Marshal([]map[string]any{{"name": "acme", "partition": partition, "users": map[string]string{"app": "s3cret"}}})
	must(os.WriteFile(filepath.Join(dir, "tenants.json"), tenants, 0o644))

	dird := start(dir, "dird", filepath.Join(*bin, "dird"), "-listen", dirAddr, "-db", filepath.Join(dir, "dir.db"),
		"-spawn-kvd", filepath.Join(*bin, "kvd"), "-spawn-data", filepath.Join(dir, "kv"), "-spawn-args", "-idle-suspend 2s")
	defer dird.Process.Kill()
	proxyd := start(dir, "proxyd", filepath.Join(*bin, "proxyd"), "-listen", proxyAddr, "-tenants", filepath.Join(dir, "tenants.json"),
		"-spawn-sqld", filepath.Join(*bin, "sqld"), "-directory", dirAddr, "-spawn-data", filepath.Join(dir, "sqld"),
		"-warm", fmt.Sprint(*warm), "-engine-idle", "1s", "-backend-idle", "1s")
	defer proxyd.Process.Kill()
	defer exec.Command("pkill", "-f", "--", "-directory "+dirAddr).Run()

	db, err := sql.Open("mysql", fmt.Sprintf("app@acme:s3cret@tcp(%s)/", proxyAddr))
	must(err)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(time.Hour)
	waitFor("proxyd", 30*time.Second, func() bool { return db.Ping() == nil })
	_, err = db.Exec("CREATE DATABASE app")
	must(err)
	_, err = db.Exec("CREATE TABLE app.items (id INT PRIMARY KEY, name VARCHAR(20))")
	must(err)
	_, err = db.Exec("INSERT INTO app.items VALUES (1, 'a')")
	must(err)

	var lat []time.Duration
	for r := 1; r <= *rounds; r++ {
		waitFor("compute to reach zero", 60*time.Second, func() bool {
			k, q := count("-node-id", dirAddr), count("-generic", dirAddr)
			return k == 0 && q == *warm
		})
		must(db.Ping()) // pool health check: answered by proxyd, wakes nothing
		start := time.Now()
		var n int
		must(db.QueryRow("SELECT COUNT(*) FROM app.items").Scan(&n))
		d := time.Since(start)
		lat = append(lat, d)
		fmt.Printf("round %2d: %d KV and %d query engines were running; first query %v\n", r, 0, *warm, d.Round(time.Millisecond))
		_, err := db.Exec("INSERT INTO app.items VALUES (?, 'x')", r+1)
		must(err)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p := func(q float64) time.Duration { return lat[int(q*float64(len(lat)-1)+0.5)].Round(time.Millisecond) }
	fmt.Printf("cold start through proxyd, warm=%d, n=%d: p50 %v, p99 %v, min %v, max %v\n",
		*warm, len(lat), p(0.5), p(0.99), lat[0].Round(time.Millisecond), lat[len(lat)-1].Round(time.Millisecond))
}

// count counts this run's processes whose command line has marker.
func count(marker, dirAddr string) int {
	procs, _ := filepath.Glob("/proc/[0-9]*/cmdline")
	n := 0
	for _, path := range procs {
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		cmd := strings.ReplaceAll(string(b), "\x00", " ")
		if strings.Contains(cmd, "-directory "+dirAddr) && strings.Contains(cmd, marker) {
			n++
		}
	}
	return n
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
		time.Sleep(50 * time.Millisecond)
	}
}

func must(err error) {
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}
