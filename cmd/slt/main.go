// Command slt runs sqllogictest files (scripts/slt) against the SQL layer and
// reports the pass rate per file. By default it starts an in-process KV node
// over an in-memory object store and one query engine; -dsn runs against a
// running sqld or proxyd instead.
//
//	go run ./cmd/slt -summary scripts/slt/results.md scripts/slt/test/select1.test ...
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/sqllogictest/go/logictest"
	"github.com/dolthub/sqllogictest/go/logictest/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/kvrpc"
	"github.com/salesstair-01/001/internal/objstore"
	"github.com/salesstair-01/001/internal/sqlengine"
)

func main() {
	dsn := flag.String("dsn", "", "run against this MySQL DSN (default: an in-process cluster)")
	summary := flag.String("summary", "", "write a Markdown summary here")
	logPath := flag.String("log", "", "keep the per-record result log here")
	minRate := flag.Float64("min-pass-rate", 0, "exit 1 when the overall pass rate (%) is below this")
	parallel := flag.Int("parallel", 1, "run this many files at once, each in its own process and cluster")
	recordTimeout := flag.Duration("record-timeout", time.Minute, "fail a record that runs longer; the rest of its file does not run")
	flag.Parse()
	files := flag.Args()
	start0 := time.Now()
	if len(files) == 0 {
		fmt.Fprintln(os.Stderr, "usage: slt [flags] file.test...")
		os.Exit(2)
	}
	if *parallel > 1 && len(files) > 1 {
		entries, err := runParallel(files, *parallel, *dsn, *recordTimeout)
		if err != nil {
			fail(err)
		}
		report(entries, start0, *summary, *minRate)
		return
	}
	if *dsn == "" {
		addr, stop, err := startCluster()
		if err != nil {
			fail(err)
		}
		defer stop()
		*dsn = "root@tcp(" + addr + ")/slt"
	}

	if *logPath == "" {
		f, err := os.CreateTemp("", "slt-*.log")
		if err != nil {
			fail(err)
		}
		f.Close()
		*logPath = f.Name()
		defer os.Remove(*logPath)
	}
	logFile, err := os.Create(*logPath)
	if err != nil {
		fail(err)
	}
	stdout := os.Stdout
	os.Stdout = logFile // the runner prints one line per record
	h := &harness{MysqlHarness: mysql.NewMysqlHarness(*dsn), timeout: *recordTimeout}
	for _, f := range files {
		fmt.Fprintf(os.Stderr, "running %s\n", f)
		logictest.RunTestFiles(h, f)
	}
	os.Stdout = stdout
	logFile.Close()

	entries, err := logictest.ParseResultFile(*logPath)
	if err != nil {
		fail(err)
	}
	report(entries, start0, *summary, *minRate)
}

// runParallel runs each file in a child process and collects their logs.
func runParallel(files []string, n int, dsn string, recordTimeout time.Duration) ([]*logictest.ResultLogEntry, error) {
	dir, err := os.MkdirTemp("", "slt-logs-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	self, err := os.Executable()
	if err != nil {
		return nil, err
	}
	logs := make([]string, len(files))
	sem := make(chan struct{}, n)
	var wg sync.WaitGroup
	for i, f := range files {
		logs[i] = filepath.Join(dir, fmt.Sprintf("%03d.log", i))
		wg.Add(1)
		go func(f, log string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			args := []string{"-log", log, "-record-timeout", recordTimeout.String()}
			if dsn != "" {
				args = append(args, "-dsn", dsn)
			}
			fmt.Fprintf(os.Stderr, "running %s\n", f)
			if err := exec.Command(self, append(args, f)...).Run(); err != nil {
				fmt.Fprintf(os.Stderr, "%s: %v\n", f, err)
			}
		}(f, logs[i])
	}
	wg.Wait()
	var all []*logictest.ResultLogEntry
	for _, l := range logs {
		if st, err := os.Stat(l); err != nil || st.Size() == 0 {
			continue
		}
		es, err := logictest.ParseResultFile(l)
		if err != nil {
			return nil, err
		}
		all = append(all, es...)
	}
	return all, nil
}

func report(entries []*logictest.ResultLogEntry, start time.Time, summary string, minRate float64) {
	type counts struct{ ok, notOk, skipped, timeout, dnr int }
	per := map[string]*counts{}
	total := &counts{}
	for _, e := range entries {
		c := per[e.TestFile]
		if c == nil {
			c = &counts{}
			per[e.TestFile] = c
		}
		for _, x := range []*counts{c, total} {
			switch e.Result {
			case logictest.Ok:
				x.ok++
			case logictest.NotOk:
				x.notOk++
			case logictest.Skipped:
				x.skipped++
			case logictest.Timeout:
				x.timeout++
			case logictest.DidNotRun:
				x.dnr++
			}
		}
	}
	rate := func(c *counts) float64 {
		run := c.ok + c.notOk + c.timeout + c.dnr
		if run == 0 {
			return 0
		}
		return 100 * float64(c.ok) / float64(run)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "| file | passed | failed | timeout/not run | pass rate |\n|---|---:|---:|---:|---:|\n")
	names := make([]string, 0, len(per))
	for n := range per {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		c := per[n]
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %.1f%% |\n", displayName(n), c.ok, c.notOk, c.timeout+c.dnr, rate(c))
	}
	fmt.Fprintf(&b, "| **total** | **%d** | **%d** | **%d** | **%.1f%%** |\n", total.ok, total.notOk, total.timeout+total.dnr, rate(total))
	fmt.Print(b.String())
	fmt.Printf("\n%d records in %v\n", len(entries), time.Since(start).Round(time.Second))
	if summary != "" {
		doc := "# sqllogictest results\n\nGenerated by `make slt` (`go run ./cmd/slt`): the files in\n" +
			"`scripts/slt/files.txt`, in-process KV node and query engine. Skipped records\n" +
			"(`skipif mysql`) are not counted. A record that runs over the record timeout\n" +
			"fails and the rest of its file does not run.\n\n" + b.String()
		if err := os.WriteFile(summary, []byte(doc), 0o644); err != nil {
			fail(err)
		}
	}
	if rate(total) < minRate {
		fmt.Fprintf(os.Stderr, "pass rate %.1f%% is below %.1f%%\n", rate(total), minRate)
		os.Exit(1)
	}
}

// harness is the MySQL harness with a per-record timeout (its own is 20 min).
type harness struct {
	*mysql.MysqlHarness
	timeout time.Duration
}

func (h *harness) GetTimeout() int64 { return int64(h.timeout / time.Second) }

// displayName strips everything up to the test directory.
func displayName(f string) string {
	f = filepath.ToSlash(f)
	if i := strings.Index(f, "/test/"); i >= 0 {
		return f[i+len("/test/"):]
	}
	return f
}

// startCluster runs a KV node over an in-memory store and a query engine.
func startCluster() (string, func(), error) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir, err := os.MkdirTemp("", "slt-kv-")
	if err != nil {
		return "", nil, err
	}
	node := kv.NewNode(kv.Config{Store: objstore.NewMem(), DataDir: dir, Logger: quiet})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	g := grpc.NewServer(kvrpc.ServerOption())
	kvrpc.Register(g, &kvrpc.Server{Node: node, AutoOpen: true})
	go g.Serve(lis)
	cc, err := grpc.NewClient(lis.Addr().String(), append(kvrpc.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		return "", nil, err
	}
	st := sqlengine.NewStore(kvrpc.NewClient(cc), 1)
	eng := sqlengine.NewEngine(st)
	if err := eng.EnsureDatabase(context.Background(), "slt"); err != nil {
		return "", nil, err
	}
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	addr := l2.Addr().String()
	l2.Close()
	srv, err := eng.NewServer(addr)
	if err != nil {
		return "", nil, err
	}
	go srv.Start()
	return addr, func() {
		srv.Close()
		st.Close()
		cc.Close()
		g.Stop()
		node.Close()
		os.RemoveAll(dir)
	}, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "slt:", err)
	os.Exit(1)
}
