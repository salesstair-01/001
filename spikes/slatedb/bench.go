package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	mrand "math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	slatedb "slatedb.io/slatedb-go/uniffi"
)

// bench mirrors cmd/remotebench: load a tenant in 8 MiB transactions,
// suspend (shut down with a memtable flush, like the owner's checkpoint),
// wake on a cold process state, and read. Requests go through the proxy,
// which can add latency to model S3 and counts what is sent.
func bench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	sizes := fs.String("sizes", "100,500", "tenant sizes in MB")
	latency := fs.Duration("latency", 0, "added to every store request")
	reads := fs.Int("reads", 200, "random point reads")
	rowSize := fs.Int("row", 1024, "bytes per row value")
	must(fs.Parse(args))
	ctx := context.Background()
	c := s3FromEnv()
	must(c.ensureBucket(ctx))
	p, err := startProxy(c.endpoint, *latency, 0, 0, 1)
	must(err)
	settings := map[string]string{"flush_interval": `"10ms"`}

	fmt.Println("| size | load | PUTs | MB up | cold open | GETs at open | MB down at open | first read | point p50/p99 cold | repeat p50 | scan 1000 | commit p50 | RSS |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, field := range strings.Split(*sizes, ",") {
		var mb int
		fmt.Sscan(field, &mb)
		path := fmt.Sprintf("bench-%d-%dmb", time.Now().UnixNano(), mb)
		store, err := c.objectStore(p.addr)
		must(err)

		// Load.
		db, err := openDB(store, path, settings)
		must(err)
		p.reset()
		rows := mb << 20 / *rowSize
		perTxn := 8 << 20 / *rowSize
		v := make([]byte, *rowSize)
		start := time.Now()
		for i := 0; i < rows; i += perTxn {
			b := slatedb.NewWriteBatch()
			for j := i; j < i+perTxn && j < rows; j++ {
				rand.Read(v[:len(v)/2]) // about 2:1 compressible, like remotebench
				must(b.Put(benchKey(j), append([]byte(nil), v...)))
			}
			h, err := db.Write(b)
			must(err)
			must(h.AwaitDurable())
			h.Destroy()
			b.Destroy()
		}
		flush := slatedb.FlushTypeMemTable
		must(db.ShutdownWithOptions(slatedb.CloseOptions{FlushType: &flush}))
		db.Destroy()
		load := time.Since(start)
		loadPuts, loadUp := p.puts.Load(), p.bytesOut.Load()
		store.Destroy()

		// Cold wake: a new store handle and database, nothing cached.
		store, err = c.objectStore(p.addr)
		must(err)
		p.reset()
		start = time.Now()
		db, err = openDB(store, path, settings)
		must(err)
		wake := time.Since(start)
		wakeGets, wakeDown := p.gets.Load(), p.bytesIn.Load()

		p.reset()
		readAt := func(i int) time.Duration {
			t := time.Now()
			got, err := db.Get(benchKey(i))
			must(err)
			if got == nil {
				must(fmt.Errorf("row %d missing", i))
			}
			return time.Since(t)
		}
		first := readAt(rows / 2)
		rng := mrand.New(mrand.NewSource(1))
		picks := make([]int, *reads)
		for i := range picks {
			picks[i] = rng.Intn(rows)
		}
		var cold, repeat []time.Duration
		for _, i := range picks {
			cold = append(cold, readAt(i))
		}
		for _, i := range picks {
			repeat = append(repeat, readAt(i))
		}
		lo, hi := benchKey(rows/3), benchKey(rows)
		t := time.Now()
		// Scan with readahead and parallel fetches; with Scan's defaults a
		// cold scan fetched one block per round trip (about 1 s for 1,000
		// rows on loopback).
		it, err := db.ScanWithOptions(slatedb.KeyRange{Start: &lo, StartInclusive: true, End: &hi},
			slatedb.ScanOptions{DurabilityFilter: slatedb.DurabilityLevelMemory, ReadAheadBytes: 1 << 20, CacheBlocks: true, MaxFetchTasks: 4})
		must(err)
		got := 0
		for got < 1000 {
			batch, err := it.NextBatch(uint32(1000 - got))
			must(err)
			if len(batch) == 0 {
				break
			}
			got += len(batch)
		}
		it.Destroy()
		scan := time.Since(t)
		if got != 1000 {
			must(fmt.Errorf("scan returned %d rows", got))
		}
		var commits []time.Duration
		for i := 0; i < 30; i++ {
			t := time.Now()
			h, err := db.Put([]byte(fmt.Sprintf("c%04d", i)), []byte("x"))
			must(err)
			must(h.AwaitDurable())
			h.Destroy()
			commits = append(commits, time.Since(t))
		}
		rss := rssMB()
		must(db.Shutdown())
		db.Destroy()
		store.Destroy()

		fmt.Printf("| %d MB | %v | %d | %.0f | %v | %d | %.1f | %v | %v/%v | %v | %v | %v | %d MB |\n",
			mb, load.Round(time.Millisecond), loadPuts, float64(loadUp)/(1<<20),
			wake.Round(time.Millisecond), wakeGets, float64(wakeDown)/(1<<20),
			first.Round(time.Microsecond), pct(cold, 50), pct(cold, 99), pct(repeat, 50),
			scan.Round(time.Microsecond), pct(commits, 50), rss)
		must(deletePrefix(ctx, c, path+"/"))
	}
}

func benchKey(i int) []byte {
	return binary.BigEndian.AppendUint64([]byte("u"), uint64(i))
}

func pct(ds []time.Duration, p int) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[(len(s)-1)*p/100].Round(time.Microsecond)
}

// rssMB reads this process's resident memory.
func rssMB() int {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			var kb int
			fmt.Sscan(strings.TrimSpace(strings.TrimPrefix(line, "VmRSS:")), &kb)
			return kb / 1024
		}
	}
	return -1
}

func threads() int {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "Threads:") {
			var n int
			fmt.Sscan(strings.TrimSpace(strings.TrimPrefix(line, "Threads:")), &n)
			return n
		}
	}
	return -1
}

// deletePrefix removes a run's objects, so repeated runs do not fill the disk.
func deletePrefix(ctx context.Context, c s3, prefix string) error {
	cl, err := c.client()
	if err != nil {
		return err
	}
	for obj := range cl.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		if err := cl.RemoveObject(ctx, c.bucket, obj.Key, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
	}
	return nil
}
