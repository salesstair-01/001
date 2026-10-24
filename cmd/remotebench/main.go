// Command remotebench compares the two ways the KV owner keeps SSTs on
// MinIO: uploaded at checkpoints and downloaded on wake (the default), or
// written by Pebble straight to the object store and read lazily
// (Config.RemoteSSTs, the spike). For each tenant size and mode it loads
// rows, suspends, wakes on an empty host, and reports wake time, read
// latency, local disk and object-store requests.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/objstore"
)

func main() {
	sizes := flag.String("sizes", "100,500", "tenant sizes in MB, comma-separated")
	endpoint := flag.String("minio-endpoint", "127.0.0.1:9000", "MinIO host:port")
	bucket := flag.String("bucket", "s2z-remotebench", "MinIO bucket")
	work := flag.String("work", "", "local working directory (default: a temp dir)")
	rowSize := flag.Int("row", 1024, "bytes per row value")
	reads := flag.Int("reads", 200, "random point reads after wake")
	latency := flag.Duration("latency", 0, "added to every object-store request, to model S3 (MinIO on loopback answers in well under 1 ms)")
	flag.Parse()

	ctx := context.Background()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := *work
	if dir == "" {
		dir, _ = os.MkdirTemp("", "remotebench")
	}
	defer os.RemoveAll(dir)

	fmt.Println("| size | mode | load | load PUTs (SST/other) | load MB up | files local/remote | cold wake | wake GETs | wake MB down | first read | point p50/p99 cold | point p50/p99 warm | scan 1000 | read GETs | local disk |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, field := range strings.Split(*sizes, ",") {
		var mb int
		fmt.Sscan(field, &mb)
		for _, remote := range []bool{false, true} {
			mode := "upload at checkpoint"
			if remote {
				mode = "remote SSTs"
			}
			minio, err := objstore.NewMinIO(ctx, objstore.MinIOConfig{
				Endpoint: *endpoint, AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: *bucket,
				Prefix: fmt.Sprintf("run-%d-%dmb-%v/", time.Now().UnixNano(), mb, remote),
			})
			must(err)
			store := &counting{Store: minio, latency: *latency}
			cfg := func(name string) kv.Config {
				return kv.Config{
					Store: store, DataDir: filepath.Join(dir, name), Logger: quiet, GCGrace: time.Hour,
					OwnerID: name, CacheMaxBytes: -1, RemoteSSTs: remote, PutRate: -1,
				}
			}

			// Load: 8 MiB transactions, then suspend (checkpoint).
			p, err := kv.Takeover(ctx, cfg("load"), 1)
			must(err)
			rows := mb << 20 / *rowSize
			perTxn := 8 << 20 / *rowSize
			val := make([]byte, *rowSize)
			start := time.Now()
			for i := 0; i < rows; i += perTxn {
				var id kv.TxnID
				rand.Read(id[:])
				must(p.Begin(p.Epoch(), id, kv.BeginOptions{}))
				var muts []kv.Mutation
				for j := i; j < i+perTxn && j < rows; j++ {
					rand.Read(val[:len(val)/2]) // about 2:1 compressible, like real rows
					row, _ := encoding.EncodeRow([]encoding.RowValue{{ID: 1, Bytes: encoding.ValueInt(int64(j))}, {ID: 2, Bytes: append([]byte(nil), val...)}})
					muts = append(muts, kv.Mutation{Op: kv.OpPut, Key: key(j), Value: row})
				}
				must(p.Mutate(p.Epoch(), id, 1, muts, 0))
				_, err := p.Commit(p.Epoch(), id)
				must(err)
			}
			must(p.Suspend(ctx))
			load := time.Since(start)
			loadOps := store.snapshot()
			os.RemoveAll(filepath.Join(dir, "load"))

			// Cold wake on an empty host, then reads.
			store.reset()
			start = time.Now()
			p, err = kv.Takeover(ctx, cfg("wake"), 1)
			must(err)
			wake := time.Since(start)
			wakeOps := store.snapshot()
			files, remoteFiles := p.FileCount(), p.RemoteCount()

			store.reset()
			var id kv.TxnID
			rand.Read(id[:])
			must(p.Begin(p.Epoch(), id, kv.BeginOptions{}))
			readAt := func(i int) time.Duration {
				t := time.Now()
				vs, err := p.Get(p.Epoch(), id, [][]byte{key(i)}, kv.ReadOptions{Stmt: 1})
				must(err)
				if vs[0] == nil {
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
			var cold, warm []time.Duration
			for _, i := range picks {
				cold = append(cold, readAt(i))
			}
			for _, i := range picks {
				warm = append(warm, readAt(i))
			}
			t := time.Now()
			kvs, _, err := p.Scan(p.Epoch(), id, key(rows/3), key(rows), 1000, kv.ReadOptions{Stmt: 1})
			must(err)
			scan := time.Since(t)
			if len(kvs) != 1000 {
				must(fmt.Errorf("scan returned %d rows", len(kvs)))
			}
			readOps := store.snapshot()
			disk := du(filepath.Join(dir, "wake"))
			p.Rollback(p.Epoch(), id)
			must(p.Close())
			os.RemoveAll(filepath.Join(dir, "wake"))

			fmt.Printf("| %d MB | %s | %v | %d/%d | %.0f | %d/%d | %v | %d | %.1f | %v | %v/%v | %v/%v | %v | %d | %.0f MB |\n",
				mb, mode, load.Round(time.Millisecond),
				loadOps.puts["sst"], loadOps.puts["other"], float64(loadOps.upBytes)/(1<<20),
				files, remoteFiles, wake.Round(time.Millisecond),
				wakeOps.gets+wakeOps.ranges, float64(wakeOps.downBytes)/(1<<20),
				first.Round(time.Microsecond),
				pct(cold, 50), pct(cold, 99), pct(warm, 50), pct(warm, 99),
				scan.Round(time.Microsecond), readOps.gets+readOps.ranges, float64(disk)/(1<<20))
		}
	}
}

func pct(ds []time.Duration, p int) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[(len(s)-1)*p/100].Round(time.Microsecond)
}

func du(dir string) int64 {
	var n int64
	_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, err := d.Info(); err == nil {
				n += info.Size()
			}
		}
		return nil
	})
	return n
}

// counting counts object-store requests and bytes.
type counting struct {
	objstore.Store
	latency time.Duration
	mu      sync.Mutex
	c       ops
}

func (c *counting) wait() {
	if c.latency > 0 {
		time.Sleep(c.latency)
	}
}

func (c *counting) List(ctx context.Context, prefix, after string) ([]string, error) {
	c.wait()
	return c.Store.List(ctx, prefix, after)
}

func (c *counting) Size(ctx context.Context, k string) (int64, error) {
	c.wait()
	return c.Store.Size(ctx, k)
}

type ops struct {
	puts               map[string]int
	gets, ranges       int
	upBytes, downBytes int64
}

func (c *counting) reset() {
	c.mu.Lock()
	c.c = ops{puts: map[string]int{}}
	c.mu.Unlock()
}

func (c *counting) snapshot() ops {
	c.mu.Lock()
	defer c.mu.Unlock()
	o := c.c
	if o.puts == nil {
		o.puts = map[string]int{}
	}
	return o
}

func (c *counting) PutIfAbsent(ctx context.Context, k string, d []byte) error {
	c.mu.Lock()
	if c.c.puts == nil {
		c.c.puts = map[string]int{}
	}
	if strings.HasSuffix(k, ".sst") {
		c.c.puts["sst"]++
	} else {
		c.c.puts["other"]++
	}
	c.c.upBytes += int64(len(d))
	c.mu.Unlock()
	c.wait()
	return c.Store.PutIfAbsent(ctx, k, d)
}

func (c *counting) Get(ctx context.Context, k string) ([]byte, error) {
	c.wait()
	b, err := c.Store.Get(ctx, k)
	c.mu.Lock()
	c.c.gets++
	c.c.downBytes += int64(len(b))
	c.mu.Unlock()
	return b, err
}

func (c *counting) GetRange(ctx context.Context, k string, off, n int64) ([]byte, error) {
	c.wait()
	b, err := c.Store.GetRange(ctx, k, off, n)
	c.mu.Lock()
	c.c.ranges++
	c.c.downBytes += int64(len(b))
	c.mu.Unlock()
	return b, err
}

func key(i int) []byte {
	k := encoding.Prefix(1, encoding.FirstUserTableID, 0)
	return binary.BigEndian.AppendUint64(append(k, encoding.MarkerValue), uint64(i)^(1<<63))
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
