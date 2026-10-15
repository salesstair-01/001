// Command waketime measures how long a suspended partition takes to wake
// (slice 6): it loads a tenant with N MB of rows through the KV API, suspends
// it, and times a takeover through to the first read, once with an empty
// local cache (every SST downloaded from MinIO) and once with the cache warm.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/salesstair-01/001/internal/encoding"
	"github.com/salesstair-01/001/internal/kv"
	"github.com/salesstair-01/001/internal/objstore"
)

func main() {
	sizes := flag.String("sizes", "100,1024", "tenant sizes in MB, comma-separated")
	endpoint := flag.String("minio-endpoint", "127.0.0.1:9000", "MinIO host:port")
	bucket := flag.String("bucket", "s2z-waketime", "MinIO bucket")
	work := flag.String("work", "", "local working directory (default: a temp dir)")
	rowSize := flag.Int("row", 1024, "bytes per row value")
	flag.Parse()

	ctx := context.Background()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := *work
	if dir == "" {
		dir, _ = os.MkdirTemp("", "waketime")
	}
	defer os.RemoveAll(dir)

	fmt.Println("| tenant size | files | cold wake | warm wake (local cache) | first read after wake |")
	fmt.Println("|---|---|---|---|---|")
	for _, field := range strings.Split(*sizes, ",") {
		var mb int
		fmt.Sscan(field, &mb)
		store, err := objstore.NewMinIO(ctx, objstore.MinIOConfig{
			Endpoint: *endpoint, AccessKey: "minioadmin", SecretKey: "minioadmin", Bucket: *bucket,
			Prefix: fmt.Sprintf("run-%d-%dmb/", time.Now().UnixNano(), mb),
		})
		must(err)
		cfg := func(name string, cache bool) kv.Config {
			c := kv.Config{Store: store, DataDir: filepath.Join(dir, name), Logger: quiet, GCGrace: time.Second, OwnerID: name}
			c.CacheDir = filepath.Join(dir, "cache")
			c.CacheMaxBytes = int64(mb+512) << 20
			if !cache {
				c.CacheMaxBytes = -1
			}
			return c
		}

		// Load: 8 MiB transactions of random rows, then suspend (checkpoint).
		p, err := kv.Takeover(ctx, cfg("load", true), 1)
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
		load := time.Since(start)
		must(p.Suspend(ctx))
		fmt.Fprintf(os.Stderr, "%d MB: loaded %d rows in %v\n", mb, rows, load.Round(time.Millisecond))

		cold, files, read1 := wake(ctx, cfg("cold", false), rows)
		// Warm the cache the way a host does: a wake that downloads and keeps the files.
		_, _, _ = wake(ctx, cfg("fill", true), rows)
		warm, _, _ := wake(ctx, cfg("warm", true), rows)
		fmt.Printf("| %d MB | %d | %v | %v | %v |\n", mb, files, cold.Round(time.Millisecond), warm.Round(time.Millisecond), read1.Round(time.Microsecond))
		os.RemoveAll(filepath.Join(dir, "cache"))
	}
}

// wake takes the partition over, reads one row, and suspends it again.
func wake(ctx context.Context, cfg kv.Config, rows int) (took time.Duration, files int, firstRead time.Duration) {
	start := time.Now()
	p, err := kv.Takeover(ctx, cfg, 1)
	must(err)
	took = time.Since(start)
	var id kv.TxnID
	rand.Read(id[:])
	must(p.Begin(p.Epoch(), id, kv.BeginOptions{}))
	r := time.Now()
	vs, err := p.Get(p.Epoch(), id, [][]byte{key(rows / 2)}, kv.ReadOptions{Stmt: 1})
	must(err)
	firstRead = time.Since(r)
	if vs[0] == nil {
		must(fmt.Errorf("row %d missing after wake", rows/2))
	}
	p.Rollback(p.Epoch(), id)
	files = p.FileCount()
	must(p.Suspend(ctx))
	os.RemoveAll(cfg.DataDir)
	return took, files, firstRead
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
