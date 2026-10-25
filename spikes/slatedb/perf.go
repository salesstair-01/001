package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

// ffi measures what each call from Go into SlateDB costs, on an in-memory
// store so that no I/O is timed: a cached point read, and a scan row by row
// (Next) and in batches (NextBatch). The owner's scans call SeekGE and Next
// many times per query, so this is the overhead Pebble does not have.
func ffi() {
	store, err := slatedb.ObjectStoreResolve("memory:///")
	must(err)
	defer store.Destroy()
	db, err := openDB(store, "ffi", nil)
	must(err)
	const n = 20000
	b := slatedb.NewWriteBatch()
	for i := 0; i < n; i++ {
		must(b.Put(benchKey(i), make([]byte, 100)))
	}
	h, err := db.Write(b)
	must(err)
	must(h.AwaitDurable())

	k := benchKey(n / 2)
	for i := 0; i < 1000; i++ { // warm
		_, _ = db.Get(k)
	}
	const gets = 100000
	t := time.Now()
	for i := 0; i < gets; i++ {
		if _, err := db.Get(k); err != nil {
			must(err)
		}
	}
	perGet := time.Since(t) / gets

	scan := func(batch uint32) time.Duration {
		lo := benchKey(0)
		it, err := db.Scan(slatedb.KeyRange{Start: &lo, StartInclusive: true})
		must(err)
		defer it.Destroy()
		rows := 0
		t := time.Now()
		for rows < n {
			if batch == 0 {
				kv, err := it.Next()
				must(err)
				if kv == nil {
					break
				}
				rows++
				continue
			}
			kvs, err := it.NextBatch(batch)
			must(err)
			if len(kvs) == 0 {
				break
			}
			rows += len(kvs)
		}
		return time.Since(t) / time.Duration(rows)
	}
	scan(0) // warm
	perNext, perBatched := scan(0), scan(1000)
	must(db.Shutdown())
	db.Destroy()
	fmt.Printf("cached Get: %v per call\nscan with Next: %v per row\nscan with NextBatch(1000): %v per row\n", perGet, perNext, perBatched)
}

// density opens many databases in one process, as one node holding many
// tenants would, and reports memory and threads, with each database's own
// cache and with one cache shared by all.
func density(args []string) {
	fs := flag.NewFlagSet("density", flag.ExitOnError)
	n := fs.Int("n", 100, "databases")
	shared := fs.Bool("shared-cache", false, "one cache for all databases")
	must(fs.Parse(args))
	ctx := context.Background()
	c := s3FromEnv()
	must(c.ensureBucket(ctx))
	store, err := c.objectStore("")
	must(err)
	defer store.Destroy()
	var cache *slatedb.DbCache
	if *shared {
		cache, err = slatedb.DbCacheNewFoyerCache(slatedb.FoyerCacheOptions{MaxCapacity: 256 << 20, Shards: 8})
		must(err)
	}
	base := fmt.Sprintf("density-%d", time.Now().UnixNano())
	rss0, th0 := rssMB(), threads()
	var dbs []*slatedb.Db
	t := time.Now()
	for i := 0; i < *n; i++ {
		b := slatedb.NewDbBuilder(fmt.Sprintf("%s/t%03d", base, i), store)
		if cache != nil {
			must(b.WithDbCache(cache, uint64(i)))
		}
		db, err := b.Build()
		must(err)
		b.Destroy()
		putDurable(db, "k")
		dbs = append(dbs, db)
	}
	opened := time.Since(t)
	runtime.GC()
	time.Sleep(2 * time.Second)
	rss1, th1 := rssMB(), threads()
	for _, db := range dbs {
		_ = db.Shutdown()
		db.Destroy()
	}
	fmt.Printf("%d databases (shared cache %v): opened in %v; RSS %d -> %d MB (%.1f MB each); threads %d -> %d\n",
		*n, *shared, opened.Round(time.Millisecond), rss0, rss1, float64(rss1-rss0)/float64(*n), th0, th1)
	must(deletePrefix(ctx, c, base+"/"))
}
