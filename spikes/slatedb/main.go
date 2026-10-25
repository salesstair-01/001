// Command slatespike tests whether SlateDB (v0.17.0, through its official
// Go binding) could replace the owner's storage layer, against Ceph RGW.
// See README.md for how to build the Rust library and run each check.
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	slatedb "slatedb.io/slatedb-go/uniffi"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: slatespike smoke|fence|writer|crash|bench|ffi|density [flags]")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "smoke":
		smoke()
	case "fence":
		fence()
	case "writer":
		writer(args)
	case "crash":
		crash(args)
	case "bench":
		bench(args)
	case "ffi":
		ffi()
	case "density":
		density(args)
	default:
		fmt.Fprintln(os.Stderr, "unknown command", cmd)
		os.Exit(2)
	}
	_ = args
}

// smoke: open, write, read, scan, close, reopen from the object store.
func smoke() {
	ctx := context.Background()
	c := s3FromEnv()
	must(c.ensureBucket(ctx))
	store, err := c.objectStore("")
	must(err)
	defer store.Destroy()
	path := fmt.Sprintf("smoke-%d", time.Now().UnixNano())

	t := time.Now()
	db, err := openDB(store, path, nil)
	must(err)
	fmt.Println("open (new db):", since(t))
	for i := 0; i < 100; i++ {
		h, err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%d", i)))
		must(err)
		if i == 99 {
			t = time.Now()
			must(h.AwaitDurable())
			fmt.Println("await durable (last of 100 puts):", since(t))
		}
		h.Destroy()
	}
	v, err := db.Get([]byte("k042"))
	must(err)
	if v == nil || !bytes.Equal(*v, []byte("v42")) {
		must(fmt.Errorf("get k042 = %v", v))
	}
	start, end := []byte("k010"), []byte("k020")
	it, err := db.Scan(slatedb.KeyRange{Start: &start, StartInclusive: true, End: &end})
	must(err)
	n := 0
	for {
		kv, err := it.Next()
		must(err)
		if kv == nil {
			break
		}
		n++
	}
	it.Destroy()
	if n != 10 {
		must(fmt.Errorf("scan returned %d rows", n))
	}
	must(db.Shutdown())
	db.Destroy()

	t = time.Now()
	db, err = openDB(store, path, nil)
	must(err)
	fmt.Println("reopen:", since(t))
	v, err = db.Get([]byte("k099"))
	must(err)
	if v == nil || !bytes.Equal(*v, []byte("v99")) {
		must(fmt.Errorf("after reopen k099 = %v", v))
	}
	must(db.Shutdown())
	db.Destroy()
	fmt.Println("smoke: ok")
}
