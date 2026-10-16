package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/salesstair-01/001/internal/metrics"
)

// 100 tenants share one KV node and one query engine through the proxy, and
// none sees another's data or schema (slice 9).
func TestHundredTenants(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	const n = 100
	var tenants []Tenant
	for i := 0; i < n; i++ {
		tenants = append(tenants, Tenant{Name: fmt.Sprintf("t%03d", i), Partition: uint64(1000 + i), Users: map[string]string{"app": "s3cret"}})
	}
	wakesBefore := metricValue(t, `s2z_dir_wakes_total{reason="first_use"}`)
	s := newStack(t, stackOpts{sharedKV: true, warm: 1, backendIdle: time.Minute, engineIdle: time.Minute, tenants: tenants})
	waitFor(t, "a warm engine", func() bool { _, q := s.live(); return q == 1 })

	dbs := make([]*sql.DB, n)
	for i := range dbs {
		dbs[i] = s.db(t, "app@"+tenants[i].Name, "")
		dbs[i].SetMaxOpenConns(2)
	}
	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, n)
	sem := make(chan struct{}, 25)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			db, name := dbs[i], tenants[i].Name
			for _, q := range []string{
				"CREATE DATABASE app",
				"CREATE TABLE app.t (id INT PRIMARY KEY, owner VARCHAR(10), KEY io (owner))",
				fmt.Sprintf("CREATE TABLE app.only_%s (x INT)", name),
			} {
				if _, err := db.Exec(q); err != nil {
					errs <- fmt.Errorf("%s: %s: %w", name, q, err)
					return
				}
			}
			for j := 0; j < 10; j++ {
				if _, err := db.Exec("INSERT INTO app.t VALUES (?, ?)", j, name); err != nil {
					errs <- fmt.Errorf("%s: insert: %w", name, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("%d tenants created and loaded in %v", n, time.Since(start).Round(time.Millisecond))

	for i, db := range dbs {
		name := tenants[i].Name
		ctx := context.Background()
		if got := scalar(t, db, "SELECT COUNT(*) FROM app.t"); got != "10" {
			t.Fatalf("%s: %s rows", name, got)
		}
		if got := scalar(t, db, "SELECT COUNT(*) FROM app.t WHERE owner <> ?", name); got != "0" {
			t.Fatalf("%s sees %s rows of other tenants", name, got)
		}
		var tables []string
		rows, err := db.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'app' ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var tn string
			_ = rows.Scan(&tn)
			tables = append(tables, strings.ToLower(tn))
		}
		rows.Close()
		if want := "only_" + name + ",t"; strings.Join(tables, ",") != want {
			t.Fatalf("%s sees tables %v, want %s", name, tables, want)
		}
	}
	if k, q := s.live(); k != 1 || q != 1 {
		t.Fatalf("want one KV node and one query engine, got %d and %d", k, q)
	}
	if got := metricValue(t, `s2z_dir_wakes_total{reason="first_use"}`) - wakesBefore; got != n {
		t.Fatalf("first-use wakes: %v", got)
	}
	text := metrics.Default.Text()
	for _, want := range []string{`s2z_kv_puts_total{partition="1000"}`, `s2z_kv_puts_total{partition="1099"}`,
		`s2z_proxy_queries_total{tenant="t042"}`, "s2z_kv_commit_seconds_count"} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics lack %s", want)
		}
	}
}

func metricValue(t *testing.T, series string) float64 {
	for _, line := range strings.Split(metrics.Default.Text(), "\n") {
		if strings.HasPrefix(line, series+" ") {
			var v float64
			fmt.Sscan(strings.TrimPrefix(line, series+" "), &v)
			return v
		}
	}
	return 0
}
