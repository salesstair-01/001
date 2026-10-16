// Command drivers-go runs the driver-matrix scenario with go-sql-driver/mysql.
package main

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd = env("DRV_USER", "root"), env("DRV_PASSWORD", "")
	cfg.Net, cfg.Addr = "tcp", env("DRV_HOST", "127.0.0.1")+":"+env("DRV_PORT", "3306")
	cfg.ParseTime, cfg.AllowNativePasswords = true, true
	boot, err := sql.Open("mysql", cfg.FormatDSN())
	check("connect", err)
	_, err = boot.Exec("CREATE DATABASE IF NOT EXISTS " + env("DRV_DB", "app"))
	check("create database", err)
	boot.Close()
	cfg.DBName = env("DRV_DB", "app")
	db, err := sql.Open("mysql", cfg.FormatDSN())
	check("connect", err)
	defer db.Close()
	check("ping", db.Ping())

	exec := func(q string, args ...any) sql.Result {
		r, err := db.Exec(q, args...)
		check(q, err)
		return r
	}
	exec("DROP TABLE IF EXISTS drv_go")
	exec(`CREATE TABLE drv_go (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL,
		amount DECIMAL(10,2), created DATETIME, data BLOB, flag BOOL, UNIQUE KEY un (name))`)
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	r := exec("INSERT INTO drv_go (name, amount, created, data, flag) VALUES (?, ?, ?, ?, ?)",
		"héllo 🌍", "12.34", when, []byte{0, 1, 2, 255}, true)
	id, err := r.LastInsertId()
	check("LastInsertId", err)
	expect("LastInsertId", id, int64(1))
	r = exec("INSERT INTO drv_go (name, amount, created, data, flag) VALUES (?, NULL, NULL, NULL, NULL), (?, ?, ?, ?, ?)",
		"nulls", "second", "-0.50", when, []byte("x"), false)
	n, _ := r.RowsAffected()
	expect("RowsAffected", n, int64(2))

	var (
		name    string
		amount  sql.NullString
		created sql.NullTime
		data    []byte
		flag    sql.NullBool
	)
	check("select", db.QueryRow("SELECT name, amount, created, data, flag FROM drv_go WHERE id = ?", 1).
		Scan(&name, &amount, &created, &data, &flag))
	expect("utf8mb4 string", name, "héllo 🌍")
	expect("decimal", amount.String, "12.34")
	expect("datetime", created.Time.Equal(when), true)
	expect("blob", fmt.Sprint(data), fmt.Sprint([]byte{0, 1, 2, 255}))
	expect("bool", flag.Bool, true)
	check("select nulls", db.QueryRow("SELECT amount, created, data, flag FROM drv_go WHERE name = ?", "nulls").
		Scan(&amount, &created, &data, &flag))
	expect("NULLs", amount.Valid || created.Valid || data != nil || flag.Valid, false)

	tx, err := db.Begin()
	check("begin", err)
	_, err = tx.Exec("UPDATE drv_go SET amount = amount + 1 WHERE id = 1")
	check("update in txn", err)
	check("rollback", tx.Rollback())
	tx, _ = db.Begin()
	_, err = tx.Exec("UPDATE drv_go SET amount = amount + 1 WHERE id = 1")
	check("update in txn", err)
	check("commit", tx.Commit())
	check("select", db.QueryRow("SELECT amount FROM drv_go WHERE id = 1").Scan(&amount))
	expect("commit/rollback", amount.String, "13.34")

	_, err = db.Exec("INSERT INTO drv_go (name) VALUES (?)", "second")
	var me *mysql.MySQLError
	expect("duplicate key error", errors.As(err, &me) && me.Number == 1062, true)
	fmt.Println("go: PASS")
}

func check(what string, err error) {
	if err != nil {
		fmt.Printf("go: FAIL %s: %v\n", what, err)
		os.Exit(1)
	}
}

func expect[T comparable](what string, got, want T) {
	if got != want {
		fmt.Printf("go: FAIL %s: got %v, want %v\n", what, got, want)
		os.Exit(1)
	}
}
