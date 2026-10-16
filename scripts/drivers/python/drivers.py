"""Driver-matrix scenario with PyMySQL (text protocol)."""
import datetime
import decimal
import os
import sys

import pymysql


def fail(what, got=None, want=None):
    print(f"python: FAIL {what}: got {got!r}, want {want!r}")
    sys.exit(1)


def expect(what, got, want):
    if got != want:
        fail(what, got, want)


conn = pymysql.connect(
    host=os.environ.get("DRV_HOST", "127.0.0.1"), port=int(os.environ.get("DRV_PORT", "3306")),
    user=os.environ.get("DRV_USER", "root"), password=os.environ.get("DRV_PASSWORD", ""),
    charset="utf8mb4", autocommit=True)
cur = conn.cursor()
cur.execute("CREATE DATABASE IF NOT EXISTS " + os.environ.get("DRV_DB", "app"))
conn.select_db(os.environ.get("DRV_DB", "app"))
cur.execute("DROP TABLE IF EXISTS drv_python")
cur.execute("""CREATE TABLE drv_python (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL,
    amount DECIMAL(10,2), created DATETIME, data BLOB, flag BOOL, UNIQUE KEY un (name))""")
when = datetime.datetime(2026, 1, 2, 3, 4, 5)
cur.execute("INSERT INTO drv_python (name, amount, created, data, flag) VALUES (%s, %s, %s, %s, %s)",
            ("héllo 🌍", decimal.Decimal("12.34"), when, b"\x00\x01\x02\xff", True))
expect("lastrowid", cur.lastrowid, 1)
n = cur.execute("INSERT INTO drv_python (name, amount, created, data, flag) VALUES (%s, NULL, NULL, NULL, NULL), (%s, %s, %s, %s, %s)",
                ("nulls", "second", decimal.Decimal("-0.50"), when, b"x", False))
expect("rows affected", n, 2)

cur.execute("SELECT name, amount, created, data, flag FROM drv_python WHERE id = %s", (1,))
name, amount, created, data, flag = cur.fetchone()
expect("utf8mb4 string", name, "héllo 🌍")
expect("decimal", amount, decimal.Decimal("12.34"))
expect("datetime", created, when)
expect("blob", bytes(data), b"\x00\x01\x02\xff")
expect("bool", flag, 1)
cur.execute("SELECT amount, created, data, flag FROM drv_python WHERE name = %s", ("nulls",))
expect("NULLs", cur.fetchone(), (None, None, None, None))

conn.begin()
cur.execute("UPDATE drv_python SET amount = amount + 1 WHERE id = 1")
conn.rollback()
conn.begin()
cur.execute("UPDATE drv_python SET amount = amount + 1 WHERE id = 1")
conn.commit()
cur.execute("SELECT amount FROM drv_python WHERE id = 1")
expect("commit/rollback", cur.fetchone()[0], decimal.Decimal("13.34"))

try:
    cur.execute("INSERT INTO drv_python (name) VALUES (%s)", ("second",))
    fail("duplicate key error", "no error", 1062)
except pymysql.err.IntegrityError as e:
    expect("duplicate key error", e.args[0], 1062)
print("python: PASS")
