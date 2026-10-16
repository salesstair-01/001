# Driver matrix

The same scenario in four client stacks, run against any MySQL endpoint
(`sqld` directly, or `proxyd`):

| driver | protocol features exercised |
|---|---|
| Go `go-sql-driver/mysql` | binary-protocol prepared statements, `parseTime` |
| Python PyMySQL | text protocol, client-side parameter escaping |
| Node `mysql2` | `execute()` binary prepared statements, `query()` text |
| Java Connector/J 8.4 | `useServerPrepStmts=true`, `getGeneratedKeys` |

Each one creates a table `drv_<driver>`, then checks: parameterised inserts
of every column type (INT, VARCHAR with 4-byte UTF-8, DECIMAL, DATETIME,
BLOB, BOOL, NULL), `LAST_INSERT_ID`/generated keys, rows affected, reading
the values back with their types, commit and rollback, and error 1062 on a
duplicate key.

```
scripts/drivers/run.sh                       # all drivers, 127.0.0.1:3306 root
DRV_HOST=127.0.0.1 DRV_PORT=3307 DRV_USER='app@acme' DRV_PASSWORD=s3cret scripts/drivers/run.sh go python
```

`run.sh` fetches PyMySQL, mysql2 and Connector/J into `scripts/drivers/.deps`
on first use (pip, npm, Maven).
