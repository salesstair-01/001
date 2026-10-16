# sqllogictest

A subset of the [sqllogictest](https://www.sqlite.org/sqllogictest/) suite, in
the copy kept at github.com/dolthub/sqllogictest, run through its Go MySQL
harness against an in-process KV node and query engine.

```sh
make slt    # fetches the files in files.txt, runs them 4 at a time, writes results.md
go run ./cmd/slt -dsn 'root@tcp(127.0.0.1:3306)/slt' scripts/slt/test/select1.test   # against a running sqld
```

- `files.txt` — the files run, pinned to the commit in `fetch.sh`.
- `results.md` — the last run, per file. Track the pass rate by committing it.
- `min-pass-rate` — CI fails below this overall rate.

Status: no record returns a wrong result. The only misses are in
`select4.test`: for a 7-way join (line 31487) go-mysql-server's planner nests
the whole cross product of five tables under each row of a sixth, about 39M
row combinations, so the record times out (60 s) and the rest of the file
does not run.

Bugs this suite found and fixed: locking reads through non-unique indexes
returned no rows; overlapping key spans returned rows twice; `ORDER BY ...
DESC` over an index came back ascending; views were not stored; nested joins
re-read the same spans once per outer row (now served from a per-statement
read cache).
