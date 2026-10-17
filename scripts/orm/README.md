# ORM compatibility suites

An application's whole life through a real ORM, run against any MySQL
endpoint (`sqld` directly, or `proxyd`), and kept passing on MySQL 8.0 too.

## SQLAlchemy 2.0 + Alembic (`sqlalchemy/`)

A small blog app (`models.py`: users, posts, tags, a many-to-many link,
foreign keys with cascades, enums, decimals, JSON, server defaults) and two
Alembic migrations. `suite.py` checks:

- `alembic upgrade head`, `downgrade base`, `upgrade head` again;
- autogenerate comparing the models with the live schema finds nothing to
  change, and reflection reports the tables, columns, indexes, unique and
  foreign keys;
- ORM inserts with Python and server defaults and AUTO_INCREMENT ids;
  relationships with lazy, joined and selectin loading; bulk inserts;
  `IntegrityError` on duplicates; `INSERT ... ON DUPLICATE KEY UPDATE`;
  rollback and commit; `with_for_update()` blocking a second writer; bulk
  UPDATE/DELETE row counts; pagination, aggregates, `GROUP BY ... HAVING`,
  `EXISTS`, subqueries, `LIKE`, `IN`; JSON, enum and decimal round trips;
  delete-orphan cascades.

```
scripts/orm/run.sh                                            # root@127.0.0.1:3306
ORM_PORT=3307 ORM_USER='app@acme' ORM_PASSWORD=s3cret scripts/orm/run.sh   # through proxyd
```

Status: 16/16 against `sqld`, through `proxyd`, and on MySQL 8.0.46.

What it found and fixed: column defaults that are expressions
(`DEFAULT now()`, `CURRENT_TIMESTAMP`, `ON UPDATE`, generated columns) did
not survive a reload; foreign keys were refused (now stored and enforced, with
shared locks on parent rows); and the handshake did not announce autocommit,
so PyMySQL never sent `SET autocommit = 0` and every statement of a
SQLAlchemy transaction committed on its own.
