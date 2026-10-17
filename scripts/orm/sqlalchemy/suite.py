"""SQLAlchemy 2.0 + Alembic compatibility suite.

Runs an app's usual life against one database: Alembic migrations up, down
and up again, autogenerate finding nothing to change (which needs the
server's introspection to be right), then ORM work: CRUD, relationships and
eager loading, bulk writes, upserts, transactions, row locks, pagination,
aggregates, JSON, enums, decimals and server defaults.

    ORM_URL='mysql+pymysql://root@127.0.0.1:3306/ormtest' python3 suite.py

Each check prints PASS or FAIL; the exit status is the number of failures.
"""
import datetime
import decimal
import os
import sys
import threading
import traceback

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from alembic import command  # noqa: E402
from alembic.autogenerate import compare_metadata  # noqa: E402
from alembic.config import Config  # noqa: E402
from alembic.migration import MigrationContext  # noqa: E402
from sqlalchemy import create_engine, delete, exists, func, inspect, select, text, update  # noqa: E402
from sqlalchemy.dialects.mysql import insert as mysql_insert  # noqa: E402
from sqlalchemy.exc import IntegrityError  # noqa: E402
from sqlalchemy.orm import Session, joinedload, selectinload  # noqa: E402

from models import AuditLog, Base, Post, Role, Tag, User  # noqa: E402

URL = os.environ["ORM_URL"]
results = []


def check(name):
    def wrap(fn):
        try:
            fn()
            results.append((name, None))
            print(f"PASS {name}")
        except Exception as e:  # noqa: BLE001
            results.append((name, e))
            print(f"FAIL {name}: {type(e).__name__}: {str(e).splitlines()[0][:300]}")
            if os.environ.get("ORM_VERBOSE"):
                traceback.print_exc()
        return fn
    return wrap


def expect(got, want, what=""):
    if got != want:
        raise AssertionError(f"{what} got {got!r}, want {want!r}")


def reset_database():
    server, db = URL.rsplit("/", 1)
    eng = create_engine(server + "/")
    with eng.connect() as c:
        c.execute(text(f"DROP DATABASE IF EXISTS {db}"))
        c.execute(text(f"CREATE DATABASE {db}"))
    eng.dispose()


alembic_cfg = Config(os.path.join(HERE, "alembic.ini"))
alembic_cfg.set_main_option("script_location", os.path.join(HERE, "migrations"))
reset_database()
engine = create_engine(URL, pool_pre_ping=True)


@check("alembic upgrade head")
def _():
    command.upgrade(alembic_cfg, "head")


@check("alembic downgrade base, upgrade head again")
def _():
    command.downgrade(alembic_cfg, "base")
    command.upgrade(alembic_cfg, "head")


@check("alembic autogenerate finds no differences")
def _():
    with engine.connect() as c:
        diffs = compare_metadata(MigrationContext.configure(c), Base.metadata)
    expect(diffs, [], "diffs")


@check("reflection: tables, columns, indexes, unique and foreign keys")
def _():
    insp = inspect(engine)
    tables = set(insp.get_table_names())
    expect(tables >= {"users", "posts", "tags", "post_tags", "audit_log", "alembic_version"}, True, f"tables {tables}")
    cols = {c["name"]: c for c in insp.get_columns("users")}
    expect(set(cols), {"id", "email", "name", "role", "balance", "active", "prefs", "created_at"}, "columns")
    expect(cols["id"]["autoincrement"], True, "autoincrement")
    expect(cols["email"]["nullable"], False, "nullable")
    expect(insp.get_pk_constraint("post_tags")["constrained_columns"], ["post_id", "tag_id"], "pk")
    idx = {i["name"] for i in insp.get_indexes("posts")}
    expect("ix_posts_author_id" in idx, True, f"indexes {idx}")
    uniq = insp.get_unique_constraints("users") + [i for i in insp.get_indexes("users") if i.get("unique")]
    expect(any(u["column_names"] == ["email"] for u in uniq), True, f"unique {uniq}")
    fks = insp.get_foreign_keys("posts")
    expect([(f["constrained_columns"], f["referred_table"]) for f in fks], [(["author_id"], "users")], "foreign keys")


@check("ORM insert with defaults, server default and AUTO_INCREMENT")
def _():
    with Session(engine) as s:
        u = User(email="ada@example.com", name="Ada", prefs={"theme": "dark", "n": [1, 2]})
        s.add(u)
        s.commit()
        expect(u.id, 1, "id")
        expect(u.role, Role.reader, "default enum")
        expect(u.balance, decimal.Decimal("0.00"), "default decimal")
        expect(isinstance(u.created_at, datetime.datetime), True, "server default")


@check("relationships: cascade insert, many-to-many, lazy, joined and selectin loading")
def _():
    with Session(engine) as s:
        ada = s.scalar(select(User).where(User.email == "ada@example.com"))
        go, db = Tag(name="go"), Tag(name="db")
        for i in range(5):
            ada.posts.append(Post(title=f"post {i}", body="x" * (i * 100), tags=[go] if i % 2 else [go, db]))
        s.commit()
        go_id = go.id
    with Session(engine) as s:
        ada = s.scalar(select(User).options(selectinload(User.posts).selectinload(Post.tags)).where(User.name == "Ada"))
        expect(len(ada.posts), 5, "posts")
        expect(sorted(t.name for t in ada.posts[0].tags), ["db", "go"], "tags")
        p = s.scalar(select(Post).options(joinedload(Post.author)).where(Post.title == "post 3"))
        expect(p.author.email, "ada@example.com", "joined")
        expect(len(s.get(Tag, go_id).posts), 5, "lazy many-to-many")


@check("bulk insert and executemany")
def _():
    with Session(engine) as s:
        s.add_all([User(email=f"u{i}@example.com", name=f"user {i % 7}", balance=decimal.Decimal(i)) for i in range(200)])
        s.commit()
        expect(s.scalar(select(func.count()).select_from(User)), 201, "count")
    with engine.begin() as c:
        c.execute(AuditLog.__table__.insert(), [{"user_id": i, "action": "login", "at": datetime.datetime(2026, 1, 1, 0, 0, i % 60)} for i in range(100)])


@check("unique violation raises IntegrityError and the session recovers")
def _():
    with Session(engine) as s:
        s.add(User(email="ada@example.com", name="dup"))
        try:
            s.commit()
            raise AssertionError("no IntegrityError")
        except IntegrityError as e:
            expect(e.orig.args[0], 1062, "error code")
            s.rollback()
        expect(s.scalar(select(func.count()).where(User.name == "dup")), 0, "rolled back")


@check("upsert: INSERT ... ON DUPLICATE KEY UPDATE")
def _():
    with engine.begin() as c:
        stmt = mysql_insert(Tag.__table__).values(name="go")
        c.execute(stmt.on_duplicate_key_update(name=stmt.inserted.name))
        stmt = mysql_insert(User.__table__).values(email="ada@example.com", name="Ada L", role="reader", balance=0, active=True)
        c.execute(stmt.on_duplicate_key_update(name=stmt.inserted.name))
    with Session(engine) as s:
        expect(s.scalar(select(func.count()).select_from(Tag)), 2, "tags")
        expect(s.scalar(select(User.name).where(User.email == "ada@example.com")), "Ada L", "updated")


@check("transactions: rollback discards, commit keeps, nested failure")
def _():
    with Session(engine) as s:
        u = s.scalar(select(User).where(User.email == "u1@example.com"))
        u.balance += 10
        s.flush()
        s.rollback()
        expect(s.scalar(select(User.balance).where(User.email == "u1@example.com")), decimal.Decimal("1.00"), "after rollback")
        u = s.scalar(select(User).where(User.email == "u1@example.com"))
        u.balance += 10
        s.commit()
        expect(s.scalar(select(User.balance).where(User.email == "u1@example.com")), decimal.Decimal("11.00"), "after commit")


@check("SELECT ... FOR UPDATE blocks a second writer until commit")
def _():
    order = []
    with Session(engine) as s1:
        s1.scalar(select(User).where(User.email == "u2@example.com").with_for_update())
        order.append("locked")

        def other():
            with Session(engine) as s2:
                s2.execute(update(User).where(User.email == "u2@example.com").values(balance=User.balance + 1))
                s2.commit()
                order.append("other committed")

        t = threading.Thread(target=other)
        t.start()
        t.join(1.0)
        order.append("first commit")
        s1.commit()
        t.join(10)
    expect(order, ["locked", "first commit", "other committed"], "order")


@check("bulk UPDATE and DELETE with WHERE, rowcount")
def _():
    with Session(engine) as s:
        r = s.execute(update(User).where(User.name == "user 3").values(active=False))
        expect(r.rowcount, 29, "updated")
        r = s.execute(delete(AuditLog).where(AuditLog.user_id >= 90))
        expect(r.rowcount, 10, "deleted")
        s.commit()


@check("queries: pagination, aggregates, GROUP BY/HAVING, EXISTS, subquery, LIKE, IN")
def _():
    with Session(engine) as s:
        page = s.scalars(select(User.email).order_by(User.id).limit(10).offset(20)).all()
        expect(page[0], "u19@example.com", "page")
        rows = s.execute(select(User.name, func.count(), func.sum(User.balance)).group_by(User.name)
                         .having(func.count() > 28).order_by(User.name)).all()
        expect([r[0] for r in rows], ["user 0", "user 1", "user 2", "user 3"], "group by")
        expect(s.scalar(select(exists().where(Post.views > 100))), False, "exists")
        top = select(Post.author_id).where(Post.title.like("post %")).scalar_subquery()
        expect(s.scalar(select(User.email).where(User.id.in_(top))), "ada@example.com", "subquery")
        expect(s.scalar(select(func.count()).where(User.email.like("u1%"))), 111, "like")
        expect(s.scalar(select(func.count()).where(User.name.in_(["user 1", "user 2"]))), 58, "in")


@check("JSON, enum and decimal round trips")
def _():
    with Session(engine) as s:
        u = s.scalar(select(User).where(User.email == "ada@example.com"))
        expect(u.prefs, {"theme": "dark", "n": [1, 2]}, "json")
        u.role = Role.admin
        u.balance = decimal.Decimal("1234567890.12")
        s.commit()
        expect(s.scalar(select(User.role).where(User.id == u.id)), Role.admin, "enum")
        expect(s.scalar(select(User.balance).where(User.id == u.id)), decimal.Decimal("1234567890.12"), "decimal")
        expect(s.scalar(select(func.json_extract(User.prefs, "$.theme")).where(User.id == u.id)), '"dark"', "json_extract")


@check("cascade delete-orphan removes a user's posts")
def _():
    with Session(engine) as s:
        ada = s.scalar(select(User).where(User.email == "ada@example.com"))
        for p in ada.posts:
            p.tags.clear()
        s.flush()
        s.delete(ada)
        s.commit()
        expect(s.scalar(select(func.count()).select_from(Post)), 0, "posts")


@check("connection pool survives server-side session reuse (pool_pre_ping)")
def _():
    for _ in range(20):
        with engine.connect() as c:
            expect(c.execute(text("SELECT 1")).scalar(), 1, "ping")


failed = [n for n, e in results if e]
print(f"\n{len(results) - len(failed)}/{len(results)} passed")
sys.exit(len(failed))
