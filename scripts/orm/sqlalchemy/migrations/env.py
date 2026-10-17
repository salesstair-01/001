import os
import sys

from alembic import context
from sqlalchemy import create_engine

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models import Base  # noqa: E402

target_metadata = Base.metadata


def run_migrations_online():
    engine = create_engine(os.environ["ORM_URL"])
    with engine.connect() as conn:
        context.configure(connection=conn, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
