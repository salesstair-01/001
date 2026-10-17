"""add users.prefs and audit_log"""
import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"


def upgrade():
    op.add_column("users", sa.Column("prefs", sa.JSON))
    op.create_table(
        "audit_log",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("user_id", sa.Integer),
        sa.Column("action", sa.String(40), nullable=False),
        sa.Column("at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("user_id", "action", "at", name="uq_audit_user_action_at"),
    )
    op.create_index("ix_audit_log_user_id", "audit_log", ["user_id"])


def downgrade():
    op.drop_table("audit_log")
    op.drop_column("users", "prefs")
