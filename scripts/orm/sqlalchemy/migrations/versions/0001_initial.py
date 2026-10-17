"""initial schema"""
import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None


def upgrade():
    op.create_table(
        "users",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("email", sa.String(120), nullable=False),
        sa.Column("name", sa.String(80), nullable=False),
        sa.Column("role", sa.Enum("reader", "writer", "admin", name="role"), nullable=False),
        sa.Column("balance", sa.Numeric(12, 2), nullable=False),
        sa.Column("active", sa.Boolean, nullable=False),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("email"),
    )
    op.create_index("ix_users_name", "users", ["name"])
    op.create_table(
        "posts",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("author_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("body", sa.Text, nullable=False),
        sa.Column("views", sa.Integer, nullable=False),
        sa.Column("published_at", sa.DateTime),
    )
    op.create_index("ix_posts_author_id", "posts", ["author_id"])
    op.create_table(
        "tags",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(40), nullable=False),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "post_tags",
        sa.Column("post_id", sa.Integer, sa.ForeignKey("posts.id"), primary_key=True),
        sa.Column("tag_id", sa.Integer, sa.ForeignKey("tags.id"), primary_key=True),
    )


def downgrade():
    op.drop_table("post_tags")
    op.drop_table("tags")
    op.drop_table("posts")  # drops its indexes, which its foreign key needs until then
    op.drop_table("users")
