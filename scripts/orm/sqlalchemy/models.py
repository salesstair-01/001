"""A small blog app, written the way SQLAlchemy apps usually are."""
import datetime
import decimal
import enum

from sqlalchemy import (JSON, Boolean, DateTime, Enum, ForeignKey, Integer, Numeric, String, Text,
                        UniqueConstraint, func)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Role(enum.Enum):
    reader = "reader"
    writer = "writer"
    admin = "admin"


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    role: Mapped[Role] = mapped_column(Enum(Role), nullable=False, default=Role.reader)
    balance: Mapped[decimal.Decimal] = mapped_column(Numeric(12, 2), nullable=False, default=decimal.Decimal("0"))
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    prefs: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    posts: Mapped[list["Post"]] = relationship(back_populates="author", cascade="all, delete-orphan")


class Post(Base):
    __tablename__ = "posts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    author_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    views: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    published_at: Mapped[datetime.datetime | None] = mapped_column(DateTime)
    author: Mapped[User] = relationship(back_populates="posts")
    tags: Mapped[list["Tag"]] = relationship(secondary="post_tags", back_populates="posts")


class Tag(Base):
    __tablename__ = "tags"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(40), nullable=False, unique=True)
    posts: Mapped[list[Post]] = relationship(secondary="post_tags", back_populates="tags")


class PostTag(Base):
    __tablename__ = "post_tags"
    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id"), primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("tags.id"), primary_key=True)


class AuditLog(Base):
    """Added by the second migration."""
    __tablename__ = "audit_log"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int | None] = mapped_column(Integer, index=True)
    action: Mapped[str] = mapped_column(String(40), nullable=False)
    at: Mapped[datetime.datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    __table_args__ = (UniqueConstraint("user_id", "action", "at", name="uq_audit_user_action_at"),)
