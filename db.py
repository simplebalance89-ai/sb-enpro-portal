"""
Enpro Filtration Mastermind — Database layer.

Async SQLAlchemy + asyncpg against Render Postgres.
Tables: users, conversations.
Schema is auto-created on startup (init_db). Idempotent.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import AsyncIterator, Optional

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger(__name__)


def _normalize_db_url(url: str) -> str:
    """Render gives postgres:// or postgresql://; SQLAlchemy async wants postgresql+asyncpg://."""
    if not url:
        return url
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    if url.startswith("postgresql://") and "+asyncpg" not in url:
        url = "postgresql+asyncpg://" + url[len("postgresql://") :]
    return url


DATABASE_URL = _normalize_db_url(os.environ.get("DATABASE_URL", ""))

# Engine is None until init_db() is called with a real URL.
_engine = None
_SessionLocal: Optional[async_sessionmaker[AsyncSession]] = None


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    name = Column(String(255), nullable=False, default="")
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Conversation(Base):
    __tablename__ = "conversations"

    id = Column(BigInteger, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    role = Column(String(16), nullable=False)  # "user" | "assistant"
    content = Column(Text, nullable=False)
    # Structured products attached to this turn (assistant turns only).
    # Lets the coreference upgrade in router.py inject [PRIOR TURN PRODUCTS]
    # without having to re-parse rendered markdown out of `content`.
    products_json = Column(JSONB, nullable=True)
    # Idempotency hash: hash(user_id, role, content, minute_bucket). Lets us
    # skip duplicate writes when a client retries within ~60s.
    turn_hash = Column(String(64), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)

    __table_args__ = (
        Index("ix_conversations_user_created", "user_id", "created_at"),
    )


async def init_db() -> bool:
    """Initialize engine + create tables. Returns True if ready, False if no DATABASE_URL."""
    global _engine, _SessionLocal

    if not DATABASE_URL:
        logger.warning("DATABASE_URL not set — auth and conversation memory disabled")
        return False

    _engine = create_async_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=5,
        pool_recycle=300,
    )
    _SessionLocal = async_sessionmaker(_engine, expire_on_commit=False, class_=AsyncSession)

    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Idempotent migration for deploys made before products_json/turn_hash
        # existed. SQLAlchemy create_all only creates missing TABLES, not
        # missing columns on existing tables. Postgres-specific.
        await conn.execute(text(
            "ALTER TABLE conversations "
            "ADD COLUMN IF NOT EXISTS products_json JSONB"
        ))
        await conn.execute(text(
            "ALTER TABLE conversations "
            "ADD COLUMN IF NOT EXISTS turn_hash VARCHAR(64)"
        ))
        await conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_conversations_turn_hash "
            "ON conversations (turn_hash)"
        ))

    logger.info("Database initialized (users, conversations)")
    return True


async def close_db() -> None:
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None


def is_ready() -> bool:
    return _SessionLocal is not None


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency."""
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized")
    async with _SessionLocal() as session:
        yield session


# Convenience for non-FastAPI call sites (background tasks, scripts)
def session_factory() -> async_sessionmaker[AsyncSession]:
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized")
    return _SessionLocal
