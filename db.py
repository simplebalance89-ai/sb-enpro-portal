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
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
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
    # Customer Intelligence layer (additive, optional). When set, the FM
    # will surface this user's owned customers, their order history, top
    # parts, and active quotes when relevant. NULL = generic salesperson
    # mode (V2.11 catalog-only experience). The value is a P21 salesrep
    # rep_id from PO Portal Salespeople.csv (e.g. "1042" or "MOREC00").
    rep_id = Column(String(20), nullable=True, index=True)


# ---------------------------------------------------------------------------
# Customer Intelligence tables (per-rep partitioned)
# ---------------------------------------------------------------------------
# Every table below has a composite primary key starting with `rep_id`,
# so any per-rep query is an index scan and there is NO bypass path —
# the WHERE rep_id = ? clause is structural, not policy.
# Same customer can appear under multiple rep_ids if multiple reps have
# ever taken an order for them; that's correct.


class CustomerMaster(Base):
    __tablename__ = "customer_master"

    rep_id = Column(String(20), nullable=False)
    customer_id = Column(Integer, nullable=False)

    customer_name = Column(String(255), nullable=False, default="")
    legal_name = Column(String(255), nullable=True)
    credit_status = Column(String(20), nullable=True)
    credit_limit = Column(Numeric(14, 2), nullable=True)
    terms = Column(String(50), nullable=True)
    salesrep_owner = Column(String(20), nullable=True)  # the official P21 owner if different from rep_id
    mail_city = Column(String(80), nullable=True)
    mail_state = Column(String(40), nullable=True)
    central_phone = Column(String(50), nullable=True)
    email_address = Column(String(255), nullable=True)
    national_account = Column(String(2), nullable=True)
    total_so_count = Column(Integer, nullable=True, default=0)
    last_order_date = Column(Date, nullable=True)
    sfdc_account_id = Column(String(50), nullable=True)
    refreshed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("rep_id", "customer_id", name="pk_customer_master"),
        Index("ix_customer_master_name", "rep_id", "customer_name"),
    )


class CustomerTopPart(Base):
    __tablename__ = "customer_top_parts"

    rep_id = Column(String(20), nullable=False)
    customer_id = Column(Integer, nullable=False)
    inv_mast_uid = Column(BigInteger, nullable=False)

    customer_part_number = Column(String(80), nullable=True)
    part_description = Column(Text, nullable=True)
    total_qty = Column(Numeric(14, 2), nullable=True)
    total_extended_price = Column(Numeric(14, 2), nullable=True)
    order_count = Column(Integer, nullable=True)
    last_ordered_date = Column(Date, nullable=True)
    refreshed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("rep_id", "customer_id", "inv_mast_uid", name="pk_customer_top_parts"),
        Index("ix_customer_top_parts_lookup", "rep_id", "customer_id"),
    )


class CustomerOrder(Base):
    __tablename__ = "customer_orders"

    rep_id = Column(String(20), nullable=False)
    customer_id = Column(Integer, nullable=False)
    order_no = Column(String(40), nullable=False)

    order_date = Column(Date, nullable=True)
    po_no = Column(String(80), nullable=True)
    extended_price = Column(Numeric(14, 2), nullable=True)
    ship2_city = Column(String(80), nullable=True)
    ship2_state = Column(String(40), nullable=True)
    line_count = Column(Integer, nullable=True)
    completed = Column(String(2), nullable=True)
    refreshed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("rep_id", "customer_id", "order_no", name="pk_customer_orders"),
        Index("ix_customer_orders_recent", "rep_id", "customer_id", "order_date"),
    )


class CustomerQuote(Base):
    __tablename__ = "customer_quotes"

    rep_id = Column(String(20), nullable=False)
    customer_id = Column(Integer, nullable=True)  # nullable — fuzzy match may fail
    quote_number = Column(String(40), nullable=False)

    quote_name = Column(Text, nullable=True)
    status = Column(String(40), nullable=True)
    customer_name_raw = Column(String(255), nullable=True)  # original free-text for diagnostics
    contact_name = Column(String(255), nullable=True)
    extended_price = Column(Numeric(14, 2), nullable=True)
    freight_terms = Column(String(80), nullable=True)
    payment_terms = Column(String(80), nullable=True)
    est_completion = Column(String(40), nullable=True)
    created_date = Column(Date, nullable=True)
    refreshed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("rep_id", "quote_number", name="pk_customer_quotes"),
        Index("ix_customer_quotes_lookup", "rep_id", "customer_id"),
        Index("ix_customer_quotes_status", "rep_id", "status"),
    )


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
    # skip duplicate writes when a client retries within ~60s. UNIQUE so the
    # database enforces dedup even when the application's read-then-write
    # check races (two concurrent retries both pass the lookup, both write,
    # the loser gets IntegrityError and is silently dropped).
    turn_hash = Column(String(64), nullable=True, unique=True, index=True)
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
        # UNIQUE constraint enforces idempotent writes at the DB layer.
        # Use a unique index (instead of ALTER TABLE ADD CONSTRAINT) so
        # IF NOT EXISTS works on every Postgres version Render runs.
        await conn.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_conversations_turn_hash "
            "ON conversations (turn_hash)"
        ))
        # Customer Intelligence migration — adds users.rep_id if missing
        # so deploys made before this column existed pick it up.
        await conn.execute(text(
            "ALTER TABLE users "
            "ADD COLUMN IF NOT EXISTS rep_id VARCHAR(20)"
        ))
        await conn.execute(text(
            "CREATE INDEX IF NOT EXISTS ix_users_rep_id "
            "ON users (rep_id)"
        ))

    logger.info("Database initialized (users, conversations, customer_intel)")
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
