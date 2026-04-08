"""
Enpro Filtration Mastermind — Per-user conversation memory.

Stores last 7 days of (user, assistant) turns per user in Postgres.
On every chat turn we append both messages and inject recent history into the
GPT prompt so the assistant carries context across the session/week.

Background cleanup: deletes rows older than 7 days every hour.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional, Sequence

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from db import Conversation, session_factory

logger = logging.getLogger(__name__)

RETENTION_DAYS = 7
# Cap how many turns we inject into the GPT prompt per request to bound token cost.
# 30 messages = ~15 user/assistant pairs. Plenty for "what we were just talking about"
# without blowing the context window or burning tokens.
MAX_HISTORY_MESSAGES = 30
# Hard cap on how much we store per single message — protects DB from runaway responses.
MAX_CONTENT_CHARS = 8000


def _truncate(content: str) -> str:
    if len(content) <= MAX_CONTENT_CHARS:
        return content
    return content[:MAX_CONTENT_CHARS] + "…[truncated]"


def _turn_hash(user_id: int, role: str, content: str) -> str:
    """
    Idempotency key bucketed by minute. Lets us detect retries: if the same
    user posts the same message twice within ~60s (network retry, duplicate
    submit), the hash collides and we skip the second insert. Different
    minutes get different hashes, so legitimate repeated questions still
    persist.
    """
    bucket = int(datetime.now(timezone.utc).timestamp() // 60)
    raw = f"{user_id}|{role}|{content}|{bucket}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


async def _exists_recent(session: AsyncSession, turn_hash: str) -> bool:
    result = await session.execute(
        select(Conversation.id).where(Conversation.turn_hash == turn_hash).limit(1)
    )
    return result.scalar_one_or_none() is not None


async def append_message(
    session: AsyncSession,
    user_id: int,
    role: str,
    content: str,
    products: Optional[List[Any]] = None,
) -> None:
    """Append a single message. Caller commits.

    Idempotent within the current minute via SHA-256 turn_hash dedupe.
    """
    if role not in ("user", "assistant"):
        raise ValueError(f"invalid role: {role}")
    if not content:
        return
    truncated = _truncate(content)
    th = _turn_hash(user_id, role, truncated)
    if await _exists_recent(session, th):
        logger.info(f"conversation_memory: dedup skip ({role}, user={user_id})")
        return
    session.add(
        Conversation(
            user_id=user_id,
            role=role,
            content=truncated,
            products_json=products if products else None,
            turn_hash=th,
        )
    )


async def append_turn(
    session: AsyncSession,
    user_id: int,
    user_message: str,
    assistant_message: str,
    products: Optional[List[Any]] = None,
) -> None:
    """Append a user+assistant pair and commit.

    `products`, if provided, is the structured products list returned by the
    handler — attached to the assistant turn so future coreference upgrades
    ("compare those two") can inject the real product records back into the
    GPT prompt instead of relying on rendered markdown alone.

    Idempotency: append_message dedups via turn_hash read; UNIQUE INDEX
    on turn_hash catches the race when two concurrent retries both pass
    the read. We catch IntegrityError and silently roll back — losing
    the duplicate row is the entire point.
    """
    await append_message(session, user_id, "user", user_message)
    await append_message(session, user_id, "assistant", assistant_message, products=products)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        logger.info(f"conversation_memory: race-dedup on commit (user={user_id})")


async def get_recent_history(
    session: AsyncSession,
    user_id: int,
    max_messages: int = MAX_HISTORY_MESSAGES,
) -> List[dict]:
    """
    Return recent history as OpenAI chat messages, oldest first, capped at
    `max_messages` and bounded to RETENTION_DAYS days. Each dict carries
    `role`, `content`, and (when present) `products` so the router can
    re-inject structured prior turn products on coreference.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    stmt = (
        select(Conversation)
        .where(Conversation.user_id == user_id, Conversation.created_at >= cutoff)
        .order_by(Conversation.created_at.desc())
        .limit(max_messages)
    )
    result = await session.execute(stmt)
    rows: Sequence[Conversation] = result.scalars().all()
    # Reverse to chronological order for the prompt.
    out: List[dict] = []
    for r in reversed(rows):
        msg: dict = {"role": r.role, "content": r.content}
        if r.products_json:
            msg["products"] = r.products_json
        out.append(msg)
    return out


async def clear_user_history(session: AsyncSession, user_id: int) -> int:
    """Delete all history for a user. Returns row count."""
    result = await session.execute(
        delete(Conversation).where(Conversation.user_id == user_id)
    )
    await session.commit()
    return result.rowcount or 0


async def purge_expired() -> int:
    """Delete conversations older than RETENTION_DAYS. Returns row count."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    factory = session_factory()
    async with factory() as session:
        result = await session.execute(
            delete(Conversation).where(Conversation.created_at < cutoff)
        )
        await session.commit()
        return result.rowcount or 0


async def cleanup_loop(interval_seconds: int = 3600) -> None:
    """Background task: hourly purge of expired conversation rows."""
    while True:
        try:
            deleted = await purge_expired()
            if deleted:
                logger.info(f"conversation_memory: purged {deleted} expired rows")
        except Exception as e:
            logger.error(f"conversation_memory cleanup failed: {e}")
        await asyncio.sleep(interval_seconds)
