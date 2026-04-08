"""
Enpro Filtration Mastermind — Per-user conversation memory.

Stores last 7 days of (user, assistant) turns per user in Postgres.
On every chat turn we append both messages and inject recent history into the
GPT prompt so the assistant carries context across the session/week.

Background cleanup: deletes rows older than 7 days every hour.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Sequence

from sqlalchemy import delete, select
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


async def append_message(
    session: AsyncSession,
    user_id: int,
    role: str,
    content: str,
) -> None:
    """Append a single message. Caller commits."""
    if role not in ("user", "assistant"):
        raise ValueError(f"invalid role: {role}")
    if not content:
        return
    session.add(
        Conversation(
            user_id=user_id,
            role=role,
            content=_truncate(content),
        )
    )


async def append_turn(
    session: AsyncSession,
    user_id: int,
    user_message: str,
    assistant_message: str,
) -> None:
    """Append a user+assistant pair and commit."""
    await append_message(session, user_id, "user", user_message)
    await append_message(session, user_id, "assistant", assistant_message)
    await session.commit()


async def get_recent_history(
    session: AsyncSession,
    user_id: int,
    max_messages: int = MAX_HISTORY_MESSAGES,
) -> List[dict]:
    """
    Return recent history as OpenAI chat messages, oldest first, capped at
    `max_messages` and bounded to RETENTION_DAYS days.
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
    return [{"role": r.role, "content": r.content} for r in reversed(rows)]


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
