"""
Enpro Filtration Mastermind — Authentication.

Email + password login, bcrypt hashing, signed session cookie (itsdangerous, 7-day).
Designed for the pilot: small, hand-seeded user list, no self-signup.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import bcrypt
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db import User, get_session, is_ready as db_ready

logger = logging.getLogger(__name__)

SESSION_COOKIE_NAME = "fm_session"
SESSION_MAX_AGE_SECONDS = 7 * 24 * 60 * 60  # 7 days

# Pilot pin — shared across all seeded users. Override via env GLOBAL_PIN.
GLOBAL_PIN = os.environ.get("GLOBAL_PIN", "0000")
# Pilot user list — auto-seeded on startup if missing.
PILOT_USERS = [
    {"email": "peter.wilson@conveyance365.com", "name": "Peter Wilson"},
    {"email": "andrew@conveyance365.com", "name": "Andrew Taylor"},
    {"email": "grant_cook@enproinc.com", "name": "Grant Cook"},
    {"email": "john_burnett@enproinc.com", "name": "John Burnett"},
]

_SECRET = os.environ.get("SESSION_SECRET", "")
_serializer: Optional[URLSafeTimedSerializer] = None


def _get_serializer() -> URLSafeTimedSerializer:
    global _serializer
    if _serializer is None:
        if not _SECRET:
            raise RuntimeError("SESSION_SECRET env var not set")
        _serializer = URLSafeTimedSerializer(_SECRET, salt="fm-session-v1")
    return _serializer


# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Session cookie
# ---------------------------------------------------------------------------

def issue_session(user_id: int) -> str:
    return _get_serializer().dumps({"uid": user_id, "iat": int(datetime.utcnow().timestamp())})


def read_session(token: str) -> Optional[int]:
    try:
        data = _get_serializer().loads(token, max_age=SESSION_MAX_AGE_SECONDS)
        return int(data.get("uid"))
    except (BadSignature, SignatureExpired, ValueError, TypeError):
        return None


def set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------

async def get_current_user(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> User:
    """FastAPI dependency: returns the logged-in User or raises 401."""
    if not db_ready():
        raise HTTPException(status_code=503, detail="Auth not configured")
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    user_id = read_session(token)
    if user_id is None:
        raise HTTPException(status_code=401, detail="Session expired")
    user = await session.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    return user


async def get_current_user_optional(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> Optional[User]:
    """Soft variant — returns None instead of raising. For endpoints that should
    work with or without auth during the pilot rollout."""
    if not db_ready():
        return None
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    user_id = read_session(token)
    if user_id is None:
        return None
    return await session.get(User, user_id)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/auth", tags=["auth"])


class LoginRequest(BaseModel):
    user_id: int
    pin: str = Field(min_length=1, max_length=20)


class LoginResponse(BaseModel):
    id: int
    email: str
    name: str


class UserOption(BaseModel):
    id: int
    name: str


@router.get("/users", response_model=list[UserOption])
async def list_users(session: AsyncSession = Depends(get_session)):
    """Public — pilot user dropdown for login screen."""
    if not db_ready():
        raise HTTPException(status_code=503, detail="Auth not configured")
    result = await session.execute(select(User).order_by(User.id))
    return [UserOption(id=u.id, name=u.name) for u in result.scalars().all()]


@router.post("/login", response_model=LoginResponse)
async def login(
    req: LoginRequest,
    response: Response,
    session: AsyncSession = Depends(get_session),
):
    if not db_ready():
        raise HTTPException(status_code=503, detail="Auth not configured")

    user = await session.get(User, req.user_id)
    if user is None or req.pin != GLOBAL_PIN:
        raise HTTPException(status_code=401, detail="Invalid PIN")

    token = issue_session(user.id)
    set_session_cookie(response, token)
    return LoginResponse(id=user.id, email=user.email, name=user.name)


async def seed_pilot_users(session: AsyncSession) -> int:
    """Insert any pilot users that don't already exist. Returns count inserted."""
    inserted = 0
    pin_hash = hash_password(GLOBAL_PIN)
    for u in PILOT_USERS:
        existing = (await session.execute(
            select(User).where(User.email == u["email"])
        )).scalar_one_or_none()
        if existing is None:
            session.add(User(
                email=u["email"],
                name=u["name"],
                password_hash=pin_hash,
            ))
            inserted += 1
    if inserted:
        await session.commit()
    return inserted


@router.post("/logout")
async def logout(response: Response):
    clear_session_cookie(response)
    return {"ok": True}


@router.get("/me", response_model=LoginResponse)
async def me(user: User = Depends(get_current_user)):
    return LoginResponse(id=user.id, email=user.email, name=user.name)
