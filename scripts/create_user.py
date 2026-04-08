"""
Create or update a Filtration Mastermind user.

Usage:
    DATABASE_URL=postgresql://... python scripts/create_user.py \
        --email peter@example.com --name "Peter Wilson" --password "set-a-strong-one"

If --password is omitted, a 16-char random password is generated and printed
once. Run from the repo root so `db` and `auth` import correctly.
"""

from __future__ import annotations

import argparse
import asyncio
import secrets
import string
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import select  # noqa: E402

from db import User, init_db, session_factory  # noqa: E402
from auth import hash_password  # noqa: E402


def _random_password(length: int = 16) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", required=True)
    parser.add_argument("--name", default="")
    parser.add_argument("--password", default=None)
    args = parser.parse_args()

    if not await init_db():
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    password = args.password or _random_password()
    pw_hash = hash_password(password)

    factory = session_factory()
    async with factory() as session:
        existing = (await session.execute(
            select(User).where(User.email == args.email.lower())
        )).scalar_one_or_none()

        if existing:
            existing.password_hash = pw_hash
            if args.name:
                existing.name = args.name
            action = "updated"
        else:
            session.add(User(
                email=args.email.lower(),
                name=args.name,
                password_hash=pw_hash,
            ))
            action = "created"

        await session.commit()

    print(f"User {action}: {args.email}")
    if args.password is None:
        print(f"Generated password (save now, will not be shown again): {password}")


if __name__ == "__main__":
    asyncio.run(main())
