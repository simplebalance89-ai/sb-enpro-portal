"""
Enpro Filtration Mastermind — Customer Intelligence query module.

Per-rep partitioned customer history. Every public function takes a
`user_rep_id` and runs `WHERE rep_id = user_rep_id` against Postgres —
ACL is structural at the SQL layer, not policy at the application layer.
A logged-in salesperson literally cannot retrieve another rep's customers.

Fed by scripts/ingest_customer_intel.py which writes the 4 per-rep tables
from the source CSVs (PO Portal Customers + SO Header + SO Lines + Dynamics
quotes). Refresh nightly via cron or manually re-run.

Soft-fall everywhere: if customer intel hasn't been ingested yet, every
function returns empty. The catalog FM keeps working unchanged for any
user without rep_id set.

KNOWN LIMITATION (V2.12) — `customer_top_parts` is sourced from PO Portal
SO Lines, which tracks ALL EnPro product lines (filtration, vibrating
equipment, sealing, instrumentation, tariffs). Some "top parts" rows are
not filtration. There's no clean inv_mast_uid → filtration catalog key
yet. The system prompt warns the model to weight filtration history
heavier than non-filtration. Future work: filter at ETL time against the
FM 19,470-product catalog or get an inv_mast_uid ↔ filtration crosswalk.
The other 3 tables (customer_master, customer_orders, customer_quotes)
are clean signal regardless.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional, Sequence

from sqlalchemy import select, desc, func
from sqlalchemy.ext.asyncio import AsyncSession

from db import (
    CustomerMaster,
    CustomerOrder,
    CustomerQuote,
    CustomerTopPart,
    is_ready as db_ready,
    session_factory,
)

logger = logging.getLogger("enpro.customer_intel")

# Cap on how many customers / quotes / parts to surface in any one prompt.
# Bounded so the GPT context window stays reasonable even for high-volume reps.
MAX_TOP_CUSTOMERS = 10
MAX_QUOTES = 8
MAX_RECENT_ORDERS = 5
MAX_TOP_PARTS = 5


# ---------------------------------------------------------------------------
# Catalog of all customer names this rep owns — used by extract_customer_mention
# ---------------------------------------------------------------------------
# Cached in-process per rep_id so we don't re-query Postgres for every chat
# turn just to check if the user mentioned a customer. Refreshed when the
# nightly ETL runs and a new instance picks it up.

_REP_CUSTOMER_INDEX_CACHE: dict[str, list[dict]] = {}


async def _build_rep_customer_index(rep_id: str) -> list[dict]:
    """Pull every (customer_id, customer_name, legal_name) for this rep
    from Postgres. Used as the lookup target for customer-name mentions
    in chat messages."""
    if not db_ready():
        return []
    factory = session_factory()
    async with factory() as session:
        result = await session.execute(
            select(
                CustomerMaster.customer_id,
                CustomerMaster.customer_name,
                CustomerMaster.legal_name,
            ).where(CustomerMaster.rep_id == rep_id)
        )
        rows = result.all()
    return [
        {
            "customer_id": r.customer_id,
            "customer_name": r.customer_name or "",
            "legal_name": r.legal_name or "",
        }
        for r in rows
    ]


async def get_rep_customer_index(rep_id: str) -> list[dict]:
    """Cached accessor for the rep's customer list."""
    if not rep_id:
        return []
    cached = _REP_CUSTOMER_INDEX_CACHE.get(rep_id)
    if cached is not None:
        return cached
    fresh = await _build_rep_customer_index(rep_id)
    _REP_CUSTOMER_INDEX_CACHE[rep_id] = fresh
    return fresh


def invalidate_rep_customer_index(rep_id: Optional[str] = None) -> None:
    """Drop cached customer index — call after the ETL re-runs."""
    if rep_id:
        _REP_CUSTOMER_INDEX_CACHE.pop(rep_id, None)
    else:
        _REP_CUSTOMER_INDEX_CACHE.clear()


# ---------------------------------------------------------------------------
# Mention extraction — does this user message reference a customer this rep owns?
# ---------------------------------------------------------------------------

def extract_customer_mention(message: str, customer_index: list[dict]) -> Optional[dict]:
    """
    Scan the user message for any customer name in this rep's book. Returns
    the matched customer dict ({customer_id, customer_name, legal_name}) or
    None. Conservative substring match — fuzzy matching is too noisy at
    chat-turn frequency.

    Strategy: longest customer name first (so "ADM Decatur" wins over "ADM"),
    case-insensitive substring search. Skips customer names shorter than 4
    chars to avoid false positives on common short tokens.
    """
    if not message or not customer_index:
        return None
    msg_upper = message.upper()
    candidates = []
    for c in customer_index:
        for name_field in ("customer_name", "legal_name"):
            name = (c.get(name_field) or "").strip()
            if len(name) < 4:
                continue
            if name.upper() in msg_upper:
                candidates.append((len(name), c))
                break  # don't double-count one customer for both name fields
    if not candidates:
        return None
    # Longest match wins
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


# ---------------------------------------------------------------------------
# Per-customer fetch — full intel package
# ---------------------------------------------------------------------------

async def fetch_customer_intel(rep_id: str, customer_id: int) -> dict:
    """
    Pull the customer intel block for one (rep_id, customer_id):
    profile + recent orders + active quotes. Returns a dict ready to
    JSON-serialize into the [CUSTOMER CONTEXT] prompt block.

    NOTE V2.12: customer_top_parts is intentionally NOT queried here —
    the underlying data mixes filtration with non-filtration EnPro product
    lines (vibrating equipment, sealing, instrumentation) and we don't yet
    have a clean inv_mast_uid → filtration catalog crosswalk to filter it.
    The table is still populated by the ETL so the data is available; it
    just isn't surfaced in chat until we have the filter in place.
    """
    if not db_ready() or not rep_id or customer_id is None:
        return {}
    factory = session_factory()
    async with factory() as session:
        # 1. Customer master row
        master_row = (await session.execute(
            select(CustomerMaster).where(
                CustomerMaster.rep_id == rep_id,
                CustomerMaster.customer_id == customer_id,
            )
        )).scalar_one_or_none()
        if master_row is None:
            return {}

        # 2. Recent orders (last N)
        order_rows = (await session.execute(
            select(CustomerOrder)
            .where(
                CustomerOrder.rep_id == rep_id,
                CustomerOrder.customer_id == customer_id,
            )
            .order_by(desc(CustomerOrder.order_date))
            .limit(MAX_RECENT_ORDERS)
        )).scalars().all()

        # 3. Active / recent quotes
        quote_rows = (await session.execute(
            select(CustomerQuote)
            .where(
                CustomerQuote.rep_id == rep_id,
                CustomerQuote.customer_id == customer_id,
            )
            .order_by(desc(CustomerQuote.created_date))
            .limit(MAX_QUOTES)
        )).scalars().all()

    return {
        "profile": _master_to_dict(master_row),
        "recent_orders": [_order_to_dict(o) for o in order_rows],
        "quotes": [_quote_to_dict(q) for q in quote_rows],
    }


async def get_my_top_customers(rep_id: str, limit: int = MAX_TOP_CUSTOMERS) -> list[dict]:
    """Top customers for this rep, ranked by total_so_count."""
    if not db_ready() or not rep_id:
        return []
    factory = session_factory()
    async with factory() as session:
        result = await session.execute(
            select(CustomerMaster)
            .where(CustomerMaster.rep_id == rep_id)
            .order_by(desc(CustomerMaster.total_so_count))
            .limit(limit)
        )
        rows = result.scalars().all()
    return [_master_to_dict(r) for r in rows]


async def get_my_open_quotes(rep_id: str, limit: int = MAX_QUOTES) -> list[dict]:
    """Active quotes for this rep, newest first."""
    if not db_ready() or not rep_id:
        return []
    factory = session_factory()
    async with factory() as session:
        result = await session.execute(
            select(CustomerQuote)
            .where(
                CustomerQuote.rep_id == rep_id,
                CustomerQuote.status.ilike("active%"),
            )
            .order_by(desc(CustomerQuote.created_date))
            .limit(limit)
        )
        rows = result.scalars().all()
    return [_quote_to_dict(q) for q in rows]


# ---------------------------------------------------------------------------
# Row → dict serializers (decimal/date safe for JSON)
# ---------------------------------------------------------------------------

def _master_to_dict(r: CustomerMaster) -> dict:
    return {
        "customer_id": r.customer_id,
        "customer_name": r.customer_name,
        "legal_name": r.legal_name,
        "credit_status": r.credit_status,
        "credit_limit": float(r.credit_limit) if r.credit_limit is not None else None,
        "terms": r.terms,
        "city": r.mail_city,
        "state": r.mail_state,
        "phone": r.central_phone,
        "email": r.email_address,
        "national_account": r.national_account == "Y",
        "total_orders": r.total_so_count,
        "last_order_date": r.last_order_date.isoformat() if r.last_order_date else None,
    }


def _order_to_dict(r: CustomerOrder) -> dict:
    return {
        "order_no": r.order_no,
        "order_date": r.order_date.isoformat() if r.order_date else None,
        "po_no": r.po_no,
        "extended_price": float(r.extended_price) if r.extended_price is not None else None,
        "ship2_city": r.ship2_city,
        "ship2_state": r.ship2_state,
        "line_count": r.line_count,
        "completed": r.completed == "Y",
    }


def _part_to_dict(r: CustomerTopPart) -> dict:
    return {
        "part_number": r.customer_part_number,
        "description": (r.part_description or "")[:120],
        "total_qty": float(r.total_qty) if r.total_qty is not None else None,
        "total_spend": float(r.total_extended_price) if r.total_extended_price is not None else None,
        "order_count": r.order_count,
        "last_ordered": r.last_ordered_date.isoformat() if r.last_ordered_date else None,
    }


def _quote_to_dict(r: CustomerQuote) -> dict:
    return {
        "quote_number": r.quote_number,
        "quote_name": r.quote_name,
        "status": r.status,
        "contact_name": r.contact_name,
        "extended_price": float(r.extended_price) if r.extended_price is not None else None,
        "freight_terms": r.freight_terms,
        "payment_terms": r.payment_terms,
        "est_completion": r.est_completion,
        "created_date": r.created_date.isoformat() if r.created_date else None,
    }
