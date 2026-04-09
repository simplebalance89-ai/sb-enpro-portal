"""
Enpro Filtration Mastermind Portal — Quote State
Tracks quote-ready fields in the background as users talk naturally.
"""

from __future__ import annotations

import copy
import logging
import re
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from rapidfuzz import fuzz, process

from search import lookup_part

logger = logging.getLogger("enpro.quote_state")

_SESSIONS: dict[str, dict[str, Any]] = {}
_COMPANY_SUFFIXES = (
    "inc",
    "inc.",
    "llc",
    "corp",
    "corporation",
    "company",
    "co",
    "ltd",
    "lp",
    "plc",
)


def _plain(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _default_state(session_id: str) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "customer": {
            "company_name": None,
            "contact_name": None,
            "email": None,
            "phone": None,
            "ship_to": None,
            "account_name": None,
        },
        "request": {
            "intent": None,
            "urgency": None,
            "notes": [],
            "application": None,
            "industry": None,
            "chemical": None,
        },
        "line_items": [],
        "resolved_context": {
            "preferred_manufacturer": None,
            "micron": None,
            "media": None,
            "temperature_f": None,
            "pressure_psi": None,
            "flow_rate": None,
            "housing": None,
            "end_config": None,
        },
        "open_questions": [],
        "warnings": [],
        "ready_for_quote": False,
        "confidence": {
            "overall": 0.0,
            "customer": 0.0,
            "line_items": 0.0,
        },
        "updated_at": datetime.utcnow().isoformat(),
    }


def get_state(session_id: str) -> dict[str, Any]:
    session_id = session_id or "default"
    if session_id not in _SESSIONS:
        _SESSIONS[session_id] = _default_state(session_id)
    return _SESSIONS[session_id]


def reset_state(session_id: str) -> dict[str, Any]:
    _SESSIONS[session_id] = _default_state(session_id)
    return snapshot(session_id)


def migrate_session(from_id: str, to_id: str) -> bool:
    """
    Re-key a quote_state session from one id to another. Used when the
    frontend transitions from a pre-auth random UUID to a stable per-user
    session id (`u<id>`) on first login — without this, the user's
    in-progress quote cart silently disappears the moment they sign in.

    Returns True if anything was migrated, False if there was nothing to
    move (no source session, or source/dest are the same).
    """
    if not from_id or not to_id or from_id == to_id:
        return False
    if from_id not in _SESSIONS:
        return False

    src = _SESSIONS.pop(from_id)
    src["session_id"] = to_id

    # If the destination already has state (e.g. another tab on the same
    # user already started something), prefer the more populated one.
    existing = _SESSIONS.get(to_id)
    if existing:
        src_items = len(src.get("line_items") or [])
        existing_items = len(existing.get("line_items") or [])
        if existing_items >= src_items:
            # Keep existing, drop the migrated one
            return False

    _SESSIONS[to_id] = src
    logger.info(f"quote_state: migrated session {from_id[:8]}... → {to_id}")
    return True


def snapshot(session_id: str) -> dict[str, Any]:
    state = get_state(session_id)
    _refresh_readiness(state)
    return copy.deepcopy(state)


def update_from_message(
    session_id: str,
    message: str,
    df: pd.DataFrame,
    *,
    intent: Optional[str] = None,
) -> dict[str, Any]:
    state = get_state(session_id)
    if not message:
        return snapshot(session_id)

    text = message.strip()
    lower = text.lower()

    if intent:
        state["request"]["intent"] = intent

    _extract_customer_fields(state, text)
    _extract_request_fields(state, text)
    _extract_part_and_specs(state, text, df)

    if any(token in lower for token in ("urgent", "asap", "today", "rush")):
        state["request"]["urgency"] = "urgent"

    _refresh_readiness(state)
    return snapshot(session_id)


def update_from_lookup(session_id: str, product: dict[str, Any]) -> dict[str, Any]:
    state = get_state(session_id)
    _upsert_line_item(state, product, source="lookup")
    _refresh_readiness(state)
    return snapshot(session_id)


def update_from_search(session_id: str, query: str, results: list[dict[str, Any]]) -> dict[str, Any]:
    state = get_state(session_id)
    if query:
        state["request"]["notes"].append(f"search:{query}")
        state["request"]["notes"] = state["request"]["notes"][-10:]

    if results:
        top = results[:5]
        alternatives = []
        for product in top:
            pn = product.get("Part_Number")
            if pn:
                alternatives.append(
                    {
                        "part_number": pn,
                        "manufacturer": product.get("Final_Manufacturer"),
                        "description": product.get("Description"),
                        "micron": product.get("Micron"),
                    }
                )

        line_item = _primary_line_item(state)
        if line_item:
            line_item["alternatives"] = alternatives
        else:
            state["warnings"].append("Search returned candidates before a primary part was confirmed.")
            state["warnings"] = state["warnings"][-10:]

    _refresh_readiness(state)
    return snapshot(session_id)


def update_from_chemical(session_id: str, chemical: str) -> dict[str, Any]:
    state = get_state(session_id)
    if chemical:
        state["request"]["chemical"] = chemical.strip()
    _refresh_readiness(state)
    return snapshot(session_id)


def merge_into_quote_request(session_id: str, quote_payload: dict[str, Any]) -> dict[str, Any]:
    state = get_state(session_id)
    customer = state["customer"]
    line_items = state["line_items"]

    payload = dict(quote_payload)
    payload["company"] = payload.get("company") or customer["company_name"] or customer["account_name"] or ""
    payload["contact_name"] = payload.get("contact_name") or customer["contact_name"] or ""
    payload["contact_email"] = payload.get("contact_email") or customer["email"] or ""
    payload["contact_phone"] = payload.get("contact_phone") or customer["phone"] or ""
    payload["ship_to"] = payload.get("ship_to") or customer["ship_to"] or ""

    if not payload.get("items"):
        payload["items"] = []
        for item in line_items:
            resolved = item.get("resolved", {})
            payload["items"].append(
                {
                    "part_number": resolved.get("part_number") or item["raw_input"].get("part_number") or "",
                    "description": resolved.get("description") or item["raw_input"].get("description") or "",
                    "quantity": item.get("quantity") or 1,
                    "price": resolved.get("price") or "",
                }
            )

    note_parts = [payload.get("notes", "").strip()]
    if state["request"].get("application"):
        note_parts.append(f"Application: {state['request']['application']}")
    if state["request"].get("chemical"):
        note_parts.append(f"Chemical: {state['request']['chemical']}")
    if state["open_questions"]:
        note_parts.append("Open questions: " + "; ".join(state["open_questions"]))
    payload["notes"] = "\n".join([n for n in note_parts if n]).strip()
    return payload


def _extract_customer_fields(state: dict[str, Any], text: str) -> None:
    customer = state["customer"]

    email_match = re.search(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", text, re.IGNORECASE)
    if email_match:
        customer["email"] = email_match.group(0)

    phone_match = re.search(r"(?:\+?1[\s\-\.]?)?(?:\(?\d{3}\)?[\s\-\.]?)\d{3}[\s\-\.]?\d{4}", text)
    if phone_match:
        customer["phone"] = phone_match.group(0)

    ship_match = re.search(r"\bship(?:\s|-)?to\b[:\s]+([A-Za-z0-9 ,\-/]+)", text, re.IGNORECASE)
    if ship_match:
        customer["ship_to"] = ship_match.group(1).strip(" .,")

    contact_match = re.search(r"\bcontact(?:\s+name)?\b[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})", text)
    if contact_match:
        customer["contact_name"] = contact_match.group(1).strip()

    company_match = re.search(
        r"\b(?:for|customer|company|account)\b[:\s]+([A-Z][A-Za-z0-9&,\-\. ]{2,60})",
        text,
    )
    if company_match:
        candidate = _clean_company_candidate(company_match.group(1))
        if candidate and len(candidate) > 2:
            customer["company_name"] = candidate
            customer["account_name"] = candidate
            return

    named_company = re.search(
        r"\b([A-Z][A-Za-z0-9&,\-\.]+(?:\s+[A-Z][A-Za-z0-9&,\-\.]+){0,4}\s+(?:Inc\.?|LLC|Corp\.?|Corporation|Company|Co\.?|Ltd\.?))\b",
        text,
    )
    if named_company:
        candidate = _clean_company_candidate(named_company.group(1))
        customer["company_name"] = candidate
        customer["account_name"] = candidate


def _clean_company_candidate(candidate: str) -> str:
    cleaned = candidate.strip(" .,")
    cleaned = re.split(
        r"\b(?:chemical compatibility|compatible with|ship(?:\s|-)?to|application|industry|micron|qty|quantity|need)\b",
        cleaned,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0].strip(" .,")
    return cleaned


def _extract_request_fields(state: dict[str, Any], text: str) -> None:
    lower = text.lower()

    chemical_match = re.search(
        r"(?:chemical compatibility(?: check)?(?: for| of)?|compatible with|for chemical)\s+([A-Za-z0-9 ,\-/]+)",
        text,
        re.IGNORECASE,
    )
    if chemical_match:
        state["request"]["chemical"] = chemical_match.group(1).strip(" .")

    micron_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:micron|um|μm)\b", lower)
    if micron_match:
        state["resolved_context"]["micron"] = float(micron_match.group(1))

    qty_match = re.search(r"\b(?:qty|quantity|need|quote|for)\s+(\d{1,5})\b", lower)
    if qty_match:
        quantity = int(qty_match.group(1))
        line_item = _primary_line_item(state, create=True)
        line_item["quantity"] = quantity

    manufacturer_phrase = re.search(r"\bfrom\s+([A-Za-z0-9&,\-\. ]{2,40})", text, re.IGNORECASE)
    if manufacturer_phrase:
        state["resolved_context"]["preferred_manufacturer"] = manufacturer_phrase.group(1).strip(" .,")

    application_match = re.search(
        r"\b(?:for|in)\s+(refinery|brewery|pharmaceutical|municipal water|wastewater|chemical processing|petrochemical|dairy|food|beverage)\b",
        lower,
    )
    if application_match:
        state["request"]["application"] = application_match.group(1)
        state["request"]["industry"] = application_match.group(1)


def _extract_part_and_specs(state: dict[str, Any], text: str, df: pd.DataFrame) -> None:
    part_candidates = _find_part_candidates(text, df)
    if part_candidates:
        best = part_candidates[0]
        _upsert_line_item(state, best, source="message")

    manufacturer = _resolve_manufacturer(text, df)
    if manufacturer:
        state["resolved_context"]["preferred_manufacturer"] = manufacturer["value"]


def _find_part_candidates(text: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    """Extract part-number-shaped tokens from the user message and resolve
    each one against the catalog. V2.14.5 — added strict shape filtering
    to stop bare English words like 'HVAC', 'data', 'center', 'meeting'
    from being lookup_part'd against the catalog and silently inserting
    junk line items (the 'Box with Return Label' leak).

    A token is considered part-number-shaped only if it contains BOTH at
    least one letter AND at least one digit (most real part numbers do —
    HC9020, CLR130, EPE-10-5, 200190-000001, etc.). Pure-alpha tokens
    like 'HVAC' or 'meeting' are skipped here. Pure-digit tokens like
    '1000' (years, quantities) are also skipped — those are best
    handled by the structured spec extractor in voice_query.
    """
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()

    tokens = re.findall(r"[A-Za-z0-9/\-]{4,}", text)
    for token in tokens:
        # Require BOTH a letter and a digit — skip pure words and pure numbers
        has_letter = any(c.isalpha() for c in token)
        has_digit = any(c.isdigit() for c in token)
        if not (has_letter and has_digit):
            continue
        product = lookup_part(df, token)
        if product and product.get("Part_Number") not in seen:
            seen.add(product["Part_Number"])
            candidates.append(product)

    return candidates


def _resolve_manufacturer(text: str, df: pd.DataFrame) -> Optional[dict[str, Any]]:
    if "Final_Manufacturer" not in df.columns:
        return None

    manufacturers = sorted({str(v).strip() for v in df["Final_Manufacturer"].fillna("") if str(v).strip()})
    if not manufacturers:
        return None

    match = process.extractOne(
        text,
        manufacturers,
        scorer=fuzz.token_set_ratio,
        score_cutoff=88,
    )
    if not match:
        return None

    value, score, _ = match
    return {"value": value, "confidence": round(score / 100.0, 2)}


def _upsert_line_item(state: dict[str, Any], product: dict[str, Any], *, source: str) -> None:
    part_number = _plain(product.get("Part_Number"))
    if not part_number:
        return

    existing = None
    for item in state["line_items"]:
        if item["resolved"].get("part_number") == part_number or item["raw_input"].get("part_number") == part_number:
            existing = item
            break

    if existing is None:
        for item in state["line_items"]:
            if not item["resolved"].get("part_number") and not item["raw_input"].get("part_number"):
                existing = item
                break

    if existing is None:
        existing = {
            "line_id": f"line-{len(state['line_items']) + 1}",
            "raw_input": {
                "part_number": part_number,
                "manufacturer": _plain(product.get("Final_Manufacturer")),
                "description": _plain(product.get("Description")),
                "micron": _plain(product.get("Micron")),
                "media": _plain(product.get("Media")),
                "quantity": None,
                "length": None,
                "diameter": None,
                "notes": source,
            },
            "resolved": {},
            "alternatives": [],
            "quantity": 1,
            "confidence": 1.0,
            "status": "candidate",
            "needs_confirmation": False,
            "confirmation_reason": None,
        }
        state["line_items"].append(existing)
    else:
        existing["raw_input"]["part_number"] = part_number
        existing["raw_input"]["manufacturer"] = _plain(product.get("Final_Manufacturer"))
        existing["raw_input"]["description"] = _plain(product.get("Description"))
        existing["raw_input"]["micron"] = _plain(product.get("Micron"))
        existing["raw_input"]["media"] = _plain(product.get("Media"))
        existing["raw_input"]["notes"] = source

    existing["resolved"] = {
        "part_number": part_number,
        "manufacturer": _plain(product.get("Final_Manufacturer")),
        "description": _plain(product.get("Description")),
        "product_type": _plain(product.get("Product_Type")),
        "micron": _plain(product.get("Micron")),
        "media": _plain(product.get("Media")),
        "price": _plain(product.get("Price")),
        "stock_status": _plain((product.get("Stock") or {}).get("status")) if isinstance(product.get("Stock"), dict) else _plain(product.get("Stock")),
        "total_stock": _plain(product.get("Total_Stock")),
    }
    existing["status"] = "resolved"
    existing["confidence"] = 1.0

    if product.get("Final_Manufacturer"):
        state["resolved_context"]["preferred_manufacturer"] = product.get("Final_Manufacturer")
    if product.get("Micron") not in ("", None):
        state["resolved_context"]["micron"] = product.get("Micron")
    if product.get("Media"):
        state["resolved_context"]["media"] = product.get("Media")


def _primary_line_item(state: dict[str, Any], create: bool = False) -> Optional[dict[str, Any]]:
    if state["line_items"]:
        return state["line_items"][0]
    if create:
        item = {
            "line_id": "line-1",
            "raw_input": {
                "part_number": None,
                "manufacturer": None,
                "description": None,
                "micron": None,
                "media": None,
                "quantity": None,
                "length": None,
                "diameter": None,
                "notes": None,
            },
            "resolved": {},
            "alternatives": [],
            "quantity": 1,
            "confidence": 0.0,
            "status": "candidate",
            "needs_confirmation": False,
            "confirmation_reason": None,
        }
        state["line_items"].append(item)
        return item
    return None


def _refresh_readiness(state: dict[str, Any]) -> None:
    open_questions: list[str] = []
    warnings: list[str] = []

    customer = state["customer"]
    customer_score = 0.0
    if customer["company_name"] or customer["account_name"]:
        customer_score += 0.6
    else:
        open_questions.append("Which company or customer should this quote be for?")
    if customer["contact_name"]:
        customer_score += 0.2
    if customer["email"] or customer["phone"]:
        customer_score += 0.2

    line_item_score = 0.0
    resolved_items = 0
    for item in state["line_items"]:
        if item["resolved"].get("part_number"):
            resolved_items += 1
            line_item_score += 0.5
        else:
            item["needs_confirmation"] = True
            item["confirmation_reason"] = "Part not resolved"
        if item.get("quantity"):
            line_item_score += 0.2
        else:
            open_questions.append("How many units do you need for each quoted part?")

        if item.get("alternatives") and len(item["alternatives"]) > 1:
            warnings.append("Multiple alternatives are available; confirm the final part before quoting.")

    state["open_questions"] = list(dict.fromkeys(open_questions))[:8]
    state["warnings"] = list(dict.fromkeys(warnings))[:8]

    state["confidence"]["customer"] = round(min(customer_score, 1.0), 2)
    state["confidence"]["line_items"] = round(min(line_item_score, 1.0), 2)
    state["confidence"]["overall"] = round(
        min((state["confidence"]["customer"] + state["confidence"]["line_items"]) / 2.0, 1.0),
        2,
    )
    state["ready_for_quote"] = bool(
        resolved_items > 0
        and all(item.get("quantity") for item in state["line_items"] if item["resolved"].get("part_number"))
        and (customer["company_name"] or customer["account_name"])
    )
    state["updated_at"] = datetime.utcnow().isoformat()
