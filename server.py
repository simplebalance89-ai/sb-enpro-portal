"""
Enpro Filtration Mastermind Portal — FastAPI Server
Main application with chat, lookup, search, chemical check, and widget endpoints.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import httpx
import pandas as pd
from fastapi import Depends, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from config import settings
from data_loader import load_static, load_inventory, load_chemicals, merge_data
from search import search_products, lookup_part, suggest_parts
from router import handle_message
from azure_client import health_check as azure_health_check, close_client
from governance import run_pre_checks
from quote_state import (
    merge_into_quote_request,
    reset_state as reset_quote_state,
    snapshot as snapshot_quote_state,
    update_from_chemical,
    update_from_lookup,
    update_from_message,
    update_from_search,
)
from voice_search import init_voice_search, voice_search_pipeline
from voice_gate import VoiceGate
from voice_echo import VoiceEcho

import auth
import conversation_memory
from db import close_db, get_session, init_db, is_ready as db_ready, session_factory
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import DBAPIError, OperationalError

logger = logging.getLogger("enpro.server")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

# ---------------------------------------------------------------------------
# App state — holds loaded DataFrames
# ---------------------------------------------------------------------------

class AppState:
    df: pd.DataFrame = pd.DataFrame()
    chemicals_df: pd.DataFrame = pd.DataFrame()
    static_df: pd.DataFrame = pd.DataFrame()
    inventory_df: pd.DataFrame = pd.DataFrame()
    last_inventory_load: Optional[datetime] = None
    data_loaded: bool = False
    voice_gate: Optional[VoiceGate] = None
    voice_echo: Optional[VoiceEcho] = None


state = AppState()


# ---------------------------------------------------------------------------
# Background inventory refresh (hourly)
# ---------------------------------------------------------------------------

async def _refresh_inventory_loop():
    """Background task: reload inventory from Azure Blob every hour."""
    while True:
        await asyncio.sleep(3600)  # 1 hour
        try:
            logger.info("Refreshing inventory data...")
            inv = await asyncio.to_thread(load_inventory)
            if not inv.empty:
                merged = await asyncio.to_thread(merge_data, state.static_df, inv)
                state.inventory_df = inv
                state.df = merged
                state.last_inventory_load = datetime.utcnow()
                logger.info(f"Inventory refreshed: {len(inv)} rows")
            else:
                logger.warning("Inventory refresh returned empty — keeping stale data")
        except Exception as e:
            logger.error(f"Inventory refresh failed: {e}")


# ---------------------------------------------------------------------------
# Lifespan — load data on startup, clean up on shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load data from Azure Blob on startup, start background refresh."""
    logger.info("Enpro Filtration Mastermind Portal starting...")

    # Load all data
    try:
        state.static_df = load_static()
        state.inventory_df = load_inventory()
        state.chemicals_df = load_chemicals()
        state.df = merge_data(state.static_df, state.inventory_df)
        state.last_inventory_load = datetime.utcnow()
        state.data_loaded = not state.df.empty
        if state.data_loaded:
            logger.info(
                f"Data loaded: {len(state.df)} products, "
                f"{len(state.chemicals_df)} chemical entries"
            )
            # Initialize voice search vocabulary from product data
            init_voice_search(state.df)
            # Initialize Voice Echo (predictive pre-fetch system)
            try:
                state.voice_gate = VoiceGate.from_dataframe(state.df)
                state.voice_echo = VoiceEcho(state.voice_gate, delay_seconds=8, defer_seconds=15)  # 8s initial + 15s deep lookup = 23s total
                logger.info("Voice Echo initialized with predictive pre-fetch")
            except Exception as ve:
                logger.error(f"Voice Echo init failed: {ve}")
        else:
            logger.error("Startup data load completed with zero products; portal will stay degraded")
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        state.data_loaded = False

    # Start background inventory refresh
    refresh_task = asyncio.create_task(_refresh_inventory_loop())

    # Initialize Postgres (auth + conversation memory). Soft-fail if DATABASE_URL unset.
    cleanup_task = None
    try:
        if await init_db():
            # Seed pilot users (idempotent)
            from db import session_factory
            async with session_factory()() as s:
                inserted = await auth.seed_pilot_users(s)
                if inserted:
                    logger.info(f"Seeded {inserted} pilot users")
            cleanup_task = asyncio.create_task(conversation_memory.cleanup_loop())
            logger.info("Auth + conversation memory ready")
    except Exception as db_err:
        logger.error(f"Database init failed (auth/memory disabled): {db_err}")

    yield

    # Shutdown
    refresh_task.cancel()
    if cleanup_task is not None:
        cleanup_task.cancel()
    await close_db()
    await close_client()
    logger.info("Enpro Filtration Mastermind Portal stopped.")


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Enpro Filtration Mastermind Portal",
    version="2.7.0",
    description="AI-powered filtration product search, recommendation, and quote engine.",
    lifespan=lifespan,
)

# CORS — allow all origins for embed/widget use (credentials=False per spec when origin=*)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting — protect Azure OpenAI spend
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Auth router (/api/auth/login, /logout, /me)
app.include_router(auth.router)


# Prevent stale HTML — browsers must revalidate index.html / login.html on every load.
# Static assets (JS/CSS/images) can still be cached normally.
@app.middleware("http")
async def no_cache_html(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path.endswith(".html"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


# Admin auth dependency — require X-Admin-Token header if ADMIN_TOKEN is set
async def verify_admin(x_admin_token: str = Header(default="")):
    if settings.ADMIN_TOKEN and x_admin_token != settings.ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Forbidden")


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    message: str = Field(max_length=2000)
    session_id: str = "default"
    mode: str = "standard"  # standard, demo, guided


class LookupRequest(BaseModel):
    part_number: str = Field(max_length=200)
    session_id: str = "default"


class SearchRequest(BaseModel):
    query: str = Field(max_length=500)
    field: Optional[str] = None
    session_id: str = "default"


class ChemicalRequest(BaseModel):
    chemical: str = Field(max_length=200)
    session_id: str = "default"


class SuggestRequest(BaseModel):
    query: str = Field(max_length=200)
    session_id: str = "default"


class ReportRequest(BaseModel):
    part_number: str = Field(max_length=200)
    reason: str = Field(default="", max_length=500)
    session_id: str = Field(default="", max_length=100)


class QuoteStateResetRequest(BaseModel):
    session_id: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    """Health check — data status + Azure OpenAI connectivity."""
    azure_status = await azure_health_check()
    return {
        "status": "healthy" if state.data_loaded else "degraded",
        "data_loaded": state.data_loaded,
        "product_count": len(state.df),
        "chemical_count": len(state.chemicals_df),
        "last_inventory_refresh": (
            state.last_inventory_load.isoformat() if state.last_inventory_load else None
        ),
        "azure_openai": azure_status,
        "timestamp": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Auth + memory helpers — shared by /api/chat, /api/chemical, /api/voice-search-text
# ---------------------------------------------------------------------------

async def _chat_auth_and_history(request: Request) -> tuple[Optional[int], list]:
    """
    Phase 1 of any history-aware endpoint: short DB session, look up the
    optional logged-in user + their recent history.

    SOFT by design: returns (None, []) if DB is unconfigured, no cookie is
    present, or the cookie is invalid. Endpoints that require auth (like
    /api/chat) must enforce it themselves after calling this helper.
    Endpoints that should keep working for anonymous callers (voice search,
    chemical compatibility) just use whatever this returns.
    """
    if not db_ready():
        return None, []
    try:
        async with session_factory()() as session:
            user = await auth.get_current_user_optional(request, session)
            if user is None:
                return None, []
            history = await conversation_memory.get_recent_history(session, user.id)
            return user.id, history
    except (OperationalError, DBAPIError, ConnectionError) as e:
        logger.error(f"DB connection error during auth/history fetch: {e}", exc_info=True)
        return None, []


async def _persist_turn(
    user_id: Optional[int],
    user_message: str,
    assistant_message: str,
    products: Optional[list] = None,
) -> None:
    """
    Phase 3: short DB session, append the (user, assistant) pair to memory.
    No-op if user_id is None or DB is unconfigured. Errors are logged but
    never raised — persistence failure must not break the user-facing reply.
    """
    if user_id is None or not db_ready():
        return
    try:
        async with session_factory()() as session:
            await conversation_memory.append_turn(
                session,
                user_id=user_id,
                user_message=user_message,
                assistant_message=assistant_message,
                products=products,
            )
    except Exception as e:
        logger.error(f"Failed to persist conversation turn: {e}")


@app.post("/api/chat")
@limiter.limit("20/minute")
async def chat(request: Request, req: ChatRequest):
    """
    Main chat endpoint. Classifies intent, runs governance, routes to handler.

    DB strategy: three short-lived sessions, never holding a connection
    across the GPT call. Phase 1 = auth + history fetch. Phase 2 = handler.
    Phase 3 = persist the turn. Soft-falls to legacy stateless mode if
    DATABASE_URL is unconfigured.
    """
    if not state.data_loaded:
        return JSONResponse(
            status_code=503,
            content={"error": "Data not loaded yet. Try again in a moment."},
        )

    user_id, history = await _chat_auth_and_history(request)

    # /api/chat REQUIRES auth when DB is configured. Voice + chemical
    # endpoints stay open for anonymous callers (the soft helper above
    # returns None for them); /api/chat does not.
    if db_ready() and user_id is None:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        update_from_message(req.session_id, req.message, state.df)
        result = await handle_message(
            message=req.message,
            session_id=req.session_id,
            mode=req.mode,
            df=state.df,
            chemicals_df=state.chemicals_df,
            history=history or None,
        )
        result["quote_state"] = snapshot_quote_state(req.session_id)
    except Exception as e:
        logger.error(f"Chat error: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": "Something went wrong. Try again or contact Enpro directly.",
                "detail": str(e),
            },
        )

    await _persist_turn(
        user_id,
        req.message,
        result.get("response", ""),
        products=result.get("products"),
    )
    return result


@app.post("/api/chat/reset")
async def chat_reset(request: Request):
    """Clear the logged-in user's conversation memory ('start fresh').

    Soft-fall: returns 503 if auth/DB is not configured (consistent with
    /api/chat) instead of crashing on the dependency injector.
    """
    if not db_ready():
        return JSONResponse(
            status_code=503,
            content={"error": "Auth/memory not configured"},
        )
    async with session_factory()() as session:
        user = await auth.get_current_user(request, session)
        deleted = await conversation_memory.clear_user_history(session, user.id)
    return {"ok": True, "deleted": deleted}


@app.post("/api/lookup")
async def lookup(request: Request, req: LookupRequest):
    """Direct part number lookup — Pandas only, $0 cost.

    History-aware: when the caller is logged in, the lookup is persisted
    to the 7-day memory so a follow-up "compare those two" can find the
    part the user just looked up via the modal.
    """
    if not state.data_loaded:
        return JSONResponse(status_code=503, content={"error": "Data not loaded."})

    user_id, _history = await _chat_auth_and_history(request)
    product = lookup_part(state.df, req.part_number)

    if product:
        result = {
            "found": True,
            "product": product,
            "quote_state": update_from_lookup(req.session_id, product),
        }
        await _persist_turn(
            user_id,
            f"Lookup: {req.part_number}",
            f"Found {req.part_number}: {product.get('Description', '')[:120]}",
            products=[product],
        )
        return result

    update_from_message(req.session_id, req.part_number, state.df, intent="lookup")
    await _persist_turn(
        user_id,
        f"Lookup: {req.part_number}",
        f"No product found for '{req.part_number}'.",
    )
    return {
        "found": False,
        "message": f"No product found for '{req.part_number}'.",
        "quote_state": snapshot_quote_state(req.session_id),
    }


@app.post("/api/search")
async def search(request: Request, req: SearchRequest):
    """Search products by query — Pandas only, $0 cost.

    History-aware: persists the query and top result summary so the
    user's modal-driven searches feed the conversational memory.
    """
    if not state.data_loaded:
        return JSONResponse(status_code=503, content={"error": "Data not loaded."})

    user_id, _history = await _chat_auth_and_history(request)
    result = search_products(state.df, req.query, field=req.field)
    update_from_message(req.session_id, req.query, state.df, intent="search")
    result["quote_state"] = update_from_search(req.session_id, req.query, result.get("results", []))

    products = result.get("results") or []
    total = result.get("total_found", len(products))
    if products:
        first = products[0] if isinstance(products[0], dict) else {}
        first_pn = first.get("Part_Number") or first.get("Alt_Code") or "(unknown)"
        summary = f"Search returned {total} results. Top: {first_pn}."
    else:
        summary = f"Search returned no matches for '{req.query}'."
    await _persist_turn(user_id, f"Search: {req.query}", summary, products=products[:5])
    return result


@app.post("/api/chemical")
@limiter.limit("15/minute")
async def chemical_check(request: Request, req: ChemicalRequest):
    """
    Chemical compatibility check. Routes through GPT-4.1 with chemical
    crosswalk context. History-aware: pulls 7-day memory and persists turn.
    """
    if not state.data_loaded:
        return JSONResponse(status_code=503, content={"error": "Data not loaded."})

    user_id, history = await _chat_auth_and_history(request)
    chem_message = f"Chemical compatibility: {req.chemical}"

    try:
        update_from_message(req.session_id, req.chemical, state.df, intent="chemical")
        result = await handle_message(
            message=chem_message,
            session_id=req.session_id or "chemical_check",
            mode="standard",
            df=state.df,
            chemicals_df=state.chemicals_df,
            history=history or None,
        )
        result["quote_state"] = update_from_chemical(req.session_id, req.chemical)
        await _persist_turn(
            user_id,
            chem_message,
            result.get("response", ""),
            products=result.get("products"),
        )
        return result
    except Exception as e:
        logger.error(f"Chemical check error: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Chemical check failed.", "detail": str(e)},
        )


@app.get("/api/suggest")
async def suggest(q: str = "", in_stock: str = "all"):
    """Typeahead suggestions for part number lookup. Cascade: exact → starts_with → sequential.
    in_stock: all (default), in_stock (only Qty > 0)
    """
    if not state.data_loaded or len(q) < 2:
        return {"suggestions": []}

    df = state.df
    if in_stock == "in_stock" and "Total_Stock" in df.columns:
        df = df[df["Total_Stock"] > 0]

    suggestions = suggest_parts(df, q, max_results=10)
    return {"suggestions": suggestions}


@app.get("/api/parts/list")
async def parts_list(limit: int = 100, in_stock: str = "all"):
    """Return list of parts for dropdowns. Used by Compare Parts.
    limit: max number of parts to return (default 100)
    in_stock: all (default), in_stock (only Qty > 0)
    """
    if not state.data_loaded:
        return {"parts": []}

    df = state.df
    if in_stock == "in_stock" and "Total_Stock" in df.columns:
        df = df[df["Total_Stock"] > 0]

    # Return top parts with Part_Number and Description
    results = []
    for _, row in df.head(limit).iterrows():
        results.append({
            "Part_Number": str(row.get("Part_Number", "")),
            "Description": str(row.get("Description", "")),
            "Final_Manufacturer": str(row.get("Final_Manufacturer", row.get("Manufacturer", "")))
        })
    
    return {"parts": results}


@app.get("/api/manufacturers/list")
async def manufacturers_list():
    """Return curated manufacturer list for dropdowns."""
    # Curated manufacturer list — consolidated (Pall includes Trincor, PowerFlow, Applied Energy)
    # Based on crosswalk product counts, 50+ products threshold, deduped
    CURATED_MANUFACTURERS = sorted([
        "AAF",
        "AJR Filtration Inc.",
        "American Filter Manufacturing Inc.",
        "Amiad Filtration Systems",
        "Andronaco",
        "Atlas Copco Compressors",
        "Banner Industries",
        "Capsule Technologies",
        "Clear Blue Filtration",
        "Cobetter",
        "Critical Process Filtration Inc",
        "Delta Pure Filtration",
        "Donaldson",
        "Duravalve",
        "Edmac Compressor Parts",
        "Enpro, Incorporated",
        "ErtelAlsop",
        "Filtrafine Corporation",
        "Filtrox North America Inc.",
        "Flowserve",
        "FTC-Filtration Technology",
        "Global Filter LLC",
        "Graver Technologies",
        "Hydac",
        "Industrial Technologies (PPC/Hankison)",
        "Islip Flow Controls Inc.",
        "Johnson Filtration",
        "Jonell Filtration Group",
        "Koch Filter Corporation",
        "Le Sac Corporation",
        "Lechler Inc",
        "McMaster-Carr Supply Co.",
        "National Oilwell Varco L.P.",
        "Pall Corporation",
        "Pentair Filtration",
        "Porvair Filtration Group Inc",
        "Quincy Compressor LLC",
        "Rosedale Products Inc",
        "Royal Filter",
        "Saint Gobain Performance",
        "Schroeder Industries",
        "Shelco Filters",
        "Solventum Corporation",
        "Swift Filters Inc.",
        "United Filtration Systems",
    ])
    return {"manufacturers": CURATED_MANUFACTURERS, "count": len(CURATED_MANUFACTURERS)}


@app.get("/api/product-types/list")
async def product_types_list():
    """Return curated filtration product types for dropdown."""
    # Curated list — filters only, no non-filtration items, consolidated dupes
    CURATED_PRODUCT_TYPES = sorted([
        "Air Filter",
        "Bag Filter",
        "Bags",
        "Capsule Filter",
        "Cartridges",
        "Compressor/Filter",
        "Depth Sheets",
        "Elements",
        "Filters (General)",
        "Housings",
        "Membranes",
        "Screens / Separators",
    ])
    return {"product_types": CURATED_PRODUCT_TYPES}


@app.get("/api/chemicals/list")
async def chemicals_list():
    """Return list of chemical names for dropdown. Pandas only, $0 cost."""
    if not state.data_loaded or state.chemicals_df.empty:
        return {"chemicals": []}

    # Try to find the chemical name column
    name_col = None
    for col in state.chemicals_df.columns:
        col_lower = col.lower()
        if "chemical" in col_lower or "media" in col_lower or "name" in col_lower:
            name_col = col
            break
    if not name_col:
        # Fall back to first column
        name_col = state.chemicals_df.columns[0]

    chemicals = sorted(
        state.chemicals_df[name_col]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )
    # Remove empty strings
    chemicals = [c for c in chemicals if c and c != ""]
    return {"chemicals": chemicals}


@app.get("/api/quote-state/{session_id}")
async def get_quote_state(session_id: str):
    """Return the background quote state for the active session."""
    return {"quote_state": snapshot_quote_state(session_id)}


@app.post("/api/quote-state/reset")
async def quote_state_reset(req: QuoteStateResetRequest):
    """Reset background quote state for a session."""
    return {"quote_state": reset_quote_state(req.session_id)}


@app.post("/api/report")
async def report_product(req: ReportRequest):
    """Flag a product card for Peter's review. Saves to persistent log."""
    import os
    from datetime import datetime

    report = {
        "part_number": req.part_number,
        "reason": req.reason,
        "session_id": req.session_id,
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Append to reports file
    reports_file = os.path.join("data", "reports.json")
    os.makedirs("data", exist_ok=True)

    try:
        import json
        existing = []
        if os.path.exists(reports_file):
            with open(reports_file, "r") as f:
                existing = json.load(f)
        existing.append(report)
        with open(reports_file, "w") as f:
            json.dump(existing, f, indent=2)
        logger.info(f"Report filed: {req.part_number}")
        return {"status": "reported", "report": report}
    except Exception as e:
        logger.error(f"Report save failed: {e}")
        return {"status": "error", "detail": str(e)}


@app.get("/api/reports", dependencies=[Depends(verify_admin)])
async def get_reports():
    """Get all filed reports (admin view)."""
    import os
    import json
    reports_file = os.path.join("data", "reports.json")
    if os.path.exists(reports_file):
        with open(reports_file, "r") as f:
            reports = json.load(f)
        return {"reports": reports, "count": len(reports)}
    return {"reports": [], "count": 0}


class CompareSuggestRequest(BaseModel):
    part_number: str = Field(max_length=200)


@app.post("/api/compare-suggestions")
async def compare_suggestions(req: CompareSuggestRequest):
    """Find similar products grouped by category for smart comparison."""
    if not state.data_loaded:
        return JSONResponse(status_code=503, content={"error": "Data not loaded."})

    from search import find_similar_products
    result = find_similar_products(state.df, req.part_number)
    return result


class EmailReportRequest(BaseModel):
    subject: str = Field(default="Enpro FM Portal — Report", max_length=200)
    body: str = Field(default="", max_length=2000)
    reports: list = Field(default_factory=list)


@app.post("/api/email-report", dependencies=[Depends(verify_admin)])
@limiter.limit("3/minute")
async def email_report(request: Request, req: EmailReportRequest):
    """Email reports to Peter. Uses SMTP configured via environment variables."""
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    smtp_host = settings.smtp_host if hasattr(settings, 'smtp_host') else ""
    smtp_port = settings.smtp_port if hasattr(settings, 'smtp_port') else 587
    smtp_user = settings.smtp_user if hasattr(settings, 'smtp_user') else ""
    smtp_pass = settings.smtp_pass if hasattr(settings, 'smtp_pass') else ""
    report_email = settings.report_email if hasattr(settings, 'report_email') else ""

    if not all([smtp_host, smtp_user, smtp_pass, report_email]):
        # Fallback: save to disk when SMTP not configured
        import os
        import json
        email_log = os.path.join("data", "email_queue.json")
        os.makedirs("data", exist_ok=True)
        entry = {
            "subject": req.subject,
            "body": req.body,
            "reports": req.reports,
            "timestamp": datetime.utcnow().isoformat(),
            "status": "queued_no_smtp",
        }
        existing = []
        if os.path.exists(email_log):
            with open(email_log, "r") as f:
                existing = json.load(f)
        existing.append(entry)
        with open(email_log, "w") as f:
            json.dump(existing, f, indent=2)
        return {"status": "queued", "message": "SMTP not configured. Report saved to queue."}

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = req.subject
        msg["From"] = smtp_user
        msg["To"] = report_email

        # Build HTML body
        html_parts = [
            "<html><body>",
            f"<h2>{req.subject}</h2>",
        ]
        if req.body:
            from html import escape as html_escape
            html_parts.append(f"<p>{html_escape(str(req.body))}</p>")
        if req.reports:
            from html import escape as html_escape
            html_parts.append("<table border='1' cellpadding='8' cellspacing='0' style='border-collapse:collapse;'>")
            html_parts.append("<tr><th>Part Number</th><th>Reason</th><th>Session</th><th>Time</th></tr>")
            for r in req.reports:
                html_parts.append(
                    f"<tr><td>{html_escape(str(r.get('part_number','')))}</td>"
                    f"<td>{html_escape(str(r.get('reason','')))}</td>"
                    f"<td>{html_escape(str(r.get('session_id',''))[:8])}</td>"
                    f"<td>{html_escape(str(r.get('timestamp','')))}</td></tr>"
                )
            html_parts.append("</table>")
        html_parts.append("<br><p><em>— Enpro Filtration Mastermind</em></p></body></html>")

        msg.attach(MIMEText("\n".join(html_parts), "html"))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, [report_email], msg.as_string())

        return {"status": "sent", "to": report_email}
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return {"status": "error", "detail": str(e)}


class QuoteRequest(BaseModel):
    company: str = Field(default="", max_length=200)
    contact_name: str = Field(default="", max_length=200)
    contact_email: str = Field(default="", max_length=200)
    contact_phone: str = Field(default="", max_length=50)
    ship_to: str = Field(default="", max_length=500)
    items: list = Field(default_factory=list)
    notes: str = Field(default="", max_length=2000)
    session_id: str = Field(default="", max_length=100)


@app.post("/api/quote")
async def save_quote(req: QuoteRequest):
    """Save a quote draft. Optionally emails to Peter."""
    import os
    import json

    merged = merge_into_quote_request(
        req.session_id,
        {
            "company": req.company,
            "contact_name": req.contact_name,
            "contact_email": req.contact_email,
            "contact_phone": req.contact_phone,
            "ship_to": req.ship_to,
            "items": req.items,
            "notes": req.notes,
        },
    )

    quote = {
        "id": f"Q-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "company": merged.get("company", ""),
        "contact_name": merged.get("contact_name", ""),
        "contact_email": merged.get("contact_email", ""),
        "contact_phone": merged.get("contact_phone", ""),
        "ship_to": merged.get("ship_to", ""),
        "items": merged.get("items", []),
        "notes": merged.get("notes", ""),
        "session_id": req.session_id,
        "timestamp": datetime.utcnow().isoformat(),
        "status": "draft",
    }

    quotes_file = os.path.join("data", "quotes.json")
    os.makedirs("data", exist_ok=True)
    existing = []
    if os.path.exists(quotes_file):
        with open(quotes_file, "r") as f:
            existing = json.load(f)
    existing.append(quote)
    with open(quotes_file, "w") as f:
        json.dump(existing, f, indent=2)

    logger.info(f"Quote saved: {quote['id']}")
    return {"status": "saved", "quote": quote}


# ---------------------------------------------------------------------------
# Voice Search Endpoints
# ---------------------------------------------------------------------------

def _whisper_endpoint() -> str:
    """Build Azure Whisper endpoint. Uses dedicated Whisper resource, falls back to main."""
    base = settings.AZURE_WHISPER_ENDPOINT or settings.AZURE_OPENAI_ENDPOINT
    return (
        f"{base.rstrip('/')}/openai/deployments/"
        f"{settings.AZURE_WHISPER_DEPLOYMENT}/audio/transcriptions"
        f"?api-version={settings.AZURE_WHISPER_API_VERSION}"
    )


def _whisper_key() -> str:
    """Whisper API key. Uses dedicated key, falls back to main."""
    return settings.AZURE_WHISPER_KEY or settings.AZURE_OPENAI_KEY


async def _transcribe(audio_bytes: bytes, filename: str, content_type: str) -> str:
    """Transcribe audio via Azure Whisper. Returns transcript text or raises."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            _whisper_endpoint(),
            headers={"api-key": _whisper_key()},
            files={
                "file": (filename, audio_bytes, content_type),
                "response_format": (None, "json"),
                "language": (None, "en"),
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("text", "").strip()


@app.post("/api/stt")
async def stt(file: UploadFile = File(...)):
    """Speech-to-text via Azure Whisper. Accepts audio blob from MediaRecorder."""
    if not _whisper_key():
        return JSONResponse(status_code=500, content={"error": "Azure Whisper not configured."})

    audio_bytes = await file.read()
    if not audio_bytes:
        return JSONResponse(status_code=400, content={"error": "Empty audio file."})
    if len(audio_bytes) > 25 * 1024 * 1024:
        return JSONResponse(status_code=413, content={"error": "Audio file too large (max 25MB)."})

    try:
        transcript = await _transcribe(
            audio_bytes, file.filename or "audio.webm", file.content_type or "audio/webm"
        )
        logger.info(f"STT transcript: '{transcript}'")
        return {"text": transcript}

    except httpx.HTTPStatusError as e:
        logger.error(f"Azure Whisper error: {e.response.text}")
        return JSONResponse(
            status_code=e.response.status_code,
            content={"error": f"Azure Whisper error: {e.response.text}"},
        )
    except Exception as e:
        logger.error(f"STT failed: {e}")
        return JSONResponse(status_code=500, content={"error": f"STT failed: {str(e)}"})


@app.post("/api/voice-search")
@limiter.limit("15/minute")
async def voice_search(request: Request, file: UploadFile = File(...)):
    """
    Full voice search pipeline: audio → STT → pre-process → extract → fuzzy resolve → search.
    Accepts audio blob, returns product results with confidence metadata.
    """
    if not state.data_loaded:
        return JSONResponse(status_code=503, content={"error": "Data not loaded."})

    if not _whisper_key():
        return JSONResponse(status_code=500, content={"error": "Azure Whisper not configured."})

    audio_bytes = await file.read()
    if not audio_bytes:
        return JSONResponse(status_code=400, content={"error": "Empty audio file."})
    if len(audio_bytes) > 25 * 1024 * 1024:  # 25MB limit
        return JSONResponse(status_code=413, content={"error": "Audio file too large (max 25MB)."})

    try:
        transcript = await _transcribe(
            audio_bytes, file.filename or "audio.webm", file.content_type or "audio/webm"
        )
        if not transcript:
            return {"results": [], "total_found": 0, "transcript": "", "error": "Could not transcribe audio."}

    except Exception as e:
        logger.error(f"Voice search STT failed: {e}")
        return JSONResponse(status_code=500, content={"error": f"STT failed: {str(e)}"})

    # Step 2: Run the voice search pipeline
    try:
        result = await voice_search_pipeline(transcript, state.df)
        return result
    except Exception as e:
        logger.error(f"Voice search pipeline error: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Voice search failed: {str(e)}", "transcript": transcript},
        )


@app.post("/api/voice-search-text")
async def voice_search_text(request: Request, req: ChatRequest):
    """
    Voice search pipeline from text (for testing without mic).
    Same pipeline as voice-search but skips STT. History-aware: persists
    transcript turn into the user's 7-day memory so voice queries are
    visible in future coreference upgrades.
    """
    if not state.data_loaded:
        return JSONResponse(status_code=503, content={"error": "Data not loaded."})

    user_id, _history = await _chat_auth_and_history(request)

    try:
        result = await voice_search_pipeline(req.message, state.df)
    except Exception as e:
        logger.error(f"Voice search text error: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Voice search failed: {str(e)}"},
        )

    # Render a compact assistant summary so the memory layer captures something
    # meaningful (not just the empty pipeline metadata). The summary includes
    # an ISO timestamp suffix so a legitimate retry of the same query within
    # the dedup window doesn't silently collide on identical content.
    products = result.get("results") or []
    total = result.get("total_found", len(products))
    if products:
        first = products[0] if isinstance(products[0], dict) else {}
        first_pn = first.get("Part_Number") or first.get("Alt_Code") or "(unknown)"
        summary = f"Voice search returned {total} results. Top: {first_pn}."
    else:
        summary = "Voice search returned no matches."
    summary += f" [ts:{datetime.utcnow().isoformat(timespec='seconds')}]"

    await _persist_turn(user_id, req.message, summary, products=products)
    return result


# ---------------------------------------------------------------------------
# Voice Echo Endpoints — Predictive Pre-Fetch System
# ---------------------------------------------------------------------------

class VoiceEchoRequest(BaseModel):
    query: str = Field(max_length=500)
    session_id: str = "default"
    defer: bool = False


class VoiceEchoStatusRequest(BaseModel):
    session_id: str = "default"


@app.post("/api/voice-echo")
async def voice_echo_endpoint(req: VoiceEchoRequest):
    """
    Voice Echo — Predictive voice search with accuracy grading and deferred responses.
    
    For deep queries (specs, manufacturer, crosswalk), set defer=True to get
    "Give me a second while I look that up..." immediately, then poll /voice-echo-status
    for the echo response (~10 seconds later).
    """
    if not state.data_loaded or not state.voice_echo:
        return JSONResponse(
            status_code=503, 
            content={"error": "Voice Echo not initialized. Data loading..."}
        )
    
    try:
        # Determine if this is a deep query that should be deferred
        is_deep = state.voice_echo._is_deep_query(req.query)
        should_defer = req.defer and is_deep
        
        # Process query
        response, grade = state.voice_echo.query(
            req.query, 
            defer=should_defer
        )
        
        return {
            "response": response,
            "accuracy_pct": grade.accuracy_pct,
            "match_type": grade.match_type,
            "products_found": grade.products_found,
            "deferred": grade.match_type == "deferred",
            "echo_ready": False if grade.match_type == "deferred" else True,
            "session_id": req.session_id,
        }
    except Exception as e:
        logger.error(f"Voice Echo error: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Voice Echo failed: {str(e)}"},
        )


@app.post("/api/voice-echo-next")
async def voice_echo_next(req: VoiceEchoRequest):
    """
    Get next echo prediction (when user says 'next' or 'what else').
    Returns the next best prediction from the echo cache.
    """
    if not state.data_loaded or not state.voice_echo:
        return JSONResponse(
            status_code=503, 
            content={"error": "Voice Echo not initialized."}
        )
    
    try:
        echo_response = state.voice_echo.next_echo(req.query)
        return {
            "response": echo_response,
            "session_id": req.session_id,
        }
    except Exception as e:
        logger.error(f"Voice Echo next error: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed: {str(e)}"},
        )


@app.get("/api/voice-echo-status")
async def voice_echo_status(session_id: str = "default"):
    """
    Get Voice Echo system status — accuracy stats, cache size, learned patterns.
    """
    if not state.voice_echo:
        return {"status": "initializing"}
    
    return state.voice_echo.get_stats()


@app.get("/api/voice-echo-cache")
async def voice_echo_cache():
    """
    Get current echo cache contents (for debugging).
    """
    if not state.voice_echo:
        return {"cache": []}
    
    cache_items = []
    for key, echo in state.voice_echo.echo_cache.items():
        cache_items.append({
            "query": key,
            "confidence": echo.confidence,
            "products": len(echo.products),
            "source": echo.source_query,
        })
    
    return {
        "cache_size": len(cache_items),
        "cache": sorted(cache_items, key=lambda x: -x["confidence"])[:20]
    }


@app.post("/api/voice-echo-learn")
async def voice_echo_learn(req: VoiceEchoRequest):
    """
    Manually trigger pattern learning (query A -> query B).
    Used when user follows up with a related query.
    """
    if not state.voice_echo:
        return JSONResponse(status_code=503, content={"error": "Not initialized"})
    
    # This would typically be called internally when we detect a follow-up
    # For now, just acknowledge
    return {"status": "learning", "query": req.query}


# ---------------------------------------------------------------------------
# Widget
# ---------------------------------------------------------------------------

@app.get("/widget.js")
async def widget_js():
    """
    Embeddable JavaScript widget for third-party sites.
    Drop <script src="https://your-domain/widget.js"></script> on any page.
    """
    js = """
(function() {
    'use strict';

    var FM_API = window.FM_API_URL || (document.currentScript && document.currentScript.src.replace('/widget.js', '')) || '';

    // Create chat widget container
    var container = document.createElement('div');
    container.id = 'enpro-fm-widget';
    container.innerHTML = `
        <div id="fm-toggle" style="position:fixed;bottom:20px;right:20px;width:60px;height:60px;background:#1a56db;border-radius:50%;cursor:pointer;box-shadow:0 4px 12px rgba(0,0,0,0.3);display:flex;align-items:center;justify-content:center;z-index:10000;transition:transform 0.2s;">
            <svg width="28" height="28" fill="white" viewBox="0 0 24 24"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H6l-2 2V4h16v12z"/></svg>
        </div>
        <div id="fm-chat" style="display:none;position:fixed;bottom:90px;right:20px;width:380px;max-height:520px;background:white;border-radius:12px;box-shadow:0 8px 32px rgba(0,0,0,0.2);z-index:10000;font-family:-apple-system,BlinkMacSystemFont,sans-serif;overflow:hidden;">
            <div style="background:#1a56db;color:white;padding:14px 16px;font-weight:600;font-size:15px;">Enpro Filtration Mastermind</div>
            <div id="fm-messages" style="height:360px;overflow-y:auto;padding:12px;"></div>
            <div style="padding:8px 12px;border-top:1px solid #e5e7eb;display:flex;gap:8px;">
                <input id="fm-input" type="text" placeholder="Ask about filters..." style="flex:1;padding:8px 12px;border:1px solid #d1d5db;border-radius:8px;font-size:14px;outline:none;" />
                <button id="fm-send" style="background:#1a56db;color:white;border:none;border-radius:8px;padding:8px 14px;cursor:pointer;font-size:14px;">Send</button>
            </div>
        </div>
    `;
    document.body.appendChild(container);

    // Toggle chat
    document.getElementById('fm-toggle').onclick = function() {
        var chat = document.getElementById('fm-chat');
        chat.style.display = chat.style.display === 'none' ? 'block' : 'none';
    };

    // Send message
    function sendMessage() {
        var input = document.getElementById('fm-input');
        var msg = input.value.trim();
        if (!msg) return;
        addMessage(msg, 'user');
        input.value = '';

        fetch(FM_API + '/api/chat', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({message: msg, session_id: 'widget_' + Date.now()})
        })
        .then(function(r) { return r.json(); })
        .then(function(data) { addMessage(data.response || 'No response.', 'bot'); })
        .catch(function() { addMessage('Connection error. Try again.', 'bot'); });
    }

    function addMessage(text, sender) {
        var div = document.createElement('div');
        div.style.cssText = 'margin-bottom:10px;padding:8px 12px;border-radius:8px;max-width:85%;word-wrap:break-word;font-size:14px;line-height:1.4;' +
            (sender === 'user' ? 'background:#e8f0fe;margin-left:auto;' : 'background:#f3f4f6;');
        div.textContent = text;
        document.getElementById('fm-messages').appendChild(div);
        document.getElementById('fm-messages').scrollTop = 999999;
    }

    document.getElementById('fm-send').onclick = sendMessage;
    document.getElementById('fm-input').onkeypress = function(e) { if (e.key === 'Enter') sendMessage(); };

    // Welcome message
    addMessage('Welcome to the Enpro Filtration Mastermind! Ask me about any filter, part number, or application.', 'bot');
})();
""".strip()
    return PlainTextResponse(content=js, media_type="application/javascript")


# Static file serving for frontend (mount last so API routes take priority)
try:
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
except Exception:
    logger.warning("Static directory not found — frontend serving disabled")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
