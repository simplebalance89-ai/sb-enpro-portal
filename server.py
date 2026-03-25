"""
EnPro Filtration Mastermind Portal — FastAPI Server
Main application with chat, lookup, search, chemical check, and widget endpoints.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import settings
from data_loader import load_static, load_inventory, load_chemicals, merge_data
from search import search_products, lookup_part, suggest_parts
from router import handle_message
from azure_client import health_check as azure_health_check, close_client
from governance import run_pre_checks

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
            inv = load_inventory()
            if not inv.empty:
                state.inventory_df = inv
                state.df = merge_data(state.static_df, state.inventory_df)
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
    logger.info("EnPro Filtration Mastermind Portal starting...")

    # Load all data
    try:
        state.static_df = load_static()
        state.inventory_df = load_inventory()
        state.chemicals_df = load_chemicals()
        state.df = merge_data(state.static_df, state.inventory_df)
        state.last_inventory_load = datetime.utcnow()
        state.data_loaded = True
        logger.info(
            f"Data loaded: {len(state.df)} products, "
            f"{len(state.chemicals_df)} chemical entries"
        )
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        state.data_loaded = False

    # Start background inventory refresh
    refresh_task = asyncio.create_task(_refresh_inventory_loop())

    yield

    # Shutdown
    refresh_task.cancel()
    await close_client()
    logger.info("EnPro Filtration Mastermind Portal stopped.")


# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="EnPro Filtration Mastermind Portal",
    version="1.0.0",
    description="AI-powered filtration product search, recommendation, and quote engine.",
    lifespan=lifespan,
)

# CORS — allow all origins for embed/widget use
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    message: str
    session_id: str = "default"
    mode: str = "standard"  # standard, demo, guided


class LookupRequest(BaseModel):
    part_number: str


class SearchRequest(BaseModel):
    query: str
    field: Optional[str] = None


class ChemicalRequest(BaseModel):
    chemical: str


class SuggestRequest(BaseModel):
    query: str


class ReportRequest(BaseModel):
    part_number: str
    reason: str = ""
    session_id: str = ""


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


@app.post("/api/chat")
async def chat(req: ChatRequest):
    """
    Main chat endpoint. Classifies intent, runs governance, routes to handler.
    """
    if not state.data_loaded:
        return JSONResponse(
            status_code=503,
            content={"error": "Data not loaded yet. Try again in a moment."},
        )

    try:
        result = await handle_message(
            message=req.message,
            session_id=req.session_id,
            mode=req.mode,
            df=state.df,
            chemicals_df=state.chemicals_df,
        )
        return result
    except Exception as e:
        logger.error(f"Chat error: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": "Something went wrong. Try again or contact EnPro directly.",
                "detail": str(e),
            },
        )


@app.post("/api/lookup")
async def lookup(req: LookupRequest):
    """Direct part number lookup — Pandas only, $0 cost."""
    if not state.data_loaded:
        return JSONResponse(status_code=503, content={"error": "Data not loaded."})

    product = lookup_part(state.df, req.part_number)
    if product:
        return {"found": True, "product": product}
    return {"found": False, "message": f"No product found for '{req.part_number}'."}


@app.post("/api/search")
async def search(req: SearchRequest):
    """Search products by query — Pandas only, $0 cost."""
    if not state.data_loaded:
        return JSONResponse(status_code=503, content={"error": "Data not loaded."})

    result = search_products(state.df, req.query, field=req.field)
    return result


@app.post("/api/chemical")
async def chemical_check(req: ChemicalRequest):
    """
    Chemical compatibility check. Routes through GPT-4.1 with chemical crosswalk context.
    """
    if not state.data_loaded:
        return JSONResponse(status_code=503, content={"error": "Data not loaded."})

    try:
        result = await handle_message(
            message=f"Chemical compatibility: {req.chemical}",
            session_id="chemical_check",
            mode="standard",
            df=state.df,
            chemicals_df=state.chemicals_df,
        )
        return result
    except Exception as e:
        logger.error(f"Chemical check error: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Chemical check failed.", "detail": str(e)},
        )


@app.get("/api/suggest")
async def suggest(q: str = "", mode: str = "exact", in_stock: str = "all"):
    """Typeahead suggestions for part number lookup. Pandas only, $0 cost.
    mode: exact (default), starts_with, contains
    in_stock: all (default), in_stock (only Qty > 0)
    """
    if not state.data_loaded or len(q) < 2:
        return {"suggestions": []}

    df = state.df
    if in_stock == "in_stock" and "Total_Stock" in df.columns:
        df = df[df["Total_Stock"] > 0]

    suggestions = suggest_parts(df, q, max_results=10, mode=mode)
    return {"suggestions": suggestions}


@app.get("/api/manufacturers/list")
async def manufacturers_list():
    """Return curated manufacturer list for dropdowns."""
    # Curated list of key filtration manufacturers — clean, no dupes, no junk
    CURATED_MANUFACTURERS = sorted([
        "AAF",
        "Accelerated Production",
        "AJR Filtration Inc.",
        "American Filter Manufacturing Inc.",
        "Amiad Filtration Systems",
        "Andronaco/Hills McCanna",
        "Andronaco/Pureflex",
        "Atlas Copco Compressors",
        "Banner Industries",
        "Brabazon",
        "Capsule Technologies",
        "Cobetter",
        "Critical Process Filtration Inc",
        "Edmac Compressor Parts",
        "Enpro, Incorporated",
        "Filtrox North America Inc.",
        "Filtrafine Corporation",
        "FTC-Filtration Technology",
        "Global Filter LLC",
        "Graver Technologies",
        "Industrial Technologies & Services Americas (PPC)",
        "Islip Flow Controls Inc.",
        "Johnson Filtration",
        "Jonell Filtration Group",
        "Koch Filter Corporation",
        "Le Sac Corporation",
        "M & M Control Service Inc",
        "McMaster-Carr Supply Co.",
        "National Oilwell Varco L.P.",
        "Pall",
        "Pall Trincor",
        "Pentair Filtration",
        "Porvair Filtration Group Inc",
        "PowerFlow Fluid Systems LLC",
        "Pulsafeeder Inc NY",
        "Quincy Compressor LLC",
        "Rosedale Products Inc",
        "Saint Gobain Performance",
        "Schroeder Industries",
        "Shelco Filters",
        "Sunsource",
        "Swift Filters Inc.",
        "United Filtration Systems",
        "Weber Scientific",
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


@app.get("/api/reports")
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
    part_number: str


@app.post("/api/compare-suggestions")
async def compare_suggestions(req: CompareSuggestRequest):
    """Find similar products grouped by category for smart comparison."""
    if not state.data_loaded:
        return JSONResponse(status_code=503, content={"error": "Data not loaded."})

    from search import find_similar_products
    result = find_similar_products(state.df, req.part_number)
    return result


class EmailReportRequest(BaseModel):
    subject: str = "EnPro FM Portal — Report"
    body: str = ""
    reports: list = []


@app.post("/api/email-report")
async def email_report(req: EmailReportRequest):
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
            html_parts.append(f"<p>{req.body}</p>")
        if req.reports:
            html_parts.append("<table border='1' cellpadding='8' cellspacing='0' style='border-collapse:collapse;'>")
            html_parts.append("<tr><th>Part Number</th><th>Reason</th><th>Session</th><th>Time</th></tr>")
            for r in req.reports:
                html_parts.append(
                    f"<tr><td>{r.get('part_number','')}</td>"
                    f"<td>{r.get('reason','')}</td>"
                    f"<td>{r.get('session_id','')[:8]}</td>"
                    f"<td>{r.get('timestamp','')}</td></tr>"
                )
            html_parts.append("</table>")
        html_parts.append("<br><p><em>— EnPro Filtration Mastermind</em></p></body></html>")

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
    company: str = ""
    contact_name: str = ""
    contact_email: str = ""
    contact_phone: str = ""
    ship_to: str = ""
    items: list = []  # [{part_number, description, quantity, price}]
    notes: str = ""
    session_id: str = ""


@app.post("/api/quote")
async def save_quote(req: QuoteRequest):
    """Save a quote draft. Optionally emails to Peter."""
    import os
    import json

    quote = {
        "id": f"Q-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "company": req.company,
        "contact_name": req.contact_name,
        "contact_email": req.contact_email,
        "contact_phone": req.contact_phone,
        "ship_to": req.ship_to,
        "items": req.items,
        "notes": req.notes,
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
            <div style="background:#1a56db;color:white;padding:14px 16px;font-weight:600;font-size:15px;">EnPro Filtration Mastermind</div>
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
    addMessage('Welcome to the EnPro Filtration Mastermind! Ask me about any filter, part number, or application.', 'bot');
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
