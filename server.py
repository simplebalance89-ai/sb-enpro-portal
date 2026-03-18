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
async def suggest(q: str = "", mode: str = "exact"):
    """Typeahead suggestions for part number lookup. Pandas only, $0 cost.
    mode: exact (default), starts_with, contains
    """
    if not state.data_loaded or len(q) < 2:
        return {"suggestions": []}

    suggestions = suggest_parts(state.df, q, max_results=10, mode=mode)
    return {"suggestions": suggestions}


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
