"""
main.py — Skylark BI Agent (Groq)
FastAPI backend. Serves frontend + exposes /api/query.

Credential resolution order:
  1. HTTP headers (X-Monday-Key, X-Deals-Board, X-Wo-Board)
  2. Environment variables
"""

import os
import sys
from typing import Optional

# Ensure that local modules within backend/ can be imported directly
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import httpx
from groq import AuthenticationError as GroqAuthError, RateLimitError as GroqRateLimit
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

app = FastAPI(title="Skylark BI Agent (Groq)", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Models ────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    message: str
    history: list = []


# ── Credential helpers ────────────────────────────────────────────────────

def _resolve(header_val: Optional[str], env_key: str) -> str:
    return (header_val or "").strip() or os.getenv(env_key, "").strip()


# ── Routes ────────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health(
    x_monday_key:  Optional[str] = Header(None),
    x_deals_board: Optional[str] = Header(None),
    x_wo_board:    Optional[str] = Header(None),
):
    return {
        "status":               "ok",
        "monday_configured":    bool(_resolve(x_monday_key,  "MONDAY_API_KEY")),
        "groq_configured":      bool(os.getenv("GROQ_API_KEY", "")),
        "deals_board":          _resolve(x_deals_board, "DEALS_BOARD_ID") or "not set",
        "wo_board":             _resolve(x_wo_board,    "WORKORDERS_BOARD_ID") or "not set",
    }


@app.get("/api/verify-boards")
async def verify_boards(
    x_monday_key:  Optional[str] = Header(None),
    x_deals_board: Optional[str] = Header(None),
    x_wo_board:    Optional[str] = Header(None),
):
    """Test both board IDs are accessible and return board names."""
    monday_key     = _resolve(x_monday_key,  "MONDAY_API_KEY")
    deals_board_id = _resolve(x_deals_board, "DEALS_BOARD_ID")
    wo_board_id    = _resolve(x_wo_board,    "WORKORDERS_BOARD_ID")

    if not monday_key:
        return {"deals": "error: no API key", "workorders": "error: no API key"}

    from monday_client import MondayClient
    client = MondayClient(api_key=monday_key)
    results = {}

    for name, board_id in [("deals", deals_board_id), ("workorders", wo_board_id)]:
        if not board_id:
            results[name] = "error: board ID not set"
            continue
        try:
            info = await client.get_columns(board_id)
            if "error" in info:
                results[name] = f"error: {info['error']}"
            else:
                results[name] = f"ok: {info['board_name']} ({len(info['columns'])} columns)"
        except Exception as e:
            results[name] = f"error: {str(e)[:150]}"

    return results


@app.post("/api/query")
async def query(
    req: QueryRequest,
    x_monday_key:  Optional[str] = Header(None),
    x_deals_board: Optional[str] = Header(None),
    x_wo_board:    Optional[str] = Header(None),
):
    monday_key     = _resolve(x_monday_key,  "MONDAY_API_KEY")
    groq_key       = os.getenv("GROQ_API_KEY", "")
    deals_board_id = _resolve(x_deals_board, "DEALS_BOARD_ID")
    wo_board_id    = _resolve(x_wo_board,    "WORKORDERS_BOARD_ID")

    if not monday_key:
        raise HTTPException(400, "Monday.com API key is required")
    if not groq_key:
        raise HTTPException(500, "GROQ_API_KEY is not set on the server")
    if not deals_board_id or not wo_board_id:
        raise HTTPException(400, "Both board IDs are required (Deals + Work Orders)")

    from agent import BIAgent
    agent = BIAgent(
        groq_key       = groq_key,
        monday_key     = monday_key,
        deals_board_id = deals_board_id,
        wo_board_id    = wo_board_id,
    )

    try:
        result = await agent.query(req.message, req.history)
        return result
    except GroqAuthError:
        raise HTTPException(401, "Invalid Groq API key — check GROQ_API_KEY")
    except GroqRateLimit:
        raise HTTPException(429, "Groq rate limit hit — please wait a moment and retry")
    except httpx.TimeoutException:
        raise HTTPException(504, "Monday.com API timed out — try again")
    except Exception as exc:
        raise HTTPException(500, str(exc))


# ── Serve frontend ────────────────────────────────────────────────────────
_fe = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(_fe):
    app.mount("/", StaticFiles(directory=_fe, html=True), name="frontend")
