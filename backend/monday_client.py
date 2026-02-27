"""
monday_client.py
Robust Monday.com GraphQL v2 client.

Key fix: 'title' is NOT a valid field on column_values in Monday API 2024-10+.
We fetch column titles separately via the board columns query, then map by id.
"""

import os
import json
import re
import httpx
from typing import Optional
from datetime import datetime

MONDAY_API_URL = "https://api.monday.com/v2"
API_VERSION    = "2024-10"   # latest stable — fixes "Cannot query field title" error


# ── Normalisation helpers ─────────────────────────────────────────────────

def _extract_text(col: dict) -> Optional[str]:
    """Pull the best human-readable string from a column_values entry."""
    text = col.get("text")
    if text and str(text).strip():
        return str(text).strip()

    raw = col.get("value")
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                for key in ("text", "name", "label", "display_value"):
                    v = parsed.get(key)
                    if v and str(v).strip():
                        return str(v).strip()
            elif isinstance(parsed, (int, float)):
                return str(parsed)
            elif isinstance(parsed, str) and parsed.strip():
                return parsed.strip()
        except (json.JSONDecodeError, TypeError):
            pass

    return None


def _row_from_item(item: dict, col_id_to_title: dict) -> dict:
    """Convert a raw Monday.com item → clean flat dict using column title map."""
    row = {
        "_id":   item.get("id"),
        "_name": (item.get("name") or "").strip(),
    }
    for col in item.get("column_values", []):
        col_id = col.get("id", "unknown")
        # Use the pre-fetched title map; fall back to id if missing
        title = col_id_to_title.get(col_id, col_id)
        text  = _extract_text(col)

        if text is None or str(text).strip() in ("", "—", "-", "null", "None"):
            row[title] = None
        else:
            row[title] = text

    return row


# ── Client ───────────────────────────────────────────────────────────────

class MondayClient:
    """Async Monday.com GraphQL client — compatible with API version 2024-10."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("MONDAY_API_KEY", "")
        if not self.api_key:
            raise ValueError("Monday API key is required")
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type":  "application/json",
            "API-Version":   API_VERSION,
        }

    async def _gql(self, query: str, variables: dict | None = None) -> dict:
        """Execute a GraphQL query. Raises RuntimeError on API errors."""
        payload: dict = {"query": query}
        if variables:
            payload["variables"] = variables

        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.post(MONDAY_API_URL, json=payload, headers=self.headers)

        # Surface HTTP errors with body context
        if resp.status_code != 200:
            raise RuntimeError(
                f"Monday.com HTTP {resp.status_code}: {resp.text[:300]}"
            )

        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"Monday.com API error: {json.dumps(data['errors'])}")

        return data.get("data", {})

    # ── Column schema ───────────────────────────────────────────────────

    async def get_columns(self, board_id: str) -> dict:
        """Return board name + columns list (id, title, type)."""
        q = """
        query ($b: ID!) {
          boards(ids: [$b]) {
            name
            columns { id title type }
          }
        }"""
        data = await self._gql(q, {"b": board_id})
        boards = data.get("boards", [])
        if not boards:
            return {"error": f"Board {board_id} not found or not accessible"}
        return {
            "board_name": boards[0]["name"],
            "columns":    boards[0]["columns"],
        }

    async def _fetch_col_map(self, board_id: str) -> dict[str, str]:
        """Return {col_id: col_title} for a board."""
        result = await self.get_columns(board_id)
        if "error" in result:
            return {}
        return {c["id"]: c["title"] for c in result.get("columns", [])}

    # ── Item fetching with cursor pagination ────────────────────────────

    async def get_all_items(self, board_id: str, page_size: int = 100) -> list[dict]:
        """
        Fetch ALL items from a board using cursor pagination.
        Fetches column titles first to map col_id → readable title.
        Returns list of clean flat dicts.
        """
        # Step 1: get column id→title map
        col_map = await self._fetch_col_map(board_id)

        all_rows: list[dict] = []
        cursor: Optional[str] = None

        while True:
            if cursor:
                # Pagination continuation — NOTE: no 'title' in column_values
                q = """
                query ($limit: Int!, $cursor: String!) {
                  next_items_page(limit: $limit, cursor: $cursor) {
                    cursor
                    items {
                      id
                      name
                      column_values { id text value }
                    }
                  }
                }"""
                data = await self._gql(q, {"limit": page_size, "cursor": cursor})
                page = data.get("next_items_page", {})
            else:
                q = """
                query ($b: ID!, $limit: Int!) {
                  boards(ids: [$b]) {
                    items_page(limit: $limit) {
                      cursor
                      items {
                        id
                        name
                        column_values { id text value }
                      }
                    }
                  }
                }"""
                data = await self._gql(q, {"b": board_id, "limit": page_size})
                boards = data.get("boards", [])
                if not boards:
                    raise RuntimeError(
                        f"Board {board_id} returned no data. "
                        "Check the board ID and that your API token has access to it."
                    )
                page = boards[0].get("items_page", {})

            items = page.get("items", [])
            all_rows.extend(_row_from_item(it, col_map) for it in items)
            cursor = page.get("cursor")

            if not cursor or len(items) < page_size:
                break

        return all_rows
