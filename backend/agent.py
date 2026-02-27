"""
agent.py — Skylark BI Agent (Groq backend)

Uses Groq's OpenAI-compatible API with function/tool calling.
Model: llama-3.3-70b-versatile (best free reasoning + tool use on Groq)
Fallback: llama3-groq-70b-8192-tool-use-preview (dedicated tool-use model)

Groq tool calling format (OpenAI-compatible):
  tools = [{"type": "function", "function": {name, description, parameters}}]
  Response: choice.message.tool_calls[].function.{name, arguments}
  Then inject: {"role": "tool", "tool_call_id": ..., "content": result}
"""

import json
import os
from datetime import datetime
from typing import Optional

from groq import Groq

from monday_client import MondayClient
from data_cleaner import (
    clean_deals, clean_workorders, quality_report,
    fmt_inr, parse_number,
)

# ── Model selection ───────────────────────────────────────────────────────
# llama-3.3-70b-versatile: best reasoning, supports tool calling, 128K ctx
# Use tool-use-preview as fallback if needed
MODEL = "llama-3.3-70b-versatile"

# ── Tool schema (OpenAI function-calling format) ──────────────────────────
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_board_items",
            "description": (
                "Fetch ALL live items from a Monday.com board. "
                "Use board='deals' for the Deal Funnel pipeline, "
                "or board='workorders' for the Work Order Tracker. "
                "Makes a LIVE API call every time — no caching. "
                "Returns cleaned, normalised rows as JSON."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "board": {
                        "type": "string",
                        "enum": ["deals", "workorders"],
                        "description": "Which board to fetch from Monday.com"
                    },
                    "reason": {
                        "type": "string",
                        "description": "Why you need this data (shown in trace)"
                    }
                },
                "required": ["board"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_board_columns",
            "description": (
                "Get column definitions (id, title, type) for a board. "
                "Call this to discover what fields exist before querying items."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "board": {
                        "type": "string",
                        "enum": ["deals", "workorders"]
                    }
                },
                "required": ["board"]
            }
        }
    },
]

# ── System prompt ─────────────────────────────────────────────────────────
SYSTEM = """\
You are a senior BI analyst for Skylark Drones, an enterprise drone-services company.
You answer founder-level business questions using LIVE data from two Monday.com boards.

BOARD 1 — Deal Funnel (deals):
  Fields: deal_name, owner_code, client_code, status (Open/Won/Dead/On Hold),
          deal_value (₹ masked), stage (A–O lettered), sector, closure_probability
          (High/Medium/Low), created_date, tentative_close_date
  Sectors: Mining, Powerline, Renewables, Railways, Construction, Others, DSP,
           Tender, Aviation, Security and Surveillance, Manufacturing

BOARD 2 — Work Order Tracker (workorders):
  Fields: deal_name, customer_code, serial_no, execution_status
          (Completed/Ongoing/Not Started/…), sector, type_of_work,
          amount_excl_gst, amount_incl_gst, billed_incl_gst, collected, receivable,
          billing_status, wo_status, personnel_code

## Rules
1. Always call get_board_items BEFORE answering any quantitative question.
2. Use get_board_columns when you need to discover field names first.
3. Parse numbers by stripping ₹ / commas. Treat null/missing as 0 when aggregating.
4. Normalise sectors case-insensitively.
5. State exactly how many records you analysed.
6. Give SPECIFIC numbers (₹ values, counts, %, rankings).
7. Always end with a **⚠ Data Quality** section noting missing/ambiguous values.
   If data is clean, write: "Data quality: no critical issues found."
8. CLARIFYING QUESTIONS: If a query is genuinely ambiguous, ask ONE focused question
   formatted as: "🤔 Quick clarification: [question]"
   If the query is clear, fetch data immediately without asking.
9. Use conversation history for follow-up context.
10. CRITICAL — NEVER HALLUCINATE: If a tool returns an error, respond with:
    "❌ Could not fetch data: [error]. Please verify your board ID and API token."
    DO NOT make up numbers or say "Let's assume we analyzed X records."
"""


# ── Tool executor ─────────────────────────────────────────────────────────

async def _run_tool(
    name: str,
    args: dict,
    monday_key: str,
    deals_board_id: str,
    wo_board_id: str,
) -> dict:
    board_map = {"deals": deals_board_id, "workorders": wo_board_id}
    client = MondayClient(api_key=monday_key)

    if name == "get_board_columns":
        return await client.get_columns(board_map[args["board"]])

    if name == "get_board_items":
        board_id = board_map[args["board"]]
        raw = await client.get_all_items(board_id)
        if args["board"] == "deals":
            df   = clean_deals(raw)
            rows = df.where(df.notna(), None).to_dict(orient="records")
        else:
            df   = clean_workorders(raw)
            rows = df.where(df.notna(), None).to_dict(orient="records")
        
        # Truncate to avoid Groq TPM limits (12k tokens max)
        truncated_rows = rows[:30]
        return {
            "board": args["board"], 
            "total_rows": len(rows), 
            "returned_rows": len(truncated_rows),
            "note": "Output truncated to 30 rows due to API token limits",
            "rows": truncated_rows
        }

    return {"error": f"Unknown tool: {name}"}


# ── Agent ─────────────────────────────────────────────────────────────────

class BIAgent:
    def __init__(
        self,
        groq_key: str,
        monday_key: str,
        deals_board_id: str,
        wo_board_id: str,
    ):
        self.client        = Groq(api_key=groq_key)
        self.monday_key    = monday_key
        self.deals_id      = deals_board_id
        self.wo_id         = wo_board_id

    async def query(self, user_message: str, history: list[dict]) -> dict:
        """
        Agentic loop using Groq function calling.
        Returns {answer, trace, quality}.
        """
        trace: list[dict] = []
        fetched_deals_df = None
        fetched_wo_df    = None

        # Build messages in OpenAI format
        messages = [{"role": "system", "content": SYSTEM}]
        for h in history:
            messages.append({"role": h["role"], "content": h["content"]})
        messages.append({"role": "user", "content": user_message})

        MAX_LOOPS = 8  # prevent infinite tool loops
        loop_count = 0

        while loop_count < MAX_LOOPS:
            loop_count += 1

            # ── Call Groq ────────────────────────────────────────────────
            response = self.client.chat.completions.create(
                model=MODEL,
                messages=messages,
                tools=TOOLS,
                tool_choice="auto",
                max_tokens=4096,
                temperature=0.2,
            )

            msg     = response.choices[0].message
            finish  = response.choices[0].finish_reason

            # ── No tool calls → final answer ─────────────────────────────
            if finish == "stop" or not msg.tool_calls:
                answer = msg.content or "⚠️ No response generated."
                trace.append(_event("answer", {"text": answer}, "Groq→Agent"))

                quality = None
                if fetched_deals_df is not None or fetched_wo_df is not None:
                    import pandas as pd
                    d_df = fetched_deals_df   if fetched_deals_df is not None else pd.DataFrame()
                    w_df = fetched_wo_df      if fetched_wo_df    is not None else pd.DataFrame()
                    quality = quality_report(d_df, w_df)

                return {"answer": answer, "trace": trace, "quality": quality}

            # ── Execute each tool call ────────────────────────────────────
            # Add assistant message with tool_calls to history
            messages.append({
                "role":       "assistant",
                "content":    msg.content or "",
                "tool_calls": [
                    {
                        "id":       tc.id,
                        "type":     "function",
                        "function": {
                            "name":      tc.function.name,
                            "arguments": tc.function.arguments,
                        }
                    }
                    for tc in msg.tool_calls
                ]
            })

            for tc in msg.tool_calls:
                fn_name = tc.function.name
                try:
                    args = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    args = {}

                trace.append(_event("tool_call", {
                    "tool":   fn_name,
                    "board":  args.get("board", "—"),
                    "reason": args.get("reason", ""),
                    "input":  args,
                }, "Agent→Monday.com"))

                try:
                    result = await _run_tool(
                        fn_name, args,
                        self.monday_key,
                        self.deals_id,
                        self.wo_id,
                    )

                    # Trace + capture DataFrames for quality report
                    if fn_name == "get_board_items" and "rows" in result:
                        board = args.get("board")
                        rows  = result["rows"]
                        if board == "deals":
                            import pandas as pd
                            fetched_deals_df = clean_deals(rows) if rows else pd.DataFrame()
                        elif board == "workorders":
                            import pandas as pd
                            fetched_wo_df = clean_workorders(rows) if rows else pd.DataFrame()

                        trace.append(_event("tool_result", {
                            "tool":         fn_name,
                            "board":        board,
                            "rows_fetched": result["total_rows"],
                            "columns":      list(rows[0].keys()) if rows else [],
                            "sample":       rows[:2],
                        }, "Monday.com→Agent"))

                    elif fn_name == "get_board_columns" and "columns" in result:
                        trace.append(_event("tool_result", {
                            "tool":          fn_name,
                            "board":         args.get("board"),
                            "board_name":    result.get("board_name"),
                            "columns_found": len(result["columns"]),
                            "columns":       result["columns"],
                        }, "Monday.com→Agent"))

                    else:
                        trace.append(_event("tool_result", {
                            "tool":   fn_name,
                            "result": result,
                        }, "Monday.com→Agent"))

                    result_str = json.dumps(result, default=str)

                except Exception as exc:
                    err_msg = str(exc)
                    result_str = json.dumps({"error": err_msg})
                    trace.append(_event("tool_error", {
                        "tool":  fn_name,
                        "error": err_msg,
                    }, "Error"))

                # Inject tool result back into message history
                messages.append({
                    "role":         "tool",
                    "tool_call_id": tc.id,
                    "content":      result_str,
                })

        # Safety: if loop limit hit
        return {
            "answer": "⚠️ Agent loop limit reached. Please try a more specific question.",
            "trace":  trace,
            "quality": None,
        }


def _event(kind: str, content: dict, source: str) -> dict:
    return {
        "type":      kind,
        "source":    source,
        "content":   content,
        "timestamp": datetime.now().isoformat(),
    }
