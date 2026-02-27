"""
scripts/monday_import.py
Import Deal_funnel_Data.xlsx and Work_Order_Tracker_Data.xlsx into Monday.com.

Usage:
    python scripts/monday_import.py \
        --api-key  YOUR_MONDAY_TOKEN \
        --workspace YOUR_WORKSPACE_ID

Prints board IDs to add to your .env at the end.
"""

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://api.monday.com/v2"


def gql(api_key: str, query: str, variables: dict | None = None) -> dict:
    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
        "API-Version": "2024-01",
    }
    r = requests.post(
        API_URL,
        json={"query": query, "variables": variables or {}},
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(f"Monday error: {data['errors']}")
    return data["data"]


def create_board(api_key: str, workspace_id: str, name: str) -> str:
    q = """
    mutation ($name: String!, $kind: BoardKind!, $ws: ID) {
      create_board(board_name: $name, board_kind: $kind, workspace_id: $ws) {
        id
      }
    }"""
    d = gql(api_key, q, {"name": name, "kind": "public", "ws": workspace_id})
    return d["create_board"]["id"]


def create_column(api_key: str, board_id: str, title: str, col_type: str) -> str | None:
    q = """
    mutation ($b: ID!, $title: String!, $type: ColumnType!) {
      create_column(board_id: $b, title: $title, column_type: $type) { id }
    }"""
    try:
        d = gql(api_key, q, {"b": board_id, "title": title, "type": col_type})
        return d["create_column"]["id"]
    except Exception as e:
        print(f"    ⚠ column '{title}': {e}")
        return None


def create_item(api_key: str, board_id: str, name: str, col_values: dict) -> bool:
    q = """
    mutation ($b: ID!, $name: String!, $cv: JSON!) {
      create_item(board_id: $b, item_name: $name, column_values: $cv) { id }
    }"""
    cv = {k: v for k, v in col_values.items() if v is not None}
    try:
        gql(api_key, q, {"b": board_id, "name": name[:255], "cv": json.dumps(cv)})
        return True
    except Exception as e:
        print(f"    ⚠ item '{name[:40]}': {e}")
        return False


def safe_str(v, maxlen: int = 255) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s[:maxlen] if s else None


def safe_date(v) -> dict | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    import re
    from datetime import datetime
    s = str(v).strip()[:10]
    for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y"]:
        try:
            return {"date": datetime.strptime(s, fmt).strftime("%Y-%m-%d")}
        except ValueError:
            pass
    return None


def safe_num(v) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    import re
    s = re.sub(r"[₹$€£,\s]", "", str(v)).strip()
    try:
        return str(float(s))
    except ValueError:
        return None


# ── Deals ──────────────────────────────────────────────────────────────────

DEALS_COLUMNS = [
    ("Owner Code",           "text"),
    ("Client Code",          "text"),
    ("Deal Status",          "status"),
    ("Close Date (Actual)",  "date"),
    ("Closure Probability",  "status"),
    ("Deal Value (₹ Masked)","numbers"),
    ("Tentative Close Date", "date"),
    ("Deal Stage",           "status"),
    ("Product",              "text"),
    ("Sector",               "text"),
    ("Created Date",         "date"),
]

def import_deals(api_key: str, workspace_id: str, path: str) -> str:
    print("\n📋 Creating 'Deal Funnel' board…")
    board_id = create_board(api_key, workspace_id, "Deal Funnel")
    print(f"   Board ID: {board_id}")

    print("   Creating columns…")
    col_ids: dict[str, str] = {}
    for title, ctype in DEALS_COLUMNS:
        cid = create_column(api_key, board_id, title, ctype)
        if cid:
            col_ids[title] = cid
        time.sleep(0.25)

    df = pd.read_excel(path)
    # Drop sentinel header rows
    for col, bad in [("Deal Status", "Deal Status"), ("Deal Stage", "Deal Stage"),
                     ("Sector/service", "Sector/service")]:
        if col in df.columns:
            df = df[df[col] != bad]
    df = df.reset_index(drop=True)
    print(f"   Importing {len(df)} deals…")

    ok = fail = 0
    for _, row in df.iterrows():
        cv: dict = {}
        def text(src, dst):
            v = safe_str(row.get(src))
            if v and dst in col_ids: cv[col_ids[dst]] = v
        def date(src, dst):
            v = safe_date(row.get(src))
            if v and dst in col_ids: cv[col_ids[dst]] = v
        def num(src, dst):
            v = safe_num(row.get(src))
            if v and dst in col_ids: cv[col_ids[dst]] = v
        def status(src, dst):
            v = safe_str(row.get(src))
            if v and dst in col_ids: cv[col_ids[dst]] = {"label": v}

        text("Owner code",         "Owner Code")
        text("Client Code",        "Client Code")
        status("Deal Status",      "Deal Status")
        date("Close Date (A)",     "Close Date (Actual)")
        status("Closure Probability", "Closure Probability")
        num("Masked Deal value",   "Deal Value (₹ Masked)")
        date("Tentative Close Date", "Tentative Close Date")
        status("Deal Stage",       "Deal Stage")
        text("Product deal",       "Product")
        text("Sector/service",     "Sector")
        date("Created Date",       "Created Date")

        name = safe_str(row.get("Deal Name")) or f"Deal_{_}"
        if create_item(api_key, board_id, name, cv):
            ok += 1
        else:
            fail += 1
        time.sleep(0.18)

    print(f"   ✅ {ok} imported | {fail} failed")
    return board_id


# ── Work Orders ────────────────────────────────────────────────────────────

WO_COLUMNS = [
    ("Customer Code",        "text"),
    ("Serial #",             "text"),
    ("Nature of Work",       "text"),
    ("Execution Status",     "status"),
    ("Data Delivery Date",   "date"),
    ("Date of PO/LOI",       "date"),
    ("Sector",               "text"),
    ("Type of Work",         "text"),
    ("Amount Excl GST (₹)",  "numbers"),
    ("Amount Incl GST (₹)",  "numbers"),
    ("Billed Incl GST (₹)",  "numbers"),
    ("Collected (₹)",        "numbers"),
    ("Receivable (₹)",       "numbers"),
    ("WO Status",            "status"),
    ("Billing Status",       "status"),
    ("Personnel Code",       "text"),
]

def import_workorders(api_key: str, workspace_id: str, path: str) -> str:
    print("\n📋 Creating 'Work Order Tracker' board…")
    board_id = create_board(api_key, workspace_id, "Work Order Tracker")
    print(f"   Board ID: {board_id}")

    print("   Creating columns…")
    col_ids: dict[str, str] = {}
    for title, ctype in WO_COLUMNS:
        cid = create_column(api_key, board_id, title, ctype)
        if cid:
            col_ids[title] = cid
        time.sleep(0.25)

    df = pd.read_excel(path, header=1)
    df = df.dropna(how="all").reset_index(drop=True)
    print(f"   Importing {len(df)} work orders…")

    ok = fail = 0
    for _, row in df.iterrows():
        cv: dict = {}
        def text(src, dst):
            v = safe_str(row.get(src))
            if v and dst in col_ids: cv[col_ids[dst]] = v
        def date(src, dst):
            v = safe_date(row.get(src))
            if v and dst in col_ids: cv[col_ids[dst]] = v
        def num(src, dst):
            v = safe_num(row.get(src))
            if v and dst in col_ids: cv[col_ids[dst]] = v
        def status(src, dst):
            v = safe_str(row.get(src))
            if v and dst in col_ids: cv[col_ids[dst]] = {"label": v}

        text("Customer Name Code",           "Customer Code")
        text("Serial #",                     "Serial #")
        text("Nature of Work",               "Nature of Work")
        status("Execution Status",           "Execution Status")
        date("Data Delivery Date",           "Data Delivery Date")
        date("Date of PO/LOI",               "Date of PO/LOI")
        text("Sector",                       "Sector")
        text("Type of Work",                 "Type of Work")
        num("Amount in Rupees (Excl of GST) (Masked)", "Amount Excl GST (₹)")
        num("Amount in Rupees (Incl of GST) (Masked)", "Amount Incl GST (₹)")
        num("Billed Value in Rupees (Incl of GST.) (Masked)", "Billed Incl GST (₹)")
        num("Collected Amount in Rupees (Incl of GST.) (Masked)", "Collected (₹)")
        num("Amount Receivable (Masked)",    "Receivable (₹)")
        status("WO Status (billed)",         "WO Status")
        status("Billing Status",             "Billing Status")
        text("BD/KAM Personnel code",        "Personnel Code")

        name = safe_str(row.get("Deal name masked")) or f"WO_{_}"
        if create_item(api_key, board_id, name, cv):
            ok += 1
        else:
            fail += 1
        time.sleep(0.18)

    print(f"   ✅ {ok} imported | {fail} failed")
    return board_id


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Import Skylark data into Monday.com")
    p.add_argument("--api-key",    required=True)
    p.add_argument("--workspace",  required=True)
    p.add_argument("--deals-file", default="Deal_funnel_Data.xlsx")
    p.add_argument("--wo-file",    default="Work_Order_Tracker_Data.xlsx")
    args = p.parse_args()

    print("🚀 Skylark Drones → Monday.com importer")

    if not Path(args.deals_file).exists():
        print(f"❌ File not found: {args.deals_file}")
        sys.exit(1)
    if not Path(args.wo_file).exists():
        print(f"❌ File not found: {args.wo_file}")
        sys.exit(1)

    deals_id = import_deals(args.api_key, args.workspace, args.deals_file)
    wo_id    = import_workorders(args.api_key, args.workspace, args.wo_file)

    print("\n" + "="*52)
    print("✅  IMPORT COMPLETE — add to your .env:")
    print(f"MONDAY_API_KEY={args.api_key}")
    print(f"DEALS_BOARD_ID={deals_id}")
    print(f"WORKORDERS_BOARD_ID={wo_id}")
    print("="*52)


if __name__ == "__main__":
    main()
