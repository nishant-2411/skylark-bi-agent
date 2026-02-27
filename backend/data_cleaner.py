"""
data_cleaner.py
Domain-specific cleaning + enrichment for Skylark Drones BI data.

Handles:
- Sentinel/header rows embedded in data (e.g. 'Deal Status', 'Sector/service')
- Normalised sector names (case-insensitive, strip)
- Deal stage letter-code extraction and labelling
- Numeric parsing for Indian-format large numbers
- Date normalisation
- Data-quality report generation
"""

import re
from typing import Optional

import pandas as pd


# ── Constants ──────────────────────────────────────────────────────────────

DEAL_STAGE_MAP = {
    "a": "A – Lead Generated",
    "b": "B – Sales Qualified Lead",
    "c": "C – Demo Done",
    "d": "D – Feasibility",
    "e": "E – Proposal/Commercials Sent",
    "f": "F – Negotiations",
    "g": "G – Project Won",
    "h": "H – Work Order Received",
    "i": "I – POC",
    "j": "J – Invoice Sent",
    "k": "K – Amount Accrued",
    "l": "L – Project Lost",
    "m": "M – Projects On Hold",
    "n": "N – Not Relevant At The Moment",
    "o": "O – Not Relevant At All",
}

# Rows that are header repeats embedded in data
_SENTINEL_DEAL_STATUS  = {"deal status", "deal_status"}
_SENTINEL_DEAL_STAGE   = {"deal stage", "deal_stage"}
_SENTINEL_SECTOR       = {"sector/service", "sector"}

ACTIVE_DEAL_STATUSES   = {"open"}
WON_DEAL_STATUSES      = {"won"}
DEAD_DEAL_STATUSES     = {"dead", "lost", "project lost"}
ON_HOLD_STATUSES       = {"on hold", "hold"}

COMPLETED_WO_STATUSES  = {"completed"}
ONGOING_WO_STATUSES    = {"ongoing", "in progress", "executed until current month",
                          "partial completed", "pause / struck",
                          "details pending from client"}


# ── Number helpers ─────────────────────────────────────────────────────────

def parse_number(value) -> Optional[float]:
    """₹1,23,456.78 → 123456.78. Returns None on failure."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = re.sub(r"[₹$€£,\s]", "", str(value)).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fmt_inr(value: Optional[float]) -> str:
    """Format float as ₹ with Cr/L shorthand for readability."""
    if value is None:
        return "N/A"
    if abs(value) >= 1e7:
        return f"₹{value/1e7:.2f} Cr"
    if abs(value) >= 1e5:
        return f"₹{value/1e5:.2f} L"
    return f"₹{value:,.0f}"


# ── Deal stage normalisation ───────────────────────────────────────────────

def normalise_stage(stage: Optional[str]) -> Optional[str]:
    if not stage:
        return None
    m = re.match(r"^([a-zA-Z])\.", str(stage).strip())
    if m:
        code = m.group(1).lower()
        return DEAL_STAGE_MAP.get(code, stage)
    return stage


def stage_group(stage: Optional[str]) -> str:
    """Bucket stages into pipeline funnel groups."""
    if not stage:
        return "Unknown"
    s = str(stage).lower()
    if any(x in s for x in ["lead generated", "sales qualified"]):
        return "Early Stage"
    if any(x in s for x in ["demo", "feasibility"]):
        return "Qualification"
    if any(x in s for x in ["proposal", "negotiat", "poc"]):
        return "Active Pursuit"
    if any(x in s for x in ["work order", "project won", "invoice", "amount accrued"]):
        return "Won/Execution"
    if any(x in s for x in ["lost", "not relevant", "on hold"]):
        return "Closed/Inactive"
    return "Other"


# ── Sector normalisation ───────────────────────────────────────────────────

def normalise_sector(sector: Optional[str]) -> Optional[str]:
    if not sector:
        return None
    s = str(sector).strip()
    if s.lower() in _SENTINEL_SECTOR:
        return None
    return s.title()


# ── Deals cleaner ──────────────────────────────────────────────────────────

def clean_deals(rows: list[dict]) -> pd.DataFrame:
    """
    Accept raw rows (from Monday.com or XLSX-import fallback).
    Returns a clean DataFrame with enriched columns.
    """
    df = pd.DataFrame(rows)

    # Rename to canonical names regardless of casing quirks
    rename = {c: c for c in df.columns}
    for col in df.columns:
        lc = col.lower().replace(" ", "_")
        if lc in ("deal_name", "_name"):
            rename[col] = "deal_name"
        elif "owner" in lc:
            rename[col] = "owner_code"
        elif "client" in lc or "company" in lc:
            rename[col] = "client_code"
        elif lc == "deal_status":
            rename[col] = "status"
        elif "close_date" in lc and "tentative" not in lc and "actual" not in lc:
            rename[col] = "close_date_actual"
        elif "closure_probability" in lc or "probability" in lc:
            rename[col] = "closure_probability"
        elif "masked_deal_value" in lc or "deal_value" in lc or "value" in lc:
            rename[col] = "deal_value_raw"
        elif "tentative" in lc and "close" in lc:
            rename[col] = "tentative_close_date"
        elif lc == "deal_stage":
            rename[col] = "stage_raw"
        elif "product" in lc:
            rename[col] = "product"
        elif "sector" in lc or "service" in lc:
            rename[col] = "sector_raw"
        elif "created" in lc:
            rename[col] = "created_date"

    df = df.rename(columns=rename)
    # Drop duplicate columns that might result from renaming
    df = df.loc[:, ~df.columns.duplicated(keep='last')]

    # Ensure required columns exist
    for col in ["deal_name", "status", "stage_raw", "sector_raw", "deal_value_raw",
                "closure_probability", "owner_code"]:
        if col not in df.columns:
            df[col] = None

    # Drop sentinel rows (header-repeat rows embedded in data)
    mask = (
        df["status"].str.lower().isin(_SENTINEL_DEAL_STATUS) |
        df["stage_raw"].str.lower().isin(_SENTINEL_DEAL_STAGE) |
        df["sector_raw"].str.lower().isin(_SENTINEL_SECTOR)
    )
    df = df[~mask].reset_index(drop=True)

    # Clean + normalise
    df["status"]              = df["status"].str.strip().str.title()
    df["deal_value"]          = df["deal_value_raw"].apply(parse_number)
    df["stage"]               = df["stage_raw"].apply(normalise_stage)
    df["stage_group"]         = df["stage"].apply(stage_group)
    df["sector"]              = df["sector_raw"].apply(normalise_sector)
    df["closure_probability"] = df["closure_probability"].str.strip().str.title()

    # Classify status
    df["is_open"]    = df["status"].str.lower().isin(ACTIVE_DEAL_STATUSES)
    df["is_won"]     = df["status"].str.lower().isin(WON_DEAL_STATUSES)
    df["is_dead"]    = df["status"].str.lower().isin(DEAD_DEAL_STATUSES)
    df["is_on_hold"] = df["status"].str.lower().isin(ON_HOLD_STATUSES)

    return df


# ── Work Orders cleaner ────────────────────────────────────────────────────

def clean_workorders(rows: list[dict]) -> pd.DataFrame:
    """
    Accept raw rows from Monday.com (after column normalisation).
    Returns a clean DataFrame.
    """
    df = pd.DataFrame(rows)

    rename = {c: c for c in df.columns}
    for col in df.columns:
        lc = col.lower().replace(" ", "_")
        if "deal_name" in lc or lc == "_name":
            rename[col] = "deal_name"
        elif "customer" in lc or ("company" in lc and "name" in lc):
            rename[col] = "customer_code"
        elif "serial" in lc:
            rename[col] = "serial_no"
        elif "nature" in lc:
            rename[col] = "nature_of_work"
        elif "execution_status" in lc:
            rename[col] = "execution_status"
        elif "sector" in lc:
            rename[col] = "sector_raw"
        elif "type_of_work" in lc:
            rename[col] = "type_of_work"
        elif "amount" in lc and "excl" in lc and "billed" not in lc and "be_billed" not in lc:
            rename[col] = "amount_excl_gst_raw"
        elif "amount" in lc and "incl" in lc and "billed" not in lc and "be_billed" not in lc and "collected" not in lc:
            rename[col] = "amount_incl_gst_raw"
        elif "billed_value" in lc and "excl" in lc:
            rename[col] = "billed_excl_gst_raw"
        elif "billed_value" in lc and "incl" in lc:
            rename[col] = "billed_incl_gst_raw"
        elif "collected_amount" in lc or ("collected" in lc and "amount" in lc):
            rename[col] = "collected_raw"
        elif "amount_receivable" in lc or "receivable" in lc:
            rename[col] = "receivable_raw"
        elif "wo_status" in lc or ("status" in lc and "billed" in lc):
            rename[col] = "wo_status"
        elif "collection_status" in lc or ("collection" in lc and "status" in lc):
            rename[col] = "collection_status"
        elif "billing_status" in lc:
            rename[col] = "billing_status"
        elif "personnel" in lc or "kam" in lc or "bd" in lc:
            rename[col] = "personnel_code"
        elif "po" in lc or "loi" in lc:
            rename[col] = "po_date"
        elif "invoice_date" in lc or ("invoice" in lc and "date" in lc):
            rename[col] = "last_invoice_date"

    df = df.rename(columns=rename)
    # Drop duplicate columns that might result from renaming
    df = df.loc[:, ~df.columns.duplicated(keep='last')]

    # Ensure columns exist
    for col in ["deal_name", "execution_status", "sector_raw",
                "amount_excl_gst_raw", "amount_incl_gst_raw",
                "billed_incl_gst_raw", "collected_raw", "receivable_raw",
                "billing_status", "wo_status"]:
        if col not in df.columns:
            df[col] = None

    # Parse financials
    df["amount_excl_gst"]  = df["amount_excl_gst_raw"].apply(parse_number)
    df["amount_incl_gst"]  = df["amount_incl_gst_raw"].apply(parse_number)
    df["billed_incl_gst"]  = df["billed_incl_gst_raw"].apply(parse_number)
    df["collected"]        = df["collected_raw"].apply(parse_number)
    df["receivable"]       = df["receivable_raw"].apply(parse_number)

    # Normalise
    df["sector"]           = df["sector_raw"].apply(normalise_sector)
    df["execution_status"] = df["execution_status"].str.strip()

    # Status booleans
    df["is_completed"] = df["execution_status"].str.lower().isin(COMPLETED_WO_STATUSES)
    df["is_ongoing"]   = df["execution_status"].str.lower().isin(ONGOING_WO_STATUSES)

    # Fillna financials to 0 for aggregation
    for col in ["amount_excl_gst", "amount_incl_gst", "billed_incl_gst",
                "collected", "receivable"]:
        df[col] = df[col].fillna(0.0)

    return df


# ── Data-quality report ────────────────────────────────────────────────────

def quality_report(deals_df: pd.DataFrame, wo_df: pd.DataFrame) -> dict:
    """Return a structured data-quality summary."""
    def _completeness(df: pd.DataFrame) -> float:
        total = df.shape[0] * df.shape[1]
        if total == 0:
            return 100.0
        filled = df.notna().sum().sum()
        return round(filled / total * 100, 1)

    issues = []

    # Deals
    if not deals_df.empty:
        if "deal_value" in deals_df.columns:
            missing_val = deals_df["deal_value"].isna().sum()
            if missing_val:
                issues.append(f"{missing_val} deals have no deal value")
        if "sector" in deals_df.columns:
            missing_sector = deals_df["sector"].isna().sum()
            if missing_sector:
                issues.append(f"{missing_sector} deals have no sector")
        if "stage" in deals_df.columns:
            missing_stage = deals_df["stage"].isna().sum()
            if missing_stage:
                issues.append(f"{missing_stage} deals have no stage")

    # WO
    if not wo_df.empty:
        if "amount_excl_gst" in wo_df.columns:
            zero_amount = (wo_df["amount_excl_gst"] == 0).sum()
            if zero_amount:
                issues.append(f"{zero_amount} work orders have ₹0 / missing amount")
        if "collected" in wo_df.columns:
            zero_collected = (wo_df["collected"] == 0).sum()
            if zero_collected:
                issues.append(f"{zero_collected} work orders show ₹0 collected")

    return {
        "deals_rows":            len(deals_df),
        "wo_rows":               len(wo_df),
        "deals_completeness":    f"{_completeness(deals_df)}%",
        "wo_completeness":       f"{_completeness(wo_df)}%",
        "issues":                issues,
    }
