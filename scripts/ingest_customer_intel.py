"""
Customer Intelligence ETL — reads source CSVs from C:\\Claude\\Work\\EnPro\\Ariba_Coupa\\data
and builds 4 per-rep partitioned datasets:

  customer_master       — one row per (rep_id, customer_id)
  customer_top_parts    — top 10 parts per (rep_id, customer_id) by spend + freq
  customer_orders       — last 24 months of orders per (rep_id, customer_id)
  customer_quotes       — Dynamics active quotes fuzzy-matched to customer_id

Rep ownership is derived from the `taker` field on PO Portal SO Header.csv —
NOT from Customers.salesrep_id (which is the placeholder 1067 in every sample
row). A customer that's been worked by multiple reps appears under each one.

Usage:
  # Dry run (default) — prints sample output, row counts, no DB writes
  python scripts/ingest_customer_intel.py

  # Custom source path
  python scripts/ingest_customer_intel.py --source "C:\\path\\to\\csvs"

  # Write to Postgres (requires DATABASE_URL env var)
  python scripts/ingest_customer_intel.py --write

  # Both
  python scripts/ingest_customer_intel.py --source "..." --write
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("ingest_customer_intel")


# ---------------------------------------------------------------------------
# CSV loaders — handle BOM, "NULL" strings, encoding quirks
# ---------------------------------------------------------------------------

NULL_STRINGS = ["NULL", "null", "NaN", "nan", "<NA>", ""]


def _read_csv(path: Path, **kwargs) -> pd.DataFrame:
    """Robust CSV reader that strips BOM, treats "NULL" as NaN, falls back encoding."""
    if not path.exists():
        raise FileNotFoundError(f"Missing source CSV: {path}")
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", na_values=NULL_STRINGS, low_memory=False, **kwargs)
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="latin-1", na_values=NULL_STRINGS, low_memory=False, **kwargs)
    # Strip whitespace from string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip().replace({"nan": None, "NaT": None})
    return df


# ---------------------------------------------------------------------------
# Build customer_master — Customers + Ship-To + Defaults joined per rep_id
# ---------------------------------------------------------------------------

def build_customer_master(
    customers: pd.DataFrame,
    ship_to: pd.DataFrame,
    defaults: pd.DataFrame,
    rep_to_customers: pd.DataFrame,
    so_header: pd.DataFrame,
) -> pd.DataFrame:
    """One row per (rep_id, customer_id). Joins customer master to per-rep
    ownership derived from SO Header.taker. Same customer may appear under
    multiple rep_ids — that's correct."""

    # Aggregate so_count + last_order_date from header (more reliable than
    # the so_count column on Ship-To which is per-ship-to-record)
    so_agg = (
        so_header.assign(order_date=pd.to_datetime(so_header.get("order_date"), errors="coerce"))
        .groupby("customer_id")
        .agg(
            total_so_count=("order_no", "nunique"),
            last_order_date=("order_date", "max"),
        )
        .reset_index()
    )

    cust = customers[[
        "customer_id", "customer_name", "legal_name", "credit_status",
        "credit_limit", "terms_id", "salesrep_id", "national_account_flag",
        "mail_city", "mail_state", "central_phone_number", "email_address",
        "sfdc_account_id",
    ]].drop_duplicates(subset=["customer_id"])

    # Coerce customer_id to int (some files may have it as string)
    cust["customer_id"] = pd.to_numeric(cust["customer_id"], errors="coerce")
    cust = cust.dropna(subset=["customer_id"])
    cust["customer_id"] = cust["customer_id"].astype(int)

    # Merge so aggregates
    cust = cust.merge(so_agg, on="customer_id", how="left")

    # Merge rep_to_customers (many-to-many): one row per rep_id × customer_id
    out = rep_to_customers.merge(cust, on="customer_id", how="left")

    # Rename to match the schema
    out = out.rename(columns={
        "terms_id": "terms",
        "salesrep_id": "salesrep_owner",
        "national_account_flag": "national_account",
        "central_phone_number": "central_phone",
    })
    out = out[[
        "rep_id", "customer_id", "customer_name", "legal_name", "credit_status",
        "credit_limit", "terms", "salesrep_owner", "mail_city", "mail_state",
        "central_phone", "email_address", "national_account", "total_so_count",
        "last_order_date", "sfdc_account_id",
    ]]
    out = out.dropna(subset=["customer_name"])
    out = out.drop_duplicates(subset=["rep_id", "customer_id"])
    return out


# ---------------------------------------------------------------------------
# Build customer_top_parts — top 10 parts per (rep_id, customer_id)
# ---------------------------------------------------------------------------

def build_customer_top_parts(
    so_header: pd.DataFrame,
    so_lines: pd.DataFrame,
    top_n: int = 10,
) -> pd.DataFrame:
    """Join header→lines, group by (taker, customer_id, inv_mast_uid),
    aggregate qty + spend + count, keep top N per customer."""

    hdr = so_header[["oe_hdr_uid", "customer_id", "taker", "order_date", "completed", "cancel_flag", "delete_flag"]].copy()
    hdr = hdr[(hdr["delete_flag"] != "Y") & (hdr["cancel_flag"] != "Y")]
    hdr["customer_id"] = pd.to_numeric(hdr["customer_id"], errors="coerce")
    hdr = hdr.dropna(subset=["customer_id", "taker"])
    hdr["customer_id"] = hdr["customer_id"].astype(int)
    hdr["order_date"] = pd.to_datetime(hdr["order_date"], errors="coerce")
    hdr = hdr.rename(columns={"taker": "rep_id"})

    lines = so_lines[[
        "oe_hdr_uid", "inv_mast_uid", "customer_part_number",
        "extended_desc", "qty_ordered", "extended_price",
    ]].copy()
    lines = lines.dropna(subset=["oe_hdr_uid", "inv_mast_uid"])
    lines["inv_mast_uid"] = pd.to_numeric(lines["inv_mast_uid"], errors="coerce").astype("Int64")
    lines["qty_ordered"] = pd.to_numeric(lines["qty_ordered"], errors="coerce").fillna(0)
    lines["extended_price"] = pd.to_numeric(lines["extended_price"], errors="coerce").fillna(0)

    merged = lines.merge(hdr[["oe_hdr_uid", "customer_id", "rep_id", "order_date"]], on="oe_hdr_uid", how="inner")

    grouped = (
        merged.groupby(["rep_id", "customer_id", "inv_mast_uid"], dropna=False)
        .agg(
            customer_part_number=("customer_part_number", "first"),
            part_description=("extended_desc", "first"),
            total_qty=("qty_ordered", "sum"),
            total_extended_price=("extended_price", "sum"),
            order_count=("oe_hdr_uid", "nunique"),
            last_ordered_date=("order_date", "max"),
        )
        .reset_index()
    )

    # Top N per (rep_id, customer_id) by total_extended_price
    grouped = grouped.sort_values(
        ["rep_id", "customer_id", "total_extended_price"],
        ascending=[True, True, False],
    )
    top = grouped.groupby(["rep_id", "customer_id"]).head(top_n).reset_index(drop=True)
    return top


# ---------------------------------------------------------------------------
# Build customer_orders — last 24 months per (rep_id, customer_id)
# ---------------------------------------------------------------------------

def build_customer_orders(
    so_header: pd.DataFrame,
    so_lines: pd.DataFrame,
    months: int = 24,
) -> pd.DataFrame:
    hdr = so_header[[
        "order_no", "oe_hdr_uid", "customer_id", "taker", "order_date",
        "po_no", "ship2_city", "ship2_state", "completed", "cancel_flag", "delete_flag",
    ]].copy()
    hdr = hdr[(hdr["delete_flag"] != "Y") & (hdr["cancel_flag"] != "Y")]
    hdr["customer_id"] = pd.to_numeric(hdr["customer_id"], errors="coerce")
    hdr = hdr.dropna(subset=["customer_id", "taker", "order_no"])
    hdr["customer_id"] = hdr["customer_id"].astype(int)
    hdr["order_date"] = pd.to_datetime(hdr["order_date"], errors="coerce")
    hdr = hdr.rename(columns={"taker": "rep_id"})

    cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.DateOffset(months=months)
    hdr = hdr[hdr["order_date"] >= cutoff]

    # Per-order aggregates from lines
    lines = so_lines[["oe_hdr_uid", "extended_price"]].copy()
    lines["extended_price"] = pd.to_numeric(lines["extended_price"], errors="coerce").fillna(0)
    line_agg = (
        lines.groupby("oe_hdr_uid")
        .agg(extended_price=("extended_price", "sum"), line_count=("oe_hdr_uid", "size"))
        .reset_index()
    )
    hdr = hdr.merge(line_agg, on="oe_hdr_uid", how="left")
    hdr["extended_price"] = hdr["extended_price"].fillna(0)
    hdr["line_count"] = hdr["line_count"].fillna(0).astype(int)

    out = hdr[[
        "rep_id", "customer_id", "order_no", "order_date", "po_no",
        "extended_price", "ship2_city", "ship2_state", "line_count", "completed",
    ]].drop_duplicates(subset=["rep_id", "customer_id", "order_no"])
    return out


# ---------------------------------------------------------------------------
# Build rep→customers ownership map from SO Header.taker
# ---------------------------------------------------------------------------

def build_rep_to_customers(so_header: pd.DataFrame) -> pd.DataFrame:
    """Distinct (rep_id, customer_id) pairs derived from SO Header.taker.
    A customer that's been worked by multiple reps shows up under each.
    Filters out cancelled/deleted orders."""
    hdr = so_header[["taker", "customer_id", "cancel_flag", "delete_flag"]].copy()
    hdr = hdr[(hdr["delete_flag"] != "Y") & (hdr["cancel_flag"] != "Y")]
    hdr = hdr.dropna(subset=["taker", "customer_id"])
    hdr["customer_id"] = pd.to_numeric(hdr["customer_id"], errors="coerce")
    hdr = hdr.dropna(subset=["customer_id"])
    hdr["customer_id"] = hdr["customer_id"].astype(int)
    out = (
        hdr.rename(columns={"taker": "rep_id"})[["rep_id", "customer_id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return out


# ---------------------------------------------------------------------------
# Build customer_quotes — Dynamics active quotes fuzzy-matched to customer_id
# ---------------------------------------------------------------------------

def build_customer_quotes(
    quotes: pd.DataFrame,
    customers: pd.DataFrame,
    rep_to_customers: pd.DataFrame,
) -> pd.DataFrame:
    """
    Dynamics active quotes carry only free-text customer_name.
    Fuzzy-match against customer master, then explode by rep_id ownership
    so each owning rep sees the quotes for customers they work.
    """
    try:
        from rapidfuzz import process, fuzz
    except ImportError:
        logger.error("rapidfuzz not installed — quote fuzzy matching disabled")
        return pd.DataFrame()

    # Normalize quote rows
    q = quotes.copy()
    q["customer_name"] = q["customer_name"].fillna("").str.strip()
    q = q[q["customer_name"] != ""]
    q["created_date"] = pd.to_datetime(q.get("created"), errors="coerce")

    # Build customer name → customer_id index
    cust_idx = customers[["customer_id", "customer_name", "legal_name"]].drop_duplicates(subset=["customer_id"])
    cust_idx["customer_id"] = pd.to_numeric(cust_idx["customer_id"], errors="coerce")
    cust_idx = cust_idx.dropna(subset=["customer_id"])
    cust_idx["customer_id"] = cust_idx["customer_id"].astype(int)

    # Build a lookup list combining customer_name + legal_name → customer_id
    name_to_id: dict[str, int] = {}
    for _, row in cust_idx.iterrows():
        cid = int(row["customer_id"])
        for nm in (row["customer_name"], row.get("legal_name")):
            if isinstance(nm, str) and nm.strip():
                name_to_id.setdefault(nm.strip().upper(), cid)

    name_keys = list(name_to_id.keys())

    def match(q_name: str) -> Optional[int]:
        if not q_name or not name_keys:
            return None
        target = q_name.strip().upper()
        # Exact / startswith fast path
        if target in name_to_id:
            return name_to_id[target]
        result = process.extractOne(target, name_keys, scorer=fuzz.token_set_ratio, score_cutoff=85)
        if result:
            return name_to_id[result[0]]
        return None

    q["customer_id"] = q["customer_name"].map(match)

    matched = q.dropna(subset=["customer_id"]).copy()
    matched["customer_id"] = matched["customer_id"].astype(int)

    matched = matched.rename(columns={"customer_name": "customer_name_raw"})

    # Explode by rep_id — each quote gets one row per rep that owns the customer
    matched = matched.merge(rep_to_customers, on="customer_id", how="inner")

    out = matched[[
        "rep_id", "customer_id", "quote_number", "quote_name", "status",
        "customer_name_raw", "contact_name", "extended_price",
        "freight_terms", "payment_terms", "est_completion", "created_date",
    ]]
    out["extended_price"] = pd.to_numeric(out["extended_price"], errors="coerce")
    out = out.drop_duplicates(subset=["rep_id", "quote_number"])

    unmatched_count = q["customer_id"].isna().sum()
    if unmatched_count:
        logger.info(f"customer_quotes: {unmatched_count} quotes had no customer_id match (free-text fuzzy fail)")
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        default=r"C:\Claude\Work\EnPro\Ariba_Coupa\data",
        help="Directory containing the source CSVs",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Bulk-insert into Postgres (requires DATABASE_URL). Default is dry-run.",
    )
    parser.add_argument("--quotes-only", action="store_true", help="Only rebuild quotes table")
    parser.add_argument(
        "--out",
        default=str(Path(__file__).resolve().parent.parent / "data" / "customer_intel"),
        help="Local output directory for the 4 derived CSVs (always written, dry-run or not)",
    )
    args = parser.parse_args()

    src = Path(args.source)
    if not src.exists():
        logger.error(f"Source directory not found: {src}")
        sys.exit(1)

    logger.info(f"Source: {src}")
    logger.info(f"Mode: {'WRITE TO POSTGRES' if args.write else 'DRY RUN (no DB writes)'}")

    # Load source CSVs
    logger.info("Loading source CSVs…")
    customers = _read_csv(src / "PO Portal Customers.csv")
    ship_to = _read_csv(src / "PO Portal Customers Ship-To.csv")
    defaults = _read_csv(src / "PO Portal Customer Defaults.csv")
    so_header = _read_csv(src / "PO Portal SO Header.csv")
    so_lines = _read_csv(src / "PO Portal SO Lines.csv")
    quotes = _read_csv(src / "dynamics_quotes_active.csv")

    logger.info(f"  customers       : {len(customers):>6,} rows")
    logger.info(f"  ship_to         : {len(ship_to):>6,} rows")
    logger.info(f"  defaults        : {len(defaults):>6,} rows")
    logger.info(f"  so_header       : {len(so_header):>6,} rows")
    logger.info(f"  so_lines        : {len(so_lines):>6,} rows")
    logger.info(f"  quotes (active) : {len(quotes):>6,} rows")

    # Build derived tables
    logger.info("Building rep_to_customers from SO Header.taker…")
    rep_to_customers = build_rep_to_customers(so_header)
    logger.info(f"  rep_to_customers: {len(rep_to_customers):,} (rep, customer) pairs")
    logger.info(f"  unique rep_ids  : {rep_to_customers['rep_id'].nunique():,}")
    logger.info(f"  unique customers: {rep_to_customers['customer_id'].nunique():,}")

    logger.info("Building customer_master…")
    cust_master = build_customer_master(customers, ship_to, defaults, rep_to_customers, so_header)
    logger.info(f"  customer_master : {len(cust_master):,} rows")

    logger.info("Building customer_top_parts…")
    top_parts = build_customer_top_parts(so_header, so_lines)
    logger.info(f"  customer_top_parts: {len(top_parts):,} rows")

    logger.info("Building customer_orders (last 24 months)…")
    cust_orders = build_customer_orders(so_header, so_lines)
    logger.info(f"  customer_orders : {len(cust_orders):,} rows")

    logger.info("Building customer_quotes (fuzzy match)…")
    cust_quotes = build_customer_quotes(quotes, customers, rep_to_customers)
    logger.info(f"  customer_quotes : {len(cust_quotes):,} rows")

    # ── Save derived tables to local CSV (BEFORE any print so a console
    #    encoding crash on Windows can't prevent the files from landing) ──
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    cust_master.to_csv(out_dir / "customer_master.csv", index=False)
    top_parts.to_csv(out_dir / "customer_top_parts.csv", index=False)
    cust_orders.to_csv(out_dir / "customer_orders.csv", index=False)
    cust_quotes.to_csv(out_dir / "customer_quotes.csv", index=False)

    # ── DRY RUN OUTPUT — ASCII-only separators for Windows cp1252 console ──
    print()
    print("=" * 80)
    print("DRY-RUN OUTPUT - sample rows from each derived table")
    print("=" * 80)

    def show(name: str, df: pd.DataFrame, n: int = 3):
        print(f"\n--- {name} ({len(df):,} rows) ---")
        if df.empty:
            print("  (empty)")
            return
        with pd.option_context("display.max_columns", None, "display.width", 200, "display.max_colwidth", 40):
            try:
                print(df.head(n).to_string(index=False))
            except UnicodeEncodeError:
                # Fallback if a row contains non-ASCII chars Windows console can't render
                safe = df.head(n).astype(str).map(lambda v: v.encode("ascii", "replace").decode("ascii") if isinstance(v, str) else v)
                print(safe.to_string(index=False))

    show("customer_master", cust_master)
    show("customer_top_parts", top_parts)
    show("customer_orders", cust_orders)
    show("customer_quotes", cust_quotes)

    print()
    print(f"--- Saved 4 CSVs to {out_dir} ---")
    for name in ("customer_master", "customer_top_parts", "customer_orders", "customer_quotes"):
        p = out_dir / f"{name}.csv"
        if p.exists():
            print(f"  {p.name}: {p.stat().st_size:,} bytes")

    print()
    print("--- Top 5 reps by customer_count ---")
    rep_counts = (
        rep_to_customers.groupby("rep_id")
        .agg(customer_count=("customer_id", "nunique"))
        .sort_values("customer_count", ascending=False)
        .head(5)
    )
    print(rep_counts.to_string())

    print()
    print("--- Distinct salesrep_id values on Customers.csv (top 10) ---")
    if "salesrep_id" in customers.columns:
        sr = customers["salesrep_id"].dropna().value_counts().head(10)
        print(sr.to_string())
        print(f"  unique salesrep_id values: {customers['salesrep_id'].nunique()}")

    if not args.write:
        print()
        print("DRY RUN complete. Pass --write to insert into Postgres.")
        return

    # ── WRITE PATH ──
    print()
    print("=" * 80)
    print("WRITE MODE — bulk insert into Postgres")
    print("=" * 80)

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        logger.error("DATABASE_URL not set — cannot write")
        sys.exit(1)

    import asyncio
    from db import init_db, session_factory, CustomerMaster, CustomerTopPart, CustomerOrder, CustomerQuote

    async def write_all():
        if not await init_db():
            logger.error("init_db failed")
            return
        factory = session_factory()
        async with factory() as session:
            # Wipe + replace pattern (simple, idempotent for nightly refresh)
            from sqlalchemy import delete
            await session.execute(delete(CustomerQuote))
            await session.execute(delete(CustomerOrder))
            await session.execute(delete(CustomerTopPart))
            await session.execute(delete(CustomerMaster))
            await session.commit()

            def _records(df: pd.DataFrame) -> list[dict]:
                # Replace NaN with None for SQL compatibility
                return df.where(pd.notnull(df), None).to_dict(orient="records")

            await session.run_sync(lambda s: s.bulk_insert_mappings(CustomerMaster, _records(cust_master)))
            await session.run_sync(lambda s: s.bulk_insert_mappings(CustomerTopPart, _records(top_parts)))
            await session.run_sync(lambda s: s.bulk_insert_mappings(CustomerOrder, _records(cust_orders)))
            await session.run_sync(lambda s: s.bulk_insert_mappings(CustomerQuote, _records(cust_quotes)))
            await session.commit()
        logger.info("Write complete.")

    asyncio.run(write_all())


if __name__ == "__main__":
    main()
