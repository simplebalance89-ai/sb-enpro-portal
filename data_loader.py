"""
EnPro Filtration Mastermind Portal — Data Loader
Loads static crosswalk, live inventory, and chemical crosswalk from Azure Blob Storage.
Merges static specs with live inventory on Part_Number.
"""

import pandas as pd
import os
import logging
from typing import Optional

logger = logging.getLogger("enpro.data_loader")

SAS = os.environ.get("AZURE_BLOB_SAS", "")
BASE = "https://enproaidata.blob.core.windows.net/fm-data"


def _blob_url(filename: str) -> str:
    """Build Azure Blob URL with SAS token."""
    token = os.environ.get("AZURE_BLOB_SAS", SAS)
    sep = "?" if token and not token.startswith("?") else ""
    return f"{BASE}/{filename}{sep}{token}"


def load_static() -> pd.DataFrame:
    """Load static crosswalk CSV from Azure Blob. Cached at startup."""
    url = _blob_url("static_crosswalk.csv")
    logger.info("Loading static crosswalk from Azure Blob...")
    try:
        df = pd.read_csv(url, dtype=str).fillna("")
        logger.info(f"Static crosswalk loaded: {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"Failed to load static crosswalk: {e}")
        return pd.DataFrame()


def load_inventory() -> pd.DataFrame:
    """Load live inventory CSV from Azure Blob. Refreshed hourly."""
    url = _blob_url("inventory_live.csv")
    logger.info("Loading live inventory from Azure Blob...")
    try:
        df = pd.read_csv(url, dtype=str).fillna("")
        logger.info(f"Live inventory loaded: {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to load live inventory: {e}")
        return pd.DataFrame()


def load_chemicals() -> pd.DataFrame:
    """Load chemical crosswalk CSV from Azure Blob. Cached at startup."""
    url = _blob_url("chemical_crosswalk.csv")
    logger.info("Loading chemical crosswalk from Azure Blob...")
    try:
        df = pd.read_csv(url, dtype=str).fillna("")
        logger.info(f"Chemical crosswalk loaded: {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to load chemical crosswalk: {e}")
        return pd.DataFrame()


def merge_data(static: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    """
    Merge static specs with live inventory on Part_Number.
    Drops stale inventory columns from static, merges fresh ones from inventory.
    Converts numeric columns and computes Total_Stock.
    """
    if static.empty:
        logger.warning("Static crosswalk is empty — returning empty DataFrame")
        return pd.DataFrame()

    inv_cols = [
        "Part_Number",
        "Qty_Loc_10",
        "Qty_Loc_12",
        "Qty_Loc_22",
        "Qty_Loc_30",
        "Qty_Total",
        "Qty_Backordered_Total",
        "Price_1",
        "Last_Sell_Price",
        "Export_Timestamp",
    ]

    # Drop stale inventory columns from static (keep Part_Number)
    drop_cols = [c for c in inv_cols[1:] if c in static.columns]
    static_clean = static.drop(columns=drop_cols, errors="ignore")

    if inventory.empty:
        logger.warning("Inventory is empty — returning static data without inventory merge")
        merged = static_clean.copy()
    else:
        # Only select inventory columns that exist
        available_inv = [c for c in inv_cols if c in inventory.columns]
        merged = static_clean.merge(inventory[available_inv], on="Part_Number", how="left")

    # Numeric conversions
    numeric_cols = [
        "Qty_Loc_10",
        "Qty_Loc_12",
        "Qty_Loc_22",
        "Qty_Loc_30",
        "Qty_Total",
        "Price_1",
        "Last_Sell_Price",
        "Micron",
        "Max_Temp_F",
        "Max_PSI",
    ]
    for col in numeric_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)

    # Compute Total_Stock from location quantities
    stock_cols = ["Qty_Loc_10", "Qty_Loc_12", "Qty_Loc_22", "Qty_Loc_30"]
    available_stock = [c for c in stock_cols if c in merged.columns]
    if available_stock:
        merged["Total_Stock"] = merged[available_stock].sum(axis=1).astype(int)
    else:
        merged["Total_Stock"] = 0

    logger.info(f"Merged dataset: {len(merged)} rows")
    return merged
