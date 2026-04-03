"""
Enpro Filtration Mastermind Portal — Data Loader
Loads static crosswalk, live inventory, and chemical crosswalk from Azure Blob Storage.
Merges static specs with live inventory on Part_Number.
"""

import io
import logging
import os

import pandas as pd
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger("enpro.data_loader")

SAS = os.environ.get("AZURE_BLOB_SAS", "")
CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
CONTAINER = os.environ.get("AZURE_BLOB_CONTAINER", "fm-data")
BASE = os.environ.get(
    "AZURE_BLOB_BASE",
    "https://enproaidatav1.blob.core.windows.net/fm-data",
)


def _blob_url(filename: str) -> str:
    """Build Azure Blob URL with SAS token."""
    token = os.environ.get("AZURE_BLOB_SAS", SAS)
    sep = "?" if token and not token.startswith("?") else ""
    return f"{BASE}/{filename}{sep}{token}"


def _read_csv(filename: str, **kwargs) -> pd.DataFrame:
    """Read a CSV from Blob Storage using connection string first, SAS URL fallback second."""
    if CONNECTION_STRING:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        blob_client = blob_service.get_blob_client(container=CONTAINER, blob=filename)
        data = blob_client.download_blob().readall()
        return pd.read_csv(io.BytesIO(data), dtype=str, **kwargs).fillna("")

    return pd.read_csv(_blob_url(filename), dtype=str, **kwargs).fillna("")


def load_static() -> pd.DataFrame:
    """Load static crosswalk CSV from Azure Blob. Cached at startup."""
    logger.info("Loading static crosswalk from Azure Blob...")
    try:
        df = _read_csv("static_crosswalk.csv")
        # Normalize column names: CSV has Micron_Final/Media_Final but code expects Micron/Media
        drop_source = [c for c in df.columns if c.endswith("_Source")]
        if drop_source:
            df = df.drop(columns=drop_source, errors="ignore")
            logger.info(f"Dropped {len(drop_source)} _Source columns")
        rename = {c: c.replace("_Final", "") for c in df.columns if c.endswith("_Final")}
        if rename:
            # Drop original columns that would conflict with renamed _Final columns
            conflicts = [target for target in rename.values() if target in df.columns]
            if conflicts:
                df = df.drop(columns=conflicts, errors="ignore")
                logger.info(f"Dropped {len(conflicts)} columns superseded by _Final: {conflicts}")
            df = df.rename(columns=rename)
            logger.info(f"Renamed {len(rename)} _Final columns: {list(rename.values())}")
        logger.info(f"Static crosswalk loaded: {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.warning(f"Failed to load static crosswalk: {e}")
        logger.info("Falling back to inventory_live.csv for static product data...")
        try:
            df = _read_csv("inventory_live.csv")
            logger.info(f"Static fallback loaded: {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as fallback_error:
            logger.error(f"Static fallback also failed: {fallback_error}")
            return pd.DataFrame()


def load_inventory() -> pd.DataFrame:
    """Load live inventory CSV from Azure Blob. Refreshed hourly."""
    logger.info("Loading live inventory from Azure Blob...")
    try:
        df = _read_csv("inventory_live.csv")
        # Normalize column names: SQL view uses P21_Item_ID and Qty_Loc10 (no underscore)
        # but merge_data() joins on Part_Number and expects Qty_Loc_10 (with underscore)
        rename_map = {}
        if "P21_Item_ID" in df.columns and "Part_Number" not in df.columns:
            rename_map["P21_Item_ID"] = "Part_Number"
        if "Qty_Loc10" in df.columns and "Qty_Loc_10" not in df.columns:
            rename_map["Qty_Loc10"] = "Qty_Loc_10"
            rename_map["Qty_Loc12"] = "Qty_Loc_12"
            rename_map["Qty_Loc22"] = "Qty_Loc_22"
            rename_map["Qty_Loc30"] = "Qty_Loc_30"
        if rename_map:
            df = df.rename(columns=rename_map)
            logger.info(f"Inventory columns renamed: {rename_map}")
        logger.info(f"Live inventory loaded: {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to load live inventory: {e}")
        return pd.DataFrame()


def load_chemicals() -> pd.DataFrame:
    """Load chemical crosswalk CSV from Azure Blob. Cached at startup."""
    logger.info("Loading chemical crosswalk from Azure Blob...")
    try:
        df = _read_csv("chemical_crosswalk.csv", encoding="latin-1")
        logger.info(f"Chemical crosswalk loaded: {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Failed to load chemical crosswalk (trying fallback): {e}")
        try:
            df = _read_csv("chemical_crosswalk.csv", encoding="utf-8", encoding_errors="replace")
            logger.info(f"Chemical crosswalk loaded (fallback): {len(df)} rows")
            return df
        except Exception as e2:
            logger.error(f"Chemical crosswalk fallback also failed: {e2}")
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

    # Apply manufacturer + product type display mappings
    merged = _apply_display_mappings(merged)

    logger.info(f"Merged dataset: {len(merged)} rows")
    return merged


def _apply_display_mappings(df: pd.DataFrame) -> pd.DataFrame:
    """Map Product_Group codes to clean manufacturer names, consolidate product types."""
    import json

    mapping_path = os.path.join(os.path.dirname(__file__), "data", "manufacturer_mapping.json")
    if not os.path.exists(mapping_path):
        logger.warning("Manufacturer mapping file not found — skipping display mappings")
        return df

    try:
        with open(mapping_path, "r") as f:
            mappings = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load manufacturer mapping: {e}")
        return df

    mfr_map = mappings.get("manufacturer_map", {})
    pt_map = mappings.get("product_type_map", {})

    # Map Product_Group prefix (first 4 chars) to display name
    if "Product_Group" in df.columns and mfr_map:
        def _map_mfr(pg):
            pg_str = str(pg).strip()
            if not pg_str or pg_str in ("", "0", "nan"):
                return ""
            prefix = pg_str[:4].upper()
            return mfr_map.get(prefix, "")

        df["Manufacturer_Display"] = df["Product_Group"].apply(_map_mfr)
        # Fill blanks with Final_Manufacturer fallback
        if "Final_Manufacturer" in df.columns:
            blank_mask = df["Manufacturer_Display"] == ""
            df.loc[blank_mask, "Manufacturer_Display"] = df.loc[blank_mask, "Final_Manufacturer"].fillna("")
        mapped_count = (df["Manufacturer_Display"] != "").sum()
        logger.info(f"Manufacturer display mapped: {mapped_count}/{len(df)} rows")

    # Map Product_Type to consolidated display name
    if "Product_Type" in df.columns and pt_map:
        df["Product_Type_Display"] = df["Product_Type"].map(pt_map).fillna(df["Product_Type"])
        logger.info(f"Product type display mapped: {len(pt_map)} categories")

    # Flag filtration products
    if "Item_Category" in df.columns:
        df["Is_Filtration"] = df["Item_Category"].astype(str).str.upper() == "OK-FILTRATION"
        filt_count = df["Is_Filtration"].sum()
        logger.info(f"Filtration products flagged (Item_Category): {filt_count}/{len(df)}")
    elif "Has_V21_Specs" in df.columns:
        df["Is_Filtration"] = df["Has_V21_Specs"].astype(str).str.upper().isin(["Y", "YES", "TRUE", "1"])
        filt_count = df["Is_Filtration"].sum()
        logger.info(f"Filtration products flagged: {filt_count}/{len(df)}")
    else:
        # Fallback: check if Micron or Media has data
        has_specs = (
            (df.get("Micron", pd.Series(dtype=float)).fillna(0).astype(float) > 0) |
            (df.get("Media", pd.Series(dtype=str)).fillna("").astype(str).str.strip() != "")
        )
        df["Is_Filtration"] = has_specs
        logger.info(f"Filtration products (spec-based): {has_specs.sum()}/{len(df)}")

    return df
