"""
EnPro Filtration Mastermind Portal — Search Engine
Pandas-based 5-column cascade search with normalization, multi-word AND,
stock filtering, and clean product formatting.
"""

import re
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger("enpro.search")

# ---------------------------------------------------------------------------
# Column cascade order (searched top-to-bottom, first match wins priority)
# ---------------------------------------------------------------------------
CASCADE_COLUMNS = [
    "Part_Number",
    "Supplier_Code",
    "Alt_Code",
    "Description",
    "Product_Type",
]

# ---------------------------------------------------------------------------
# Visible fields — only these are returned to the user
# ---------------------------------------------------------------------------
VISIBLE_FIELDS = [
    "Part_Number",
    "Description",
    "Extended_Description",
    "Product_Type",
    "Micron",
    "Media",
    "Max_Temp_F",
    "Max_PSI",
    "Flow_Rate",
    "Efficiency",
    "Final_Manufacturer",
]

# Hidden fields — searchable but NEVER displayed
HIDDEN_FIELDS = [
    "Alt_Code",
    "Supplier_Code",
    "Application",
    "Industry",
    "P21_Item_ID",
    "Product_Group",
]

# Stock location mapping
STOCK_LOCATIONS = {
    "Qty_Loc_10": "Houston General Stock",
    "Qty_Loc_12": "Charlotte",
    "Qty_Loc_22": "Houston Reserve",
    "Qty_Loc_30": "Kansas City",
}

MAX_RESULTS = 10


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------
def _normalize(text: str) -> str:
    """Lowercase, strip spaces/dashes/slashes/underscores/dots for fuzzy matching."""
    if not text:
        return ""
    return re.sub(r"[\s\-/\\_\.]+", "", str(text).lower().strip())


def _normalize_light(text: str) -> str:
    """Lowercase and strip whitespace only (for multi-word matching)."""
    return str(text).lower().strip()


# ---------------------------------------------------------------------------
# Search functions
# ---------------------------------------------------------------------------
def search_products(
    df: pd.DataFrame,
    query: str,
    field: Optional[str] = None,
    in_stock_only: bool = True,
    max_results: int = MAX_RESULTS,
) -> dict:
    """
    Search the merged product DataFrame.

    Args:
        df: Merged product DataFrame (static + inventory).
        query: User search query.
        field: Optional specific field to search (bypasses cascade).
        in_stock_only: If True, only return products with Total_Stock > 0.
        max_results: Maximum results to return.

    Returns:
        dict with 'results' (list of formatted products), 'total_found' (int),
        'query' (str), 'search_type' (str).
    """
    if df.empty or not query:
        return {"results": [], "total_found": 0, "query": query, "search_type": "empty"}

    query = query.strip()
    norm_query = _normalize(query)

    # Determine search type
    if field and field in df.columns:
        matches = _search_single_field(df, query, norm_query, field)
        search_type = f"field:{field}"
    elif _looks_like_part_number(query):
        matches = _search_exact(df, norm_query)
        search_type = "exact_lookup"
    else:
        matches = _search_cascade(df, query, norm_query)
        search_type = "cascade"

    # Stock filter
    if in_stock_only and "Total_Stock" in matches.columns and not matches.empty:
        stocked = matches[matches["Total_Stock"] > 0]
        # Fall back to all results if stock filter empties everything
        if not stocked.empty:
            matches = stocked

    total_found = len(matches)
    limited = matches.head(max_results)

    results = [format_product(row) for _, row in limited.iterrows()]

    return {
        "results": results,
        "total_found": total_found,
        "query": query,
        "search_type": search_type,
    }


_DESCRIPTION_WORDS = {
    "micron", "filter", "element", "cartridge", "bag", "housing",
    "membrane", "pleated", "depth", "sheet", "media", "steel",
    "stainless", "polypropylene", "polyester", "nylon", "ptfe",
    "glass", "carbon", "inch", "psi", "gpm", "temp", "flow",
    "rate", "pressure", "temperature", "compatible", "replacement",
}


def _looks_like_part_number(query: str) -> bool:
    """Heuristic: part numbers contain digits mixed with letters/dashes.
    Excludes queries with common description/spec words."""
    words = query.lower().split()
    if any(w in _DESCRIPTION_WORDS for w in words):
        return False
    has_digit = any(c.isdigit() for c in query)
    has_alpha = any(c.isalpha() for c in query)
    return has_digit and (has_alpha or "-" in query) and len(words) <= 2


def _search_exact(df: pd.DataFrame, norm_query: str) -> pd.DataFrame:
    """Exact match on Part_Number, Supplier_Code, Alt_Code (normalized)."""
    exact_cols = ["Part_Number", "Supplier_Code", "Alt_Code"]
    masks = []
    for col in exact_cols:
        if col in df.columns:
            masks.append(df[col].apply(_normalize) == norm_query)
    if not masks:
        return pd.DataFrame()
    combined = masks[0]
    for m in masks[1:]:
        combined = combined | m
    result = df[combined]
    if not result.empty:
        return result
    # Fall through to cascade if exact match fails
    return _search_cascade(df, norm_query, norm_query)


def _search_single_field(
    df: pd.DataFrame, query: str, norm_query: str, field: str
) -> pd.DataFrame:
    """Search a single specified field."""
    if field not in df.columns:
        return pd.DataFrame()
    col_normalized = df[field].apply(_normalize)
    # Try exact first
    exact = df[col_normalized == norm_query]
    if not exact.empty:
        return exact
    # Then contains
    return df[col_normalized.str.contains(norm_query, na=False)]


def _search_cascade(df: pd.DataFrame, raw_query: str, norm_query: str) -> pd.DataFrame:
    """
    5-column cascade search.
    For Part_Number/Supplier_Code/Alt_Code: normalized exact then contains.
    For description fields: multi-word AND search.
    """
    # Phase 1: Code columns (normalized)
    code_cols = ["Part_Number", "Supplier_Code", "Alt_Code"]
    for col in code_cols:
        if col not in df.columns:
            continue
        col_norm = df[col].apply(_normalize)
        # Exact
        exact = df[col_norm == norm_query]
        if not exact.empty:
            return exact
        # Contains
        contains = df[col_norm.str.contains(norm_query, na=False)]
        if not contains.empty:
            return contains

    # Phase 2: Text columns (multi-word AND)
    text_cols = [
        "Description",
        "Product_Type",
        "Final_Manufacturer",
    ]
    words = raw_query.lower().split()
    if not words:
        return pd.DataFrame()

    for col in text_cols:
        if col not in df.columns:
            continue
        col_lower = df[col].astype(str).str.lower()
        # All words must appear in the column
        mask = pd.Series([True] * len(df), index=df.index)
        for word in words:
            mask = mask & col_lower.str.contains(re.escape(word), na=False)
        matches = df[mask]
        if not matches.empty:
            return matches

    # Phase 3: Cross-column multi-word (any word in any searchable column)
    all_searchable = code_cols + text_cols
    available = [c for c in all_searchable if c in df.columns]
    if available:
        combined_text = df[available].astype(str).apply(lambda row: " ".join(row).lower(), axis=1)
        mask = pd.Series([True] * len(df), index=df.index)
        for word in words:
            mask = mask & combined_text.str.contains(re.escape(word), na=False)
        matches = df[mask]
        if not matches.empty:
            return matches

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Product formatting
# ---------------------------------------------------------------------------
def format_product(row: pd.Series) -> dict:
    """
    Format a product row into a clean dict with visible fields only.
    Applies price rules and stock location formatting.
    """
    product = {}

    # Visible fields
    for field in VISIBLE_FIELDS:
        val = row.get(field, "")
        if pd.isna(val) or val == "" or val == 0:
            continue
        product[field] = val

    # Handle dual column names — try V25 first, fall back to V5
    if "Final_Manufacturer" not in product:
        mfr = row.get("Manufacturer", "")
        if not pd.isna(mfr) and mfr != "" and mfr != 0:
            product["Final_Manufacturer"] = mfr

    # Price logic: Last_Sell_Price primary, Price_1 fallback
    last_sell = _to_float(row.get("Last_Sell_Price", 0))
    price_1 = _to_float(row.get("Price_1", 0))

    if last_sell > 0:
        product["Price"] = f"${last_sell:,.2f}"
    elif price_1 > 0:
        product["Price"] = f"${price_1:,.2f}"
    else:
        product["Price"] = "Contact EnPro for pricing"

    # Stock by location — hide zero-stock locations
    stock = {}
    for qty_col, loc_name in STOCK_LOCATIONS.items():
        qty = _to_float(row.get(qty_col, 0))
        if qty > 0:
            stock[loc_name] = int(qty)
    product["Stock"] = stock if stock else {"status": "Out of stock"}
    product["Total_Stock"] = int(_to_float(row.get("Total_Stock", 0)))

    return product


def _to_float(val) -> float:
    """Safe float conversion."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Direct lookup by part number
# ---------------------------------------------------------------------------
def lookup_part(df: pd.DataFrame, part_number: str) -> Optional[dict]:
    """Direct part number lookup. Returns formatted product or None."""
    if df.empty or not part_number:
        return None
    norm = _normalize(part_number)
    for col in ["Part_Number", "Supplier_Code", "Alt_Code"]:
        if col not in df.columns:
            continue
        match = df[df[col].apply(_normalize) == norm]
        if not match.empty:
            return format_product(match.iloc[0])
    return None


def suggest_parts(
    df: pd.DataFrame, query: str, max_results: int = 10, mode: str = "exact"
) -> list:
    """
    Fast typeahead suggestions. Returns list of dicts with Part_Number, Description, Manufacturer.
    mode: 'exact' (starts-with priority then contains), 'starts_with' (starts-with only),
          'contains' (contains only).
    Always returns up to max_results.
    """
    if df.empty or not query or len(query) < 2:
        return []

    query_lower = query.lower().strip()
    norm_query = _normalize(query)
    results = []
    seen = set()

    def _collect(matches_df):
        for _, row in matches_df.iterrows():
            pn = str(row.get("Part_Number", ""))
            if pn and pn not in seen:
                seen.add(pn)
                desc = str(row.get("Description", ""))
                mfr = str(row.get("Final_Manufacturer", row.get("Manufacturer", "")))
                results.append({"Part_Number": pn, "Description": desc, "Manufacturer": mfr})
                if len(results) >= max_results:
                    return True
        return False

    code_cols = ["Part_Number", "Supplier_Code", "Alt_Code"]

    # Starts-with phase (used by 'exact' and 'starts_with' modes)
    if mode in ("exact", "starts_with"):
        for col in code_cols:
            if col not in df.columns:
                continue
            col_norm = df[col].apply(_normalize)
            if _collect(df[col_norm.str.startswith(norm_query, na=False)]):
                return results

    # Contains phase (used by 'exact' and 'contains' modes)
    if mode in ("exact", "contains"):
        for col in code_cols:
            if col not in df.columns:
                continue
            col_norm = df[col].apply(_normalize)
            if _collect(df[col_norm.str.contains(norm_query, na=False)]):
                return results

        # Description contains
        if "Description" in df.columns and len(results) < max_results:
            desc_lower = df["Description"].astype(str).str.lower()
            _collect(df[desc_lower.str.contains(re.escape(query_lower), na=False)])

    return results


# ---------------------------------------------------------------------------
# Similar product finder
# ---------------------------------------------------------------------------
def _lookup_part_row(df: pd.DataFrame, part_number: str) -> Optional[pd.Series]:
    """Get raw DataFrame row for a part number (not formatted)."""
    if df.empty or not part_number:
        return None
    norm = _normalize(part_number)
    for col in ["Part_Number", "Supplier_Code", "Alt_Code"]:
        if col not in df.columns:
            continue
        match = df[df[col].apply(_normalize) == norm]
        if not match.empty:
            return match.iloc[0]
    return None


def find_similar_products(
    df: pd.DataFrame,
    part_number: str,
    max_per_category: int = 5,
) -> dict:
    """
    Find products similar to the given part, grouped by comparison category.
    Returns source product + categories: Other Manufacturers, Similar Specs, Same Type.
    """
    row = _lookup_part_row(df, part_number)
    if row is None:
        return {"source": None, "categories": []}

    pn = str(row.get("Part_Number", ""))
    product_type = str(row.get("Product_Type", "")).strip()
    manufacturer = str(row.get("Final_Manufacturer", row.get("Manufacturer", ""))).strip()
    micron = _to_float(row.get("Micron", 0))
    max_temp = _to_float(row.get("Max_Temp_F", 0))
    max_psi = _to_float(row.get("Max_PSI", 0))

    source = format_product(row)
    categories = []
    seen_pns = {pn}  # Track shown part numbers to avoid duplicates across categories

    def _collect(mask, label, max_n):
        """Collect formatted products from mask, prefer in-stock, skip seen."""
        filtered = df[mask & ~df["Part_Number"].astype(str).isin(seen_pns)]
        if "Total_Stock" in filtered.columns:
            stocked = filtered[filtered["Total_Stock"] > 0]
            if not stocked.empty:
                filtered = stocked
        results = []
        for _, r in filtered.head(max_n).iterrows():
            rpn = str(r.get("Part_Number", ""))
            if rpn not in seen_pns:
                seen_pns.add(rpn)
                results.append(format_product(r))
        return results

    # Category 1: Other Manufacturers — same product type, different brand
    if product_type and product_type.lower() not in ("", "0", "nan"):
        mask = (
            (df["Product_Type"].astype(str).str.lower() == product_type.lower())
            & (df["Final_Manufacturer"].astype(str).str.lower() != manufacturer.lower())
        )
        results = _collect(mask, "Other Manufacturers", max_per_category)
        if results:
            categories.append({
                "name": "Other Manufacturers",
                "desc": f"Same type ({product_type}), different brand",
                "products": results,
            })

    # Category 2: Similar Specs — micron ±50%, temp ±100F, PSI ±50%
    spec_mask = pd.Series([True] * len(df), index=df.index)
    has_spec = False
    if micron > 0:
        micron_col = pd.to_numeric(df.get("Micron", pd.Series(dtype=float)), errors="coerce").fillna(0)
        spec_mask = spec_mask & (micron_col >= micron * 0.5) & (micron_col <= micron * 2.0)
        has_spec = True
    if max_temp > 0:
        temp_col = pd.to_numeric(df.get("Max_Temp_F", pd.Series(dtype=float)), errors="coerce").fillna(0)
        spec_mask = spec_mask & (temp_col >= max_temp - 100) & (temp_col <= max_temp + 100)
        has_spec = True
    if max_psi > 0:
        psi_col = pd.to_numeric(df.get("Max_PSI", pd.Series(dtype=float)), errors="coerce").fillna(0)
        spec_mask = spec_mask & (psi_col >= max_psi * 0.5) & (psi_col <= max_psi * 1.5)
        has_spec = True

    if has_spec:
        results = _collect(spec_mask, "Similar Specs", max_per_category)
        if results:
            spec_desc_parts = []
            if micron > 0:
                spec_desc_parts.append(f"{micron} micron range")
            if max_temp > 0:
                spec_desc_parts.append(f"~{int(max_temp)}°F")
            if max_psi > 0:
                spec_desc_parts.append(f"~{int(max_psi)} PSI")
            categories.append({
                "name": "Similar Specs",
                "desc": ", ".join(spec_desc_parts),
                "products": results,
            })

    # Category 3: Same Product Type (broader — includes same manufacturer)
    if product_type and product_type.lower() not in ("", "0", "nan"):
        mask = df["Product_Type"].astype(str).str.lower() == product_type.lower()
        results = _collect(mask, "Same Type", max_per_category)
        if results:
            categories.append({
                "name": "Same Product Type",
                "desc": product_type,
                "products": results,
            })

    return {"source": source, "categories": categories}
