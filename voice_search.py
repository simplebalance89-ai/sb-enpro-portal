"""
EnPro Filtration Mastermind — Voice Search Engine
Catalog-aware voice-to-product pipeline.

Flow: transcript → pre-process → parameter extract (GPT-4.1-mini) → fuzzy resolve → Pandas search → results
"""

import re
import json
import logging
from typing import Optional

import pandas as pd
from rapidfuzz import fuzz, process
from metaphone import doublemetaphone

from azure_client import chat_completion
from config import settings
from search import search_products, format_product, _to_float

logger = logging.getLogger("enpro.voice_search")


# ---------------------------------------------------------------------------
# Synonym Table — maps voice artifacts & shorthand to canonical catalog terms
# ---------------------------------------------------------------------------
SYNONYMS = {
    # Materials / Media
    "polly pro": "Polypropylene",
    "poly pro": "Polypropylene",
    "pp": "Polypropylene",
    "polypro": "Polypropylene",
    "polyp": "Polypropylene",
    "ptfe": "PTFE",
    "teflon": "PTFE",
    "pvdf": "PVDF",
    "kynar": "PVDF",
    "glass fiber": "Glass Fiber",
    "fiberglass": "Glass Fiber",
    "glass fibre": "Glass Fiber",
    "boro": "Borosilicate Glass",
    "316": "316 Stainless Steel",
    "three sixteen": "316 Stainless Steel",
    "three sixteen stainless": "316 Stainless Steel",
    "316 ss": "316 Stainless Steel",
    "316ss": "316 Stainless Steel",
    "stainless": "Stainless Steel",
    "stainless steel": "Stainless Steel",
    "cotton": "Cotton",
    "nylon": "Nylon",
    "polyester": "Polyester",
    "cellulose": "Cellulose",
    "carbon": "Carbon",
    "activated carbon": "Activated Carbon",
    # Product Types
    "bag": "Bag Filter",
    "bags": "Bag Filter",
    "bag filter": "Bag Filter",
    "cart": "Cartridges",
    "cartridge": "Cartridges",
    "cartridges": "Cartridges",
    "housing": "Housings",
    "housings": "Housings",
    "vessel": "Housings",
    "element": "Elements",
    "elements": "Elements",
    "membrane": "Membranes",
    "membranes": "Membranes",
    "capsule": "Capsule Filter",
    "depth sheet": "Depth Sheets",
    "depth sheets": "Depth Sheets",
    "air filter": "Air Filter",
    "compressor filter": "Compressor/Filter",
    "screen": "Screens / Separators",
    "separator": "Screens / Separators",
    # Manufacturers — common voice mishears
    "pall": "Pall",
    "paul": "Pall",
    "pal": "Pall",
    "graver": "Graver Technologies",
    "graver tech": "Graver Technologies",
    "graver technologies": "Graver Technologies",
    "cobetter": "Cobetter",
    "co better": "Cobetter",
    "critical process": "Critical Process Filtration Inc",
    "cpf": "Critical Process Filtration Inc",
    "global filter": "Global Filter LLC",
    "jonell": "Jonell Filtration Group",
    "john l": "Jonell Filtration Group",
    "john el": "Jonell Filtration Group",
    "koch": "Koch Filter Corporation",
    "cook filter": "Koch Filter Corporation",
    "pentair": "Pentair Filtration",
    "penta air": "Pentair Filtration",
    "rosedale": "Rosedale Products Inc",
    "rose dale": "Rosedale Products Inc",
    "schroeder": "Schroeder Industries",
    "schrader": "Schroeder Industries",
    "shelco": "Shelco Filters",
    "shell co": "Shelco Filters",
    "swift": "Swift Filters Inc.",
    "porvair": "Porvair Filtration Group Inc",
    "por vair": "Porvair Filtration Group Inc",
    "enpro": "Enpro, Incorporated",
    "en pro": "Enpro, Incorporated",
    "mcmaster": "McMaster-Carr Supply Co.",
    "mcmaster carr": "McMaster-Carr Supply Co.",
    "saint gobain": "Saint Gobain Performance",
    "saint go bain": "Saint Gobain Performance",
    "filtrox": "Filtrox North America Inc.",
    "filtrafine": "Filtrafine Corporation",
    "ajr": "AJR Filtration Inc.",
    "aaf": "AAF",
}


# ---------------------------------------------------------------------------
# Catalog Vocabulary — built from DataFrame at startup
# ---------------------------------------------------------------------------
class CatalogVocab:
    """Extracted catalog terms with pre-computed phonetic codes."""

    def __init__(self):
        self.manufacturers: list[str] = []
        self.product_types: list[str] = []
        self.media_types: list[str] = []
        self.micron_values: list[float] = []
        self.part_numbers: list[str] = []
        # Phonetic codes for fuzzy matching
        self._mfg_phonetic: dict[str, tuple] = {}
        self._media_phonetic: dict[str, tuple] = {}

    def build(self, df: pd.DataFrame):
        """Extract vocabulary from the product DataFrame."""
        if df.empty:
            return

        mfg_col = "Final_Manufacturer" if "Final_Manufacturer" in df.columns else "Manufacturer"
        if mfg_col in df.columns:
            self.manufacturers = sorted(df[mfg_col].dropna().astype(str).str.strip().unique().tolist())
            self.manufacturers = [m for m in self.manufacturers if m and m != "0" and m.lower() != "nan"]

        if "Product_Type" in df.columns:
            self.product_types = sorted(df["Product_Type"].dropna().astype(str).str.strip().unique().tolist())
            self.product_types = [p for p in self.product_types if p and p != "0" and p.lower() != "nan"]

        if "Media" in df.columns:
            self.media_types = sorted(df["Media"].dropna().astype(str).str.strip().unique().tolist())
            self.media_types = [m for m in self.media_types if m and m != "0" and m.lower() != "nan"]

        if "Micron" in df.columns:
            microns = pd.to_numeric(df["Micron"], errors="coerce").dropna().unique()
            self.micron_values = sorted([float(m) for m in microns if m > 0])

        if "Part_Number" in df.columns:
            self.part_numbers = df["Part_Number"].dropna().astype(str).str.strip().unique().tolist()

        # Pre-compute phonetic codes
        for m in self.manufacturers:
            self._mfg_phonetic[m] = doublemetaphone(m.lower())
        for m in self.media_types:
            self._media_phonetic[m] = doublemetaphone(m.lower())

        logger.info(
            f"Voice vocab built: {len(self.manufacturers)} mfgs, "
            f"{len(self.product_types)} types, {len(self.media_types)} media, "
            f"{len(self.micron_values)} microns, {len(self.part_numbers)} parts"
        )


# Module-level vocab — populated by init_voice_search()
_vocab = CatalogVocab()


def init_voice_search(df: pd.DataFrame):
    """Initialize voice search vocab from product DataFrame. Call at startup."""
    _vocab.build(df)


# ---------------------------------------------------------------------------
# Step 1: Pre-Processor — cheap regex/rule-based cleanup
# ---------------------------------------------------------------------------
def preprocess_transcript(text: str) -> str:
    """Clean up raw STT transcript before LLM extraction."""
    if not text:
        return ""

    cleaned = text.strip()

    # Number word → digit normalization
    number_words = {
        "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
        "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
        "fifteen": "15", "twenty": "20", "twenty five": "25", "twenty-five": "25",
        "thirty": "30", "forty": "40", "fifty": "50", "seventy five": "75",
        "seventy-five": "75", "one hundred": "100", "two hundred": "200",
        "three hundred": "300", "five hundred": "500",
    }
    lower = cleaned.lower()

    # Decimal number patterns: "point nine" → ".9", "zero point two" → "0.2"
    decimal_words = {
        "zero point one": "0.1", "zero point two": "0.2", "zero point three": "0.3",
        "zero point four": "0.4", "zero point five": "0.5", "zero point six": "0.6",
        "zero point seven": "0.7", "zero point eight": "0.8", "zero point nine": "0.9",
        "point one": "0.1", "point two": "0.2", "point three": "0.3",
        "point four": "0.4", "point five": "0.5", "point six": "0.6",
        "point seven": "0.7", "point eight": "0.8", "point nine": "0.9",
        "0 point 1": "0.1", "0 point 2": "0.2", "0 point 3": "0.3",
        "0 point 4": "0.4", "0 point 5": "0.5", "0 point 9": "0.9",
        "point forty five": "0.45", "point 45": "0.45", "zero point 45": "0.45",
        "point two two": "0.22", "point 22": "0.22",
    }
    for word, digit in sorted(decimal_words.items(), key=lambda x: -len(x[0])):
        lower = re.sub(r'\b' + re.escape(word) + r'\b', digit, lower)

    for word, digit in sorted(number_words.items(), key=lambda x: -len(x[0])):
        lower = re.sub(r'\b' + re.escape(word) + r'\b', digit, lower)

    # "ten micron" → "10 micron" (already handled above)
    # "three sixteen stainless" → "316 stainless"
    lower = re.sub(r'\bthree sixteen\b', '316', lower)
    lower = re.sub(r'\bthree oh four\b', '304', lower)

    # Apply synonym table for known voice artifacts
    for voice_term, canonical in sorted(SYNONYMS.items(), key=lambda x: -len(x[0])):
        pattern = r'\b' + re.escape(voice_term) + r'\b'
        if re.search(pattern, lower, re.IGNORECASE):
            lower = re.sub(pattern, canonical, lower, flags=re.IGNORECASE)

    return lower


def detect_part_number(text: str) -> Optional[str]:
    """Detect part number patterns in transcript."""
    # Common patterns: CLR510, AB-1234-56, 12345-6789, alphanumeric codes
    patterns = [
        r'\b[A-Z]{2,5}\d{3,}[A-Z0-9]*\b',  # CLR510, ABC1234
        r'\b\d{4,}-\d{3,}\b',                # 12345-6789
        r'\b[A-Z]{1,3}-\d{3,}-\d+\b',        # AB-1234-56
        r'\b\d{6,}\b',                        # 123456 (6+ digits alone)
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0)
    return None


# ---------------------------------------------------------------------------
# Step 2: Parameter Extractor — GPT-4.1-mini structured extraction
# ---------------------------------------------------------------------------
EXTRACTION_SYSTEM_PROMPT = """You extract structured search parameters from a filtration product query.
Return JSON only — no markdown, no explanation. Fields:
- manufacturer (string): filter manufacturer name
- product_type (string): type of filter product (bag, cartridge, housing, element, membrane, etc.)
- media (string): filter media material (polypropylene, PTFE, glass fiber, stainless steel, etc.)
- micron (number): micron rating
- max_temp (number): maximum temperature in Fahrenheit
- max_psi (number): maximum PSI rating
- flow_rate (number): flow rate in GPM
- in_stock (boolean): true if user specifically asked for in-stock items
- part_number (string): specific part number if mentioned

Omit fields not mentioned. Use null for uncertain values.

Examples:
"10 micron pall filter in stock" → {"manufacturer":"Pall","micron":10,"in_stock":true}
"polypropylene bag 25 micron" → {"media":"Polypropylene","product_type":"Bag Filter","micron":25}
"CLR510" → {"part_number":"CLR510"}
"graver 5 micron cartridge rated to 200 degrees" → {"manufacturer":"Graver Technologies","micron":5,"product_type":"Cartridges","max_temp":200}
"""


async def extract_parameters(transcript: str) -> dict:
    """Use GPT-4.1-mini to extract structured search parameters from voice transcript."""
    try:
        data = await chat_completion(
            deployment=settings.AZURE_DEPLOYMENT_ROUTER,  # gpt-4.1-mini
            messages=[
                {"role": "system", "content": EXTRACTION_SYSTEM_PROMPT},
                {"role": "user", "content": transcript},
            ],
            temperature=0.0,
            max_tokens=256,
        )
        raw = data["choices"][0]["message"]["content"].strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = re.sub(r'^```(?:json)?\s*', '', raw)
            raw = re.sub(r'\s*```$', '', raw)
        params = json.loads(raw)
        logger.info(f"Extracted params: {params}")
        return params
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logger.warning(f"Parameter extraction failed: {e}")
        return {}
    except Exception as e:
        logger.error(f"GPT extraction error: {e}")
        return {}


# ---------------------------------------------------------------------------
# Step 3: Fuzzy Resolver — match extracted params against catalog vocabulary
# ---------------------------------------------------------------------------

# Field-specific thresholds (Kimi's insight)
THRESHOLDS = {
    "manufacturer": 70,   # Lenient — "altecho" should match "Altech"
    "product_type": 75,
    "media": 80,          # Stricter — PTFE and PVC are edit-distance close but different
    "part_number": 90,    # Very strict — one wrong digit = wrong product
}


def fuzzy_resolve_field(value: str, candidates: list[str], field: str) -> dict:
    """
    Resolve a single field value against catalog vocabulary.
    Returns: {resolved: str, confidence: float, method: str, original: str}
    """
    if not value or not candidates:
        return {"resolved": value, "confidence": 0.0, "method": "none", "original": value}

    value_lower = value.lower().strip()
    threshold = THRESHOLDS.get(field, 75)

    # Tier 0: Exact match
    for c in candidates:
        if c.lower() == value_lower:
            return {"resolved": c, "confidence": 1.0, "method": "exact", "original": value}

    # Tier 1: Synonym table
    syn_match = SYNONYMS.get(value_lower)
    if syn_match:
        for c in candidates:
            if c.lower() == syn_match.lower():
                return {"resolved": c, "confidence": 0.95, "method": "synonym", "original": value}

    # Tier 2: RapidFuzz (Levenshtein + token set ratio)
    result = process.extractOne(
        value_lower,
        [c.lower() for c in candidates],
        scorer=fuzz.token_set_ratio,
    )
    if result:
        match_text, score, idx = result
        if score >= threshold:
            return {
                "resolved": candidates[idx],
                "confidence": score / 100.0,
                "method": "rapidfuzz",
                "original": value,
            }

    # Tier 3: Double Metaphone (phonetic matching)
    value_phonetic = doublemetaphone(value_lower)
    best_phonetic = None
    best_phonetic_score = 0

    phonetic_map = {}
    if field == "manufacturer":
        phonetic_map = _vocab._mfg_phonetic
    elif field == "media":
        phonetic_map = _vocab._media_phonetic

    for candidate, phones in phonetic_map.items():
        for vp in value_phonetic:
            if not vp:
                continue
            for cp in phones:
                if not cp:
                    continue
                if vp == cp:
                    # Phonetic match — score based on edit distance too
                    edit_score = fuzz.ratio(value_lower, candidate.lower())
                    combined = max(edit_score, 70)  # Phonetic match guarantees at least 70
                    if combined > best_phonetic_score:
                        best_phonetic_score = combined
                        best_phonetic = candidate

    if best_phonetic and best_phonetic_score >= threshold:
        return {
            "resolved": best_phonetic,
            "confidence": best_phonetic_score / 100.0,
            "method": "metaphone",
            "original": value,
        }

    # No match above threshold — return best fuzzy match with low confidence
    if result:
        match_text, score, idx = result
        return {
            "resolved": candidates[idx],
            "confidence": score / 100.0,
            "method": "rapidfuzz_low",
            "original": value,
        }

    return {"resolved": value, "confidence": 0.0, "method": "none", "original": value}


def resolve_parameters(params: dict) -> dict:
    """
    Resolve all extracted parameters against catalog vocabulary.
    Returns resolved params with confidence metadata.
    """
    resolved = {}
    metadata = {}

    # Manufacturer
    if params.get("manufacturer"):
        r = fuzzy_resolve_field(params["manufacturer"], _vocab.manufacturers, "manufacturer")
        resolved["manufacturer"] = r["resolved"]
        metadata["manufacturer"] = r

    # Product type
    if params.get("product_type"):
        r = fuzzy_resolve_field(params["product_type"], _vocab.product_types, "product_type")
        resolved["product_type"] = r["resolved"]
        metadata["product_type"] = r

    # Media
    if params.get("media"):
        r = fuzzy_resolve_field(params["media"], _vocab.media_types, "media")
        resolved["media"] = r["resolved"]
        metadata["media"] = r

    # Micron — resolve to closest available value
    if params.get("micron") is not None:
        target = float(params["micron"])
        if _vocab.micron_values:
            closest = min(_vocab.micron_values, key=lambda x: abs(x - target))
            if closest == target:
                resolved["micron"] = target
                metadata["micron"] = {"resolved": target, "confidence": 1.0, "method": "exact", "original": target}
            else:
                resolved["micron"] = target  # Use what they asked for, even if not exact
                metadata["micron"] = {
                    "resolved": target, "confidence": 0.8, "method": "nearest",
                    "original": target, "nearest_available": closest,
                }
        else:
            resolved["micron"] = target
            metadata["micron"] = {"resolved": target, "confidence": 0.5, "method": "no_vocab", "original": target}

    # Part number — strict matching
    if params.get("part_number"):
        r = fuzzy_resolve_field(params["part_number"], _vocab.part_numbers, "part_number")
        resolved["part_number"] = r["resolved"]
        metadata["part_number"] = r

    # Pass-through fields (no fuzzy needed)
    for key in ("max_temp", "max_psi", "flow_rate", "in_stock", "application", "industry"):
        if params.get(key) is not None:
            resolved[key] = params[key]

    return {"params": resolved, "confidence": metadata}


# ---------------------------------------------------------------------------
# Step 4: Query Builder — Pandas filter from resolved parameters
# ---------------------------------------------------------------------------
def voice_query(df: pd.DataFrame, resolved: dict) -> dict:
    """
    Build Pandas query from resolved voice parameters and execute search.
    Returns results dict compatible with existing product card renderer.
    """
    params = resolved["params"]
    confidence = resolved["confidence"]

    if not params:
        return {"results": [], "total_found": 0, "query": "", "search_type": "voice_empty"}

    # If we have a part number, do direct lookup first
    if params.get("part_number"):
        from search import lookup_part
        product = lookup_part(df, params["part_number"])
        if product:
            return {
                "results": [product],
                "total_found": 1,
                "query": params["part_number"],
                "search_type": "voice_part_lookup",
                "voice_confidence": confidence,
            }

    # Build filter mask
    mask = pd.Series(True, index=df.index)
    filters_applied = []

    # Manufacturer
    if params.get("manufacturer"):
        mfg_col = "Final_Manufacturer" if "Final_Manufacturer" in df.columns else "Manufacturer"
        if mfg_col in df.columns:
            mask &= df[mfg_col].astype(str).str.contains(
                re.escape(params["manufacturer"]), case=False, na=False
            )
            filters_applied.append(f"manufacturer={params['manufacturer']}")

    # Product type
    if params.get("product_type"):
        if "Product_Type" in df.columns:
            mask &= df["Product_Type"].astype(str).str.contains(
                re.escape(params["product_type"]), case=False, na=False
            )
            filters_applied.append(f"type={params['product_type']}")

    # Media
    if params.get("media"):
        if "Media" in df.columns:
            mask &= df["Media"].astype(str).str.contains(
                re.escape(params["media"]), case=False, na=False
            )
            filters_applied.append(f"media={params['media']}")

    # Micron (exact)
    if params.get("micron") is not None:
        if "Micron" in df.columns:
            micron_col = pd.to_numeric(df["Micron"], errors="coerce").fillna(0)
            mask &= micron_col == float(params["micron"])
            filters_applied.append(f"micron={params['micron']}")

    # Max temp (products rated AT or ABOVE)
    if params.get("max_temp") is not None:
        if "Max_Temp_F" in df.columns:
            temp_col = pd.to_numeric(df["Max_Temp_F"], errors="coerce").fillna(0)
            mask &= temp_col >= float(params["max_temp"])
            filters_applied.append(f"temp>={params['max_temp']}F")

    # Max PSI (products rated AT or ABOVE)
    if params.get("max_psi") is not None:
        if "Max_PSI" in df.columns:
            psi_col = pd.to_numeric(df["Max_PSI"], errors="coerce").fillna(0)
            mask &= psi_col >= float(params["max_psi"])
            filters_applied.append(f"psi>={params['max_psi']}")

    # Application
    if params.get("application"):
        if "Application" in df.columns:
            mask &= df["Application"].astype(str).str.contains(
                re.escape(params["application"]), case=False, na=False
            )
            filters_applied.append(f"application={params['application']}")

    # Industry
    if params.get("industry"):
        if "Industry" in df.columns:
            mask &= df["Industry"].astype(str).str.contains(
                re.escape(params["industry"]), case=False, na=False
            )
            filters_applied.append(f"industry={params['industry']}")

    # In stock
    if params.get("in_stock"):
        if "Total_Stock" in df.columns:
            mask &= df["Total_Stock"] > 0
            filters_applied.append("in_stock=true")

    results_df = df[mask]
    total_found = len(results_df)

    # Relaxation: if multi-filter returns 0, drop least critical filter and retry
    if total_found == 0 and len(filters_applied) >= 2:
        # Try dropping application/industry first, then media, then product_type
        relax_order = ["application", "industry", "media", "product_type"]
        for drop_key in relax_order:
            if params.get(drop_key):
                relaxed_mask = pd.Series(True, index=df.index)
                for fkey in ["manufacturer", "product_type", "media", "micron", "max_temp", "max_psi", "application", "industry"]:
                    if fkey == drop_key or not params.get(fkey):
                        continue
                    if fkey == "manufacturer":
                        mfg_col2 = "Final_Manufacturer" if "Final_Manufacturer" in df.columns else "Manufacturer"
                        relaxed_mask &= df[mfg_col2].astype(str).str.contains(re.escape(params[fkey]), case=False, na=False)
                    elif fkey == "product_type" and "Product_Type" in df.columns:
                        relaxed_mask &= df["Product_Type"].astype(str).str.contains(re.escape(params[fkey]), case=False, na=False)
                    elif fkey == "media" and "Media" in df.columns:
                        relaxed_mask &= df["Media"].astype(str).str.contains(re.escape(params[fkey]), case=False, na=False)
                    elif fkey == "micron" and "Micron" in df.columns:
                        relaxed_mask &= pd.to_numeric(df["Micron"], errors="coerce").fillna(0) == float(params[fkey])
                    elif fkey == "application" and "Application" in df.columns:
                        relaxed_mask &= df["Application"].astype(str).str.contains(re.escape(params[fkey]), case=False, na=False)
                    elif fkey == "industry" and "Industry" in df.columns:
                        relaxed_mask &= df["Industry"].astype(str).str.contains(re.escape(params[fkey]), case=False, na=False)
                relaxed_df = df[relaxed_mask]
                if len(relaxed_df) > 0:
                    results_df = relaxed_df
                    total_found = len(results_df)
                    filters_applied.append(f"relaxed(dropped {drop_key})")
                    break

    # Prefer in-stock results if not already filtered
    if not params.get("in_stock") and "Total_Stock" in results_df.columns and not results_df.empty:
        stocked = results_df[results_df["Total_Stock"] > 0]
        if not stocked.empty:
            results_df = stocked

    results = [format_product(row) for _, row in results_df.head(5).iterrows()]

    # Calculate overall confidence
    field_confidences = [v.get("confidence", 0) for v in confidence.values() if isinstance(v, dict)]
    overall_confidence = min(field_confidences) if field_confidences else 0.0

    # Build "did you mean" suggestions for low-confidence fields
    suggestions = []
    for field_name, meta in confidence.items():
        if isinstance(meta, dict) and meta.get("confidence", 1.0) < 0.90:
            suggestions.append({
                "field": field_name,
                "original": meta.get("original", ""),
                "resolved": meta.get("resolved", ""),
                "confidence": meta.get("confidence", 0),
                "method": meta.get("method", ""),
            })

    return {
        "results": results,
        "total_found": total_found,
        "has_more": total_found > 5,
        "query": " + ".join(filters_applied) if filters_applied else "voice search",
        "search_type": "voice_search",
        "voice_confidence": confidence,
        "overall_confidence": overall_confidence,
        "suggestions": suggestions,
        "filters_applied": filters_applied,
    }


# ---------------------------------------------------------------------------
# Main Pipeline — full voice search flow
# ---------------------------------------------------------------------------
async def voice_search_pipeline(transcript: str, df: pd.DataFrame) -> dict:
    """
    Full voice search pipeline:
    1. Pre-process transcript
    2. Detect part number (fast path)
    3. Extract parameters via GPT-4.1-mini
    4. Fuzzy resolve against catalog
    5. Query DataFrame
    6. Return results with confidence

    Returns dict ready for frontend rendering.
    """
    if not transcript or not transcript.strip():
        return {
            "results": [],
            "total_found": 0,
            "transcript": "",
            "search_type": "voice_empty",
            "error": "No transcript received",
        }

    # Step 1: Pre-process
    cleaned = preprocess_transcript(transcript)
    logger.info(f"Voice search — raw: '{transcript}' → cleaned: '{cleaned}'")

    # Step 2: Fast path — direct part number detection
    part_num = detect_part_number(cleaned)
    if part_num:
        from search import lookup_part
        product = lookup_part(df, part_num)
        if product:
            return {
                "results": [product],
                "total_found": 1,
                "transcript": transcript,
                "cleaned_transcript": cleaned,
                "search_type": "voice_part_lookup",
                "overall_confidence": 1.0,
                "suggestions": [],
                "filters_applied": [f"part_number={part_num}"],
            }

    # Step 3: Extract parameters
    params = await extract_parameters(cleaned)
    if not params:
        # Fallback: use the cleaned transcript as a regular text search
        result = search_products(df, cleaned)
        result["transcript"] = transcript
        result["cleaned_transcript"] = cleaned
        result["search_type"] = "voice_fallback_text"
        result["overall_confidence"] = 0.5
        result["suggestions"] = []
        return result

    # Step 4: Fuzzy resolve
    resolved = resolve_parameters(params)

    # Step 5: Query
    result = voice_query(df, resolved)
    result["transcript"] = transcript
    result["cleaned_transcript"] = cleaned
    result["raw_params"] = params

    # Step 6: If voice query returned nothing, fall back to text search
    if not result["results"]:
        fallback = search_products(df, cleaned)
        if fallback["results"]:
            result["results"] = fallback["results"]
            result["total_found"] = fallback["total_found"]
            result["search_type"] = "voice_fallback_text"

    return result
