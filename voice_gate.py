"""
Enpro Voice Gate — Data-Aware Lookup & Response Layer
Handles missing data honestly, tries all 4 lookup paths.
"""

import pandas as pd
import re
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass
from pathlib import Path
import json
from datetime import datetime

@dataclass
class LookupResult:
    found: bool
    part_number: Optional[str]
    alt_code: Optional[str]
    supplier_code: Optional[str]
    manufacturer: Optional[str]
    description: Optional[str]
    in_stock: Optional[bool]  # None = UNKNOWN (not "no")
    qty_on_hand: Optional[int]
    price: Optional[float]  # None = not on file (not $0)
    micron: Optional[str]
    media: Optional[str]
    max_temp_f: Optional[float]
    max_psi: Optional[float]
    application: Optional[str]
    industry: Optional[str]
    match_confidence: str
    lookup_path: str  # which of the 4 tiers succeeded
    
    # Data quality flags
    stock_known: bool  # True = we have data, False = unknown
    price_known: bool  # True = valid price exists


class VoiceGate:
    """
    Data-aware voice lookup gate.
    NEVER returns "out of stock" when data is missing.
    NEVER returns "$0" as a price.
    """
    
    def __init__(self, catalog_path: str):
        """Load catalog and build multi-tier indexes."""
        self.df = pd.read_csv(catalog_path)
        
        # Clean up common null representations
        for col in self.df.columns:
            self.df[col] = self.df[col].replace(['NULL', 'null', 'NaN', 'nan', ''], pd.NA)
        
        # TIER 1: Alt_Code (100% populated per analysis)
        self.alt_code_index = {}
        for _, row in self.df.iterrows():
            alt = str(row.get('Alt_Code', '')).strip().upper()
            if alt and alt != 'NAN' and alt != 'NONE' and alt != '<NA>':
                self.alt_code_index[alt] = row
        
        # TIER 2: Part_Number (89% populated)
        self.part_number_index = {}
        for _, row in self.df.iterrows():
            pn = str(row.get('Part_Number', '')).strip().upper()
            if pn and pn != 'NAN' and pn != 'NONE' and pn != '<NA>':
                self.part_number_index[pn] = row
        
        # TIER 3: Supplier_Code (82% populated)
        self.supplier_code_index = {}
        for _, row in self.df.iterrows():
            sc = str(row.get('Supplier_Code', '')).strip().upper()
            if sc and sc != 'NAN' and sc != 'NONE' and sc != '<NA>':
                if sc not in self.supplier_code_index:
                    self.supplier_code_index[sc] = []
                self.supplier_code_index[sc].append(row)
        
        # Pall prefixes for fast Pall routing
        self.pall_prefixes = ['HC', 'UE', 'TX', 'CORAL', 'PROFILE', 'POLY', 'LLS', 'LLH', 'AC', 'DFA']
        
        print(f"VoiceGate loaded: {len(self.df):,} products")
        print(f"  Tier 1 (Alt_Code): {len(self.alt_code_index):,} indexed")
        print(f"  Tier 2 (Part_Number): {len(self.part_number_index):,} indexed")
        print(f"  Tier 3 (Supplier_Code): {len(self.supplier_code_index):,} indexed")
    
    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> "VoiceGate":
        """Create VoiceGate from existing DataFrame (for server integration)."""
        instance = cls.__new__(cls)
        instance.df = df.copy()
        
        # Clean up common null representations
        for col in instance.df.columns:
            instance.df[col] = instance.df[col].replace(['NULL', 'null', 'NaN', 'nan', ''], pd.NA)
        
        # TIER 1: Alt_Code
        instance.alt_code_index = {}
        for _, row in instance.df.iterrows():
            alt = str(row.get('Alt_Code', '')).strip().upper()
            if alt and alt not in ('NAN', 'NONE', '<NA>', ''):
                instance.alt_code_index[alt] = row
        
        # TIER 2: Part_Number
        instance.part_number_index = {}
        for _, row in instance.df.iterrows():
            pn = str(row.get('Part_Number', '')).strip().upper()
            if pn and pn not in ('NAN', 'NONE', '<NA>', ''):
                instance.part_number_index[pn] = row
        
        # TIER 3: Supplier_Code
        instance.supplier_code_index = {}
        for _, row in instance.df.iterrows():
            sc = str(row.get('Supplier_Code', '')).strip().upper()
            if sc and sc not in ('NAN', 'NONE', '<NA>', ''):
                if sc not in instance.supplier_code_index:
                    instance.supplier_code_index[sc] = []
                instance.supplier_code_index[sc].append(row)
        
        # Pall prefixes
        instance.pall_prefixes = ['HC', 'UE', 'TX', 'CORAL', 'PROFILE', 'POLY', 'LLS', 'LLH', 'AC', 'DFA']
        
        print(f"VoiceGate loaded from DataFrame: {len(instance.df):,} products")
        return instance
    
    def lookup(self, query: str) -> LookupResult:
        """
        4-Tier lookup: Alt_Code → Part_Number → Supplier_Code → Description
        """
        query = query.strip().upper()
        
        # TIER 1: Alt_Code (100% populated - always try first)
        if query in self.alt_code_index:
            return self._row_to_result(
                self.alt_code_index[query],
                confidence='exact',
                path='Alt_Code'
            )
        
        # TIER 2: Part_Number (89% populated)
        if query in self.part_number_index:
            return self._row_to_result(
                self.part_number_index[query],
                confidence='exact',
                path='Part_Number'
            )
        
        # TIER 3: Supplier_Code (82% populated)
        if query in self.supplier_code_index:
            matches = self.supplier_code_index[query]
            if len(matches) == 1:
                return self._row_to_result(
                    matches[0],
                    confidence='exact',
                    path='Supplier_Code'
                )
            else:
                # Multiple matches - ambiguity
                return self._row_to_result(
                    matches[0],
                    confidence='exact',
                    path=f'Supplier_Code (1 of {len(matches)} matches)'
                )
        
        # TIER 4: Description keyword search
        return self._description_search(query)
    
    def lookup_pall_fast(self, query: str) -> Optional[LookupResult]:
        """Fast path for Pall Corporation parts."""
        query = query.strip().upper()
        
        # Check if it looks like a Pall part
        is_pall = any(query.startswith(prefix) for prefix in self.pall_prefixes)
        
        if is_pall or 'PALL' in query:
            # Try exact Alt_Code first
            if query in self.alt_code_index:
                row = self.alt_code_index[query]
                mfg = str(row.get('Final_Manufacturer', row.get('Manufacturer', ''))).upper()
                if 'PALL' in mfg:
                    return self._row_to_result(row, 'exact', 'Pall_Alt_Code')
            
            # Try Part_Number
            if query in self.part_number_index:
                row = self.part_number_index[query]
                mfg = str(row.get('Final_Manufacturer', row.get('Manufacturer', ''))).upper()
                if 'PALL' in mfg:
                    return self._row_to_result(row, 'exact', 'Pall_Part_Number')
        
        return None
    
    def _description_search(self, query: str) -> LookupResult:
        """Fuzzy search in Description field (TIER 4 - last resort)."""
        # Extract keywords (words > 2 chars)
        keywords = [w for w in query.split() if len(w) > 2]
        
        if not keywords:
            return self._not_found_result()
        
        # Score each row by keyword matches
        best_score = 0
        best_row = None
        
        for _, row in self.df.iterrows():
            desc = str(row.get('Description', '')).upper()
            score = sum(1 for kw in keywords if kw in desc)
            
            if score > best_score:
                best_score = score
                best_row = row
        
        # Only return if we have decent matches
        if best_row and best_score >= len(keywords) / 2:
            return self._row_to_result(
                best_row,
                confidence='fuzzy',
                path=f'Description_keyword ({best_score} matches)'
            )
        
        return self._not_found_result()
    
    def _row_to_result(self, row: pd.Series, confidence: str, path: str) -> LookupResult:
        """Convert DataFrame row to LookupResult with DATA QUALITY handling."""
        
        # Handle price: $0 or NaN = None (NOT ON FILE - never show $0)
        price_raw = row.get('Price')
        price = None
        price_known = False
        try:
            if pd.notna(price_raw):
                p = float(price_raw)
                if p > 0:
                    price = p
                    price_known = True
        except:
            pass
        
        # Handle stock: use Total_Stock (computed by merge_data)
        in_stock = None
        qty = None
        stock_known = False
        try:
            total_stock = row.get('Total_Stock', 0)
            if pd.notna(total_stock):
                qty = int(float(total_stock))
                in_stock = qty > 0
                stock_known = True
        except (ValueError, TypeError):
            pass
        
        # Helper to get clean string
        def get_str(col):
            val = row.get(col)
            return str(val) if pd.notna(val) else None
        
        return LookupResult(
            found=True,
            part_number=get_str('Part_Number'),
            alt_code=get_str('Alt_Code'),
            supplier_code=get_str('Supplier_Code'),
            manufacturer=get_str('Final_Manufacturer') or get_str('Manufacturer'),
            description=get_str('Description'),
            in_stock=in_stock,  # None = UNKNOWN
            qty_on_hand=qty,
            price=price,  # None = NOT ON FILE
            micron=get_str('Micron'),
            media=get_str('Media'),
            max_temp_f=row.get('Max_Temp_F') if pd.notna(row.get('Max_Temp_F')) else None,
            max_psi=row.get('Max_PSI') if pd.notna(row.get('Max_PSI')) else None,
            application=get_str('Application'),
            industry=get_str('Industry'),
            match_confidence=confidence,
            lookup_path=path,
            stock_known=stock_known,
            price_known=price_known
        )
    
    def _not_found_result(self) -> LookupResult:
        """Return 'not found' result after all 4 tiers exhausted."""
        return LookupResult(
            found=False,
            part_number=None,
            alt_code=None,
            supplier_code=None,
            manufacturer=None,
            description=None,
            in_stock=None,
            qty_on_hand=None,
            price=None,
            micron=None,
            media=None,
            max_temp_f=None,
            max_psi=None,
            application=None,
            industry=None,
            match_confidence='none',
            lookup_path='none',
            stock_known=False,
            price_known=False
        )
    
    def search_by_criteria(self,
                          application: Optional[str] = None,
                          micron_min: Optional[float] = None,
                          micron_max: Optional[float] = None,
                          min_psi: Optional[int] = None,
                          max_psi: Optional[int] = None,
                          media: Optional[str] = None,
                          manufacturer: Optional[str] = None,
                          in_stock_only: bool = False) -> List[LookupResult]:
        """
        Search by product criteria (for Tier 1 Gates).
        """
        matches = []
        
        for _, row in self.df.iterrows():
            # Application filter (search Description too if Application missing)
            if application:
                app_field = str(row.get('Application', '')).upper()
                desc_field = str(row.get('Description', '')).upper()
                if application.upper() not in app_field and application.upper() not in desc_field:
                    continue
            
            # Micron filter
            if micron_min is not None or micron_max is not None:
                micron_val = row.get('Micron')
                if pd.notna(micron_val):
                    try:
                        m = float(micron_val)
                        if micron_min is not None and m < micron_min:
                            continue
                        if micron_max is not None and m > micron_max:
                            continue
                    except:
                        continue
            
            # PSI filter
            if min_psi is not None or max_psi is not None:
                psi = row.get('Max_PSI')
                if pd.notna(psi):
                    try:
                        p = int(float(psi))
                        if min_psi is not None and p < min_psi:
                            continue
                        if max_psi is not None and p > max_psi:
                            continue
                    except:
                        continue
            
            # Media filter
            if media:
                media_field = str(row.get('Media', '')).upper()
                if media.upper() not in media_field:
                    continue
            
            # Manufacturer filter
            if manufacturer:
                mfg = str(row.get('Final_Manufacturer', row.get('Manufacturer', ''))).upper()
                if manufacturer.upper() not in mfg:
                    continue
            
            # In stock filter — use Total_Stock
            if in_stock_only:
                try:
                    total = float(row.get('Total_Stock', 0) or 0)
                    if total <= 0:
                        continue
                except (ValueError, TypeError):
                    continue
            
            matches.append(self._row_to_result(row, 'criteria_match', 'search_by_criteria'))
        
        return matches
    
    def log_miss(self, utterance: str, intent: str, entities: dict):
        """Log queries that fall through all gates."""
        miss = {
            "timestamp": datetime.utcnow().isoformat(),
            "utterance": utterance,
            "classified_intent": intent,
            "gate_matched": False,
            "entities_extracted": entities,
            "fallback_used": True
        }
        
        # Append to miss log
        log_path = Path("data/voice_gate_misses.jsonl")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, 'a') as f:
            f.write(json.dumps(miss) + '\n')


# Response Templates — HONEST about data gaps
def format_voice_response(result: LookupResult, include_specs: bool = True) -> str:
    """
    Format lookup result with DATA-QUALITY-AWARE templates.
    NEVER says "out of stock" when data is missing.
    NEVER shows "$0" as a price.
    """
    
    if not result.found:
        return (
            "I couldn't match that exactly. Can you give me:\n"
            "1. The Alt_Code (most reliable)\n" 
            "2. The Part_Number\n"
            "3. Or describe the application and I'll find options"
        )
    
    lines = []
    
    # Part identification
    id_parts = []
    if result.alt_code:
        id_parts.append(result.alt_code)
    elif result.part_number:
        id_parts.append(result.part_number)
    
    if result.manufacturer:
        id_parts.append(f"by {result.manufacturer}")
    
    if id_parts:
        lines.append(" ".join(id_parts))
    
    # Stock status — HONEST about unknown
    if result.stock_known:
        if result.in_stock is True:
            if result.qty_on_hand and result.qty_on_hand > 0:
                lines.append(f"— {result.qty_on_hand} units confirmed in stock")
            else:
                lines.append("— In stock")
        else:
            lines.append("— Confirmed out of stock")
    else:
        lines.append("— Inventory not confirmed — check with warehouse or add to quote as pending")
    
    # Price — NEVER show $0
    if result.price_known and result.price:
        lines.append(f"— ${result.price:.2f}")
    else:
        lines.append("— Price not on file — flagging for quote team")
    
    # Specs if available
    if include_specs:
        specs = []
        if result.micron:
            specs.append(f"{result.micron} micron")
        if result.media:
            specs.append(result.media)
        if result.max_psi:
            specs.append(f"{int(result.max_psi)} PSI")
        if specs:
            lines.append(f"— Specs: {', '.join(specs)}")
    
    return "\n".join(lines)


# Tier 1 Gates — deploy these first
class Tier1Gates:
    """High-volume gates covering ~70% of rep queries."""
    
    def __init__(self, gate: VoiceGate):
        self.gate = gate
    
    def gate_1_hydraulic_lube(self) -> List[LookupResult]:
        """
        Gate 1: Hydraulic/Lube Oil + Medium PSI (75-150) + Fine micron (1-10)
        Highest volume application.
        """
        return self.gate.search_by_criteria(
            application="hydraulic",
            min_psi=75,
            max_psi=150,
            micron_min=1,
            micron_max=10
        )
    
    def gate_2_pall_crosswalk(self, query: str) -> Optional[LookupResult]:
        """
        Gate 2: Pall Corporation parts.
        41% of catalog. Direct lookup, no reasoning needed.
        """
        return self.gate.lookup_pall_fast(query)
    
    def gate_3_compressed_air(self) -> List[LookupResult]:
        """
        Gate 3: Compressed Air + Absolute efficiency.
        425 products for compressed air.
        """
        return self.gate.search_by_criteria(
            application="compressed air",
            media="Absolute"
        )


# Export for use
__all__ = ['VoiceGate', 'LookupResult', 'Tier1Gates', 'format_voice_response']


if __name__ == "__main__":
    # Test the Voice Gate
    gate = VoiceGate(r"C:\Claude\Repos\enpro-fm-portal\data\Filtration_GPT_Filters_V25.csv")
    
    # Test lookups
    test_queries = [
        "HC9021",  # Pall prefix
        "CLR10295",  # Part number
        "some_random_part",  # Should fail all 4 tiers
    ]
    
    print("\n" + "="*60)
    print("VOICE GATE TEST")
    print("="*60)
    
    for query in test_queries:
        result = gate.lookup(query)
        print(f"\nQuery: {query}")
        print(f"  Found: {result.found}")
        print(f"  Path: {result.lookup_path}")
        print(f"  Stock Known: {result.stock_known}, In Stock: {result.in_stock}")
        print(f"  Price Known: {result.price_known}, Price: {result.price}")
        if result.found:
            print(f"  Response:\n    {format_voice_response(result).replace(chr(10), chr(10)+'    ')}")
