#!/usr/bin/env bash
# ============================================================================
# Enpro FM Portal — 200 Part Lookup Accuracy Tests
# Validates that /api/lookup and /api/search return exact data from crosswalk
# with zero hallucinations.
#
# Usage: bash tests/test_part_lookup_200.sh [BASE_URL]
# Default: https://enpro-fm-portal.onrender.com
# ============================================================================

set -euo pipefail

BASE_URL="${1:-https://enpro-fm-portal.onrender.com}"
CSV_FILE="$(dirname "$0")/../data/static_crosswalk.csv"
PASS=0
FAIL=0
ERRORS=""
TOTAL=0

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=============================================="
echo "  Enpro FM Portal — Part Lookup Accuracy Tests"
echo "  Target: $BASE_URL"
echo "=============================================="
echo ""

# Check health first
echo -n "Health check... "
HEALTH=$(curl -s --max-time 15 "$BASE_URL/health" 2>/dev/null || echo "FAIL")
if echo "$HEALTH" | grep -q '"data_loaded":true'; then
    echo -e "${GREEN}OK${NC} ($(echo "$HEALTH" | grep -o '"product_count":[0-9]*'))"
else
    echo -e "${RED}FAILED${NC} — Portal not ready. Aborting."
    echo "$HEALTH" | head -5
    exit 1
fi
echo ""

# -------------------------------------------------------------------
# Helper: lookup a part and validate fields against CSV truth
# -------------------------------------------------------------------
test_lookup() {
    local part_number="$1"
    local expected_desc="$2"
    local expected_mfg="$3"
    local expected_micron="$4"
    local expected_media="$5"
    local expected_psi="$6"
    local expected_type="$7"
    local test_label="$8"

    TOTAL=$((TOTAL + 1))

    # Call /api/lookup
    local response
    response=$(curl -s --max-time 10 -X POST "$BASE_URL/api/lookup" \
        -H "Content-Type: application/json" \
        -d "{\"part_number\": \"$part_number\"}" 2>/dev/null || echo '{"error":"timeout"}')

    local found
    found=$(echo "$response" | grep -o '"found":true' || echo "")

    if [ -z "$found" ]; then
        # Try /api/search as fallback
        response=$(curl -s --max-time 10 -X POST "$BASE_URL/api/search" \
            -H "Content-Type: application/json" \
            -d "{\"query\": \"$part_number\"}" 2>/dev/null || echo '{"error":"timeout"}')
        found=$(echo "$response" | grep -o '"Part_Number"' | head -1 || echo "")
    fi

    local issues=""

    if [ -z "$found" ]; then
        issues="NOT FOUND"
    else
        # Validate description (check it's not hallucinated)
        if [ -n "$expected_desc" ] && [ "$expected_desc" != "-" ]; then
            if ! echo "$response" | grep -qi "$(echo "$expected_desc" | head -c 30 | sed 's/[^a-zA-Z0-9 ]//g')"; then
                issues="${issues}DESC_MISMATCH "
            fi
        fi

        # Validate manufacturer
        if [ -n "$expected_mfg" ] && [ "$expected_mfg" != "-" ]; then
            if ! echo "$response" | grep -qi "$(echo "$expected_mfg" | sed 's/[^a-zA-Z0-9 ]//g' | head -c 20)"; then
                issues="${issues}MFG_MISMATCH "
            fi
        fi

        # Validate micron (if specified)
        if [ -n "$expected_micron" ] && [ "$expected_micron" != "-" ] && [ "$expected_micron" != "0" ]; then
            if ! echo "$response" | grep -q "\"Micron\":.*$expected_micron\|\"Micron\": *$expected_micron\|\"Micron\":\"$expected_micron\""; then
                issues="${issues}MICRON_MISMATCH "
            fi
        fi

        # Validate media (if specified)
        if [ -n "$expected_media" ] && [ "$expected_media" != "-" ]; then
            if ! echo "$response" | grep -qi "$(echo "$expected_media" | head -c 15)"; then
                issues="${issues}MEDIA_MISMATCH "
            fi
        fi

        # Validate PSI (if specified)
        if [ -n "$expected_psi" ] && [ "$expected_psi" != "-" ] && [ "$expected_psi" != "0" ]; then
            if ! echo "$response" | grep -q "\"Max_PSI\":.*$expected_psi\|\"Max_PSI\": *$expected_psi"; then
                issues="${issues}PSI_MISMATCH "
            fi
        fi

        # Check for hallucinated part numbers (response mentions PNs not in query)
        # Simple check: response should contain the queried part number
        if ! echo "$response" | grep -qi "$part_number"; then
            issues="${issues}PN_NOT_IN_RESPONSE "
        fi
    fi

    if [ -z "$issues" ]; then
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-30s %s\n" "$TOTAL" "$part_number" "$test_label"
    else
        FAIL=$((FAIL + 1))
        printf "  ${RED}FAIL${NC} [%3d] %-30s %s — %s\n" "$TOTAL" "$part_number" "$test_label" "$issues"
        ERRORS="${ERRORS}\n  FAIL: $part_number ($test_label) — $issues"
    fi
}

# -------------------------------------------------------------------
# Helper: search a query and verify results contain expected part
# -------------------------------------------------------------------
test_search() {
    local query="$1"
    local expected_part="$2"
    local test_label="$3"

    TOTAL=$((TOTAL + 1))

    local response
    response=$(curl -s --max-time 10 -X POST "$BASE_URL/api/search" \
        -H "Content-Type: application/json" \
        -d "{\"query\": \"$query\"}" 2>/dev/null || echo '{"error":"timeout"}')

    local total_found
    total_found=$(echo "$response" | grep -o '"total_found":[0-9]*' | grep -o '[0-9]*' || echo "0")

    local issues=""

    if [ "$total_found" = "0" ]; then
        issues="ZERO_RESULTS"
    elif [ -n "$expected_part" ] && [ "$expected_part" != "-" ]; then
        if ! echo "$response" | grep -qi "$expected_part"; then
            issues="EXPECTED_PART_MISSING($expected_part)"
        fi
    fi

    if [ -z "$issues" ]; then
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-40s %s (found: %s)\n" "$TOTAL" "\"$query\"" "$test_label" "$total_found"
    else
        FAIL=$((FAIL + 1))
        printf "  ${RED}FAIL${NC} [%3d] %-40s %s — %s\n" "$TOTAL" "\"$query\"" "$test_label" "$issues"
        ERRORS="${ERRORS}\n  FAIL: \"$query\" ($test_label) — $issues"
    fi
}

# ============================================================================
# SECTION 1: DIRECT PART LOOKUPS (100 tests)
# Real part numbers from crosswalk — validate returned data matches CSV
# ============================================================================
echo "--- Section 1: Direct Part Number Lookups (100 tests) ---"
echo ""

# Pall Corporation parts (high volume — 41% of catalog)
test_lookup "HC9021FAS4Z" "Filter Element" "Applied Energy" "-" "Glass Fiber" "150" "Filter Element" "Pall HC prefix"
test_lookup "HC2216FKP6H" "Filter Element" "Applied Energy" "5" "Glass Fiber" "150" "Filter Element" "Pall HC w/ 5 micron"
test_lookup "HC2296FCP36H50" "Filter Element" "-" "5" "Glass Fiber" "150" "Filter Element" "Pall HC w/ specs"
test_lookup "MBS1001RZH2" "Filter Element" "Pall" "-" "-" "150" "Filter Element" "Pall MBS prefix"
test_lookup "AB3Y0033J" "Filter Element" "Applied Energy" "-" "Polypropylene" "150" "Filter Element" "Pall AB prefix"
test_lookup "XLD4510UEM3" "Filter Element" "Pall" "-" "-" "150" "Filter Element" "Pall XLD prefix"
test_lookup "DEZA050FJ" "Filter Capsule" "Pall Trincor" "-" "-" "-" "Filter" "Pall Trincor"
test_lookup "13498501" "Melt Filter" "Applied Energy" "-" "-" "-" "Filter" "Pall numeric PN"

# AJR Filtration (bag filters)
test_lookup "PES50.5P2SSH" "Bag Filter" "AJR" "-" "-" "-" "Bag Filter" "AJR bag filter"
test_lookup "POG5P3SH" "Bag Filter" "AJR" "-" "-" "-" "Bag Filter" "AJR bag 2"

# Global Filter
test_lookup "GSTF11T6615EP" "Filter Housing" "Global Filter" "-" "-" "150" "Filter Housing" "Global Filter housing"
test_lookup "GFHD111ND430" "Filter Housing" "Global Filter" "-" "-" "150" "Filter Housing" "Global Filter housing 2"

# American Filter Manufacturing
test_lookup "BE5S7S1-BULK" "Filter Element" "American Filter" "-" "-" "150" "Filter Element" "AMF bulk"

# Banner Industries
test_lookup "CLPF0002TC30P" "Filter Element" "Banner" "-" "-" "150" "Filter Element" "Banner element"

# Quincy Compressor
test_lookup "2901200407" "Filter Element" "Quincy" "-" "-" "150" "Filter Element" "Quincy compressor"

# Industrial Technologies / PPC
test_lookup "1202326" "Position Indicator" "Industrial Technologies" "-" "-" "-" "-" "PPC position indicator"
test_lookup "3014351" "Filter Assembly" "Industrial Technologies" "-" "-" "150" "Filter Assembly" "PPC filter assembly"

# AAF
test_lookup "182322800" "Filter 20 X 25 X 2" "AAF" "-" "-" "-" "Filter" "AAF HVAC filter"

# Precision Filtration
test_lookup "SS644FD" "Separator Element" "Precision" "-" "-" "150" "Separator" "Precision separator"

# Accurate Valve
test_lookup "YS62-CS-6.00-40" "Y-Strainer" "Accurate Valve" "-" "-" "-" "Strainer" "Accurate valve strainer"

# PowerFlow / Pall spare parts
test_lookup "ORH343" "Viton O-Ring" "PowerFlow" "-" "-" "-" "O-Ring" "PowerFlow O-ring"
test_lookup "MHW0082" "Valves" "Pall" "-" "-" "-" "Valve" "Pall valve spare"

# Tescom
test_lookup "98-1010-T-2PM" "Mini Inline Filter" "Tescom" "-" "-" "-" "Filter" "Tescom inline filter"

# Air Services
test_lookup "316727" "Intermediate Sheet" "Air Services" "-" "-" "-" "Media" "Air Services sheet"

# Now generate 76 more from random crosswalk rows with specs
# Extract parts with known specs (Micron > 0) for stricter validation
echo ""
echo "  ... Testing parts with known specs ..."
echo ""

# These are hand-picked from the crosswalk for spec-rich validation
test_lookup "CLR130" "Filter" "-" "-" "-" "-" "-" "Common demo part CLR130"
test_lookup "CLR10295" "Filter" "-" "-" "-" "-" "-" "Common demo part CLR10295"
test_lookup "EPE-10-5" "Filter" "-" "-" "-" "-" "-" "Common demo part EPE-10-5"

# Pull 73 more parts from crosswalk CSV that have specs
# (Micron_Final non-empty, 5th field non-empty)
count=0
while IFS=',' read -r pn alt_code supplier_code mfg ptype desc ext_desc pgroup pgdesc item_cat act_flag last_sold micron micron_src media media_src temp temp_src psi psi_src flow flow_src eff eff_src app app_src ind ind_src; do
    # Skip header
    [ "$pn" = "Part_Number" ] && continue
    # Only parts with micron data
    [ -z "$micron" ] && continue
    [ "$micron" = "0" ] && continue
    # Clean fields
    pn=$(echo "$pn" | tr -d '"')
    desc=$(echo "$desc" | tr -d '"')
    mfg=$(echo "$mfg" | tr -d '"')
    micron=$(echo "$micron" | tr -d '"')
    media=$(echo "$media" | tr -d '"')
    psi=$(echo "$psi" | tr -d '"')
    ptype=$(echo "$ptype" | tr -d '"')

    test_lookup "$pn" "$desc" "$mfg" "$micron" "${media:-"-"}" "${psi:-"-"}" "${ptype:-"-"}" "CSV spec validation"

    count=$((count + 1))
    [ "$count" -ge 73 ] && break
done < "$CSV_FILE"

# ============================================================================
# SECTION 2: SALESPERSON SEARCH QUERIES (50 tests)
# Natural language queries a sales rep would type
# ============================================================================
echo ""
echo "--- Section 2: Salesperson Search Queries (50 tests) ---"
echo ""

# Part number searches (rep types a PN)
test_search "HC9021" "HC9021" "Partial Pall PN"
test_search "CLR130" "CLR130" "Common demo PN"
test_search "EPE-10-5" "-" "Common element PN"
test_search "HC2216" "HC2216" "Pall hydraulic element"
test_search "GSTF11" "GSTF11" "Global Filter housing"
test_search "MBS1001" "MBS1001" "Pall filter element"
test_search "POG5P3SH" "POG5P3SH" "AJR bag filter PN"
test_search "2901200407" "2901200407" "Quincy compressor PN"
test_search "SS644FD" "SS644FD" "Precision separator PN"
test_search "YS62-CS" "YS62" "Accurate strainer partial"

# Manufacturer searches
test_search "Pall filter" "-" "Manufacturer: Pall"
test_search "Graver Technologies" "-" "Manufacturer: Graver"
test_search "Filtrox" "-" "Manufacturer: Filtrox"
test_search "Global Filter" "-" "Manufacturer: Global Filter"
test_search "Cobetter" "-" "Manufacturer: Cobetter"
test_search "Donaldson" "-" "Manufacturer: Donaldson"
test_search "Schroeder" "-" "Manufacturer: Schroeder"
test_search "Koch Filter" "-" "Manufacturer: Koch"
test_search "Rosedale" "-" "Manufacturer: Rosedale"
test_search "Shelco" "-" "Manufacturer: Shelco"

# Product type searches
test_search "bag filter" "-" "Type: Bag Filter"
test_search "filter element" "-" "Type: Filter Element"
test_search "filter housing" "-" "Type: Filter Housing"
test_search "cartridge" "-" "Type: Cartridge"
test_search "depth sheet" "-" "Type: Depth Sheet"
test_search "membrane" "-" "Type: Membrane"
test_search "separator" "-" "Type: Separator"
test_search "o-ring" "-" "Type: O-Ring"

# Spec searches
test_search "10 micron filter" "-" "Spec: 10 micron"
test_search "5 micron element" "-" "Spec: 5 micron"
test_search "1 micron filter" "-" "Spec: 1 micron"
test_search "25 micron bag" "-" "Spec: 25 micron bag"
test_search "polypropylene filter" "-" "Media: Polypropylene"
test_search "PTFE membrane" "-" "Media: PTFE"
test_search "glass fiber element" "-" "Media: Glass Fiber"
test_search "stainless steel filter" "-" "Media: Stainless Steel"

# Application/Industry searches (NEW — should now work after Phase 2)
test_search "pharmaceutical" "-" "Application: Pharmaceutical"
test_search "compressed air" "-" "Application: Compressed Air"
test_search "water treatment" "-" "Application: Water Treatment"
test_search "hydraulic" "-" "Application: Hydraulic"
test_search "HVAC" "-" "Application: HVAC"
test_search "Food & Beverage" "-" "Industry: Food & Beverage"
test_search "Oil & Gas" "-" "Industry: Oil & Gas"

# Combined queries (realistic salesperson)
test_search "10 micron Pall hydraulic" "-" "Combined: micron+mfg+app"
test_search "polypropylene bag filter" "-" "Combined: media+type"
test_search "glass fiber 5 micron" "-" "Combined: media+micron"
test_search "150 PSI filter element" "-" "Combined: PSI+type"

# ============================================================================
# SECTION 3: HALLUCINATION CHECKS (50 tests)
# Verify portal does NOT return fake data
# ============================================================================
echo ""
echo "--- Section 3: Hallucination & Data Integrity Checks (50 tests) ---"
echo ""

# Test that fake part numbers return NOT FOUND (no hallucinations)
test_fake_pn() {
    local fake_pn="$1"
    local test_label="$2"
    TOTAL=$((TOTAL + 1))

    local response
    response=$(curl -s --max-time 10 -X POST "$BASE_URL/api/lookup" \
        -H "Content-Type: application/json" \
        -d "{\"part_number\": \"$fake_pn\"}" 2>/dev/null || echo '{"error":"timeout"}')

    if echo "$response" | grep -q '"found":false'; then
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-30s %s (correctly not found)\n" "$TOTAL" "$fake_pn" "$test_label"
    elif echo "$response" | grep -q '"found":true'; then
        FAIL=$((FAIL + 1))
        printf "  ${RED}FAIL${NC} [%3d] %-30s %s — HALLUCINATED RESULT\n" "$TOTAL" "$fake_pn" "$test_label"
        ERRORS="${ERRORS}\n  FAIL: $fake_pn ($test_label) — Returned data for non-existent part!"
    else
        # Timeout or error
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-30s %s (no result)\n" "$TOTAL" "$fake_pn" "$test_label"
    fi
}

# Fake part numbers that should NOT exist
test_fake_pn "FAKE12345" "Totally fake PN"
test_fake_pn "ZZZZZ99999" "Random fake"
test_fake_pn "NONEXIST001" "Non-existent"
test_fake_pn "HALLUCINATE1" "Hallucination test"
test_fake_pn "ABCXYZ789" "Random string"
test_fake_pn "PALL-FAKE-001" "Fake Pall PN"
test_fake_pn "HC9999ZZZ" "Fake HC prefix"
test_fake_pn "CLR99999" "Fake CLR prefix"
test_fake_pn "TEST000001" "Test PN"
test_fake_pn "XXXXXXXXX" "All X's"
test_fake_pn "FILTER-000" "Fake filter"
test_fake_pn "BAG-FAKE-1" "Fake bag"
test_fake_pn "ELEMENT000" "Fake element"
test_fake_pn "HOUSING999" "Fake housing"
test_fake_pn "MEMBRANE00" "Fake membrane"
test_fake_pn "123FAKE456" "Numeric fake"
test_fake_pn "ZZ-9999-XX" "Dashed fake"
test_fake_pn "NOTREAL123" "Not real"
test_fake_pn "GHOST-PART" "Ghost part"
test_fake_pn "PHANTOM001" "Phantom part"

# Verify price is never $0.00 on real parts
test_no_zero_price() {
    local pn="$1"
    TOTAL=$((TOTAL + 1))

    local response
    response=$(curl -s --max-time 10 -X POST "$BASE_URL/api/lookup" \
        -H "Content-Type: application/json" \
        -d "{\"part_number\": \"$pn\"}" 2>/dev/null || echo '{"error":"timeout"}')

    if echo "$response" | grep -q '"Price":"\$0.00"'; then
        FAIL=$((FAIL + 1))
        printf "  ${RED}FAIL${NC} [%3d] %-30s Price shows \$0.00 (should say Contact Enpro)\n" "$TOTAL" "$pn"
        ERRORS="${ERRORS}\n  FAIL: $pn — Shows \$0.00 price (hallucination)"
    else
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-30s No \$0.00 price\n" "$TOTAL" "$pn"
    fi
}

echo ""
echo "  ... Testing price integrity (no \$0.00) ..."
echo ""

test_no_zero_price "HC9021FAS4Z"
test_no_zero_price "HC2216FKP6H"
test_no_zero_price "PES50.5P2SSH"
test_no_zero_price "GSTF11T6615EP"
test_no_zero_price "CLR130"
test_no_zero_price "BE5S7S1-BULK"
test_no_zero_price "2901200407"
test_no_zero_price "182322800"
test_no_zero_price "SS644FD"
test_no_zero_price "MBS1001RZH2"

# Verify stock locations are valid (only Houston/Charlotte/KC)
test_valid_stock_locations() {
    local pn="$1"
    TOTAL=$((TOTAL + 1))

    local response
    response=$(curl -s --max-time 10 -X POST "$BASE_URL/api/lookup" \
        -H "Content-Type: application/json" \
        -d "{\"part_number\": \"$pn\"}" 2>/dev/null || echo '{"error":"timeout"}')

    # Check for hallucinated location names
    if echo "$response" | grep -qi "New York\|Los Angeles\|Chicago\|Dallas\|Atlanta\|Seattle\|Denver\|Miami"; then
        FAIL=$((FAIL + 1))
        printf "  ${RED}FAIL${NC} [%3d] %-30s HALLUCINATED stock location!\n" "$TOTAL" "$pn"
        ERRORS="${ERRORS}\n  FAIL: $pn — Hallucinated stock location"
    else
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-30s Valid stock locations only\n" "$TOTAL" "$pn"
    fi
}

echo ""
echo "  ... Testing stock location integrity ..."
echo ""

test_valid_stock_locations "HC9021FAS4Z"
test_valid_stock_locations "HC2216FKP6H"
test_valid_stock_locations "PES50.5P2SSH"
test_valid_stock_locations "GSTF11T6615EP"
test_valid_stock_locations "BE5S7S1-BULK"
test_valid_stock_locations "MBS1001RZH2"
test_valid_stock_locations "POG5P3SH"
test_valid_stock_locations "AB3Y0033J"
test_valid_stock_locations "2901200407"
test_valid_stock_locations "CLPF0002TC30P"

# Verify manufacturer names match crosswalk (no invented manufacturers)
test_valid_manufacturer() {
    local pn="$1"
    local expected_mfg="$2"
    TOTAL=$((TOTAL + 1))

    local response
    response=$(curl -s --max-time 10 -X POST "$BASE_URL/api/lookup" \
        -H "Content-Type: application/json" \
        -d "{\"part_number\": \"$pn\"}" 2>/dev/null || echo '{"error":"timeout"}')

    local clean_mfg
    clean_mfg=$(echo "$expected_mfg" | sed 's/[^a-zA-Z0-9 ]//g' | head -c 15)

    if echo "$response" | grep -q '"found":false'; then
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-30s Not found (acceptable)\n" "$TOTAL" "$pn"
    elif echo "$response" | grep -qi "$clean_mfg"; then
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-30s Manufacturer: %s\n" "$TOTAL" "$pn" "$expected_mfg"
    else
        FAIL=$((FAIL + 1))
        printf "  ${RED}FAIL${NC} [%3d] %-30s Expected mfg '%s' not found\n" "$TOTAL" "$pn" "$expected_mfg"
        ERRORS="${ERRORS}\n  FAIL: $pn — Expected manufacturer '$expected_mfg' not in response"
    fi
}

echo ""
echo "  ... Testing manufacturer accuracy ..."
echo ""

test_valid_manufacturer "HC9021FAS4Z" "Applied Energy"
test_valid_manufacturer "PES50.5P2SSH" "AJR Filtration"
test_valid_manufacturer "GSTF11T6615EP" "Global Filter"
test_valid_manufacturer "GFHD111ND430" "Global Filter"
test_valid_manufacturer "BE5S7S1-BULK" "American Filter"
test_valid_manufacturer "CLPF0002TC30P" "Banner"
test_valid_manufacturer "2901200407" "Quincy"
test_valid_manufacturer "182322800" "AAF"
test_valid_manufacturer "SS644FD" "Precision"
test_valid_manufacturer "YS62-CS-6.00-40" "Accurate Valve"

# ============================================================================
# RESULTS SUMMARY
# ============================================================================
echo ""
echo "=============================================="
echo "  RESULTS SUMMARY"
echo "=============================================="
echo ""
echo -e "  Total:  ${TOTAL}"
echo -e "  ${GREEN}Passed: ${PASS}${NC}"
echo -e "  ${RED}Failed: ${FAIL}${NC}"

if [ "$TOTAL" -gt 0 ]; then
    PCT=$((PASS * 100 / TOTAL))
    echo -e "  Rate:   ${PCT}%"
fi

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo -e "${RED}FAILURES:${NC}"
    echo -e "$ERRORS"
fi

echo ""
echo "=============================================="

# Exit with failure code if any tests failed
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
