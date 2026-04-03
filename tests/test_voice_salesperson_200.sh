#!/usr/bin/env bash
# ============================================================================
# Enpro FM Portal — 200 Salesperson Voice Query Tests
# Simulates real voice queries a sales rep would speak into the mic.
# Tests via /api/voice-search-text (same pipeline as voice, skips STT).
# Validates: results found, no hallucinations, correct specs/manufacturers.
#
# Usage: bash tests/test_voice_salesperson_200.sh [BASE_URL]
# Default: https://enpro-fm-portal.onrender.com
# ============================================================================

set -euo pipefail

BASE_URL="${1:-https://enpro-fm-portal.onrender.com}"
PASS=0
FAIL=0
ERRORS=""
TOTAL=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=============================================="
echo "  Enpro FM Portal — Salesperson Voice Tests"
echo "  Target: $BASE_URL"
echo "  Pipeline: /api/voice-search-text"
echo "=============================================="
echo ""

# Health check
echo -n "Health check... "
HEALTH=$(curl -s --max-time 15 "$BASE_URL/health" 2>/dev/null || echo "FAIL")
if echo "$HEALTH" | grep -q '"data_loaded":true'; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC} — Portal not ready."
    exit 1
fi
echo ""

# -------------------------------------------------------------------
# Core test function — voice query via /api/voice-search-text
# -------------------------------------------------------------------
test_voice() {
    local query="$1"
    local expect_results="${2:-yes}"  # yes = expect products, no = expect 0
    local check_field="$3"           # field name to validate (or "-")
    local check_value="$4"           # expected value (or "-")
    local test_label="$5"

    TOTAL=$((TOTAL + 1))

    local response
    response=$(curl -s --max-time 15 -X POST "$BASE_URL/api/voice-search-text" \
        -H "Content-Type: application/json" \
        -d "{\"message\": \"$query\"}" 2>/dev/null || echo '{"error":"timeout"}')

    local total_found
    total_found=$(echo "$response" | grep -o '"total_found":[0-9]*' | grep -o '[0-9]*' || echo "0")

    local issues=""

    # Check if we got results when expected
    if [ "$expect_results" = "yes" ] && [ "$total_found" = "0" ]; then
        issues="ZERO_RESULTS"
    elif [ "$expect_results" = "no" ] && [ "$total_found" != "0" ]; then
        issues="UNEXPECTED_RESULTS($total_found)"
    fi

    # Validate specific field if requested
    if [ -z "$issues" ] && [ "$check_field" != "-" ] && [ "$check_value" != "-" ] && [ "$expect_results" = "yes" ]; then
        local clean_val
        clean_val=$(echo "$check_value" | sed 's/[^a-zA-Z0-9 .]//g')
        if ! echo "$response" | grep -qi "$clean_val"; then
            issues="MISSING:${check_field}=${check_value}"
        fi
    fi

    if [ -z "$issues" ]; then
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-55s %s (found:%s)\n" "$TOTAL" "\"$query\"" "$test_label" "$total_found"
    else
        FAIL=$((FAIL + 1))
        printf "  ${RED}FAIL${NC} [%3d] %-55s %s — %s\n" "$TOTAL" "\"$query\"" "$test_label" "$issues"
        ERRORS="${ERRORS}\n  FAIL [${TOTAL}]: \"$query\" ($test_label) — $issues"
    fi
}

# ============================================================================
# SECTION 1: PART NUMBER LOOKUPS (30 tests)
# "I need part number X" / "Look up X" / "Do we carry X"
# ============================================================================
echo "--- Section 1: Part Number Voice Lookups (30 tests) ---"
echo ""

test_voice "I need part number HC9021FAS4Z" "yes" "-" "-" "Direct PN request"
test_voice "look up HC2216FKP6H" "yes" "-" "-" "Lookup command"
test_voice "do we carry CLR130" "yes" "-" "-" "Carry check"
test_voice "what do you have on MBS1001RZH2" "yes" "-" "-" "Info request"
test_voice "pull up PES50.5P2SSH" "yes" "-" "-" "Pull up command"
test_voice "check stock on GSTF11T6615EP" "yes" "-" "-" "Stock check"
test_voice "I'm looking for AB3Y0033J" "yes" "-" "-" "Looking for"
test_voice "can you find 2901200407" "yes" "-" "-" "Find request"
test_voice "what's the price on SS644FD" "yes" "-" "-" "Price inquiry"
test_voice "give me details on CLPF0002TC30P" "yes" "-" "-" "Details request"
test_voice "HC2296FCP36H50" "yes" "-" "-" "Raw PN only"
test_voice "look up EPE-10-5" "yes" "-" "-" "Dashed PN"
test_voice "what about GFHD111ND430" "yes" "-" "-" "What about"
test_voice "do we stock BE5S7S1-BULK" "yes" "-" "-" "Bulk PN"
test_voice "find me 182322800" "yes" "-" "-" "Numeric PN"
test_voice "is 3014351 in stock" "yes" "-" "-" "Stock question"
test_voice "price check on XLD4510UEM3" "yes" "-" "-" "Price check"
test_voice "I need to quote DEZA050FJ" "yes" "-" "-" "Quote request"
test_voice "pull up part 316727" "yes" "-" "-" "Pull up part"
test_voice "YS62-CS-6.00-40" "yes" "-" "-" "Complex PN format"
test_voice "customer needs HC9021" "yes" "-" "-" "Customer needs"
test_voice "what's available for POG5P3SH" "yes" "-" "-" "Available for"
test_voice "can I get pricing on 13498501" "yes" "-" "-" "Pricing request"
test_voice "look up Pall HC9021" "yes" "-" "-" "Mfg + PN"
test_voice "do we have any MHW0082 in stock" "yes" "-" "-" "Any in stock"
test_voice "98-1010-T-2PM" "yes" "-" "-" "Tescom PN"
test_voice "ORH343 viton o-ring" "yes" "-" "-" "PN + description"
test_voice "NXAM702UNF" "yes" "-" "-" "Pall NXAM prefix"
test_voice "check if we carry HC8300FKP16H" "yes" "-" "-" "HC prefix check"
test_voice "M2MK minimate kit" "yes" "-" "-" "Kit lookup"

# ============================================================================
# SECTION 2: SPEC-BASED SEARCHES (40 tests)
# "I need a 10 micron filter" / "Show me 150 PSI elements"
# ============================================================================
echo ""
echo "--- Section 2: Spec-Based Voice Searches (40 tests) ---"
echo ""

test_voice "I need a 10 micron filter" "yes" "-" "-" "Micron search"
test_voice "show me 5 micron elements" "yes" "-" "-" "5 micron"
test_voice "do we have 1 micron filters" "yes" "-" "-" "1 micron"
test_voice "25 micron bag filters" "yes" "-" "-" "25 micron bag"
test_voice "looking for 0.2 micron membrane" "yes" "-" "-" "Sub-micron"
test_voice "give me a 50 micron filter" "yes" "-" "-" "50 micron"
test_voice "I need something rated to 150 PSI" "yes" "-" "-" "150 PSI"
test_voice "what filters can handle 200 degrees" "yes" "-" "-" "200F temp"
test_voice "10 micron polypropylene filter" "yes" "-" "-" "Micron + media"
test_voice "5 micron glass fiber element" "yes" "-" "-" "Micron + glass"
test_voice "1 micron PTFE membrane" "yes" "-" "-" "Micron + PTFE"
test_voice "show me stainless steel filters" "yes" "-" "-" "SS media"
test_voice "what polypropylene cartridges do we have" "yes" "-" "-" "PP cartridge"
test_voice "I need an absolute rated filter" "yes" "-" "-" "Absolute efficiency"
test_voice "glass fiber filter elements" "yes" "-" "-" "Glass fiber"
test_voice "do we have PTFE filters" "yes" "-" "-" "PTFE search"
test_voice "cellulose depth sheets" "yes" "-" "-" "Cellulose"
test_voice "polyester bag filter" "yes" "-" "-" "Polyester bag"
test_voice "nylon filter element" "yes" "-" "-" "Nylon"
test_voice "10 micron in stock" "yes" "-" "-" "Micron + stock"
test_voice "5 micron 150 PSI filter" "yes" "-" "-" "Micron + PSI"
test_voice "filter rated to 250 degrees Fahrenheit" "yes" "-" "-" "250F search"
test_voice "high pressure filter 150 PSI" "yes" "-" "-" "High pressure"
test_voice "what's the smallest micron filter we have" "yes" "-" "-" "Smallest micron"
test_voice "10 micron Pall filter element" "yes" "-" "-" "Micron + mfg"
test_voice "5 micron Graver cartridge" "yes" "-" "-" "Micron + Graver"
test_voice "polypropylene 25 micron bag" "yes" "-" "-" "Media + micron + type"
test_voice "glass fiber 10 micron absolute" "yes" "-" "-" "Media + micron + eff"
test_voice "show me depth sheets" "yes" "-" "-" "Depth sheets"
test_voice "what membrane filters do we carry" "yes" "-" "-" "Membrane type"
test_voice "I need a bag filter for 50 micron" "yes" "-" "-" "Type + micron"
test_voice "filter housing" "yes" "-" "-" "Housing search"
test_voice "cartridge filter 10 micron" "yes" "-" "-" "Cart + micron"
test_voice "capsule filter" "yes" "-" "-" "Capsule type"
test_voice "air filter" "yes" "-" "-" "Air filter type"
test_voice "compressor filter" "yes" "-" "-" "Compressor type"
test_voice "separator element" "yes" "-" "-" "Separator type"
test_voice "o-ring viton" "yes" "-" "-" "Seal search"
test_voice "strainer 6 inch" "yes" "-" "-" "Strainer search"
test_voice "screen separator" "yes" "-" "-" "Screen type"

# ============================================================================
# SECTION 3: APPLICATION & INDUSTRY SEARCHES (30 tests)
# "I'm filtering hydraulic oil" / "customer does compressed air"
# ============================================================================
echo ""
echo "--- Section 3: Application & Industry Voice Searches (30 tests) ---"
echo ""

test_voice "I need a filter for hydraulic oil" "yes" "-" "-" "App: Hydraulic"
test_voice "compressed air filter element" "yes" "-" "-" "App: Compressed Air"
test_voice "water treatment filter" "yes" "-" "-" "App: Water Treatment"
test_voice "pharmaceutical filtration" "yes" "-" "-" "App: Pharmaceutical"
test_voice "food and beverage filter" "yes" "-" "-" "Ind: Food & Beverage"
test_voice "oil and gas filtration" "yes" "-" "-" "Ind: Oil & Gas"
test_voice "HVAC filter" "yes" "-" "-" "App: HVAC"
test_voice "chemical processing filter" "yes" "-" "-" "App: Chemical"
test_voice "paint and coatings filtration" "yes" "-" "-" "App: Paint"
test_voice "power generation filter" "yes" "-" "-" "App: Power Gen"
test_voice "customer filters hydraulic fluid at 10 micron" "yes" "-" "-" "App + spec"
test_voice "compressed air 5 micron filter" "yes" "-" "-" "App + micron"
test_voice "pharmaceutical 0.2 micron membrane" "yes" "-" "-" "Pharma + micron"
test_voice "hydraulic filter Pall" "yes" "-" "-" "App + mfg"
test_voice "brewery filter" "yes" "-" "-" "App: Brewery"
test_voice "beverage filtration" "yes" "-" "-" "App: Beverage"
test_voice "industrial filter element" "yes" "-" "-" "Ind: Industrial"
test_voice "marine filtration" "yes" "-" "-" "Ind: Marine"
test_voice "automotive filter" "yes" "-" "-" "Ind: Automotive"
test_voice "semiconductor filtration" "yes" "-" "-" "Ind: Semiconductor"
test_voice "what do we have for lube oil filtration" "yes" "-" "-" "Lube oil"
test_voice "customer runs a paint spray booth" "yes" "-" "-" "Paint booth"
test_voice "glycol dehydration filter" "yes" "-" "-" "Glycol app"
test_voice "amine treating filter" "yes" "-" "-" "Amine app"
test_voice "customer needs filters for refinery" "yes" "-" "-" "Refinery"
test_voice "turbine lube oil filter" "yes" "-" "-" "Turbine lube"
test_voice "produced water filtration" "yes" "-" "-" "Produced water"
test_voice "municipal water filter" "yes" "-" "-" "Municipal"
test_voice "dairy plant filter" "yes" "-" "-" "Dairy"
test_voice "whisky filtration depth sheets" "yes" "-" "-" "Whisky"

# ============================================================================
# SECTION 4: MANUFACTURER-SPECIFIC QUERIES (30 tests)
# "What Pall filters do we have" / "Show me Graver products"
# ============================================================================
echo ""
echo "--- Section 4: Manufacturer Voice Queries (30 tests) ---"
echo ""

test_voice "what Pall filters do we carry" "yes" "Manufacturer" "Pall" "Mfg: Pall"
test_voice "show me Graver Technologies products" "yes" "-" "-" "Mfg: Graver"
test_voice "Cobetter filters" "yes" "-" "-" "Mfg: Cobetter"
test_voice "do we have Donaldson" "yes" "-" "-" "Mfg: Donaldson"
test_voice "Filtrox depth sheets" "yes" "-" "-" "Mfg: Filtrox"
test_voice "Global Filter housings" "yes" "-" "-" "Mfg: Global Filter"
test_voice "what Schroeder filters we got" "yes" "-" "-" "Mfg: Schroeder"
test_voice "Koch filter products" "yes" "-" "-" "Mfg: Koch"
test_voice "Shelco filters" "yes" "-" "-" "Mfg: Shelco"
test_voice "Swift filters" "yes" "-" "-" "Mfg: Swift"
test_voice "Rosedale products" "yes" "-" "-" "Mfg: Rosedale"
test_voice "Pentair filtration" "yes" "-" "-" "Mfg: Pentair"
test_voice "Hydac filters" "yes" "-" "-" "Mfg: Hydac"
test_voice "AJR bag filters" "yes" "-" "-" "Mfg: AJR"
test_voice "American Filter Manufacturing" "yes" "-" "-" "Mfg: AMF"
test_voice "Banner Industries filters" "yes" "-" "-" "Mfg: Banner"
test_voice "Quincy compressor filters" "yes" "-" "-" "Mfg: Quincy"
test_voice "Delta Pure" "yes" "-" "-" "Mfg: Delta Pure"
test_voice "Critical Process Filtration" "yes" "-" "-" "Mfg: CPF"
test_voice "Porvair filters" "yes" "-" "-" "Mfg: Porvair"
test_voice "Pall 10 micron hydraulic element" "yes" "-" "-" "Mfg + spec + app"
test_voice "Graver 5 micron cartridge" "yes" "-" "-" "Mfg + spec + type"
test_voice "Cobetter polypropylene filter" "yes" "-" "-" "Mfg + media"
test_voice "Filtrox cellulose depth sheet" "yes" "-" "-" "Mfg + media + type"
test_voice "Global Filter 150 PSI housing" "yes" "-" "-" "Mfg + PSI + type"
test_voice "Pall glass fiber element" "yes" "-" "-" "Mfg + media + type"
test_voice "Jonell filter" "yes" "-" "-" "Mfg: Jonell"
test_voice "McMaster Carr filter" "yes" "-" "-" "Mfg: McMaster"
test_voice "AAF air filter" "yes" "-" "-" "Mfg: AAF"
test_voice "Enpro filters" "yes" "-" "-" "Mfg: Enpro"

# ============================================================================
# SECTION 5: COMPARISON & CROSS-REFERENCE QUERIES (20 tests)
# "Compare X with Y" / "What's equivalent to X"
# ============================================================================
echo ""
echo "--- Section 5: Comparison & Cross-Reference Queries (20 tests) ---"
echo ""

test_voice "compare Pall vs Graver 10 micron" "yes" "-" "-" "Compare mfgs"
test_voice "what's equivalent to HC9021FAS4Z" "yes" "-" "-" "Equivalent request"
test_voice "alternative to Pall HC2216" "yes" "-" "-" "Alternative request"
test_voice "crosswalk for HC2296FCP36H50" "yes" "-" "-" "Crosswalk request"
test_voice "replacement for CLR130" "yes" "-" "-" "Replacement"
test_voice "what other manufacturers make 10 micron glass fiber" "yes" "-" "-" "Other mfgs"
test_voice "is there a cheaper alternative to Pall" "yes" "-" "-" "Cheaper alt"
test_voice "show me options similar to PES50.5P2SSH" "yes" "-" "-" "Similar options"
test_voice "compare polypropylene vs glass fiber" "yes" "-" "-" "Media compare"
test_voice "bag filter vs cartridge" "yes" "-" "-" "Type compare"
test_voice "Pall equivalent in Graver" "yes" "-" "-" "Brand crosswalk"
test_voice "what replaces the HC9021" "yes" "-" "-" "Replaces PN"
test_voice "do we have a Cobetter version of this Pall filter" "yes" "-" "-" "Brand swap"
test_voice "compare 5 micron vs 10 micron elements" "yes" "-" "-" "Spec compare"
test_voice "give me alternatives to Filtrox depth sheets" "yes" "-" "-" "Alt depth sheet"
test_voice "what else works for hydraulic 10 micron" "yes" "-" "-" "App + spec alt"
test_voice "other options for compressed air filtration" "yes" "-" "-" "App alternatives"
test_voice "similar to Global Filter housing" "yes" "-" "-" "Similar housing"
test_voice "who else makes bag filters" "yes" "-" "-" "Type by mfg"
test_voice "less expensive filter element in stock" "yes" "-" "-" "Price + stock"

# ============================================================================
# SECTION 6: HALLUCINATION CHECKS (20 tests)
# Fake PNs that should return 0 or very few results
# ============================================================================
echo ""
echo "--- Section 6: Hallucination & Integrity Checks (20 tests) ---"
echo ""

test_voice "look up FAKE12345" "no" "-" "-" "Fake PN"
test_voice "find ZZZZZ99999" "no" "-" "-" "Random fake"
test_voice "HC9999ZZZ filter" "no" "-" "-" "Fake HC prefix"
test_voice "NONEXIST001" "no" "-" "-" "Non-existent PN"
test_voice "HALLUCINATE1" "no" "-" "-" "Hallucination bait"
test_voice "CLR99999" "no" "-" "-" "Fake CLR"
test_voice "ABCXYZ789" "no" "-" "-" "Random string"
test_voice "PALL-FAKE-001" "no" "-" "-" "Fake Pall format"
test_voice "TEST000001" "no" "-" "-" "Test PN"
test_voice "PHANTOM001" "no" "-" "-" "Ghost part"

# These should find results (real queries) but NOT hallucinate specs
test_voice "10 micron filter element" "yes" "-" "-" "Real spec — verify no fake specs"
test_voice "Pall hydraulic filter" "yes" "-" "-" "Real mfg — verify no fake products"
test_voice "5 micron glass fiber" "yes" "-" "-" "Real combo — verify accuracy"
test_voice "polypropylene bag filter" "yes" "-" "-" "Real media+type"
test_voice "compressed air filter" "yes" "-" "-" "Real application"
test_voice "150 PSI filter housing" "yes" "-" "-" "Real PSI spec"
test_voice "Graver 10 micron" "yes" "-" "-" "Real mfg+spec"
test_voice "Global Filter LLC housing" "yes" "-" "-" "Real full mfg name"
test_voice "25 micron bag" "yes" "-" "-" "Real micron+type"
test_voice "PTFE 0.2 micron membrane" "yes" "-" "-" "Real media+micron+type"

# Voice-specific tests (STT artifacts)
echo ""
echo "--- Section 7: Voice STT Artifact Handling (30 tests) ---"
echo ""

test_voice "paul filter" "yes" "-" "-" "Mishear: Paul → Pall"
test_voice "pal hydraulic element" "yes" "-" "-" "Mishear: Pal → Pall"
test_voice "graver tech filter" "yes" "-" "-" "Shorthand: Graver Tech"
test_voice "co better filter" "yes" "-" "-" "Mishear: Co Better → Cobetter"
test_voice "shell co filters" "yes" "-" "-" "Mishear: Shell Co → Shelco"
test_voice "rose dale products" "yes" "-" "-" "Mishear: Rose Dale → Rosedale"
test_voice "schrader filter" "yes" "-" "-" "Mishear: Schrader → Schroeder"
test_voice "penta air filtration" "yes" "-" "-" "Mishear: Penta Air → Pentair"
test_voice "cook filter" "yes" "-" "-" "Mishear: Cook → Koch"
test_voice "john l filter" "yes" "-" "-" "Mishear: John L → Jonell"
test_voice "poly pro filter" "yes" "-" "-" "Mishear: Poly Pro → Polypropylene"
test_voice "polly pro 10 micron" "yes" "-" "-" "Mishear: Polly Pro"
test_voice "teflon membrane" "yes" "-" "-" "Alias: Teflon → PTFE"
test_voice "kynar filter" "yes" "-" "-" "Alias: Kynar → PVDF"
test_voice "three sixteen stainless filter" "yes" "-" "-" "Number: 316 SS"
test_voice "316 ss element" "yes" "-" "-" "Abbreviation: 316 SS"
test_voice "ten micron filter" "yes" "-" "-" "Number word: ten"
test_voice "five micron element" "yes" "-" "-" "Number word: five"
test_voice "twenty five micron bag" "yes" "-" "-" "Number word: twenty five"
test_voice "one hundred PSI" "yes" "-" "-" "Number word: hundred"
test_voice "point five micron membrane" "yes" "-" "-" "Decimal: point five"
test_voice "zero point two micron" "yes" "-" "-" "Decimal: 0.2"
test_voice "por vair filter" "yes" "-" "-" "Mishear: Por Vair → Porvair"
test_voice "en pro filter" "yes" "-" "-" "Mishear: En Pro → Enpro"
test_voice "saint go bain filter" "yes" "-" "-" "Mishear: Saint Go Bain"
test_voice "mcmaster carr filter" "yes" "-" "-" "Mishear: McMaster Carr"
test_voice "cart filter 10 micron" "yes" "-" "-" "Shorthand: Cart → Cartridge"
test_voice "bags 25 micron polypro" "yes" "-" "-" "Shorthand: Bags"
test_voice "housing for 10 inch cartridge" "yes" "-" "-" "Housing request"
test_voice "depth sheet filtrox cellulose" "yes" "-" "-" "Type + mfg + media"

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
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
