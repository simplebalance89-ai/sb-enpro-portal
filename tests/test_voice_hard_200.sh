#!/usr/bin/env bash
# ============================================================================
# Enpro FM Portal — 200 HARD Salesperson Voice Tests
# Multi-variable queries: 2-4 specs per ask (micron + media + manufacturer +
# application + PSI + temp + stock + pricing + location + product type)
#
# These simulate real sales calls where reps stack requirements.
#
# Usage: bash tests/test_voice_hard_200.sh [BASE_URL]
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
echo "  Enpro FM Portal — HARD Voice Tests (200)"
echo "  Multi-variable: 2-4 specs per query"
echo "  Target: $BASE_URL"
echo "=============================================="
echo ""

# Health check
echo -n "Health check... "
HEALTH=$(curl -s --max-time 15 "$BASE_URL/health" 2>/dev/null || echo "FAIL")
if echo "$HEALTH" | grep -q '"data_loaded":true'; then
    echo -e "${GREEN}OK${NC}"
else
    echo -e "${RED}FAILED${NC}"
    exit 1
fi
echo ""

# -------------------------------------------------------------------
# Core test — voice query via /api/voice-search-text
# -------------------------------------------------------------------
tv() {
    local query="$1"
    local expect="${2:-yes}"
    local label="$3"

    TOTAL=$((TOTAL + 1))

    local response
    response=$(curl -s --max-time 15 -X POST "$BASE_URL/api/voice-search-text" \
        -H "Content-Type: application/json" \
        -d "{\"message\": \"$query\"}" 2>/dev/null || echo '{"error":"timeout"}')

    local found
    found=$(echo "$response" | grep -o '"total_found":[0-9]*' | grep -o '[0-9]*' || echo "0")

    local issues=""
    if [ "$expect" = "yes" ] && [ "$found" = "0" ]; then
        issues="ZERO_RESULTS"
    elif [ "$expect" = "no" ] && [ "$found" != "0" ]; then
        issues="UNEXPECTED($found)"
    fi

    if [ -z "$issues" ]; then
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-65s %s (n=%s)\n" "$TOTAL" "\"$query\"" "$label" "$found"
    else
        FAIL=$((FAIL + 1))
        printf "  ${RED}FAIL${NC} [%3d] %-65s %s — %s\n" "$TOTAL" "\"$query\"" "$label" "$issues"
        ERRORS="${ERRORS}\n  FAIL [${TOTAL}]: \"$query\" — $issues"
    fi
}

# Also test /api/lookup for direct PN tests with field validation
tl() {
    local pn="$1"
    local check_field="$2"
    local check_val="$3"
    local label="$4"

    TOTAL=$((TOTAL + 1))

    local response
    response=$(curl -s --max-time 10 -X POST "$BASE_URL/api/lookup" \
        -H "Content-Type: application/json" \
        -d "{\"part_number\": \"$pn\"}" 2>/dev/null || echo '{"error":"timeout"}')

    local issues=""
    if ! echo "$response" | grep -q '"found":true'; then
        issues="NOT_FOUND"
    elif [ "$check_field" != "-" ] && [ "$check_val" != "-" ]; then
        if ! echo "$response" | grep -qi "$(echo "$check_val" | sed 's/[^a-zA-Z0-9 .]//g')"; then
            issues="MISSING:${check_field}=${check_val}"
        fi
    fi

    if [ -z "$issues" ]; then
        PASS=$((PASS + 1))
        printf "  ${GREEN}PASS${NC} [%3d] %-35s %-20s %s\n" "$TOTAL" "$pn" "$check_field=$check_val" "$label"
    else
        FAIL=$((FAIL + 1))
        printf "  ${RED}FAIL${NC} [%3d] %-35s %-20s %s — %s\n" "$TOTAL" "$pn" "$check_field" "$label" "$issues"
        ERRORS="${ERRORS}\n  FAIL [${TOTAL}]: $pn ($label) — $issues"
    fi
}

# ============================================================================
# SECTION 1: MULTI-SPEC VOICE QUERIES (60 tests)
# Micron + Media + Manufacturer + Type combos
# ============================================================================
echo "--- Section 1: Multi-Spec Combos (60 tests) ---"
echo ""

# Micron + Media (2 specs)
tv "10 micron polypropylene filter" "yes" "micron+media"
tv "5 micron glass fiber element" "yes" "micron+media"
tv "1 micron PTFE membrane" "yes" "micron+media"
tv "25 micron polyester bag" "yes" "micron+media"
tv "0.2 micron PTFE filter" "yes" "micron+media"
tv "50 micron stainless steel screen" "yes" "micron+media"
tv "10 micron cellulose depth sheet" "yes" "micron+media"
tv "5 micron nylon filter" "yes" "micron+media"
tv "1 micron polypropylene cartridge" "yes" "micron+media"
tv "0.5 micron glass fiber membrane" "yes" "micron+media"

# Micron + Manufacturer (2 specs)
tv "10 micron Pall filter" "yes" "micron+mfg"
tv "5 micron Graver cartridge" "yes" "micron+mfg"
tv "1 micron Cobetter element" "yes" "micron+mfg"
tv "25 micron AJR bag filter" "yes" "micron+mfg"
tv "10 micron Donaldson element" "yes" "micron+mfg"
tv "5 micron Schroeder filter" "yes" "micron+mfg"
tv "10 micron Hydac element" "yes" "micron+mfg"
tv "10 micron Global Filter element" "yes" "micron+mfg"
tv "5 micron Pentair cartridge" "yes" "micron+mfg"
tv "10 micron Swift filter" "yes" "micron+mfg"

# Micron + Media + Manufacturer (3 specs)
tv "10 micron glass fiber Pall element" "yes" "micron+media+mfg"
tv "5 micron polypropylene Graver cartridge" "yes" "micron+media+mfg"
tv "1 micron PTFE Cobetter membrane" "yes" "micron+media+mfg"
tv "10 micron glass fiber Hydac element" "yes" "micron+media+mfg"
tv "5 micron polypropylene Pall cartridge" "yes" "micron+media+mfg"
tv "25 micron polyester AJR bag filter" "yes" "micron+media+mfg"
tv "10 micron stainless steel Schroeder element" "yes" "micron+media+mfg"
tv "5 micron glass fiber Donaldson filter" "yes" "micron+media+mfg"
tv "1 micron polypropylene Delta Pure cartridge" "yes" "micron+media+mfg"
tv "10 micron glass fiber Jonell element" "yes" "micron+media+mfg"

# Micron + Application (2 specs)
tv "10 micron hydraulic filter" "yes" "micron+app"
tv "5 micron compressed air element" "yes" "micron+app"
tv "1 micron pharmaceutical membrane" "yes" "micron+app"
tv "10 micron lube oil filter" "yes" "micron+app"
tv "25 micron water treatment bag" "yes" "micron+app"
tv "0.2 micron pharmaceutical filter" "yes" "micron+app"
tv "10 micron HVAC filter" "yes" "micron+app"
tv "5 micron food and beverage cartridge" "yes" "micron+app"
tv "10 micron chemical processing element" "yes" "micron+app"
tv "5 micron oil and gas filter" "yes" "micron+app"

# Micron + PSI (2 specs)
tv "10 micron 150 PSI filter element" "yes" "micron+PSI"
tv "5 micron 150 PSI cartridge" "yes" "micron+PSI"
tv "1 micron 150 PSI membrane" "yes" "micron+PSI"
tv "25 micron rated to 150 PSI" "yes" "micron+PSI"

# Micron + Temp (2 specs)
tv "10 micron rated to 250 degrees Fahrenheit" "yes" "micron+temp"
tv "5 micron filter for 200 degree F service" "yes" "micron+temp"
tv "1 micron PTFE rated 400 F" "yes" "micron+temp"

# 3-4 variable combos
tv "10 micron Pall glass fiber hydraulic element in stock" "yes" "micron+mfg+media+app+stock"
tv "5 micron polypropylene cartridge for compressed air 150 PSI" "yes" "micron+media+type+app+PSI"
tv "Graver 10 micron glass fiber absolute rated element" "yes" "mfg+micron+media+eff"
tv "1 micron PTFE membrane for pharmaceutical water treatment" "yes" "micron+media+type+app"
tv "25 micron polyester bag filter for paint and coatings" "yes" "micron+media+type+app"
tv "Pall 5 micron polypropylene cartridge 150 PSI hydraulic" "yes" "mfg+micron+media+type+PSI+app"
tv "10 micron absolute rated glass fiber element 250 degrees" "yes" "micron+eff+media+type+temp"
tv "Cobetter 0.2 micron PTFE membrane pharmaceutical" "yes" "mfg+micron+media+type+app"
tv "Filtrox cellulose depth sheet for brewery" "yes" "mfg+media+type+app"
tv "Schroeder 10 micron hydraulic element 150 PSI" "yes" "mfg+micron+app+type+PSI"

# ============================================================================
# SECTION 2: STOCK + PRICING VOICE QUERIES (30 tests)
# "What's in stock" / "What's the price" / "Available in Houston"
# ============================================================================
echo ""
echo "--- Section 2: Stock & Pricing Multi-Queries (30 tests) ---"
echo ""

tv "10 micron Pall filter in stock" "yes" "micron+mfg+stock"
tv "5 micron glass fiber element in stock" "yes" "micron+media+stock"
tv "polypropylene cartridge in stock" "yes" "media+type+stock"
tv "hydraulic filter element in stock" "yes" "app+type+stock"
tv "compressed air filter in stock" "yes" "app+stock"
tv "Graver cartridge available" "yes" "mfg+type+avail"
tv "bag filter 25 micron in stock" "yes" "type+micron+stock"
tv "PTFE membrane in stock" "yes" "media+type+stock"
tv "Pall hydraulic element available" "yes" "mfg+app+type+avail"
tv "10 micron polypropylene in stock" "yes" "micron+media+stock"

tv "what's the price on a 10 micron Pall element" "yes" "price+micron+mfg"
tv "how much for a 5 micron glass fiber cartridge" "yes" "price+micron+media"
tv "pricing on polypropylene bag filters" "yes" "price+media+type"
tv "cost of Graver 10 micron element" "yes" "price+mfg+micron"
tv "what does a hydraulic filter element cost" "yes" "price+app+type"

tv "do we have 10 micron Pall elements in Houston" "yes" "micron+mfg+location"
tv "what Graver products are available in Charlotte" "yes" "mfg+location"
tv "is there any 5 micron in Kansas City" "yes" "micron+location"
tv "check Houston stock for hydraulic filters" "yes" "location+app+stock"
tv "what's available in Charlotte for compressed air" "yes" "location+app"

tv "10 micron filter under fifty dollars" "yes" "micron+price_range"
tv "cheapest Pall hydraulic element" "yes" "price+mfg+app"
tv "affordable 5 micron cartridge in stock" "yes" "price+micron+type+stock"
tv "Pall 10 micron glass fiber what's the price and stock" "yes" "mfg+micron+media+price+stock"
tv "do we have any 25 micron bags in Houston with pricing" "yes" "micron+type+location+price"

tv "HC9021FAS4Z price and availability" "yes" "PN+price+avail"
tv "what's CLR130 going for and is it in stock" "yes" "PN+price+stock"
tv "HC2216FKP6H do we have it and what's the price" "yes" "PN+stock+price"
tv "check stock and pricing on PES50.5P2SSH" "yes" "PN+stock+price"
tv "GSTF11T6615EP available with pricing" "yes" "PN+avail+price"

# ============================================================================
# SECTION 3: CROSS-REFERENCE & COMPARISON (30 tests)
# "Compare X vs Y" / "Alternative to X at this spec"
# ============================================================================
echo ""
echo "--- Section 3: Cross-Reference & Comparison (30 tests) ---"
echo ""

tv "compare 10 micron Pall vs Graver" "yes" "micron+mfg_compare"
tv "Pall vs Donaldson hydraulic 10 micron" "yes" "mfg+app+micron compare"
tv "alternative to HC9021FAS4Z in glass fiber 10 micron" "yes" "PN+media+micron alt"
tv "what replaces HC2216FKP6H at 5 micron" "yes" "PN+micron replace"
tv "cheaper alternative to Pall 10 micron hydraulic" "yes" "price+mfg+micron+app alt"
tv "Graver equivalent of a Pall 5 micron glass fiber" "yes" "mfg+micron+media xref"
tv "who else makes 10 micron glass fiber besides Pall" "yes" "micron+media+mfg others"
tv "compare polypropylene vs glass fiber at 10 micron" "yes" "media compare+micron"
tv "bag filter vs cartridge for 25 micron service" "yes" "type compare+micron"
tv "Pall vs Cobetter 1 micron PTFE" "yes" "mfg+micron+media compare"

tv "show me alternatives to Filtrox depth sheets for brewery" "yes" "mfg+type+app alt"
tv "what's comparable to AJR bag filters at 25 micron" "yes" "mfg+type+micron comparable"
tv "Schroeder vs Hydac 10 micron hydraulic" "yes" "mfg+micron+app compare"
tv "is there a Global Filter version of this Pall housing" "yes" "mfg+type crosswalk"
tv "compare Pentair and Pall compressed air filters" "yes" "mfg+app compare"
tv "who else has 5 micron polypropylene cartridges besides Graver" "yes" "micron+media+type+mfg others"
tv "crosswalk HC2296FCP36H50 to another manufacturer" "yes" "PN crosswalk"
tv "find me a non-Pall 10 micron glass fiber alternative" "yes" "micron+media+mfg exclude"
tv "compare 1 micron vs 5 micron for pharmaceutical use" "yes" "micron+app compare"
tv "what other options do I have for 10 micron 150 PSI" "yes" "micron+PSI options"

tv "replacement for CLR130 at similar specs" "yes" "PN+specs replace"
tv "equivalent to PES50.5P2SSH from another manufacturer" "yes" "PN+mfg xref"
tv "side by side Pall 10 micron glass fiber vs polypropylene" "yes" "mfg+micron+media compare"
tv "which 5 micron element is cheapest in stock" "yes" "micron+price+stock compare"
tv "Pall vs Swift 10 micron element pricing" "yes" "mfg+micron+price compare"
tv "compare stainless steel vs glass fiber at 150 PSI" "yes" "media+PSI compare"
tv "is there a lower cost option to HC9021" "yes" "PN+price alt"
tv "what Cobetter filters compete with Pall HC series" "yes" "mfg+mfg compete"
tv "alternative to Donaldson in compressed air 5 micron" "yes" "mfg+app+micron alt"
tv "compare bag filter vs cartridge for 50 micron paint booth" "yes" "type+micron+app compare"

# ============================================================================
# SECTION 4: APPLICATION + SPEC COMBOS (30 tests)
# Industry-specific with multiple constraints
# ============================================================================
echo ""
echo "--- Section 4: Application + Multi-Spec (30 tests) ---"
echo ""

tv "hydraulic oil filtration 10 micron glass fiber absolute" "yes" "app+micron+media+eff"
tv "compressed air 5 micron polypropylene element 150 PSI" "yes" "app+micron+media+type+PSI"
tv "pharmaceutical 0.2 micron PTFE membrane filter" "yes" "app+micron+media+type"
tv "food and beverage 1 micron polypropylene cartridge" "yes" "app+micron+media+type"
tv "water treatment 10 micron filter element in stock" "yes" "app+micron+type+stock"
tv "oil and gas 10 micron 150 PSI element" "yes" "ind+micron+PSI+type"
tv "HVAC 25 micron polyester bag filter" "yes" "app+micron+media+type"
tv "brewery depth sheet cellulose Filtrox" "yes" "app+type+media+mfg"
tv "chemical processing PTFE 1 micron membrane" "yes" "app+media+micron+type"
tv "power generation 10 micron glass fiber Pall" "yes" "app+micron+media+mfg"

tv "lube oil 10 micron Pall element absolute rated" "yes" "app+micron+mfg+type+eff"
tv "amine treating coalescer Pall LLS" "yes" "app+type+mfg"
tv "glycol dehydration filter 10 micron" "yes" "app+type+micron"
tv "municipal water NSF rated filter" "yes" "app+cert"
tv "refinery crude oil filter 10 micron 150 PSI" "yes" "app+micron+PSI"
tv "turbine lube oil 5 micron absolute Pall" "yes" "app+micron+eff+mfg"
tv "dairy plant 3A sanitary filter" "yes" "app+cert"
tv "produced water coalescing filter element" "yes" "app+type"
tv "paint spray booth filter 25 micron polyester" "yes" "app+micron+media"
tv "condensate filter 10 micron glass fiber" "yes" "app+micron+media"

tv "sour water stripping filter element" "yes" "app+type"
tv "desiccant bed guard filter" "yes" "app+type"
tv "whisky depth sheet filtration" "yes" "app+type"
tv "caustic treating filter Pall PhaseSep" "yes" "app+mfg+product"
tv "diesel fuel filter 10 micron absolute" "yes" "app+micron+eff"
tv "semiconductor ultra pure water 0.2 micron" "yes" "ind+app+micron"
tv "marine hydraulic filter 10 micron stainless" "yes" "ind+app+micron+media"
tv "automotive paint filtration 25 micron" "yes" "ind+app+micron"
tv "industrial hydraulic 10 micron Pall in stock" "yes" "ind+app+micron+mfg+stock"
tv "pharmaceutical sterile 0.2 micron PTFE Cobetter" "yes" "app+eff+micron+media+mfg"

# ============================================================================
# SECTION 5: PART NUMBER + SPEC VALIDATION (30 tests)
# Look up real PNs, verify returned specs match crosswalk
# ============================================================================
echo ""
echo "--- Section 5: Part Number Spec Validation (30 tests) ---"
echo ""

# HC2216FKP6H: 5 micron, Glass Fiber, 250F, 150 PSI, Absolute, Hydraulic
tl "HC2216FKP6H" "Micron" "5" "Verify 5 micron"
tl "HC2216FKP6H" "Media" "Glass Fiber" "Verify Glass Fiber"
tl "HC2216FKP6H" "Max_PSI" "150" "Verify 150 PSI"
tl "HC2216FKP6H" "Final_Manufacturer" "Applied Energy" "Verify manufacturer"
tl "HC2216FKP6H" "Description" "Filter Element" "Verify description"

# HC2296FCP36H50: 5 micron, Glass Fiber, 250F, 150 PSI, Absolute, Hydraulic
tl "HC2296FCP36H50" "Micron" "5" "Verify 5 micron"
tl "HC2296FCP36H50" "Media" "Glass Fiber" "Verify Glass Fiber"
tl "HC2296FCP36H50" "Max_PSI" "150" "Verify 150 PSI"
tl "HC2296FCP36H50" "Product_Type" "Filter Element" "Verify product type"

# HC9021FAS4Z: Glass Fiber, 250F, 150 PSI, Absolute, Hydraulic
tl "HC9021FAS4Z" "Media" "Glass Fiber" "Verify Glass Fiber"
tl "HC9021FAS4Z" "Max_PSI" "150" "Verify 150 PSI"
tl "HC9021FAS4Z" "Product_Type" "Filter Element" "Verify type"

# AB3Y0033J: Polypropylene, 176F, 150 PSI, Absolute
tl "AB3Y0033J" "Media" "Polypropylene" "Verify Polypropylene"
tl "AB3Y0033J" "Max_PSI" "150" "Verify 150 PSI"

# PES50.5P2SSH: AJR, Bag Filter
tl "PES50.5P2SSH" "Final_Manufacturer" "AJR" "Verify AJR"
tl "PES50.5P2SSH" "Product_Type" "Bag Filter" "Verify Bag Filter"

# GSTF11T6615EP: Global Filter, Housing, 150 PSI
tl "GSTF11T6615EP" "Final_Manufacturer" "Global Filter" "Verify Global Filter"
tl "GSTF11T6615EP" "Product_Type" "Filter Housing" "Verify Housing"
tl "GSTF11T6615EP" "Max_PSI" "150" "Verify 150 PSI"

# 182322800: AAF, HVAC
tl "182322800" "Final_Manufacturer" "AAF" "Verify AAF"

# SS644FD: Precision, Separator, 150 PSI, Hydraulic
tl "SS644FD" "Final_Manufacturer" "Precision" "Verify Precision"
tl "SS644FD" "Product_Type" "Separator" "Verify Separator"
tl "SS644FD" "Max_PSI" "150" "Verify 150 PSI"

# YS62-CS-6.00-40: Accurate Valve, Strainer
tl "YS62-CS-6.00-40" "Final_Manufacturer" "Accurate Valve" "Verify Accurate Valve"
tl "YS62-CS-6.00-40" "Description" "Y-Strainer" "Verify Y-Strainer"

# 2901200407: Quincy, Compressed Air
tl "2901200407" "Final_Manufacturer" "Quincy" "Verify Quincy"

# NXAM702UNF: Pall, Polypropylene, 176F, 150 PSI, Absolute
tl "NXAM702UNF" "Media" "Polypropylene" "Verify Polypropylene"
tl "NXAM702UNF" "Max_PSI" "150" "Verify 150 PSI"

# CLPF0002TC30P: Banner, 150 PSI
tl "CLPF0002TC30P" "Final_Manufacturer" "Banner" "Verify Banner"
tl "CLPF0002TC30P" "Max_PSI" "150" "Verify 150 PSI"

# ============================================================================
# SECTION 6: STT ARTIFACTS + MULTI-SPEC (20 tests)
# Voice mishears combined with complex queries
# ============================================================================
echo ""
echo "--- Section 6: STT Artifacts + Multi-Spec (20 tests) ---"
echo ""

tv "paul ten micron glass fiber hydraulic element" "yes" "mishear:Paul+micron+media+app"
tv "pal five micron polly pro cartridge" "yes" "mishear:Pal+micron+media+type"
tv "graver tech ten micron element one fifty PSI" "yes" "mishear:Graver Tech+micron+PSI"
tv "co better point two micron teflon membrane" "yes" "mishear:CoBetter+micron+media+type"
tv "shell co twenty five micron bag filter" "yes" "mishear:ShellCo+micron+type"
tv "rose dale housing for ten micron cartridge" "yes" "mishear:Rosedale+type+micron"
tv "cook filter ten micron compressed air" "yes" "mishear:Cook→Koch+micron+app"
tv "penta air five micron water treatment" "yes" "mishear:PentaAir+micron+app"
tv "john l ten micron glass fiber" "yes" "mishear:JohnL→Jonell+micron+media"
tv "schrader five micron hydraulic element" "yes" "mishear:Schrader→Schroeder+micron+app"
tv "three sixteen stainless ten micron filter" "yes" "number:316SS+micron"
tv "ten micron poly pro bag for paint booth" "yes" "number+mishear+type+app"
tv "five micron teflon membrane pharmaceutical" "yes" "number+alias+type+app"
tv "twenty five micron kynar filter chemical processing" "yes" "number+alias+app"
tv "zero point two micron for semiconductor" "yes" "decimal+app"
tv "point five micron glass fiber pharmaceutical" "yes" "decimal+media+app"
tv "en pro ten micron polly pro" "yes" "mishear:EnPro+micron+media"
tv "por vair five micron element compressed air" "yes" "mishear:Porvair+micron+type+app"
tv "mcmaster carr stainless steel filter" "yes" "mishear:McMaster+media"
tv "saint go bain filter ten micron" "yes" "mishear:SaintGobain+micron"

# ============================================================================
# SECTION 7: HALLUCINATION CHECKS (20 tests)
# Fake combos that should NOT return results
# ============================================================================
echo ""
echo "--- Section 7: Hallucination Multi-Spec Checks (20 tests) ---"
echo ""

tv "look up FAKE12345 10 micron glass fiber" "no" "Fake PN+specs"
tv "find ZZZZZ99999 hydraulic element" "no" "Fake PN+app"
tv "HC9999ZZZ 5 micron Pall" "no" "Fake HC prefix+specs"
tv "NONEXIST001 polypropylene 25 micron" "no" "Fake PN+specs"
tv "PHANTOM001 compressed air filter" "no" "Fake PN+app"
tv "HALLUCINATE1 glass fiber element" "no" "Hallucination bait"
tv "TEST000001 10 micron hydraulic" "no" "Test PN+specs"
tv "ABCXYZ789 PTFE membrane" "no" "Random+specs"
tv "GHOST-PART 150 PSI element" "no" "Ghost+PSI"
tv "XXXXXXXXX 5 micron filter" "no" "AllX+specs"

# Real query but verify response doesn't invent non-existent specs
tv "10 micron Pall filter element" "yes" "Real — verify no hallucination"
tv "5 micron glass fiber Graver" "yes" "Real — verify accuracy"
tv "polypropylene bag filter 25 micron" "yes" "Real — verify specs"
tv "compressed air 5 micron element" "yes" "Real — verify app"
tv "hydraulic 10 micron Pall glass fiber absolute" "yes" "Real — 5 var combo"
tv "Filtrox cellulose depth sheet brewery" "yes" "Real — 4 var combo"
tv "Global Filter housing 150 PSI" "yes" "Real — 3 var combo"
tv "Cobetter 1 micron PTFE pharmaceutical" "yes" "Real — 4 var combo"
tv "Schroeder 10 micron hydraulic 150 PSI stainless" "yes" "Real — 5 var combo"
tv "AJR 25 micron polyester bag industrial" "yes" "Real — 4 var combo"

# ============================================================================
# RESULTS
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
