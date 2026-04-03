"""
EnPro Filtration Mastermind Portal — Governance Engine
Pre-checks (before GPT) and post-checks (after GPT) to enforce safety,
scope, and response quality.
"""

import re
import logging
from typing import Optional

logger = logging.getLogger("enpro.governance")

# ---------------------------------------------------------------------------
# Escalation triggers — any of these = "Contact EnPro Engineering"
# ---------------------------------------------------------------------------
ESCALATION_TRIGGERS = {
    "temperature": 400,   # Max temp F
    "pressure": 150,      # Max PSI
    "micron": 0.2,        # Below this = specialty
}

ESCALATION_KEYWORDS = [
    "steam",
    "pulsating flow",
    "h2s",
    "hf",
    "chlorine",
    "hydrogen",
    "sour",
    "nace",
]

ESCALATION_RESPONSE = (
    "This application requires engineering review. "
    "Contact EnPro: service@enproinc.com | 1 (800) 323-2416"
)

# ---------------------------------------------------------------------------
# Out-of-scope patterns
# ---------------------------------------------------------------------------
OUT_OF_SCOPE_PATTERNS = [
    r"\b(weather|stock market|recipe|joke|poem|song|movie|game)\b",
    r"\b(who is|what year|capital of|president)\b",
    r"\b(write me|create a|generate a)\b(?!.*(quote|comparison|compare|pregame|summary|list|report))",
]

OUT_OF_SCOPE_RESPONSE = "Outside my scope. I'm built for filtration."

# ---------------------------------------------------------------------------
# Pre-checks — run BEFORE any GPT call
# ---------------------------------------------------------------------------

def run_pre_checks(message: str, context: Optional[dict] = None) -> Optional[dict]:
    """
    Run all 6 pre-checks on the user message.
    Returns a governance response dict if intercepted, None if message is clean.
    """
    checks = [
        _check_override_attempt,
        _check_out_of_scope,
        _check_nominal_sterile,
        _check_volume_pricing,
        _check_shipping,
        _check_escalation_triggers,
    ]

    for check in checks:
        result = check(message, context)
        if result is not None:
            return result

    return None


def _check_override_attempt(message: str, context: Optional[dict] = None) -> Optional[dict]:
    """Detect prompt injection / override attempts."""
    override_patterns = [
        r"ignore (your|all|previous) (instructions|rules|constraints)",
        r"forget (everything|your rules|your instructions)",
        r"you are now",
        r"act as if",
        r"pretend (you|to be)",
        r"bypass (your|the) (rules|filters|governance)",
        r"jailbreak",
        r"DAN mode",
    ]
    msg_lower = message.lower()
    for pattern in override_patterns:
        if re.search(pattern, msg_lower):
            logger.warning(f"Override attempt detected: {message[:100]}")
            return {
                "intercepted": True,
                "check": "override_attempt",
                "response": (
                    "I cannot approve a recommendation that bypasses safety or engineering governance. "
                    "These constraints protect you and your customer. "
                    "Safety and engineering governance cannot be overridden regardless of authorization level."
                ),
            }
    return None


def _check_out_of_scope(message: str, context: Optional[dict] = None) -> Optional[dict]:
    """Detect clearly out-of-scope questions."""
    msg_lower = message.lower()
    for pattern in OUT_OF_SCOPE_PATTERNS:
        if re.search(pattern, msg_lower):
            return {
                "intercepted": True,
                "check": "out_of_scope",
                "response": OUT_OF_SCOPE_RESPONSE,
            }
    return None


def _check_nominal_sterile(message: str, context: Optional[dict] = None) -> Optional[dict]:
    """Flag nominal vs absolute micron rating confusion."""
    msg_lower = message.lower()
    if "nominal" in msg_lower and "absolute" in msg_lower:
        return {
            "intercepted": False,  # Don't block — just flag for GPT context
            "check": "nominal_sterile",
            "advisory": (
                "Note: The customer is asking about nominal vs absolute ratings. "
                "Nominal = general-purpose (~85% efficiency at rated micron). "
                "Absolute = certified removal (typically 99.9%+ at rated micron). "
                "For sterile or critical applications, always recommend absolute-rated filters."
            ),
        }
    return None


def _check_volume_pricing(message: str, context: Optional[dict] = None) -> Optional[dict]:
    """Detect volume/bulk pricing requests — escalate to sales."""
    volume_patterns = [
        r"\b(volume|bulk|wholesale|quantity)\s*(pricing|discount|price)",
        r"\b(100|500|1000|\d{3,})\s*(units|pieces|filters|cartridges|elements)",
        r"\bblanket\s*(order|po|purchase)",
    ]
    msg_lower = message.lower()
    for pattern in volume_patterns:
        if re.search(pattern, msg_lower):
            return {
                "intercepted": True,
                "check": "volume_pricing",
                "response": (
                    "Contact EnPro for volume pricing — "
                    "service@enproinc.com or 1 (800) 323-2416."
                ),
            }
    return None


def _check_shipping(message: str, context: Optional[dict] = None) -> Optional[dict]:
    """Detect shipping/delivery questions — not our domain."""
    shipping_patterns = [
        r"\b(shipping cost|delivery date|freight charge|tracking number|ship to|deliver to)\b",
        r"\b(lead time|when.*arrive|eta|ship.*order)\b",
    ]
    msg_lower = message.lower()
    for pattern in shipping_patterns:
        if re.search(pattern, msg_lower):
            return {
                "intercepted": True,
                "check": "shipping",
                "response": (
                    "Contact EnPro for shipping and delivery — "
                    "service@enproinc.com or 1 (800) 323-2416."
                ),
            }
    return None


def _check_escalation_triggers(message: str, context: Optional[dict] = None) -> Optional[dict]:
    """Detect dangerous operating conditions that require engineering review."""
    msg_lower = message.lower()

    # Keyword triggers
    for keyword in ESCALATION_KEYWORDS:
        if keyword in msg_lower:
            logger.info(f"Escalation trigger (keyword): {keyword}")
            return {
                "intercepted": True,
                "check": "escalation_triggers",
                "trigger": keyword,
                "response": ESCALATION_RESPONSE,
            }

    # Numeric triggers — temperature
    temp_match = re.search(r"(?<![a-z0-9])(\d{3,})\s*(?:degrees?\s*)?(?:f|fahrenheit)\b", msg_lower)
    if temp_match:
        temp = int(temp_match.group(1))
        if temp > ESCALATION_TRIGGERS["temperature"]:
            logger.info(f"Escalation trigger (temperature): {temp}F")
            return {
                "intercepted": True,
                "check": "escalation_triggers",
                "trigger": f"temperature:{temp}F",
                "response": ESCALATION_RESPONSE,
            }

    # Numeric triggers — pressure
    psi_match = re.search(r"(?<![a-z0-9])(\d{2,})\s*(?:psi|pounds)\b", msg_lower)
    if psi_match:
        psi = int(psi_match.group(1))
        if psi > ESCALATION_TRIGGERS["pressure"]:
            logger.info(f"Escalation trigger (pressure): {psi} PSI")
            return {
                "intercepted": True,
                "check": "escalation_triggers",
                "trigger": f"pressure:{psi}PSI",
                "response": ESCALATION_RESPONSE,
            }

    # Numeric triggers — micron (below threshold)
    micron_match = re.search(r"(?<![a-z0-9])(0\.\d+)\s*(?:micron|um)\b", msg_lower)
    if micron_match:
        micron = float(micron_match.group(1))
        if micron < ESCALATION_TRIGGERS["micron"]:
            logger.info(f"Escalation trigger (micron): {micron}")
            return {
                "intercepted": True,
                "check": "escalation_triggers",
                "trigger": f"micron:{micron}",
                "response": ESCALATION_RESPONSE,
            }

    return None


# ---------------------------------------------------------------------------
# Post-check — run AFTER GPT response, before returning to user
# ---------------------------------------------------------------------------

def run_post_check(response: str) -> dict:
    """
    Validate GPT response before returning to user.
    Checks for $0 prices, hidden field leaks, and bullet list formatting.

    Returns dict with 'valid' bool and optional 'issues' list.
    """
    issues = []

    # Check for $0.00 prices (should say "Contact EnPro")
    if re.search(r"\$0\.?0{0,2}\b", response):
        issues.append("Response contains $0 price — should say 'Contact EnPro for pricing'")

    # Check for hidden field leaks
    hidden_patterns = [
        r"\bP21_Item_ID\b",
        r"\bProduct_Group\b(?!\s*Description)",  # Product_Group but not Product_Group_Description
        r"\bSupplier_Code\b",
        r"\bAlt_Code\b",
    ]
    for pattern in hidden_patterns:
        if re.search(pattern, response):
            field = re.search(pattern, response).group(0)
            issues.append(f"Response leaks hidden field: {field}")

    # Check for bullet list formatting (responses should use bullets, not paragraphs)
    line_count = len(response.strip().split("\n"))
    if line_count > 5 and "1." not in response:
        issues.append("Response is long but not formatted as numbered list")

    return {
        "valid": len(issues) == 0,
        "issues": issues,
    }


def sanitize_response(response: str) -> str:
    """
    Clean up GPT response by fixing known issues.
    """
    # Replace $0.00 with contact message
    response = re.sub(
        r"\$0\.?0{0,2}\b",
        "Contact EnPro for pricing",
        response,
    )

    # Remove hidden field references
    for field in ["P21_Item_ID", "Supplier_Code", "Alt_Code"]:
        response = re.sub(rf"\b{field}:\s*\S+\s*", "", response)

    return response.strip()
