"""Cognomix.it — Italian surname geographic distribution.

Checks whether a surname is concentrated in the San Marino area
(Rimini, Pesaro-Urbino, Forlì-Cesena, Emilia-Romagna, Marche).

Strategy:
    1. Fetcher (primary) — scrape Cognomix distribution page.
    2. Static OSINT (fallback) — known SM-area concentrated surnames.
"""

from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Optional

from titan_veritas.config import DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX

logger = logging.getLogger(__name__)

COGNOMIX_BASE = "https://www.cognomix.it/mappe-dei-cognomi-italiani"

# Provinces / regions around San Marino
SM_AREA_INDICATORS = [
    "san marino", "rimini", "pesaro", "urbino", "forlì", "forli",
    "cesena", "emilia-romagna", "marche", "repubblica",
]


@dataclass
class CognomixResult:
    """Result of a Cognomix geographic distribution check."""
    surname: str
    search_url: str = ""
    sm_area_hit: bool = False
    indicators_found: list[str] = None
    total_families: int = 0
    error: Optional[str] = None
    method: str = "live"  # "live", "static", "failed"

    def __post_init__(self):
        if self.indicators_found is None:
            self.indicators_found = []


# ─── Known SM-area concentrated surnames ─────────────────────────────────────
KNOWN_SM_AREA_SURNAMES = {
    # Tier 1 endemic — strong concentration around SM/Rimini
    "gualandi", "terenzi", "stacchini", "belluzzi", "cecchetti",
    "macina", "gennari", "taddei", "stefanelli", "ciavatta",
    "bollini", "selva", "ceccoli", "gasperoni", "guidi",
    "biordi", "santini", "mularoni", "zonzini", "galassi",
    "michelotti", "berardi", "valentini", "zanotti", "lonfernini",
    "gobbi", "tamagnini", "cenci", "montanari", "ugolini",
    # Tier 2 — present in SM area
    "casali", "moroni", "righi", "benedettini", "canti",
    "muccioli", "crescentini", "bacciocchi", "giancecchi",
    "podeschi", "zafferani", "angeli", "matteoni", "giardi",
    "paoloni", "renzi", "venturini",
    # Common but concentrated — still useful signal
    "rossi", "bianchi", "fabbri", "guerra",
}


def _search_live(surname: str) -> CognomixResult:
    """Live search via Fetcher on Cognomix."""
    url = f"{COGNOMIX_BASE}/{surname.lower()}.php"
    result = CognomixResult(surname=surname, search_url=url, method="live")

    try:
        from scrapling import Fetcher

        resp = Fetcher.get(
            url,
            stealthy_headers=True,
            follow_redirects=True,
            timeout=15,
        )

        if resp.status == 404:
            result.error = "Surname not found on Cognomix"
            result.method = "failed"
            return result

        if resp.status != 200:
            result.error = f"HTTP {resp.status}"
            result.method = "failed"
            return result

        raw = resp.body
        html = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        html_lower = html.lower()

        # Extract total families count if available
        families_match = re.search(r"circa\s+([\d.]+)\s+famigli", html_lower)
        if families_match:
            result.total_families = int(families_match.group(1).replace(".", ""))

        # Check for SM area indicators in the distribution text
        hits = [ind for ind in SM_AREA_INDICATORS if ind in html_lower]
        if hits:
            result.sm_area_hit = True
            result.indicators_found = hits
            logger.info(f"Cognomix '{surname}': SM area confirmed ({', '.join(hits)}) [live]")
        else:
            logger.info(f"Cognomix '{surname}': no SM area concentration [live]")

        return result

    except ImportError:
        result.error = "Scrapling Fetcher not available"
        result.method = "failed"
        return result
    except Exception as e:
        result.error = str(e)
        result.method = "failed"
        logger.warning(f"Cognomix error for '{surname}': {e}")
        return result


def search_static(surname: str) -> CognomixResult:
    """Static OSINT lookup for known SM-area surnames."""
    result = CognomixResult(
        surname=surname,
        method="static",
        search_url=f"{COGNOMIX_BASE}/{surname.lower()}.php",
    )

    if surname.lower().strip() in KNOWN_SM_AREA_SURNAMES:
        result.sm_area_hit = True
        result.indicators_found = ["static_known_sm_surname"]
        logger.info(f"Cognomix static: '{surname}' confirmed SM-area surname")

    return result


def search_surnames_sync(
    surnames: list[str],
    try_live: bool = True,
) -> list[CognomixResult]:
    """Check Cognomix for multiple surnames.

    Falls back to static OSINT if live search fails.
    """
    results = []

    for surname in surnames:
        if try_live:
            result = _search_live(surname)
            if result.method == "failed":
                logger.info(f"Cognomix: falling back to static for '{surname}'")
                result = search_static(surname)
        else:
            result = search_static(surname)

        results.append(result)

        if try_live:
            time.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))

    return results
