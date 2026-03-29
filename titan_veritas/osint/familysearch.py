"""FamilySearch.org genealogical records search.

Searches the world's largest free genealogical database for
San Marino emigration records (birth, immigration, census).

Strategy:
    1. Fetcher (primary) — curl_cffi with browser impersonation.
    2. Static OSINT (fallback) — known SM emigrant surname database.

Note: FamilySearch is likely JS-rendered. If Fetcher returns empty
content, we fall back to static OSINT automatically.
"""

from __future__ import annotations

import logging
import random
import re
import time
from dataclasses import dataclass, field
from typing import Optional

from titan_veritas.config import DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX

logger = logging.getLogger(__name__)

FAMILYSEARCH_BASE = "https://www.familysearch.org/search/record/results"


@dataclass
class FamilySearchResult:
    """Result of a FamilySearch surname search."""
    surname: str
    search_url: str = ""
    total_hits: int = 0
    san_marino_hits: int = 0
    error: Optional[str] = None
    method: str = "live"  # "live", "static", "failed"

    @property
    def has_san_marino_connection(self) -> bool:
        return self.san_marino_hits > 0


# ─── Known SM surnames with FamilySearch presence ────────────────────────────
# Built from manual searches on FamilySearch for "birthplace: San Marino"
KNOWN_SM_FAMILYSEARCH = {
    # Tier 1 endemic — confirmed FamilySearch records from San Marino
    "gualandi", "terenzi", "stacchini", "belluzzi", "cecchetti",
    "macina", "gennari", "taddei", "rossi", "stefanelli",
    "ciavatta", "bollini", "selva", "ceccoli", "gasperoni",
    "guidi", "biordi", "santini", "mularoni", "zonzini",
    "galassi", "michelotti", "berardi", "valentini", "zanotti",
    "lonfernini", "gobbi", "tamagnini", "cenci", "montanari",
    "ugolini", "bianchi",
    # Tier 2 — high probability
    "casali", "moroni", "fabbri", "guerra", "righi",
    "benedettini", "canti", "muccioli", "crescentini",
    "bacciocchi", "giancecchi", "podeschi", "zafferani",
    "angeli", "bonelli", "matteoni", "mazza", "vitali",
    "paoloni", "renzi", "venturini", "giardi",
}

SM_KEYWORDS = ("san marino", "sammarinese", "sanmarinese", "repubblica di san")


def _search_live(surname: str) -> FamilySearchResult:
    """Live search via Fetcher on FamilySearch."""
    url = f"{FAMILYSEARCH_BASE}?q.surname={surname}&q.birthLikePlace=San+Marino"
    result = FamilySearchResult(surname=surname, search_url=url, method="live")

    try:
        from scrapling import Fetcher

        resp = Fetcher.get(
            url,
            stealthy_headers=True,
            follow_redirects=True,
            timeout=20,
        )

        if resp.status != 200:
            result.error = f"HTTP {resp.status}"
            result.method = "failed"
            logger.warning(f"FamilySearch HTTP {resp.status} for '{surname}'")
            return result

        raw = resp.body
        html = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)

        # Detect JS-rendered SPA (empty shell)
        if len(html) < 5000 and ('id="root"' in html or 'id="app"' in html):
            result.error = "JS-rendered SPA, no content"
            result.method = "failed"
            logger.warning(f"FamilySearch: SPA detected for '{surname}'")
            return result

        # Look for result count indicator
        # FamilySearch shows "1-20 of X,XXX" or "No results"
        count_match = re.search(r"of\s+([\d,]+)\s+results?", html, re.I)
        if count_match:
            total = int(count_match.group(1).replace(",", ""))
            result.total_hits = total
            result.san_marino_hits = total  # We searched with birthPlace=San Marino
            logger.info(f"FamilySearch '{surname}': {total} SM records [live]")
            return result

        # Check for "no results" indicator
        if re.search(r"no\s+results?|0\s+results?|nessun\s+risultat", html, re.I):
            logger.info(f"FamilySearch '{surname}': 0 records [live]")
            return result

        # If we got content but can't parse count, check for SM keywords
        html_lower = html.lower()
        if any(kw in html_lower for kw in SM_KEYWORDS):
            result.san_marino_hits = 1
            result.total_hits = 1
            logger.info(f"FamilySearch '{surname}': SM keywords found [live]")

        return result

    except ImportError:
        result.error = "Scrapling Fetcher not available"
        result.method = "failed"
        return result
    except Exception as e:
        result.error = str(e)
        result.method = "failed"
        logger.warning(f"FamilySearch error for '{surname}': {e}")
        return result


def search_static(surname: str) -> FamilySearchResult:
    """Static OSINT lookup for known SM surnames on FamilySearch."""
    result = FamilySearchResult(
        surname=surname,
        method="static",
        search_url=f"{FAMILYSEARCH_BASE}?q.surname={surname}&q.birthLikePlace=San+Marino",
    )

    if surname.lower().strip() in KNOWN_SM_FAMILYSEARCH:
        result.san_marino_hits = 1
        result.total_hits = 1
        logger.info(f"FamilySearch static: '{surname}' confirmed SM surname")

    return result


def search_surnames_sync(
    surnames: list[str],
    try_live: bool = True,
) -> list[FamilySearchResult]:
    """Search FamilySearch for multiple surnames.

    Falls back to static OSINT if live search fails.
    """
    results = []

    for surname in surnames:
        if try_live:
            result = _search_live(surname)
            if result.method == "failed":
                logger.info(f"FamilySearch: falling back to static for '{surname}'")
                result = search_static(surname)
        else:
            result = search_static(surname)

        results.append(result)

        if try_live:
            time.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))

    return results
