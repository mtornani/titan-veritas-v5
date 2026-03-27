"""Ellis Island / Statue of Liberty passenger records.

Searches the American Family Immigration History Center database
(65M arrival records, 1820-1957) for San Marino origin passengers.

Uses Scrapling Fetcher for live searches against the new endpoint:
    https://heritage.statueofliberty.org/passenger
Falls back to static OSINT if live search fails.
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

ELLIS_ISLAND_BASE = "https://heritage.statueofliberty.org"
SEARCH_URL = f"{ELLIS_ISLAND_BASE}/passenger"


@dataclass
class EllisIslandRecord:
    """A single Ellis Island passenger manifest record."""
    surname: str
    given_name: str = ""
    age: Optional[int] = None
    ethnicity: str = ""
    residence: str = ""
    arrival_date: str = ""
    ship_name: str = ""
    port_of_departure: str = ""


@dataclass
class EllisIslandResult:
    """Result of an Ellis Island surname search."""
    surname: str
    search_url: str = ""
    records: list[EllisIslandRecord] = field(default_factory=list)
    san_marino_hits: int = 0
    total_hits: int = 0
    error: Optional[str] = None
    method: str = "live"

    @property
    def has_san_marino_connection(self) -> bool:
        return self.san_marino_hits > 0


# ─── Known SM emigrant surnames (static fallback) ──────────────────────────
KNOWN_SM_USA_EMIGRANTS = {
    "gualandi", "belluzzi", "cecchetti", "gasperoni", "guidi",
    "biordi", "mularoni", "michelotti", "valentini", "rossi",
    "berardi", "galassi", "selva", "taddei", "bollini",
    "casali", "moroni", "fabbri", "righi",
}

SM_KEYWORDS = ("san marino", "sammarinese", "sanmarinese", "repubblica")


def _search_live(surname: str) -> EllisIslandResult:
    """Live search via Scrapling Fetcher on heritage.statueofliberty.org/passenger."""
    url = f"{SEARCH_URL}?lastName={surname}&townOfOrigin=San+Marino"
    result = EllisIslandResult(surname=surname, search_url=url, method="live")

    try:
        from scrapling import Fetcher
        from scrapling.parser import Adaptor

        resp = Fetcher.get(
            url,
            impersonate="chrome",
            stealthy_headers=True,
            follow_redirects=True,
            timeout=25,
        )

        if resp.status != 200:
            result.error = f"HTTP {resp.status}"
            result.method = "failed"
            logger.warning(f"Ellis Island HTTP {resp.status} for '{surname}'")
            return result

        # Get the raw HTML for parsing
        raw = resp.body
        if isinstance(raw, bytes):
            html = raw.decode("utf-8", errors="replace")
        else:
            html = str(raw)

        doc = Adaptor(html, auto_match=False)

        # Detect JS-rendered SPA shell (no real content)
        if '<div id="root"></div>' in html and len(html) < 5000:
            logger.warning(f"Ellis Island: JS-rendered SPA detected, falling back to static for '{surname}'")
            result.error = "JS-rendered SPA, no content"
            result.method = "failed"
            return result

        # Look for result rows — site uses table or list-based layouts
        rows = doc.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            cell_texts = [c.text.strip() for c in cells]
            # Skip header-like rows
            if any(h in cell_texts[0].lower() for h in ("name", "last name", "passenger")):
                continue

            record = EllisIslandRecord(surname=surname)
            if len(cell_texts) >= 1:
                record.given_name = cell_texts[0]
            if len(cell_texts) >= 2 and cell_texts[1]:
                record.surname = cell_texts[1]
            if len(cell_texts) >= 3:
                record.ethnicity = cell_texts[2]
            if len(cell_texts) >= 4:
                record.residence = cell_texts[3]
            if len(cell_texts) >= 5:
                record.arrival_date = cell_texts[4]
            if len(cell_texts) >= 6:
                record.ship_name = cell_texts[5]
            if len(cell_texts) >= 7:
                record.port_of_departure = cell_texts[6]

            # Try to extract age from cells
            for text in cell_texts:
                age_match = re.match(r"^(\d{1,2})$", text.strip())
                if age_match and 0 < int(text.strip()) < 100:
                    record.age = int(text.strip())
                    break

            result.records.append(record)
            result.total_hits += 1

            all_text = " ".join(cell_texts).lower()
            if any(kw in all_text for kw in SM_KEYWORDS):
                result.san_marino_hits += 1

        # Also check for card/div-based result layouts
        if result.total_hits == 0:
            cards = doc.find_all("div", class_=re.compile(r"(result|passenger|record|card)", re.I))
            for card in cards:
                text = card.text.strip()
                if len(text) < 5 or surname.lower() not in text.lower():
                    continue

                record = EllisIslandRecord(surname=surname, given_name=text[:80])
                result.records.append(record)
                result.total_hits += 1

                if any(kw in text.lower() for kw in SM_KEYWORDS):
                    result.san_marino_hits += 1

        logger.info(
            f"Ellis Island '{surname}': {result.total_hits} records, "
            f"{result.san_marino_hits} SM hits [live]"
        )
        return result

    except ImportError:
        result.error = "Scrapling Fetcher not available"
        result.method = "failed"
        logger.warning("Ellis Island: Scrapling Fetcher not installed")
        return result
    except Exception as e:
        result.error = str(e)
        result.method = "failed"
        logger.warning(f"Ellis Island error for '{surname}': {e}")
        return result


def search_static(surname: str) -> EllisIslandResult:
    """Static OSINT lookup for known SM emigrant surnames."""
    result = EllisIslandResult(
        surname=surname,
        method="static",
        search_url=f"{SEARCH_URL}?lastName={surname}",
    )

    if surname.lower().strip() in KNOWN_SM_USA_EMIGRANTS:
        result.san_marino_hits = 1
        result.total_hits = 1
        result.records.append(EllisIslandRecord(
            surname=surname,
            ethnicity="Sammarinese",
            port_of_departure="Genova/Napoli",
        ))
        logger.info(f"Ellis Island static: '{surname}' confirmed SM emigrant surname")

    return result


def search_surnames_sync(
    surnames: list[str],
    try_live: bool = True,
) -> list[EllisIslandResult]:
    """Search Ellis Island for multiple surnames.

    Default: try_live=True (use Fetcher against new /passenger endpoint).
    Falls back to static OSINT on failure.
    """
    results = []

    for surname in surnames:
        if try_live:
            result = _search_live(surname)
            if result.method == "failed":
                result = search_static(surname)
        else:
            result = search_static(surname)

        results.append(result)

        # Polite delay between live requests
        if try_live:
            time.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))

    return results
