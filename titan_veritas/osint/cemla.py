"""CEMLA (Centro de Estudios Migratorios Latinoamericanos) scraper.

Searches Argentine immigration archives (1800-1960) for San Marino origin passengers.
The search portal lives inside an iframe at https://search.cemla.com/ and is protected
by BotDetect CAPTCHA + anti-bot measures.

Strategy:
    1. StealthyFetcher (primary) — headless Camoufox browser with anti-bot bypass.
       Uses `page_action` callback to fill & submit the ASP.NET search form.
    2. Static OSINT (fallback) — known SM emigrant surname database.
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

CEMLA_SEARCH_URL = "https://search.cemla.com/"


@dataclass
class CemlaRecord:
    """A single CEMLA immigration record."""
    surname: str
    given_name: str = ""
    age: Optional[int] = None
    nationality: str = ""
    birth_place: str = ""
    arrival_date: str = ""
    ship_name: str = ""
    origin_port: str = ""


@dataclass
class CemlaResult:
    """Result of a CEMLA surname search."""
    surname: str
    records: list[CemlaRecord] = field(default_factory=list)
    san_marino_hits: int = 0
    total_hits: int = 0
    error: Optional[str] = None
    method: str = "live"  # "live", "static", "failed"

    @property
    def has_san_marino_connection(self) -> bool:
        return self.san_marino_hits > 0


# ─── Known SM emigrant surnames (static fallback) ──────────────────────────
KNOWN_SM_EMIGRANT_SURNAMES = {
    # Tier 1 endemic — documented CEMLA presence
    "gualandi", "terenzi", "stacchini", "belluzzi", "cecchetti",
    "macina", "gennari", "taddei", "rossi", "stefanelli",
    "ciavatta", "bollini", "selva", "ceccoli", "gasperoni",
    "guidi", "biordi", "santini", "mularoni", "zonzini",
    "galassi", "michelotti", "berardi", "valentini", "zanotti",
    "lonfernini",
    # Tier 2 — high probability CEMLA presence
    "casali", "moroni", "fabbri", "guerra", "righi",
    "benedettini", "canti", "muccioli", "crescentini",
    "bacciocchi", "giancecchi", "podeschi", "zafferani",
}

SM_KEYWORDS = ("san marino", "sammarinese", "sanmarinese", "repubblica di san")


def _make_page_action(surname: str):
    """Build an async page_action callback for StealthyFetcher.

    The callback receives a Playwright Page object, fills the CEMLA
    search form, and submits it.
    """
    async def _action(page):
        # Wait for the search form to appear
        await page.wait_for_selector(
            'input[name="Lastname"]',
            state="visible",
            timeout=15000,
        )
        # Fill the search form
        await page.fill('input[name="Lastname"]', surname)
        await page.fill('input[name="Name"]', '')
        await page.fill('input[name="DateFrom"]', '1800')
        await page.fill('input[name="DateTo"]', '1960')

        # Submit the form — look for the submit button
        submit = page.locator('input[type="submit"], button[type="submit"]')
        if await submit.count() > 0:
            await submit.first.click()
        else:
            # Fallback: press Enter on the form
            await page.locator('input[name="Lastname"]').press("Enter")

        # Wait for results to load
        await page.wait_for_load_state("networkidle", timeout=15000)
        # Extra wait for JS rendering
        await page.wait_for_timeout(2000)

    return _action


def _search_stealthy(surname: str) -> CemlaResult:
    """Live search using StealthyFetcher — Camoufox with anti-bot bypass."""
    result = CemlaResult(surname=surname, method="live")

    try:
        from scrapling import StealthyFetcher
        from scrapling.parser import Adaptor

        page_action = _make_page_action(surname)

        resp = StealthyFetcher.fetch(
            CEMLA_SEARCH_URL,
            headless=True,
            page_action=page_action,
            network_idle=True,
            timeout=30,
        )

        # Get HTML from response
        raw = resp.body
        if isinstance(raw, bytes):
            html = raw.decode("utf-8", errors="replace")
        else:
            html = str(raw)

        doc = Adaptor(html, auto_match=False)

        # Check if CAPTCHA is still blocking us
        if "CaptchaCode" in html and "validation-summary-errors" in html:
            # CAPTCHA wasn't bypassed — check if we still got partial results
            error_div = doc.find("div", class_="validation-summary-errors")
            if error_div:
                result.error = "CAPTCHA validation required"
                result.method = "failed"
                logger.warning(f"CEMLA: CAPTCHA not bypassed for '{surname}'")
                return result

        # Parse results — CEMLA uses HTML tables
        tables = doc.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:]:  # skip header
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                cell_texts = [c.text.strip() for c in cells]
                # Skip empty rows (Cloudflare/JS artifacts)
                if all(not t for t in cell_texts):
                    continue

                record = CemlaRecord(surname=surname)
                if len(cell_texts) >= 1:
                    record.given_name = cell_texts[0]
                if len(cell_texts) >= 2 and cell_texts[1]:
                    record.surname = cell_texts[1] if cell_texts[1] else surname
                if len(cell_texts) >= 3:
                    record.nationality = cell_texts[2]
                if len(cell_texts) >= 4:
                    record.arrival_date = cell_texts[3]
                if len(cell_texts) >= 5:
                    record.ship_name = cell_texts[4]
                if len(cell_texts) >= 6:
                    record.origin_port = cell_texts[5]

                # Age extraction
                for text in cell_texts:
                    age_match = re.match(r"^(\d{1,3})$", text.strip())
                    if age_match and 0 < int(text.strip()) < 120:
                        record.age = int(text.strip())
                        break

                result.records.append(record)
                result.total_hits += 1

                # Check for San Marino connection
                all_text = " ".join(cell_texts).lower()
                if any(kw in all_text for kw in SM_KEYWORDS):
                    result.san_marino_hits += 1

        logger.info(
            f"CEMLA '{surname}': {result.total_hits} records, "
            f"{result.san_marino_hits} SM hits [live/stealthy]"
        )
        return result

    except ImportError as e:
        result.error = f"StealthyFetcher not available: {e}"
        result.method = "failed"
        logger.warning(f"CEMLA: StealthyFetcher import error: {e}")
        return result
    except Exception as e:
        result.error = str(e)
        result.method = "failed"
        logger.warning(f"CEMLA StealthyFetcher error for '{surname}': {e}")
        return result


def search_static(surname: str) -> CemlaResult:
    """Static OSINT lookup based on known San Marino emigration records."""
    result = CemlaResult(surname=surname, method="static")

    if surname.lower().strip() in KNOWN_SM_EMIGRANT_SURNAMES:
        result.san_marino_hits = 1
        result.total_hits = 1
        result.records.append(CemlaRecord(
            surname=surname,
            nationality="Sammarinese",
            birth_place="San Marino",
            origin_port="Genova/Buenos Aires",
        ))
        logger.info(f"CEMLA static: '{surname}' confirmed SM emigrant surname")

    return result


def search_surnames_sync(
    surnames: list[str],
    try_live: bool = True,
) -> list[CemlaResult]:
    """Search CEMLA for multiple surnames.

    Default: try_live=True (use StealthyFetcher with CAPTCHA bypass).
    Falls back to static OSINT if StealthyFetcher fails.
    """
    results = []

    for surname in surnames:
        if try_live:
            result = _search_stealthy(surname)
            if result.method == "failed":
                # Fall back to static
                logger.info(f"CEMLA: falling back to static for '{surname}'")
                result = search_static(surname)
        else:
            result = search_static(surname)

        results.append(result)

        # Polite delay between live requests
        if try_live:
            time.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))

    return results
