"""Ellis Island passenger records scraper.

Generates parametric URL queries for the American Family Immigration History Center
database (65M arrival records, 1820–1957). Searches for San Marino origin passengers.
Uses async HTTP for concurrent surname lookups.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlencode

import aiohttp

from titan_veritas.config import USER_AGENTS, DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX

logger = logging.getLogger(__name__)

ELLIS_ISLAND_BASE = "https://heritage.statueofliberty.org"
SEARCH_PATH = "/passenger-result/czovMQ==/"


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

    @property
    def has_san_marino_connection(self) -> bool:
        return self.san_marino_hits > 0


def build_search_url(
    surname: str,
    town_of_origin: str = "San Marino",
    match_mode: str = "starts_with",
) -> str:
    """Build a parametric search URL for Ellis Island database.

    Args:
        surname: Last name to search.
        town_of_origin: Filter for city of origin (default: "San Marino").
        match_mode: One of 'exact', 'starts_with', 'sounds_like', 'contains'.
    """
    params = {
        "lastName": surname,
        "lastNameMatchMode": match_mode,
        "townOfOrigin": town_of_origin,
    }
    return f"{ELLIS_ISLAND_BASE}{SEARCH_PATH}?{urlencode(params)}"


def _random_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }


async def _search_ellis_island(
    session: aiohttp.ClientSession,
    surname: str,
) -> EllisIslandResult:
    """Query Ellis Island for passenger records matching surname + San Marino origin."""
    url = build_search_url(surname)
    result = EllisIslandResult(surname=surname, search_url=url)

    try:
        await asyncio.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))

        async with session.get(url, headers=_random_headers()) as resp:
            if resp.status != 200:
                result.error = f"HTTP {resp.status}"
                return result

            html = await resp.text()

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "lxml")

            # Parse the passenger results table
            rows = soup.find_all("tr", class_=re.compile(r"(passenger|result|record)", re.I))
            if not rows:
                # Fallback: find any table with passenger data
                tables = soup.find_all("table")
                for table in tables:
                    trs = table.find_all("tr")[1:]
                    rows.extend(trs)

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                record = EllisIslandRecord(surname=surname)
                cell_texts = [c.get_text(strip=True) for c in cells]

                # Map cells to fields
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

                result.records.append(record)
                result.total_hits += 1

                # Check for San Marino connection
                all_text = " ".join(cell_texts).lower()
                if any(kw in all_text for kw in ("san marino", "sammarinese", "repubblica")):
                    result.san_marino_hits += 1

        logger.info(
            f"Ellis Island '{surname}': {result.total_hits} records, "
            f"{result.san_marino_hits} San Marino hits"
        )
    except asyncio.TimeoutError:
        result.error = "Timeout"
        logger.warning(f"Ellis Island timeout for '{surname}'")
    except Exception as e:
        result.error = str(e)
        logger.warning(f"Ellis Island error for '{surname}': {e}")

    return result


async def search_surnames(surnames: list[str], max_concurrent: int = 3) -> list[EllisIslandResult]:
    """Search Ellis Island for multiple surnames concurrently."""
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _limited(session: aiohttp.ClientSession, name: str) -> EllisIslandResult:
        async with semaphore:
            return await _search_ellis_island(session, name)

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [_limited(session, s) for s in surnames]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    clean = []
    for r, s in zip(results, surnames):
        if isinstance(r, Exception):
            clean.append(EllisIslandResult(surname=s, error=str(r)))
        else:
            clean.append(r)
    return clean


def search_surnames_sync(surnames: list[str]) -> list[EllisIslandResult]:
    """Synchronous wrapper for async Ellis Island search."""
    return asyncio.run(search_surnames(surnames))
