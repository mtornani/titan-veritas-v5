"""CEMLA (Centro de Estudios Migratorios Latinoamericanos) async scraper.

Searches Argentine immigration archives (1800–1960) for San Marino origin passengers.
Uses async HTTP to parallelise surname lookups.
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
from dataclasses import dataclass, field
from typing import Optional

import aiohttp

from titan_veritas.config import USER_AGENTS, DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX

logger = logging.getLogger(__name__)

CEMLA_SEARCH_URL = "https://cemla.com/buscador/"


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

    @property
    def has_san_marino_connection(self) -> bool:
        return self.san_marino_hits > 0


def _random_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "es-AR,es;q=0.9",
        "Referer": CEMLA_SEARCH_URL,
    }


async def _search_cemla(
    session: aiohttp.ClientSession,
    surname: str,
) -> CemlaResult:
    """Query CEMLA search portal for immigration records of a surname."""
    result = CemlaResult(surname=surname)

    try:
        await asyncio.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))

        # CEMLA uses a POST-based search form
        form_data = {
            "apellido": surname,
            "nombre": "",
            "nacionalidad": "",
            "barco": "",
            "anio_desde": "1800",
            "anio_hasta": "1960",
        }

        async with session.post(
            CEMLA_SEARCH_URL,
            data=form_data,
            headers=_random_headers(),
        ) as resp:
            if resp.status != 200:
                result.error = f"HTTP {resp.status}"
                return result

            html = await resp.text()

            # Parse results — look for table rows with passenger data
            # CEMLA returns results in HTML tables
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "lxml")

            # Find result rows
            rows = soup.find_all("tr", class_=re.compile(r"(result|registro|fila)", re.I))
            if not rows:
                # Fallback: try all table rows after header
                tables = soup.find_all("table")
                for table in tables:
                    trs = table.find_all("tr")[1:]  # skip header
                    rows.extend(trs)

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                record = CemlaRecord(surname=surname)
                cell_texts = [c.get_text(strip=True) for c in cells]

                # Map cells to fields based on common CEMLA layout
                if len(cell_texts) >= 1:
                    record.given_name = cell_texts[0]
                if len(cell_texts) >= 2:
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
                    age_match = re.match(r"^(\d{1,3})$", text)
                    if age_match and int(text) < 120:
                        record.age = int(text)
                        break

                result.records.append(record)
                result.total_hits += 1

                # Check for San Marino connection
                all_text = " ".join(cell_texts).lower()
                if any(kw in all_text for kw in ("san marino", "sammarinese", "sanmarinese")):
                    result.san_marino_hits += 1

        logger.info(
            f"CEMLA '{surname}': {result.total_hits} records, "
            f"{result.san_marino_hits} San Marino hits"
        )
    except asyncio.TimeoutError:
        result.error = "Timeout"
        logger.warning(f"CEMLA timeout for '{surname}'")
    except Exception as e:
        result.error = str(e)
        logger.warning(f"CEMLA error for '{surname}': {e}")

    return result


async def search_surnames(surnames: list[str], max_concurrent: int = 3) -> list[CemlaResult]:
    """Search CEMLA for multiple surnames concurrently."""
    semaphore = asyncio.Semaphore(max_concurrent)

    async def _limited_search(session: aiohttp.ClientSession, name: str) -> CemlaResult:
        async with semaphore:
            return await _search_cemla(session, name)

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [_limited_search(session, s) for s in surnames]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Convert exceptions to CemlaResult with error
    clean_results = []
    for r, s in zip(results, surnames):
        if isinstance(r, Exception):
            clean_results.append(CemlaResult(surname=s, error=str(r)))
        else:
            clean_results.append(r)

    return clean_results


def search_surnames_sync(surnames: list[str]) -> list[CemlaResult]:
    """Synchronous wrapper for async CEMLA search."""
    return asyncio.run(search_surnames(surnames))
