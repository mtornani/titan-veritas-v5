"""Scraper for CEMLA (Centro de Estudios Migratorios Latinoamericanos) archives.

Target: https://cemla.com
The CEMLA database contains records of European immigrants to Argentina.
We search for passengers from San Marino.

Rate limit: 3 req/min with 5-10s jitter (strict anti-bot measures).
"""

import logging
from typing import List, Optional
import httpx
from bs4 import BeautifulSoup

from ..core.models import RawArchiveRecord
from ..core.rate_limiter import RateLimiter, get_random_user_agent

logger = logging.getLogger(__name__)

BASE_URL = "https://cemla.com"
SEARCH_URL = f"{BASE_URL}/buscador"


class CEMLAScraper:
    """Search CEMLA Argentine immigration archives for San Marino emigrants."""

    def __init__(self, rate_limiter: Optional[RateLimiter] = None):
        self.rate_limiter = rate_limiter or RateLimiter(
            requests_per_minute=3, jitter_range=(5.0, 10.0)
        )

    async def search_by_surname(self, surname: str) -> List[RawArchiveRecord]:
        """Search CEMLA for a specific surname originating from San Marino."""
        records = []

        async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
            try:
                async with self.rate_limiter:
                    # CEMLA uses a POST-based search form
                    response = await client.post(
                        SEARCH_URL,
                        data={
                            "apellido": surname,
                            "pais_origen": "San Marino",
                            "tipo_busqueda": "exacta",
                        },
                        headers={
                            "User-Agent": get_random_user_agent(),
                            "Accept": "text/html,application/xhtml+xml",
                            "Accept-Language": "es-AR,es;q=0.9,en;q=0.5",
                            "Referer": BASE_URL,
                        },
                    )

                if response.status_code != 200:
                    logger.warning(
                        f"[CEMLA] HTTP {response.status_code} for surname={surname}"
                    )
                    return records

                records = self._parse_results(response.text, surname)
                logger.info(f"[CEMLA] {surname}: found {len(records)} records")

            except httpx.TimeoutException:
                logger.warning(f"[CEMLA] Timeout searching for {surname}")
            except Exception as e:
                logger.warning(f"[CEMLA] Error searching for {surname}: {e}")

        return records

    async def search_all_surnames(self, surnames: List[str]) -> List[RawArchiveRecord]:
        """Search for multiple surnames. Respects rate limits."""
        all_records = []
        for surname in surnames:
            results = await self.search_by_surname(surname)
            all_records.extend(results)
        return all_records

    def _parse_results(self, html: str, search_surname: str) -> List[RawArchiveRecord]:
        """Parse CEMLA search results page."""
        records = []
        soup = BeautifulSoup(html, "html.parser")

        # CEMLA typically returns results in a table format
        result_rows = soup.select("table.resultados tr, .resultado-item, .registro")

        for row in result_rows:
            try:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                # Typical CEMLA columns: Apellido, Nombre, Edad, Origen, Destino, Año
                surname = cells[0].get_text(strip=True) or search_surname
                origin = "San Marino"
                destination = "Argentina"
                year = None

                # Try to extract destination city
                for cell in cells:
                    text = cell.get_text(strip=True).lower()
                    if any(city in text for city in [
                        "buenos aires", "pergamino", "córdoba", "rosario",
                        "viedma", "san nicolás", "mar del plata", "mendoza"
                    ]):
                        destination = cell.get_text(strip=True)
                        break

                # Try to extract year
                for cell in cells:
                    text = cell.get_text(strip=True)
                    if text.isdigit() and 1850 <= int(text) <= 1970:
                        year = int(text)
                        break

                # Try to extract origin details
                for cell in cells:
                    text = cell.get_text(strip=True).lower()
                    if "san marino" in text or "repubblica" in text:
                        origin = cell.get_text(strip=True)
                        break

                records.append(RawArchiveRecord(
                    surname=surname,
                    origin=origin,
                    destination=destination,
                    year=year,
                    source_url=SEARCH_URL,
                ))

            except Exception as e:
                logger.debug(f"[CEMLA] Failed to parse row: {e}")
                continue

        return records
