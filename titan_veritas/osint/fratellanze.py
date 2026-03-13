"""Scraper for the 25 official San Marino diaspora communities (Fratellanze)."""

import re
import logging
from typing import List, Optional
import httpx
from bs4 import BeautifulSoup

from ..core.models import RawArchiveRecord, CommunityContact
from ..core.rate_limiter import RateLimiter, get_random_user_agent

logger = logging.getLogger(__name__)

# Known Fratellanze with publicly available web presence
# NOTE: URLs are placeholders — real URLs must be researched and verified
COMMUNITIES = [
    {"name": "Fratellanza Sammarinese di Detroit", "city": "Detroit", "country": "USA",
     "url": "https://www.sanmarinodetroit.org"},
    {"name": "Comunità Sammarinese di Troy", "city": "Troy", "country": "USA",
     "url": "https://www.sanmarinotroy.org"},
    {"name": "Associazione Sammarinese di New York", "city": "New York", "country": "USA",
     "url": "https://www.sanmarinony.org"},
    {"name": "Fratellanza Sammarinese di Pergamino", "city": "Pergamino", "country": "Argentina",
     "url": "https://www.sanmarinopergamino.org.ar"},
    {"name": "Comunità Sammarinese di Córdoba", "city": "Córdoba", "country": "Argentina",
     "url": "https://www.sanmarinocordoba.org.ar"},
    {"name": "Associazione Sammarinese di Buenos Aires", "city": "Buenos Aires", "country": "Argentina",
     "url": "https://www.sanmarinobuenosaires.org.ar"},
    {"name": "Comunità Sammarinese di San Nicolás", "city": "San Nicolás de los Arroyos", "country": "Argentina",
     "url": "https://www.sanmarinosannicolas.org.ar"},
    {"name": "Fratellanza Sammarinese di Viedma", "city": "Viedma", "country": "Argentina",
     "url": "https://www.sanmarinoviedma.org.ar"},
    {"name": "Comunità Sammarinese di Rosario", "city": "Rosario", "country": "Argentina",
     "url": "https://www.sanmarinorosario.org.ar"},
    {"name": "Associazione Sammarinese di Mar del Plata", "city": "Mar del Plata", "country": "Argentina",
     "url": "https://www.sanmarinomardelplata.org.ar"},
    {"name": "Communauté Saint-Marinaise de Paris", "city": "Paris", "country": "France",
     "url": "https://www.saintmarin-paris.fr"},
    {"name": "Comunità Sammarinese del Belgio", "city": "Brussels", "country": "Belgium",
     "url": "https://www.sanmarinobelgio.be"},
    {"name": "Comunidade São-Marinense de São Paulo", "city": "São Paulo", "country": "Brazil",
     "url": "https://www.sanmarinosp.org.br"},
    {"name": "Comunità Sammarinese di Washington", "city": "Washington D.C.", "country": "USA",
     "url": "https://www.sanmarinowashington.org"},
    {"name": "Associazione Sammarinese di Chicago", "city": "Chicago", "country": "USA",
     "url": "https://www.sanmarinochicago.org"},
]

# Regex patterns for extracting Italian/Spanish surnames from text
SURNAME_PATTERN = re.compile(
    r'\b([A-Z][a-zà-ú]{2,}(?:\s[A-Z][a-zà-ú]{2,})?)\b'
)

EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
)


class FratellanzeScraper:
    """Scrapes San Marino diaspora community websites for surnames and contacts."""

    def __init__(self, rate_limiter: Optional[RateLimiter] = None):
        self.rate_limiter = rate_limiter or RateLimiter(
            requests_per_minute=10, jitter_range=(1.0, 3.0)
        )

    async def scrape_all(self) -> tuple[List[RawArchiveRecord], List[CommunityContact]]:
        """Scrape all known fratellanza websites. Returns (records, contacts)."""
        all_records = []
        all_contacts = []

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            for community in COMMUNITIES:
                try:
                    records, contacts = await self._scrape_single(client, community)
                    all_records.extend(records)
                    all_contacts.extend(contacts)
                    logger.info(
                        f"[Fratellanze] {community['name']}: "
                        f"{len(records)} surnames, {len(contacts)} contacts"
                    )
                except Exception as e:
                    logger.warning(f"[Fratellanze] Failed to scrape {community['name']}: {e}")
                    continue

        return all_records, all_contacts

    async def _scrape_single(self, client: httpx.AsyncClient,
                             community: dict) -> tuple[List[RawArchiveRecord], List[CommunityContact]]:
        """Scrape a single fratellanza website."""
        async with self.rate_limiter:
            response = await client.get(
                community["url"],
                headers={"User-Agent": get_random_user_agent()}
            )

        if response.status_code != 200:
            logger.warning(f"HTTP {response.status_code} for {community['url']}")
            return [], []

        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        # Extract surnames from text content
        records = self._extract_surnames(text, community)

        # Extract contact information
        contacts = self._extract_contacts(soup, text, community)

        return records, contacts

    def _extract_surnames(self, text: str, community: dict) -> List[RawArchiveRecord]:
        """Extract potential surnames from page text."""
        records = []
        # Look for capitalized words that could be surnames
        matches = SURNAME_PATTERN.findall(text)

        # Deduplicate
        seen = set()
        for match in matches:
            surname = match.strip()
            if surname.lower() in seen or len(surname) < 3:
                continue
            seen.add(surname.lower())
            records.append(RawArchiveRecord(
                surname=surname,
                origin="San Marino",
                destination=f"{community['city']}, {community['country']}",
                source_url=community["url"],
            ))

        return records

    def _extract_contacts(self, soup: BeautifulSoup, text: str,
                          community: dict) -> List[CommunityContact]:
        """Extract email addresses and contact info from page."""
        contacts = []

        # Find email addresses
        emails = EMAIL_PATTERN.findall(text)
        for email in emails:
            contacts.append(CommunityContact(
                name=community["name"],
                email=email,
                city=community["city"],
                country=community["country"],
                fratellanza_name=community["name"],
            ))

        # Also check mailto: links
        for a_tag in soup.find_all("a", href=True):
            href = a_tag.get("href", "")
            if href.startswith("mailto:"):
                email = href.replace("mailto:", "").split("?")[0].strip()
                if email and email not in [c.email for c in contacts]:
                    contacts.append(CommunityContact(
                        name=community["name"],
                        email=email,
                        city=community["city"],
                        country=community["country"],
                        fratellanza_name=community["name"],
                    ))

        return contacts
