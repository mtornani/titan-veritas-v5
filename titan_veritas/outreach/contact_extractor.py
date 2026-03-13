"""ContactExtractor: Scrapes fratellanza websites for director emails/phones."""

import re
import logging
import sqlite3
from typing import List

import httpx
from bs4 import BeautifulSoup

from ..core.models import CommunityContact
from ..core.rate_limiter import RateLimiter, get_random_user_agent, RATE_LIMITS
from ..db.repository import ClusterRepo

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
PHONE_RE = re.compile(r'[\+]?[\d\s\-\(\)]{7,20}')


class ContactExtractor:
    """Extracts contact information from diaspora community websites and directories."""

    def __init__(self, conn: sqlite3.Connection,
                 rate_limiter: RateLimiter = None):
        self.conn = conn
        self.cluster_repo = ClusterRepo(conn)
        self.rate_limiter = rate_limiter or RATE_LIMITS["fratellanze"]

    async def extract_from_all_clusters(self) -> List[CommunityContact]:
        """Scrape contacts from all known clusters that have website URLs."""
        clusters = self.cluster_repo.get_all()
        contacts = []

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            for cluster in clusters:
                url = cluster.get("website_url")
                if not url:
                    continue

                try:
                    extracted = await self._extract_from_url(client, cluster)
                    contacts.extend(extracted)

                    # Persist first extracted email to cluster record
                    if extracted:
                        self.cluster_repo.update_contact(
                            cluster_id=cluster["id"],
                            email=extracted[0].email,
                            contact_name=extracted[0].name if extracted[0].name != cluster.get("fratellanza_name") else None,
                        )
                except Exception as e:
                    logger.warning(f"[ContactExtractor] Error for cluster {cluster['id']}: {e}")

        logger.info(f"[ContactExtractor] Extracted {len(contacts)} contacts total")
        return contacts

    async def extract_from_cluster(self, cluster_id: int) -> List[CommunityContact]:
        """Extract contacts for a specific cluster."""
        clusters = self.cluster_repo.get_all()
        cluster = next((c for c in clusters if c["id"] == cluster_id), None)
        if not cluster or not cluster.get("website_url"):
            return []

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            return await self._extract_from_url(client, cluster)

    async def _extract_from_url(self, client: httpx.AsyncClient,
                                cluster: dict) -> List[CommunityContact]:
        """Fetch a URL and extract contact info."""
        url = cluster["website_url"]
        contacts = []

        async with self.rate_limiter:
            response = await client.get(
                url, headers={"User-Agent": get_random_user_agent()}
            )

        if response.status_code != 200:
            return contacts

        soup = BeautifulSoup(response.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)

        # Extract emails
        emails = EMAIL_RE.findall(text)
        # Also check mailto: links
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("mailto:"):
                email = href.replace("mailto:", "").split("?")[0].strip()
                if email and email not in emails:
                    emails.append(email)

        # Extract phone numbers from tel: links
        phones = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("tel:"):
                phone = href.replace("tel:", "").strip()
                phones.append(phone)

        # Also try phone regex on text
        if not phones:
            phone_matches = PHONE_RE.findall(text)
            phones = [p.strip() for p in phone_matches if len(p.strip()) >= 8]

        # Try to find contact person name (look near "presidente", "referente", etc.)
        contact_name = self._find_contact_name(soup)

        for email in emails:
            contacts.append(CommunityContact(
                name=contact_name or cluster.get("fratellanza_name", ""),
                email=email,
                city=cluster["city"],
                country=cluster["country"],
                fratellanza_name=cluster.get("fratellanza_name", ""),
                cluster_id=cluster["id"],
            ))

        return contacts

    def _find_contact_name(self, soup: BeautifulSoup) -> str:
        """Try to extract a contact person's name near leadership keywords."""
        keywords = ["presidente", "referente", "segretario", "contact",
                     "director", "presidente", "responsabile"]

        for keyword in keywords:
            # Search in surrounding text
            elements = soup.find_all(string=re.compile(keyword, re.IGNORECASE))
            for el in elements:
                parent = el.parent
                if parent:
                    text = parent.get_text(strip=True)
                    # Look for a capitalized name near the keyword
                    name_match = re.search(
                        r'(?:' + keyword + r')[:\s]+([A-Z][a-zà-ú]+ [A-Z][a-zà-ú]+)',
                        text, re.IGNORECASE
                    )
                    if name_match:
                        return name_match.group(1)

        return ""
