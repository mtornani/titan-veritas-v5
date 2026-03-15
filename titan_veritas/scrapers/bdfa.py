"""BDFA (Base de Datos del Fútbol Argentino) scraper.

Uses semantic DOM selectors (not absolute/index-based) with BeautifulSoup.
Uses pandas.read_html() for club history tables to avoid fragile HTML parsing.
"""

from __future__ import annotations

import logging
import random
import re
import time
from datetime import date
from typing import Optional

import httpx
import pandas as pd
from bs4 import BeautifulSoup

from titan_veritas.config import (
    BDFA_BASE,
    USER_AGENTS,
    DEFAULT_DELAY_MIN,
    DEFAULT_DELAY_MAX,
)
from titan_veritas.core.models import PlayerProfile

logger = logging.getLogger(__name__)


def _get_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.5",
    }


def _polite_delay():
    time.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))


def search_players(surname: str, client: httpx.Client | None = None) -> list[dict]:
    """Search BDFA for players matching a surname. Returns list of {name, url, bdfa_id}."""
    close_client = False
    if client is None:
        client = httpx.Client(timeout=20, follow_redirects=True)
        close_client = True

    results = []
    try:
        search_url = f"{BDFA_BASE}/buscar.asp"
        resp = client.get(
            search_url,
            params={"q": surname, "tipo": "jugadores"},
            headers=_get_headers(),
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Find player links — look for anchors pointing to player profiles
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "jugador" in href.lower() or "player" in href.lower():
                name = link.get_text(strip=True)
                if surname.lower() in name.lower():
                    # Extract numeric ID from URL
                    id_match = re.search(r"(\d+)", href)
                    bdfa_id = id_match.group(1) if id_match else None
                    full_url = href if href.startswith("http") else f"{BDFA_BASE}/{href.lstrip('/')}"
                    results.append({
                        "name": name,
                        "url": full_url,
                        "bdfa_id": bdfa_id,
                    })
        logger.info(f"BDFA search: {len(results)} results for '{surname}'")
    except httpx.HTTPStatusError as e:
        logger.warning(f"BDFA search HTTP error: {e.response.status_code}")
    except Exception as e:
        logger.warning(f"BDFA search error: {e}")
    finally:
        if close_client:
            client.close()

    return results


def _extract_dob_from_soup(soup: BeautifulSoup) -> Optional[date]:
    """Extract date of birth using semantic selectors, not positional."""
    # Strategy 1: Look for 'data-stat' attributes
    dob_cell = soup.find("td", {"data-stat": "birth_date"})
    if dob_cell:
        text = dob_cell.get_text(strip=True)
        return _parse_date_text(text)

    # Strategy 2: Find text patterns like "Nacimiento:" or "Fecha de nacimiento"
    for label_text in ("nacimiento", "fecha de nacimiento", "born", "birth"):
        label_el = soup.find(string=re.compile(label_text, re.IGNORECASE))
        if label_el:
            # Navigate to the sibling/parent that contains the date
            parent = label_el.find_parent()
            if parent:
                text = parent.get_text(strip=True)
                parsed = _parse_date_text(text)
                if parsed:
                    return parsed

    # Strategy 3: Search for date patterns in the header/bio section
    bio_section = soup.find("div", class_=re.compile(r"(bio|info|header|perfil)", re.I))
    if bio_section:
        text = bio_section.get_text()
        parsed = _parse_date_text(text)
        if parsed:
            return parsed

    return None


def _parse_date_text(text: str) -> Optional[date]:
    """Parse various date formats from text."""
    # dd/mm/yyyy or dd-mm-yyyy
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass
    # yyyy-mm-dd (ISO)
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def _extract_career_start(soup: BeautifulSoup) -> Optional[int]:
    """Extract the earliest year from career stats to estimate debut year."""
    # Look for 4-digit years in stat tables
    years = []
    for cell in soup.find_all(["td", "th"]):
        text = cell.get_text(strip=True)
        m = re.match(r"^(19|20)\d{2}$", text)
        if m:
            years.append(int(text))
    return min(years) if years else None


def _extract_club_from_tables(html: str) -> Optional[str]:
    """Use pandas.read_html to extract the most recent club from stat tables.

    This avoids fragile manual <tr>/<td> traversal — pandas handles
    inconsistent headers, missing cells, and encoding issues natively.
    """
    try:
        tables = pd.read_html(html, flavor="lxml")
    except ValueError:
        # No tables found
        return None
    except Exception as e:
        logger.debug(f"pandas.read_html error: {e}")
        return None

    for df in tables:
        # Normalise column names
        cols_lower = [str(c).lower().strip() for c in df.columns]

        # Look for a column named 'club', 'equipo', 'team'
        club_col = None
        for i, col_name in enumerate(cols_lower):
            if col_name in ("club", "equipo", "team"):
                club_col = df.columns[i]
                break

        if club_col is not None:
            # Drop NaN rows and get the last non-empty entry (most recent)
            series = df[club_col].dropna()
            if not series.empty:
                # Last row = most recent season (BDFA lists chronologically)
                club = str(series.iloc[-1]).strip()
                if club and club.lower() not in ("", "-", "nan"):
                    return club

    return None


def scrape_profile(url: str, client: httpx.Client | None = None) -> dict:
    """Scrape a BDFA player profile page. Returns metadata dict."""
    close_client = False
    if client is None:
        client = httpx.Client(timeout=20, follow_redirects=True)
        close_client = True

    result: dict = {"url": url}
    try:
        resp = client.get(url, headers=_get_headers())
        resp.raise_for_status()
        html = resp.text
        soup = BeautifulSoup(html, "lxml")

        # Extract date of birth (semantic selectors)
        dob = _extract_dob_from_soup(soup)
        if dob:
            result["date_of_birth"] = dob

        # Extract career start year
        career_start = _extract_career_start(soup)
        if career_start:
            result["career_start_year"] = career_start

        # Extract current club via pandas.read_html
        club = _extract_club_from_tables(html)
        if club:
            result["current_club"] = club

        # Extract position
        for label_text in ("posición", "puesto", "position"):
            el = soup.find(string=re.compile(label_text, re.IGNORECASE))
            if el:
                parent = el.find_parent()
                if parent:
                    text = parent.get_text(strip=True)
                    # Remove the label itself
                    pos = re.sub(rf"(?i){label_text}\s*:?\s*", "", text).strip()
                    if pos:
                        result["position"] = pos
                        break

    except httpx.HTTPStatusError as e:
        logger.warning(f"BDFA profile HTTP error for {url}: {e.response.status_code}")
    except Exception as e:
        logger.warning(f"BDFA profile error for {url}: {e}")
    finally:
        if close_client:
            client.close()

    return result


def search_and_scrape(surname: str) -> list[PlayerProfile]:
    """Full BDFA pipeline: search → scrape each profile → build PlayerProfiles."""
    client = httpx.Client(timeout=20, follow_redirects=True)
    players = []

    try:
        search_results = search_players(surname, client)

        for entry in search_results:
            _polite_delay()
            profile_data = scrape_profile(entry["url"], client)

            name_parts = entry["name"].rsplit(" ", 1)
            first_name = name_parts[0] if len(name_parts) > 1 else ""
            last_name = name_parts[-1]

            player = PlayerProfile(
                first_name=first_name,
                last_name=last_name,
                bdfa_id=entry.get("bdfa_id"),
                date_of_birth=profile_data.get("date_of_birth"),
                current_club=profile_data.get("current_club"),
                position=profile_data.get("position"),
                career_start_year=profile_data.get("career_start_year"),
                birth_country="Argentina",  # BDFA is exclusively Argentine football
            )
            players.append(player)

        logger.info(f"BDFA: scraped {len(players)} profiles for surname '{surname}'")
    finally:
        client.close()

    return players
