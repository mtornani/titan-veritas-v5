"""BDFA (Base de Datos del Futbol Argentino) scraper.

Uses Scrapling's Fetcher with curl_cffi TLS impersonation for HTTP.
Uses Scrapling's adaptive DOM parser (replaces BeautifulSoup).
Uses pandas.read_html() for club history tables.

Note: BDFA serves pages as ISO-8859-1. We decode raw bytes manually
and build a Scrapling Adaptor from the decoded HTML string.
"""

from __future__ import annotations

import logging
import random
import re
import time
from datetime import date
from io import StringIO
from typing import Optional

import pandas as pd
from scrapling import Fetcher
from scrapling.parser import Adaptor

from titan_veritas.config import (
    BDFA_BASE,
    DEFAULT_DELAY_MIN,
    DEFAULT_DELAY_MAX,
)
from titan_veritas.core.models import PlayerProfile

logger = logging.getLogger(__name__)


def _polite_delay():
    time.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))


def _fetch_page(url: str, params: dict | None = None) -> tuple:
    """Fetch a page via Fetcher. Returns (Adaptor, html_str) handling Latin-1 encoding."""
    resp = Fetcher.get(
        url,
        params=params,
        impersonate="chrome",
        stealthy_headers=True,
        follow_redirects=True,
        timeout=20,
    )
    raw = resp.body
    if isinstance(raw, bytes):
        html = raw.decode("latin-1", errors="replace")
    else:
        html = str(raw)
    doc = Adaptor(html, auto_match=False)
    return doc, html


def search_players(surname: str) -> list[dict]:
    """Search BDFA for players matching a surname. Returns list of {name, url, bdfa_id}."""
    results = []
    try:
        search_url = f"{BDFA_BASE}/buscar.asp"
        doc, _ = _fetch_page(search_url, params={"q": surname, "tipo": "jugadores"})

        # Use Scrapling's CSS selector to find player links
        for link in doc.css("a[href]"):
            href = link.attrib.get("href", "")
            if "jugador" in href.lower() or "player" in href.lower():
                name = link.text.strip() if link.text else ""
                if surname.lower() in name.lower():
                    id_match = re.search(r"(\d+)", href)
                    bdfa_id = id_match.group(1) if id_match else None
                    full_url = href if href.startswith("http") else f"{BDFA_BASE}/{href.lstrip('/')}"
                    results.append({
                        "name": name,
                        "url": full_url,
                        "bdfa_id": bdfa_id,
                    })
        logger.info("BDFA search: %d results for '%s'", len(results), surname)
    except Exception as e:
        logger.warning("BDFA search error: %s", e)

    return results


def _extract_dob_from_page(doc, html: str) -> Optional[date]:
    """Extract date of birth using Scrapling's adaptive selectors."""
    # Strategy 1: Look for 'data-stat' attributes
    dob_cell = doc.css("td[data-stat='birth_date']")
    if dob_cell:
        text = dob_cell[0].text.strip() if dob_cell[0].text else ""
        parsed = _parse_date_text(text)
        if parsed:
            return parsed

    # Strategy 2: Find text containing birth-related keywords
    for label_text in ("nacimiento", "fecha de nacimiento", "born", "birth"):
        matches = doc.find_by_text(label_text, first_match=False)
        if matches:
            for match in (matches if isinstance(matches, list) else [matches]):
                parent = match.parent
                if parent:
                    text = parent.get_all_text() if hasattr(parent, 'get_all_text') else (parent.text or "")
                    parsed = _parse_date_text(text)
                    if parsed:
                        return parsed

    # Strategy 3: Search for date patterns in bio/header sections
    for selector in ("div[class*='bio']", "div[class*='info']", "div[class*='header']", "div[class*='perfil']"):
        sections = doc.css(selector)
        if sections:
            text = sections[0].get_all_text() if hasattr(sections[0], 'get_all_text') else (sections[0].text or "")
            parsed = _parse_date_text(text)
            if parsed:
                return parsed

    # Strategy 4: Brute-force — scan entire HTML for date patterns
    parsed = _parse_date_text(html)
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


def _extract_career_start(doc) -> Optional[int]:
    """Extract the earliest year from career stats."""
    years = []
    for cell in doc.css("td, th"):
        text = (cell.text or "").strip()
        m = re.match(r"^(19|20)\d{2}$", text)
        if m:
            years.append(int(text))
    return min(years) if years else None


def _extract_club_from_tables(html: str) -> Optional[str]:
    """Use pandas.read_html to extract the most recent club from stat tables."""
    try:
        tables = pd.read_html(StringIO(html), flavor="lxml")
    except ValueError:
        return None
    except Exception as e:
        logger.debug("pandas.read_html error: %s", e)
        return None

    for df in tables:
        cols_lower = [str(c).lower().strip() for c in df.columns]
        club_col = None
        for i, col_name in enumerate(cols_lower):
            if col_name in ("club", "equipo", "team"):
                club_col = df.columns[i]
                break

        if club_col is not None:
            series = df[club_col].dropna()
            if not series.empty:
                club = str(series.iloc[-1]).strip()
                if club and club.lower() not in ("", "-", "nan"):
                    return club

    return None


def scrape_profile(url: str) -> dict:
    """Scrape a BDFA player profile page. Returns metadata dict."""
    result: dict = {"url": url}
    try:
        doc, html = _fetch_page(url)

        # Extract date of birth (adaptive selectors)
        dob = _extract_dob_from_page(doc, html)
        if dob:
            result["date_of_birth"] = dob

        # Extract career start year
        career_start = _extract_career_start(doc)
        if career_start:
            result["career_start_year"] = career_start

        # Extract current club via pandas.read_html
        if html:
            club = _extract_club_from_tables(html)
            if club:
                result["current_club"] = club

        # Extract position
        for label_text in ("posicion", "puesto", "position"):
            matches = doc.find_by_text(label_text, first_match=True)
            if matches:
                el = matches if not isinstance(matches, list) else matches[0]
                parent = el.parent
                if parent:
                    text = parent.get_all_text() if hasattr(parent, 'get_all_text') else (parent.text or "")
                    pos = re.sub(rf"(?i){label_text}\s*:?\s*", "", text).strip()
                    if pos:
                        result["position"] = pos
                        break

    except Exception as e:
        logger.warning("BDFA profile error for %s: %s", url, e)

    return result


def search_and_scrape(surname: str) -> list[PlayerProfile]:
    """Full BDFA pipeline: search → scrape each profile → build PlayerProfiles."""
    players = []

    search_results = search_players(surname)

    for entry in search_results:
        _polite_delay()
        profile_data = scrape_profile(entry["url"])

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
            birth_country="Argentina",
        )
        players.append(player)

    logger.info("BDFA: scraped %d profiles for surname '%s'", len(players), surname)
    return players
