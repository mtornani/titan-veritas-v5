"""Wikidata scraper — 2-phase approach to avoid SPARQL timeouts.

Phase 1: Lightweight SPARQL query to resolve surnames → QIDs (no OPTIONAL blocks).
Phase 2: REST API wbgetentities in batches of 50 to extract P569, P19, P54, P27.

Anti-ban strategy:
  - StealthyFetcher (headless Chromium with TLS fingerprint spoofing)
  - Bypasses Cloudflare / Wikidata IP blocks without proxy
  - Fallback: Fetcher with curl_cffi TLS impersonation
"""

from __future__ import annotations

import json as _json
import logging
import random
import time
from datetime import date
from typing import Optional

from scrapling import StealthyFetcher, Fetcher

from titan_veritas.config import (
    WIKIDATA_SPARQL,
    WIKIDATA_API,
    WIKIDATA_BATCH_SIZE,
    DEFAULT_DELAY_MIN,
    DEFAULT_DELAY_MAX,
)
from titan_veritas.core.models import PlayerProfile

logger = logging.getLogger(__name__)

# Properties we extract from entity JSON
P_DATE_OF_BIRTH = "P569"
P_PLACE_OF_BIRTH = "P19"
P_MEMBER_OF_TEAM = "P54"
P_CITIZENSHIP = "P27"


# ─── HTTP layer: StealthyFetcher (browser) → Fetcher (curl_cffi) ──────────

def _stealth_get_json(url: str, params: dict | None = None) -> dict:
    """GET JSON via StealthyFetcher — headless Chromium with fingerprint spoofing.

    This bypasses Cloudflare and Wikidata's TLS-based IP blocking.
    """
    if params:
        from urllib.parse import urlencode
        url = url + "?" + urlencode(params)

    page = StealthyFetcher.fetch(
        url,
        headless=True,
        network_idle=True,
        timeout=30000,
    )
    # The page body contains the JSON response
    text = page.text
    if not text:
        text = page.get_all_text()
    return _json.loads(text)


def _fetcher_get_json(url: str, params: dict | None = None) -> dict:
    """Fallback: GET JSON via Fetcher (curl_cffi TLS impersonation, no browser)."""
    resp = Fetcher.get(
        url,
        params=params,
        impersonate="chrome",
        stealthy_headers=True,
        follow_redirects=True,
        timeout=30,
    )
    return resp.json()


def _get_json(url: str, params: dict | None = None) -> dict:
    """Try Fetcher (curl_cffi) first — fast, no browser. Fall back to StealthyFetcher."""
    # Attempt 1: Fetcher (curl_cffi TLS impersonation — fast, no browser)
    try:
        data = _fetcher_get_json(url, params)
        logger.debug("Fetcher succeeded for %s", url.split("?")[0])
        return data
    except Exception as e:
        logger.info("Fetcher failed: %s — trying StealthyFetcher", e)

    # Attempt 2: StealthyFetcher (full headless browser — slower but strongest stealth)
    try:
        data = _stealth_get_json(url, params)
        logger.debug("StealthyFetcher succeeded for %s", url.split("?")[0])
        return data
    except Exception as e:
        logger.warning("Both fetchers failed for %s: %s", url.split("?")[0], e)
        raise


# ─── PHASE 1: Lightweight SPARQL for QID resolution ───────────────────────

SPARQL_TEMPLATE = """
SELECT ?item ?itemLabel WHERE {{
  ?item wdt:P106 wd:Q937857 .
  ?item rdfs:label ?itemLabel .
  FILTER(LANG(?itemLabel) = "it" || LANG(?itemLabel) = "en" || LANG(?itemLabel) = "es")
  FILTER(CONTAINS(LCASE(?itemLabel), "{surname_lower}"))
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "it,en,es" }}
}}
LIMIT 200
"""


def _parse_sparql_bindings(data: dict) -> list[dict]:
    """Extract QID/label pairs from SPARQL JSON response."""
    results = []
    seen = set()
    for binding in data.get("results", {}).get("bindings", []):
        uri = binding.get("item", {}).get("value", "")
        qid = uri.split("/")[-1] if "/" in uri else ""
        label = binding.get("itemLabel", {}).get("value", "")
        if qid and qid not in seen:
            seen.add(qid)
            results.append({"qid": qid, "label": label})
    return results


def resolve_qids(surname: str) -> list[dict]:
    """Get QIDs for footballers matching a surname. Returns [{qid, label}, ...]."""
    query = SPARQL_TEMPLATE.format(surname_lower=surname.lower())
    try:
        data = _get_json(WIKIDATA_SPARQL, {"query": query, "format": "json"})
        results = _parse_sparql_bindings(data)
        logger.info("SPARQL resolved %d QIDs for surname '%s'", len(results), surname)
        return results
    except Exception as e:
        logger.warning("SPARQL failed for '%s': %s", surname, e)
        return []


# ─── PHASE 2: REST API wbgetentities in batches of 50 ─────────────────────

def _extract_time_value(claim: dict) -> Optional[str]:
    """Extract time string like '+1990-05-12T00:00:00Z' → '1990-05-12'."""
    try:
        tv = claim["mainsnak"]["datavalue"]["value"]["time"]
        return tv.lstrip("+").split("T")[0]
    except (KeyError, TypeError, IndexError):
        return None


def _extract_entity_id(claim: dict) -> Optional[str]:
    """Extract a QID reference from a claim."""
    try:
        return claim["mainsnak"]["datavalue"]["value"]["id"]
    except (KeyError, TypeError):
        return None


def _fetch_entities(qids: list[str], props: str = "claims") -> dict:
    """Fetch Wikidata entities via stealth HTTP."""
    params = {
        "action": "wbgetentities",
        "ids": "|".join(qids[:WIKIDATA_BATCH_SIZE]),
        "props": props,
        "format": "json",
    }
    data = _get_json(WIKIDATA_API, params)
    return data.get("entities", {})


def enrich_batch(qids: list[str]) -> dict[str, dict]:
    """Fetch entity data for up to 50 QIDs. Returns {qid: {dob, birthplace_qid, ...}}."""
    if not qids:
        return {}

    results = {}
    try:
        entities = _fetch_entities(qids, "claims")

        for qid, entity in entities.items():
            claims = entity.get("claims", {})
            info: dict = {"qid": qid}

            # P569 — date of birth
            if P_DATE_OF_BIRTH in claims:
                info["date_of_birth"] = _extract_time_value(claims[P_DATE_OF_BIRTH][0])

            # P19 — place of birth (returns QID, needs label resolution)
            if P_PLACE_OF_BIRTH in claims:
                info["birthplace_qid"] = _extract_entity_id(claims[P_PLACE_OF_BIRTH][0])

            # P54 — member of sports team (most recent = last in list)
            if P_MEMBER_OF_TEAM in claims:
                teams = claims[P_MEMBER_OF_TEAM]
                info["team_qid"] = _extract_entity_id(teams[-1])

            # P27 — citizenship
            if P_CITIZENSHIP in claims:
                info["nationality_qids"] = [
                    _extract_entity_id(c) for c in claims[P_CITIZENSHIP]
                    if _extract_entity_id(c)
                ]

            results[qid] = info
    except Exception as e:
        logger.warning("wbgetentities error: %s", e)

    return results


def resolve_labels(qids: list[str]) -> dict[str, str]:
    """Resolve a list of QIDs to their human-readable labels."""
    if not qids:
        return {}

    labels = {}
    try:
        for i in range(0, len(qids), WIKIDATA_BATCH_SIZE):
            batch = qids[i : i + WIKIDATA_BATCH_SIZE]
            params = {
                "action": "wbgetentities",
                "ids": "|".join(batch),
                "props": "labels",
                "languages": "it|en|es",
                "format": "json",
            }
            entities = _get_json(WIKIDATA_API, params).get("entities", {})
            for qid, ent in entities.items():
                for lang in ("it", "en", "es"):
                    if lang in ent.get("labels", {}):
                        labels[qid] = ent["labels"][lang]["value"]
                        break
    except Exception as e:
        logger.warning("Label resolution error: %s", e)

    return labels


def search_surname(surname: str) -> list[PlayerProfile]:
    """Full pipeline: resolve QIDs → enrich → build PlayerProfile list."""
    players = []

    # Phase 1: QID resolution
    qid_entries = resolve_qids(surname)
    if not qid_entries:
        return []

    all_qids = [e["qid"] for e in qid_entries]
    label_map = {e["qid"]: e["label"] for e in qid_entries}

    # Phase 2: enrich in batches
    all_enriched = {}
    for i in range(0, len(all_qids), WIKIDATA_BATCH_SIZE):
        batch = all_qids[i : i + WIKIDATA_BATCH_SIZE]
        enriched = enrich_batch(batch)
        all_enriched.update(enriched)
        # Polite delay between batches
        time.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))

    # Collect QIDs that need label resolution
    qids_to_resolve = set()
    for info in all_enriched.values():
        if info.get("birthplace_qid"):
            qids_to_resolve.add(info["birthplace_qid"])
        if info.get("team_qid"):
            qids_to_resolve.add(info["team_qid"])
        for nqid in info.get("nationality_qids", []):
            if nqid:
                qids_to_resolve.add(nqid)

    resolved_labels = resolve_labels(list(qids_to_resolve))

    # Build PlayerProfile objects
    for qid, info in all_enriched.items():
        label = label_map.get(qid, "")
        parts = label.rsplit(" ", 1) if " " in label else ("", label)
        first_name = parts[0] if len(parts) > 1 else ""
        last_name = parts[-1]

        if surname.lower() not in last_name.lower():
            continue

        dob = None
        if info.get("date_of_birth"):
            try:
                dob = date.fromisoformat(info["date_of_birth"])
            except ValueError:
                pass

        birth_place = resolved_labels.get(info.get("birthplace_qid", ""))
        current_club = resolved_labels.get(info.get("team_qid", ""))
        nationalities = [
            resolved_labels.get(nq, "") for nq in info.get("nationality_qids", [])
            if resolved_labels.get(nq)
        ]

        player = PlayerProfile(
            first_name=first_name,
            last_name=last_name,
            wikidata_qid=qid,
            date_of_birth=dob,
            birth_place=birth_place,
            nationalities=nationalities,
            current_club=current_club,
        )
        players.append(player)

    logger.info("Wikidata: found %d players for surname '%s'", len(players), surname)
    return players
