"""Wikidata scraper — 2-phase approach to avoid SPARQL timeouts.

Phase 1: Lightweight SPARQL query to resolve surnames → QIDs (no OPTIONAL blocks).
Phase 2: REST API wbgetentities in batches of 50 to extract P569, P19, P54, P27.
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Optional

import httpx

from titan_veritas.config import (
    WIKIDATA_SPARQL,
    WIKIDATA_API,
    WIKIDATA_BATCH_SIZE,
    USER_AGENTS,
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


def _sparql_headers() -> dict:
    import random
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/sparql-results+json",
    }


def _rest_headers() -> dict:
    import random
    return {"User-Agent": random.choice(USER_AGENTS)}


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


def resolve_qids(surname: str, client: httpx.Client | None = None) -> list[dict]:
    """Get QIDs for footballers matching a surname. Returns [{qid, label}, ...]."""
    query = SPARQL_TEMPLATE.format(surname_lower=surname.lower())
    close_client = False
    if client is None:
        client = httpx.Client(timeout=30)
        close_client = True

    try:
        resp = client.get(
            WIKIDATA_SPARQL,
            params={"query": query, "format": "json"},
            headers=_sparql_headers(),
        )
        resp.raise_for_status()
        data = resp.json()
        results = []
        seen = set()
        for binding in data.get("results", {}).get("bindings", []):
            uri = binding.get("item", {}).get("value", "")
            qid = uri.split("/")[-1] if "/" in uri else ""
            label = binding.get("itemLabel", {}).get("value", "")
            if qid and qid not in seen:
                seen.add(qid)
                results.append({"qid": qid, "label": label})
        logger.info(f"SPARQL resolved {len(results)} QIDs for surname '{surname}'")
        return results
    except httpx.HTTPStatusError as e:
        logger.warning(f"SPARQL error for '{surname}': {e.response.status_code}")
        return []
    except Exception as e:
        logger.warning(f"SPARQL exception for '{surname}': {e}")
        return []
    finally:
        if close_client:
            client.close()


# ─── PHASE 2: REST API wbgetentities in batches of 50 ─────────────────────

def _extract_time_value(claim: dict) -> Optional[str]:
    """Extract time string like '+1990-05-12T00:00:00Z' → '1990-05-12'."""
    try:
        tv = claim["mainsnak"]["datavalue"]["value"]["time"]
        # Format: +YYYY-MM-DDT00:00:00Z
        return tv.lstrip("+").split("T")[0]
    except (KeyError, TypeError, IndexError):
        return None


def _extract_entity_id(claim: dict) -> Optional[str]:
    """Extract a QID reference from a claim."""
    try:
        return claim["mainsnak"]["datavalue"]["value"]["id"]
    except (KeyError, TypeError):
        return None


def enrich_batch(
    qids: list[str],
    client: httpx.Client | None = None,
) -> dict[str, dict]:
    """Fetch entity data for up to 50 QIDs. Returns {qid: {dob, birthplace_qid, team_qid, nationality_qids}}."""
    if not qids:
        return {}
    close_client = False
    if client is None:
        client = httpx.Client(timeout=30)
        close_client = True

    results = {}
    try:
        resp = client.get(
            WIKIDATA_API,
            params={
                "action": "wbgetentities",
                "ids": "|".join(qids[:WIKIDATA_BATCH_SIZE]),
                "props": "claims",
                "format": "json",
            },
            headers=_rest_headers(),
        )
        resp.raise_for_status()
        entities = resp.json().get("entities", {})

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
                # Take the last team entry (usually most recent)
                info["team_qid"] = _extract_entity_id(teams[-1])

            # P27 — citizenship
            if P_CITIZENSHIP in claims:
                info["nationality_qids"] = [
                    _extract_entity_id(c) for c in claims[P_CITIZENSHIP]
                    if _extract_entity_id(c)
                ]

            results[qid] = info
    except Exception as e:
        logger.warning(f"wbgetentities error: {e}")
    finally:
        if close_client:
            client.close()

    return results


def resolve_labels(qids: list[str], client: httpx.Client | None = None) -> dict[str, str]:
    """Resolve a list of QIDs to their human-readable labels."""
    if not qids:
        return {}
    close_client = False
    if client is None:
        client = httpx.Client(timeout=30)
        close_client = True

    labels = {}
    try:
        # Process in batches of 50
        for i in range(0, len(qids), WIKIDATA_BATCH_SIZE):
            batch = qids[i : i + WIKIDATA_BATCH_SIZE]
            resp = client.get(
                WIKIDATA_API,
                params={
                    "action": "wbgetentities",
                    "ids": "|".join(batch),
                    "props": "labels",
                    "languages": "it|en|es",
                    "format": "json",
                },
                headers=_rest_headers(),
            )
            resp.raise_for_status()
            entities = resp.json().get("entities", {})
            for qid, ent in entities.items():
                for lang in ("it", "en", "es"):
                    if lang in ent.get("labels", {}):
                        labels[qid] = ent["labels"][lang]["value"]
                        break
    except Exception as e:
        logger.warning(f"Label resolution error: {e}")
    finally:
        if close_client:
            client.close()

    return labels


def search_surname(surname: str) -> list[PlayerProfile]:
    """Full pipeline: resolve QIDs → enrich → build PlayerProfile list."""
    import random
    import time

    players = []
    client = httpx.Client(timeout=30)

    try:
        # Phase 1: QID resolution
        qid_entries = resolve_qids(surname, client)
        if not qid_entries:
            return []

        all_qids = [e["qid"] for e in qid_entries]
        label_map = {e["qid"]: e["label"] for e in qid_entries}

        # Phase 2: enrich in batches
        all_enriched = {}
        for i in range(0, len(all_qids), WIKIDATA_BATCH_SIZE):
            batch = all_qids[i : i + WIKIDATA_BATCH_SIZE]
            enriched = enrich_batch(batch, client)
            all_enriched.update(enriched)
            # Polite delay between batches
            time.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))

        # Collect QIDs that need label resolution (birthplaces, teams, nationalities)
        qids_to_resolve = set()
        for info in all_enriched.values():
            if "birthplace_qid" in info and info["birthplace_qid"]:
                qids_to_resolve.add(info["birthplace_qid"])
            if "team_qid" in info and info["team_qid"]:
                qids_to_resolve.add(info["team_qid"])
            for nqid in info.get("nationality_qids", []):
                if nqid:
                    qids_to_resolve.add(nqid)

        # Resolve all labels
        resolved_labels = resolve_labels(list(qids_to_resolve), client)

        # Build PlayerProfile objects
        for qid, info in all_enriched.items():
            label = label_map.get(qid, "")
            parts = label.rsplit(" ", 1) if " " in label else ("", label)
            first_name = parts[0] if len(parts) > 1 else ""
            last_name = parts[-1]

            # Only keep players whose last name matches the target surname
            if surname.lower() not in last_name.lower():
                continue

            dob = None
            if "date_of_birth" in info and info["date_of_birth"]:
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

        logger.info(f"Wikidata: found {len(players)} players for surname '{surname}'")
    finally:
        client.close()

    return players
