"""VariantEngine: Orchestrator for the Rosetta Stone OSINT pipeline.

Scrapes all sources → generates phonetic variants → persists to DB.
Confidence = phonetic_match * 0.4 + fuzzy_ratio * 0.3 + geo_context * 0.3
"""

import logging
import sqlite3
from typing import List, Tuple, Optional

import jellyfish
from thefuzz import fuzz

from ..core.models import RawArchiveRecord, SurnameVariant
from ..core.rate_limiter import RATE_LIMITS
from ..db.repository import SurnameRepo, ClusterRepo
from .fratellanze import FratellanzeScraper
from .ellis_island import EllisIslandScraper
from .cemla import CEMLAScraper

logger = logging.getLogger(__name__)

# Cities that are known San Marino diaspora hubs (boost geo_context score)
HISTORICAL_HUBS = {
    "detroit", "troy", "new york", "chicago", "washington",
    "pergamino", "córdoba", "cordoba", "buenos aires", "rosario",
    "viedma", "san nicolás", "san nicolas", "mar del plata",
    "mendoza", "bahía blanca", "bahia blanca", "la plata",
    "paris", "lyon", "brussels", "são paulo", "sao paulo",
    "rio de janeiro", "montevideo", "zürich", "zurich", "munich",
}


class VariantEngine:
    """Orchestrates the full OSINT surname discovery pipeline."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.surname_repo = SurnameRepo(conn)
        self.cluster_repo = ClusterRepo(conn)

    async def run_full_discovery(self, sources: str = "all") -> dict:
        """Run the complete discovery pipeline. Returns stats dict."""
        stats = {"fratellanze_records": 0, "ellis_records": 0, "cemla_records": 0,
                 "variants_generated": 0, "contacts_found": 0}

        # Get all original surnames from DB for searching archives
        originals = self.surname_repo.get_all_originals()
        original_names = [o["name"] for o in originals]

        all_archive_records: List[RawArchiveRecord] = []

        # Scrape Fratellanze
        if sources in ("all", "fratellanze"):
            logger.info("[VariantEngine] Scraping Fratellanze...")
            scraper = FratellanzeScraper(RATE_LIMITS["fratellanze"])
            records, contacts = await scraper.scrape_all()
            all_archive_records.extend(records)
            stats["fratellanze_records"] = len(records)
            stats["contacts_found"] = len(contacts)

            # Persist contacts to cluster table
            for contact in contacts:
                self.cluster_repo.upsert(
                    city=contact.city, country=contact.country,
                    fratellanza_name=contact.fratellanza_name,
                    contact_email=contact.email,
                )

        # Scrape Ellis Island
        if sources in ("all", "ellis"):
            logger.info("[VariantEngine] Scraping Ellis Island archives...")
            scraper = EllisIslandScraper(RATE_LIMITS["archive"])
            records = await scraper.search_all_surnames(original_names[:20])  # Limit to avoid ban
            all_archive_records.extend(records)
            stats["ellis_records"] = len(records)

        # Scrape CEMLA
        if sources in ("all", "cemla"):
            logger.info("[VariantEngine] Scraping CEMLA archives...")
            scraper = CEMLAScraper(RATE_LIMITS["archive"])
            records = await scraper.search_all_surnames(original_names[:20])
            all_archive_records.extend(records)
            stats["cemla_records"] = len(records)

        # Generate and persist variants
        variants = self._expand_phonetically(all_archive_records, original_names)
        count = self._persist_variants(variants)
        stats["variants_generated"] = count

        logger.info(f"[VariantEngine] Discovery complete: {stats}")
        return stats

    def _expand_phonetically(self, records: List[RawArchiveRecord],
                             original_names: List[str]) -> List[SurnameVariant]:
        """Map discovered surnames to original SM surnames using phonetic matching."""
        variants = []
        seen = set()

        for record in records:
            discovered = record.surname.strip()
            if not discovered or discovered.lower() in seen:
                continue
            seen.add(discovered.lower())

            # Skip if it's already an exact match to an original
            if discovered.lower() in [o.lower() for o in original_names]:
                continue

            # Find the best matching original surname
            best_match = self._find_best_original_match(
                discovered, original_names, record.destination
            )
            if best_match:
                original, confidence, method = best_match
                variants.append(SurnameVariant(
                    original=original,
                    variant=discovered,
                    confidence=confidence,
                    method=method,
                    source=record.source_url or "archive",
                    source_url=record.source_url,
                ))

        return variants

    def _find_best_original_match(self, candidate: str, originals: List[str],
                                  destination: str = "") -> Optional[Tuple[str, float, str]]:
        """Find the best matching original surname for a candidate.

        Confidence = phonetic_match * 0.4 + fuzzy_ratio * 0.3 + geo_context * 0.3
        """
        best_score = 0.0
        best_original = None
        best_method = ""

        candidate_lower = candidate.lower()
        candidate_meta = jellyfish.metaphone(candidate_lower)
        candidate_soundex = jellyfish.soundex(candidate_lower)

        # Determine geo_context score
        geo_score = 0.0
        dest_lower = destination.lower()
        for hub in HISTORICAL_HUBS:
            if hub in dest_lower:
                geo_score = 100.0
                break
        if geo_score == 0.0 and dest_lower:
            geo_score = 30.0  # Known destination but not a hub

        for original in originals:
            original_lower = original.lower()

            # Phonetic match score (0-100)
            phonetic_score = 0.0
            original_meta = jellyfish.metaphone(original_lower)
            original_soundex = jellyfish.soundex(original_lower)

            if candidate_meta == original_meta:
                phonetic_score = 100.0
            elif candidate_soundex == original_soundex:
                phonetic_score = 80.0
            else:
                # Partial metaphone similarity via Levenshtein on the codes
                if candidate_meta and original_meta:
                    meta_dist = jellyfish.levenshtein_distance(candidate_meta, original_meta)
                    max_len = max(len(candidate_meta), len(original_meta))
                    if max_len > 0:
                        phonetic_score = max(0, (1 - meta_dist / max_len)) * 70

            # Fuzzy match score (0-100)
            fuzzy_score = fuzz.token_sort_ratio(candidate_lower, original_lower)

            # Combined confidence
            confidence = (
                phonetic_score * 0.4 +
                fuzzy_score * 0.3 +
                geo_score * 0.3
            )

            if confidence > best_score and confidence >= 50.0:
                best_score = confidence
                best_original = original
                if phonetic_score >= 80:
                    best_method = "phonetic"
                elif fuzzy_score >= 85:
                    best_method = "fuzzy"
                else:
                    best_method = "combined"

        if best_original:
            return best_original, round(best_score, 1), best_method
        return None

    def _persist_variants(self, variants: List[SurnameVariant]) -> int:
        """Persist discovered variants to the database. Returns count of new entries."""
        count = 0
        for v in variants:
            # Find the original_surname_id
            original = self.surname_repo.get_original_by_name(v.original)
            if not original:
                continue

            result = self.surname_repo.add_variant(
                original_surname_id=original["id"],
                variant=v.variant,
                confidence=v.confidence,
                method=v.method,
                source=v.source,
                source_url=v.source_url,
            )
            if result:
                count += 1

        return count

    def expand_from_archive_record(self, discovered_surname: str,
                                   destination: str = "") -> Optional[Tuple[str, float, str]]:
        """Given a surname found in an archive, find the closest original SM surname.

        This is the inverse of RosettaStone.is_likely_sammarinese() — given an archive
        record from Detroit with surname "Gasparony", determines this maps to original
        "Gasperoni" with confidence score.
        """
        originals = self.surname_repo.get_all_originals()
        original_names = [o["name"] for o in originals]
        return self._find_best_original_match(discovered_surname, original_names, destination)
