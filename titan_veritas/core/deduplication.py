"""Deduplication engine using RapidFuzz for fuzzy name matching.

Merges candidate records that are likely the same person based on:
- Fuzzy name similarity (>85% via token_sort_ratio)
- Matching DOB, age, club, or BDFA/Wikidata IDs
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from rapidfuzz import fuzz

from titan_veritas.db.connection import Database

logger = logging.getLogger(__name__)

# Thresholds
NAME_SIMILARITY_THRESHOLD = 85  # token_sort_ratio
NAME_EXACT_THRESHOLD = 95       # near-exact match


@dataclass
class DuplicateGroup:
    """A group of candidate IDs believed to be the same person."""
    primary_id: int
    duplicate_ids: list[int]
    reason: str
    similarity: float


def find_duplicates(db: Database, include_filtered: bool = False) -> list[DuplicateGroup]:
    """Identify duplicate candidate groups using fuzzy matching.

    Strategy:
    1. Exact name match (trivial)
    2. Fuzzy name match within same surname
    3. Cross-match via shared BDFA/Wikidata/API-Football IDs
    """
    where = "" if include_filtered else "WHERE is_filtered_out = 0"
    rows = db.execute(
        f"SELECT id, first_name, last_name, date_of_birth, age, "
        f"current_club, wikidata_qid, bdfa_id, api_football_id, titan_score "
        f"FROM candidate {where} ORDER BY last_name, titan_score DESC"
    ).fetchall()

    candidates = [dict(r) for r in rows]
    groups: list[DuplicateGroup] = []
    merged_ids: set[int] = set()

    # Phase 1: Exact ID matches (BDFA, Wikidata, API-Football)
    id_fields = ["bdfa_id", "wikidata_qid", "api_football_id"]
    for field in id_fields:
        id_map: dict = {}
        for c in candidates:
            val = c.get(field)
            if val:
                if val in id_map:
                    id_map[val].append(c)
                else:
                    id_map[val] = [c]

        for val, group in id_map.items():
            if len(group) > 1:
                # Keep highest scoring
                group.sort(key=lambda x: x["titan_score"], reverse=True)
                primary = group[0]
                dupes = [g["id"] for g in group[1:] if g["id"] not in merged_ids]
                if dupes:
                    groups.append(DuplicateGroup(
                        primary_id=primary["id"],
                        duplicate_ids=dupes,
                        reason=f"Same {field}: {val}",
                        similarity=100.0,
                    ))
                    merged_ids.update(dupes)

    # Phase 2: Fuzzy name matching within same surname
    by_surname: dict[str, list[dict]] = {}
    for c in candidates:
        if c["id"] in merged_ids:
            continue
        key = c["last_name"].lower().strip()
        by_surname.setdefault(key, []).append(c)

    for surname, group in by_surname.items():
        if len(group) < 2:
            continue

        for i in range(len(group)):
            if group[i]["id"] in merged_ids:
                continue
            for j in range(i + 1, len(group)):
                if group[j]["id"] in merged_ids:
                    continue

                a, b = group[i], group[j]
                name_a = f"{a['first_name']} {a['last_name']}"
                name_b = f"{b['first_name']} {b['last_name']}"

                score = fuzz.token_sort_ratio(name_a, name_b)

                if score >= NAME_SIMILARITY_THRESHOLD:
                    # Additional confirmation checks
                    confirmed = score >= NAME_EXACT_THRESHOLD
                    if not confirmed:
                        # Check supporting evidence
                        if a["date_of_birth"] and a["date_of_birth"] == b["date_of_birth"]:
                            confirmed = True
                        elif a["age"] and b["age"] and abs((a["age"] or 0) - (b["age"] or 0)) <= 1:
                            confirmed = True
                        elif a["current_club"] and a["current_club"] == b["current_club"]:
                            confirmed = True

                    if confirmed:
                        # Keep the one with higher score (more data)
                        primary = a if a["titan_score"] >= b["titan_score"] else b
                        dupe = b if primary is a else a
                        groups.append(DuplicateGroup(
                            primary_id=primary["id"],
                            duplicate_ids=[dupe["id"]],
                            reason=f"Fuzzy match: '{name_a}' ~ '{name_b}'",
                            similarity=score,
                        ))
                        merged_ids.add(dupe["id"])

    return groups


def merge_duplicates(db: Database, groups: list[DuplicateGroup], dry_run: bool = False) -> int:
    """Merge duplicate groups by enriching primary and filtering duplicates.

    For each group:
    - Copy non-null fields from duplicates to primary (fill gaps)
    - Mark duplicates as filtered with reason 'duplicate_of:{primary_id}'
    """
    merged_count = 0

    # Fields that can be filled from duplicates
    fillable = [
        "date_of_birth", "age", "birth_place", "birth_country",
        "current_club", "current_league", "position", "career_start_year",
        "wikidata_qid", "bdfa_id", "api_football_id",
    ]

    for group in groups:
        if dry_run:
            merged_count += len(group.duplicate_ids)
            continue

        # Get primary record
        primary = db.execute(
            "SELECT * FROM candidate WHERE id = ?", (group.primary_id,)
        ).fetchone()
        if not primary:
            continue

        primary = dict(primary)

        # Merge fillable fields from duplicates
        for dupe_id in group.duplicate_ids:
            dupe = db.execute(
                "SELECT * FROM candidate WHERE id = ?", (dupe_id,)
            ).fetchone()
            if not dupe:
                continue
            dupe = dict(dupe)

            updates = []
            params = []
            for field in fillable:
                if not primary.get(field) and dupe.get(field):
                    updates.append(f"{field} = ?")
                    params.append(dupe[field])
                    primary[field] = dupe[field]  # Update local copy

            # Merge OSINT flags (OR logic)
            if dupe.get("cemla_hit") and not primary.get("cemla_hit"):
                updates.append("cemla_hit = 1")
            if dupe.get("ellis_island_hit") and not primary.get("ellis_island_hit"):
                updates.append("ellis_island_hit = 1")

            if updates:
                updates.append("updated_at = datetime('now')")
                sql = f"UPDATE candidate SET {', '.join(updates)} WHERE id = ?"
                params.append(group.primary_id)
                db.execute(sql, tuple(params))

            # Mark duplicate as filtered
            db.execute(
                "UPDATE candidate SET is_filtered_out = 1, "
                "filter_reason = ?, updated_at = datetime('now') WHERE id = ?",
                (f"duplicate_of:{group.primary_id}", dupe_id),
            )
            merged_count += 1

    if not dry_run:
        db.commit()

    return merged_count
