"""Export pipeline — generates JSON for React HUD, CSV, and summary stats."""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

from titan_veritas.db.connection import Database
from titan_veritas.db.repository import CandidateRepo

logger = logging.getLogger(__name__)


def export_json(db: Database, output_path: str | Path, include_filtered: bool = False) -> int:
    """Export candidates to JSON for React HUD consumption.

    Returns the number of records exported.
    """
    repo = CandidateRepo(db)
    candidates = repo.get_all(include_filtered=include_filtered)

    # Transform DB rows to HUD-friendly format
    records = []
    for c in candidates:
        record = {
            "id": c["id"],
            "first_name": c["first_name"],
            "last_name": c["last_name"],
            "full_name": f"{c['first_name']} {c['last_name']}",
            "age": c["age"],
            "date_of_birth": c["date_of_birth"],
            "birth_place": c["birth_place"],
            "birth_country": c["birth_country"],
            "nationalities": json.loads(c["nationalities"]) if c["nationalities"] else [],
            "current_club": c["current_club"],
            "current_league": c["current_league"],
            "position": c["position"],
            "titan_score": c["titan_score"],
            "tier": c["tier"],
            "score_breakdown": json.loads(c["score_breakdown"]) if c["score_breakdown"] else {},
            "cemla_hit": bool(c["cemla_hit"]),
            "ellis_island_hit": bool(c["ellis_island_hit"]),
            "wikidata_qid": c["wikidata_qid"],
            "wikidata_url": f"https://www.wikidata.org/wiki/{c['wikidata_qid']}" if c["wikidata_qid"] else None,
        }
        records.append(record)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(f"Exported {len(records)} candidates to {output}")
    return len(records)


def export_csv(db: Database, output_path: str | Path, include_filtered: bool = False) -> int:
    """Export candidates to CSV. Returns count exported."""
    repo = CandidateRepo(db)
    candidates = repo.get_all(include_filtered=include_filtered)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "last_name", "first_name", "age", "date_of_birth",
        "birth_country", "nationalities", "current_club", "current_league",
        "position", "titan_score", "tier", "cemla_hit", "ellis_island_hit",
        "wikidata_qid",
    ]

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for c in candidates:
            row = dict(c)
            row["nationalities"] = ", ".join(
                json.loads(c["nationalities"]) if c["nationalities"] else []
            )
            row["cemla_hit"] = "Yes" if c["cemla_hit"] else ""
            row["ellis_island_hit"] = "Yes" if c["ellis_island_hit"] else ""
            writer.writerow(row)

    logger.info(f"Exported {len(candidates)} candidates to CSV: {output}")
    return len(candidates)
