"""CRUD repositories for each domain entity."""

from __future__ import annotations

import json
from datetime import date
from typing import Optional

from titan_veritas.core.models import PlayerProfile
from titan_veritas.db.connection import Database


class SurnameRepo:
    def __init__(self, db: Database):
        self.db = db

    def get_all(self, tier: int | None = None) -> list[dict]:
        if tier:
            rows = self.db.execute("SELECT * FROM surname WHERE tier = ?", (tier,))
        else:
            rows = self.db.execute("SELECT * FROM surname ORDER BY tier, incidence DESC")
        return [dict(r) for r in rows.fetchall()]

    def get_by_name(self, name: str) -> dict | None:
        row = self.db.execute("SELECT * FROM surname WHERE name = ?", (name,)).fetchone()
        return dict(row) if row else None

    def add_variant(self, surname_id: int, variant: str, confidence: float, method: str):
        self.db.execute(
            "INSERT OR IGNORE INTO surname_variant (surname_id, variant, confidence, method) "
            "VALUES (?, ?, ?, ?)",
            (surname_id, variant, confidence, method),
        )
        self.db.commit()

    def get_variants(self, surname_id: int) -> list[dict]:
        rows = self.db.execute(
            "SELECT * FROM surname_variant WHERE surname_id = ? ORDER BY confidence DESC",
            (surname_id,),
        )
        return [dict(r) for r in rows.fetchall()]


class CandidateRepo:
    def __init__(self, db: Database):
        self.db = db

    def upsert(self, p: PlayerProfile) -> int:
        """Insert or update a candidate. Returns the row id."""
        dob_iso = p.date_of_birth.isoformat() if p.date_of_birth else None

        if dob_iso:
            # Exact match on name + DOB (strong identity)
            existing = self.db.execute(
                "SELECT id FROM candidate WHERE first_name = ? AND last_name = ? "
                "AND date_of_birth = ?",
                (p.first_name, p.last_name, dob_iso),
            ).fetchone()
            if not existing:
                # Also match records with NULL DOB (fill in missing DOB)
                existing = self.db.execute(
                    "SELECT id FROM candidate WHERE first_name = ? AND last_name = ? "
                    "AND date_of_birth IS NULL",
                    (p.first_name, p.last_name),
                ).fetchone()
        else:
            # No DOB: require additional context to match (birth_country or current_club)
            existing = None
            if p.birth_country:
                existing = self.db.execute(
                    "SELECT id FROM candidate WHERE first_name = ? AND last_name = ? "
                    "AND date_of_birth IS NULL AND birth_country = ?",
                    (p.first_name, p.last_name, p.birth_country),
                ).fetchone()
            if not existing and p.current_club:
                existing = self.db.execute(
                    "SELECT id FROM candidate WHERE first_name = ? AND last_name = ? "
                    "AND date_of_birth IS NULL AND current_club = ?",
                    (p.first_name, p.last_name, p.current_club),
                ).fetchone()
            if not existing:
                # Last resort: only match if there's exactly one record with this name
                rows = self.db.execute(
                    "SELECT id FROM candidate WHERE first_name = ? AND last_name = ? "
                    "AND date_of_birth IS NULL",
                    (p.first_name, p.last_name),
                ).fetchall()
                if len(rows) == 1:
                    existing = rows[0]

        dob_str = p.date_of_birth.isoformat() if p.date_of_birth else None
        nationalities_json = json.dumps(p.nationalities, ensure_ascii=False)
        breakdown_json = json.dumps(p.score_breakdown, ensure_ascii=False)
        osint_json = json.dumps(p.osint_details, ensure_ascii=False)

        if existing:
            self.db.execute(
                """UPDATE candidate SET
                    wikidata_qid=?, bdfa_id=?, api_football_id=?,
                    date_of_birth=?, age=?, birth_place=?, birth_country=?,
                    nationalities=?, current_club=?, current_league=?, position=?,
                    career_start_year=?, titan_score=?, tier=?, score_breakdown=?,
                    is_filtered_out=?, filter_reason=?,
                    cemla_hit=?, ellis_island_hit=?, osint_details=?,
                    updated_at=datetime('now')
                WHERE id=?""",
                (
                    p.wikidata_qid, p.bdfa_id, p.api_football_id,
                    dob_str, p.estimated_age, p.birth_place, p.birth_country,
                    nationalities_json, p.current_club, p.current_league, p.position,
                    p.career_start_year, p.titan_score, p.tier, breakdown_json,
                    int(p.is_filtered_out), p.filter_reason,
                    int(p.cemla_hit), int(p.ellis_island_hit), osint_json,
                    existing["id"],
                ),
            )
            self.db.commit()
            return existing["id"]

        cur = self.db.execute(
            """INSERT INTO candidate (
                first_name, last_name, wikidata_qid, bdfa_id, api_football_id,
                date_of_birth, age, birth_place, birth_country, nationalities,
                current_club, current_league, position, career_start_year,
                titan_score, tier, score_breakdown, is_filtered_out, filter_reason,
                cemla_hit, ellis_island_hit, osint_details
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                p.first_name, p.last_name, p.wikidata_qid, p.bdfa_id, p.api_football_id,
                dob_str, p.estimated_age, p.birth_place, p.birth_country,
                nationalities_json, p.current_club, p.current_league, p.position,
                p.career_start_year, p.titan_score, p.tier, breakdown_json,
                int(p.is_filtered_out), p.filter_reason,
                int(p.cemla_hit), int(p.ellis_island_hit), osint_json,
            ),
        )
        self.db.commit()
        return cur.lastrowid

    def get_all(self, include_filtered: bool = False) -> list[dict]:
        if include_filtered:
            rows = self.db.execute("SELECT * FROM candidate ORDER BY titan_score DESC")
        else:
            rows = self.db.execute(
                "SELECT * FROM candidate WHERE is_filtered_out = 0 ORDER BY titan_score DESC"
            )
        return [dict(r) for r in rows.fetchall()]

    def get_by_surname(self, surname: str) -> list[dict]:
        rows = self.db.execute(
            "SELECT * FROM candidate WHERE last_name = ? ORDER BY titan_score DESC",
            (surname,),
        )
        return [dict(r) for r in rows.fetchall()]

    def count(self, include_filtered: bool = False) -> int:
        if include_filtered:
            row = self.db.execute("SELECT COUNT(*) as c FROM candidate")
        else:
            row = self.db.execute("SELECT COUNT(*) as c FROM candidate WHERE is_filtered_out = 0")
        return row.fetchone()["c"]

    def stats(self) -> dict:
        total = self.count(include_filtered=True)
        active = self.count(include_filtered=False)
        with_dob = self.db.execute(
            "SELECT COUNT(*) as c FROM candidate WHERE date_of_birth IS NOT NULL"
        ).fetchone()["c"]
        with_club = self.db.execute(
            "SELECT COUNT(*) as c FROM candidate WHERE current_club IS NOT NULL"
        ).fetchone()["c"]
        avg_score = self.db.execute(
            "SELECT AVG(titan_score) as a FROM candidate WHERE is_filtered_out = 0"
        ).fetchone()["a"] or 0
        return {
            "total": total,
            "active": active,
            "filtered_out": total - active,
            "with_dob": with_dob,
            "with_club": with_club,
            "avg_score": round(avg_score, 1),
            "dob_coverage": f"{with_dob / total * 100:.1f}%" if total else "0%",
            "club_coverage": f"{with_club / total * 100:.1f}%" if total else "0%",
        }


    def import_from_seed(self, seed_path: str) -> int:
        """Import candidates from a JSON seed file (offline mode).

        Returns the number of records imported.
        """
        import json as _json
        from datetime import date as _date
        from pathlib import Path

        from titan_veritas.core.models import PlayerProfile
        from titan_veritas.core.scoring import score_player

        data = _json.loads(Path(seed_path).read_text(encoding="utf-8"))
        count = 0

        for rec in data:
            dob = None
            if rec.get("date_of_birth"):
                try:
                    dob = _date.fromisoformat(rec["date_of_birth"])
                except (ValueError, TypeError):
                    pass

            p = PlayerProfile(
                first_name=rec.get("first_name", ""),
                last_name=rec.get("last_name", ""),
                wikidata_qid=rec.get("wikidata_qid"),
                bdfa_id=rec.get("bdfa_id"),
                api_football_id=rec.get("api_football_id"),
                date_of_birth=dob,
                age=rec.get("age"),
                birth_place=rec.get("birth_place"),
                birth_country=rec.get("birth_country"),
                nationalities=rec.get("nationalities", []),
                current_club=rec.get("current_club"),
                current_league=rec.get("current_league"),
                position=rec.get("position"),
                career_start_year=rec.get("career_start_year"),
            )

            p = score_player(p)
            try:
                self.upsert(p)
                count += 1
            except Exception:
                pass

        return count


class CacheRepo:
    """API response cache to avoid wasting quota."""

    def __init__(self, db: Database):
        self.db = db

    def get(self, source: str, key: str) -> Optional[str]:
        row = self.db.execute(
            "SELECT payload FROM api_cache WHERE source = ? AND key = ?",
            (source, key),
        ).fetchone()
        return row["payload"] if row else None

    def put(self, source: str, key: str, payload: str):
        self.db.execute(
            "INSERT OR REPLACE INTO api_cache (source, key, payload, fetched_at) "
            "VALUES (?, ?, ?, datetime('now'))",
            (source, key, payload),
        )
        self.db.commit()

    def has(self, source: str, key: str) -> bool:
        row = self.db.execute(
            "SELECT 1 FROM api_cache WHERE source = ? AND key = ?",
            (source, key),
        ).fetchone()
        return row is not None
