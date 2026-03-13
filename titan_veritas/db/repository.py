"""Repository classes for CRUD operations on the TITAN VERITAS database."""

import json
import hashlib
import sqlite3
from typing import List, Optional, Tuple


class SurnameRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get_all_originals(self, tier: Optional[int] = None) -> List[dict]:
        if tier:
            rows = self.conn.execute(
                "SELECT * FROM original_surname WHERE tier = ? ORDER BY name", (tier,)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM original_surname ORDER BY tier, name"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_original_by_name(self, name: str) -> Optional[dict]:
        row = self.conn.execute(
            "SELECT * FROM original_surname WHERE name = ? COLLATE NOCASE", (name,)
        ).fetchone()
        return dict(row) if row else None

    def add_original(self, name: str, tier: int) -> int:
        cursor = self.conn.execute(
            "INSERT OR IGNORE INTO original_surname (name, tier) VALUES (?, ?)",
            (name, tier),
        )
        self.conn.commit()
        return cursor.lastrowid

    def add_variant(self, original_surname_id: int, variant: str, confidence: float,
                    method: str, source: str = "", source_url: str = "") -> int:
        cursor = self.conn.execute(
            """INSERT OR IGNORE INTO surname_variant
               (original_surname_id, variant, confidence, method, source, source_url)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (original_surname_id, variant, confidence, method, source, source_url),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_variants(self, original_surname_id: int) -> List[dict]:
        rows = self.conn.execute(
            "SELECT * FROM surname_variant WHERE original_surname_id = ? ORDER BY confidence DESC",
            (original_surname_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_all_expanded_surnames(self) -> Tuple[List[str], List[str]]:
        """Return (tier1_names, tier2_names) including high-confidence variants."""
        tier1 = []
        tier2 = []
        rows = self.conn.execute(
            "SELECT name, tier FROM original_surname"
        ).fetchall()
        for r in rows:
            if r["tier"] == 1:
                tier1.append(r["name"].lower())
            else:
                tier2.append(r["name"].lower())

        # Add high-confidence variants (>= 75%) to their parent's tier
        variant_rows = self.conn.execute(
            """SELECT sv.variant, os.tier FROM surname_variant sv
               JOIN original_surname os ON sv.original_surname_id = os.id
               WHERE sv.confidence >= 75"""
        ).fetchall()
        for r in variant_rows:
            name = r["variant"].lower()
            if r["tier"] == 1:
                if name not in tier1:
                    tier1.append(name)
            else:
                if name not in tier2:
                    tier2.append(name)

        return tier1, tier2

    def get_stats(self) -> dict:
        originals = self.conn.execute("SELECT COUNT(*) as c FROM original_surname").fetchone()["c"]
        variants = self.conn.execute("SELECT COUNT(*) as c FROM surname_variant").fetchone()["c"]
        t1 = self.conn.execute("SELECT COUNT(*) as c FROM original_surname WHERE tier=1").fetchone()["c"]
        t2 = self.conn.execute("SELECT COUNT(*) as c FROM original_surname WHERE tier=2").fetchone()["c"]
        return {"originals": originals, "variants": variants, "tier1": t1, "tier2": t2}


class ClusterRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get_all(self) -> List[dict]:
        rows = self.conn.execute(
            "SELECT * FROM geographic_cluster ORDER BY country, city"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_by_country(self, country: str) -> List[dict]:
        rows = self.conn.execute(
            "SELECT * FROM geographic_cluster WHERE country = ? COLLATE NOCASE ORDER BY city",
            (country,),
        ).fetchall()
        return [dict(r) for r in rows]

    def update_contact(self, cluster_id: int, email: str = None, phone: str = None,
                       contact_name: str = None):
        updates = []
        params = []
        if email:
            updates.append("contact_email = ?")
            params.append(email)
        if phone:
            updates.append("contact_phone = ?")
            params.append(phone)
        if contact_name:
            updates.append("contact_name = ?")
            params.append(contact_name)
        if not updates:
            return
        params.append(cluster_id)
        self.conn.execute(
            f"UPDATE geographic_cluster SET {', '.join(updates)} WHERE id = ?", params
        )
        self.conn.commit()

    def upsert(self, city: str, country: str, region: str = None,
               fratellanza_name: str = None, **kwargs) -> int:
        existing = self.conn.execute(
            "SELECT id FROM geographic_cluster WHERE city = ? AND country = ? COLLATE NOCASE",
            (city, country),
        ).fetchone()
        if existing:
            return existing["id"]
        cursor = self.conn.execute(
            """INSERT INTO geographic_cluster (city, region, country, fratellanza_name,
               contact_email, contact_phone, contact_name, website_url)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (city, region, country, fratellanza_name,
             kwargs.get("contact_email"), kwargs.get("contact_phone"),
             kwargs.get("contact_name"), kwargs.get("website_url")),
        )
        self.conn.commit()
        return cursor.lastrowid


class CandidateRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert(self, first_name: str, last_name: str, source: str, **kwargs) -> int:
        existing = self.conn.execute(
            """SELECT id FROM candidate
               WHERE first_name = ? COLLATE NOCASE AND last_name = ? COLLATE NOCASE AND source = ?""",
            (first_name, last_name, source),
        ).fetchone()
        if existing:
            return existing["id"]

        nats = kwargs.get("nationalities", [])
        breakdown = kwargs.get("score_breakdown", [])
        cursor = self.conn.execute(
            """INSERT INTO candidate (first_name, last_name, known_as, birth_date, age,
               birth_city, birth_country, nationalities, current_club, current_league,
               source, source_url, titan_score, tier, score_breakdown,
               is_lethal_filtered, filter_reason, surname_variant_id, cluster_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (first_name, last_name, kwargs.get("known_as"), kwargs.get("birth_date"),
             kwargs.get("age"), kwargs.get("birth_city"), kwargs.get("birth_country"),
             json.dumps(nats) if isinstance(nats, list) else nats,
             kwargs.get("current_club"), kwargs.get("current_league"),
             source, kwargs.get("source_url", ""),
             kwargs.get("titan_score", 0), kwargs.get("tier"),
             json.dumps(breakdown) if isinstance(breakdown, list) else breakdown,
             1 if kwargs.get("is_lethal_filtered") else 0,
             kwargs.get("filter_reason"),
             kwargs.get("surname_variant_id"), kwargs.get("cluster_id")),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_top_scored(self, limit: int = 50) -> List[dict]:
        rows = self.conn.execute(
            """SELECT * FROM candidate WHERE is_lethal_filtered = 0
               ORDER BY titan_score DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_by_source(self, source: str) -> List[dict]:
        rows = self.conn.execute(
            "SELECT * FROM candidate WHERE source = ? ORDER BY titan_score DESC",
            (source,),
        ).fetchall()
        return [dict(r) for r in rows]


class OutreachRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create_draft(self, target_email: str, subject: str, body: str,
                     cluster_id: int = None, target_name: str = None,
                     candidate_id: int = None) -> int:
        body_hash = hashlib.sha256(body.encode()).hexdigest()
        # Check for duplicate
        existing = self.conn.execute(
            "SELECT id FROM outreach_log WHERE email_body_hash = ? AND target_email = ?",
            (body_hash, target_email),
        ).fetchone()
        if existing:
            return existing["id"]

        cursor = self.conn.execute(
            """INSERT INTO outreach_log (candidate_id, cluster_id, target_email, target_name,
               email_subject, email_body_hash, status)
               VALUES (?, ?, ?, ?, ?, ?, 'DRAFT')""",
            (candidate_id, cluster_id, target_email, target_name, subject, body_hash),
        )
        self.conn.commit()
        return cursor.lastrowid

    def mark_sent(self, outreach_id: int, gmail_message_id: str, gmail_thread_id: str):
        self.conn.execute(
            """UPDATE outreach_log SET status='SENT', sent_at=datetime('now'),
               gmail_message_id=?, gmail_thread_id=?, updated_at=datetime('now')
               WHERE id=?""",
            (gmail_message_id, gmail_thread_id, outreach_id),
        )
        self.conn.commit()

    def mark_replied(self, outreach_id: int):
        self.conn.execute(
            """UPDATE outreach_log SET status='REPLIED', replied_at=datetime('now'),
               updated_at=datetime('now') WHERE id=?""",
            (outreach_id,),
        )
        self.conn.commit()

    def mark_validated(self, outreach_id: int, llm_extraction: str, confidence: float):
        self.conn.execute(
            """UPDATE outreach_log SET status='VALIDATED', llm_extraction=?,
               llm_confidence=?, updated_at=datetime('now') WHERE id=?""",
            (llm_extraction, confidence, outreach_id),
        )
        self.conn.commit()

    def mark_status(self, outreach_id: int, status: str, notes: str = None):
        self.conn.execute(
            """UPDATE outreach_log SET status=?, notes=?, updated_at=datetime('now')
               WHERE id=?""",
            (status, notes, outreach_id),
        )
        self.conn.commit()

    def get_pending_threads(self) -> List[dict]:
        """Get all outreach records awaiting reply (SENT or DELIVERED)."""
        rows = self.conn.execute(
            """SELECT * FROM outreach_log WHERE status IN ('SENT', 'DELIVERED')
               AND gmail_thread_id IS NOT NULL ORDER BY sent_at""",
        ).fetchall()
        return [dict(r) for r in rows]

    def get_stats(self) -> dict:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as c FROM outreach_log GROUP BY status"
        ).fetchall()
        return {r["status"]: r["c"] for r in rows}
