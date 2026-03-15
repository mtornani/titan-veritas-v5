"""Database schema DDL and seed data."""

from __future__ import annotations

from titan_veritas.config import TIER1_SURNAMES, TIER2_SURNAMES
from titan_veritas.db.connection import Database

DDL = """
CREATE TABLE IF NOT EXISTS surname (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT    NOT NULL UNIQUE,
    tier        INTEGER NOT NULL DEFAULT 3,
    incidence   INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS surname_variant (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    surname_id          INTEGER NOT NULL REFERENCES surname(id),
    variant             TEXT    NOT NULL,
    confidence          REAL    NOT NULL DEFAULT 0.0,
    method              TEXT,
    UNIQUE(surname_id, variant)
);

CREATE TABLE IF NOT EXISTS geographic_cluster (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    city                TEXT    NOT NULL,
    country             TEXT    NOT NULL,
    fratellanza_name    TEXT,
    contact_info        TEXT,
    UNIQUE(city, country)
);

CREATE TABLE IF NOT EXISTS candidate (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name          TEXT    NOT NULL,
    last_name           TEXT    NOT NULL,
    wikidata_qid        TEXT,
    bdfa_id             TEXT,
    api_football_id     INTEGER,
    date_of_birth       TEXT,
    age                 INTEGER,
    birth_place         TEXT,
    birth_country       TEXT,
    nationalities       TEXT    DEFAULT '[]',
    current_club        TEXT,
    current_league      TEXT,
    position            TEXT,
    career_start_year   INTEGER,
    titan_score         REAL    NOT NULL DEFAULT 0.0,
    tier                INTEGER NOT NULL DEFAULT 3,
    score_breakdown     TEXT    DEFAULT '{}',
    is_filtered_out     INTEGER NOT NULL DEFAULT 0,
    filter_reason       TEXT,
    cemla_hit           INTEGER NOT NULL DEFAULT 0,
    ellis_island_hit    INTEGER NOT NULL DEFAULT 0,
    osint_details       TEXT    DEFAULT '{}',
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(first_name, last_name, date_of_birth)
);

CREATE TABLE IF NOT EXISTS api_cache (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT    NOT NULL,
    key         TEXT    NOT NULL,
    payload     TEXT    NOT NULL,
    fetched_at  TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source, key)
);

CREATE INDEX IF NOT EXISTS idx_candidate_last_name ON candidate(last_name);
CREATE INDEX IF NOT EXISTS idx_candidate_score ON candidate(titan_score DESC);
CREATE INDEX IF NOT EXISTS idx_candidate_tier ON candidate(tier);
CREATE INDEX IF NOT EXISTS idx_surname_variant_sid ON surname_variant(surname_id);
CREATE INDEX IF NOT EXISTS idx_api_cache_lookup ON api_cache(source, key);
"""

# Tier-1 incidence data from Forebears/Geneanet
TIER1_INCIDENCE = {
    "Gualandi": 54, "Terenzi": 41, "Stacchini": 39, "Belluzzi": 38,
    "Cecchetti": 37, "Bianchi": 36, "Macina": 35, "Gennari": 33,
    "Taddei": 31, "Rossi": 30, "Stefanelli": 30, "Ciavatta": 28,
    "Bollini": 27, "Albani": 24, "Selva": 23, "Ceccoli": 22,
    "Gasperoni": 22, "Guidi": 22, "Biordi": 20, "Santini": 20,
    "Mularoni": 18, "Zonzini": 16, "Galassi": 15, "Michelotti": 14,
    "Berardi": 13, "Valentini": 12, "Zanotti": 11, "Lonfernini": 10,
}

CLUSTERS = [
    ("Detroit", "United States", "Fratellanza Sammarinese di Detroit"),
    ("New York", "United States", "San Marino Society of Greater NY"),
    ("Buenos Aires", "Argentina", "Associazione Sammarinese di Buenos Aires"),
    ("Pergamino", "Argentina", "Comunità Sammarinese di Pergamino"),
    ("Córdoba", "Argentina", "Comunità Sammarinese di Córdoba"),
    ("General Baldissera", "Argentina", "Fratellanza di General Baldissera"),
    ("Rosario", "Argentina", None),
    ("Jujuy", "Argentina", None),
    ("Patagonia", "Argentina", None),
    ("São Paulo", "Brazil", "Associazione Sammarinese del Brasile"),
    ("Paris", "France", "Association des Sammarinais de France"),
    ("Bruxelles", "Belgium", None),
    ("Lyon", "France", None),
    ("Rimini", "Italy", None),
    ("Bologna", "Italy", None),
]


def init_db(db: Database) -> None:
    """Create all tables and indexes."""
    db.conn.executescript(DDL)
    db.commit()


def seed_surnames(db: Database) -> int:
    """Insert tier-1 and tier-2 surnames. Returns count inserted."""
    count = 0
    for name in TIER1_SURNAMES:
        inc = TIER1_INCIDENCE.get(name, 0)
        try:
            db.execute(
                "INSERT OR IGNORE INTO surname (name, tier, incidence) VALUES (?, 1, ?)",
                (name, inc),
            )
            count += 1
        except Exception:
            pass
    for name in TIER2_SURNAMES:
        try:
            db.execute(
                "INSERT OR IGNORE INTO surname (name, tier, incidence) VALUES (?, 2, 0)",
                (name,),
            )
            count += 1
        except Exception:
            pass
    db.commit()
    return count


def seed_clusters(db: Database) -> int:
    """Insert geographic clusters. Returns count inserted."""
    count = 0
    for city, country, frat in CLUSTERS:
        try:
            db.execute(
                "INSERT OR IGNORE INTO geographic_cluster (city, country, fratellanza_name) "
                "VALUES (?, ?, ?)",
                (city, country, frat),
            )
            count += 1
        except Exception:
            pass
    db.commit()
    return count
