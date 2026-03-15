"""Centralised configuration loaded from .env and sensible defaults."""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = os.getenv("TITAN_DB_PATH", str(BASE_DIR / "titan_veritas.db"))

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_BASE = "https://v3.football.api-sports.io"
API_FOOTBALL_DAILY_LIMIT = int(os.getenv("API_FOOTBALL_DAILY_LIMIT", "100"))

# Argentine lower-division league IDs in API-Football
ARGENTINA_LEAGUES = {
    "Liga Profesional": 128,
    "Primera Nacional": 131,
    "Primera B Metropolitana": 132,
    "Torneo Federal A": 133,
    "Primera C": 134,
    "Primera D": 135,
    "Torneo Proyección": 521,
}

# Wikidata
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_BATCH_SIZE = 50

# BDFA
BDFA_BASE = "https://www.bdfa.com.ar"

# Rate limiting
DEFAULT_DELAY_MIN = 1.5
DEFAULT_DELAY_MAX = 4.5
MAX_CONCURRENT = 3

# San Marino endemic surnames — Tier 1 (highest confidence)
TIER1_SURNAMES: list[str] = [
    "Gualandi", "Terenzi", "Stacchini", "Belluzzi", "Cecchetti",
    "Bianchi", "Macina", "Gennari", "Taddei", "Rossi",
    "Stefanelli", "Ciavatta", "Bollini", "Albani", "Selva",
    "Ceccoli", "Gasperoni", "Guidi", "Biordi", "Santini",
    "Mularoni", "Zonzini", "Galassi", "Michelotti", "Berardi",
    "Valentini", "Zanotti", "Lonfernini",
]

# Tier 2 — high probability but also common in broader Italy
TIER2_SURNAMES: list[str] = [
    "Casali", "Moroni", "Fabbri", "Guerra", "Righi",
    "Renzi", "Mazza", "Benedettini", "Canti", "Gobbi",
    "Muccioli", "Montanari", "Matteoni", "Bonelli", "Crescentini",
    "Bacciocchi", "Giancecchi", "Podeschi", "Simoncini", "Zafferani",
]

ALL_SURNAMES = TIER1_SURNAMES + TIER2_SURNAMES

# Diaspora hubs with geographic weight
DIASPORA_HUBS: dict[str, int] = {
    "Argentina": 25,
    "United States": 20,
    "Brazil": 20,
    "France": 15,
    "Belgium": 15,
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 Safari/17.4",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Edg/124.0",
]
