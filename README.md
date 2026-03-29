# TITAN VERITAS v6.3

San Marino Diaspora Football Intelligence — OSINT system to identify footballers
of Sammarinese descent eligible for the national team via jus sanguinis.

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env  # add your API-Football key
python titan.py init-db
python titan.py search --surname Gasperoni
python titan.py score
python titan.py export
```

## Commands

| Command | Description |
|---------|-------------|
| `init-db` | Create DB schema, seed surnames |
| `search` | Discovery pipeline (Wikidata + BDFA + API-Football) |
| `enrich` | OSINT enrichment (CEMLA + Ellis Island + FamilySearch + Cognomix) |
| `bdfa-enrich` | Scrape BDFA profiles for missing data |
| `dedupe` | Fuzzy deduplication |
| `tier3-cutoff` | Filter low-value Tier 3 candidates |
| `score` | Recalculate TITAN scores |
| `export` | Export to JSON (HUD) and CSV |
| `stats` | Print DB statistics |

## HUD

Live dashboard: https://mtornani.github.io/titan-veritas-v5/

Built with React 18 + Vite. To rebuild:

```bash
cd titan-hud && npm install && npm run build && npm run deploy
```

## Data Sources

- **Wikidata** — SPARQL + REST API for player identification
- **BDFA** (bdfa.com.ar) — Argentine football database
- **API-Football** (v3, free tier 100 req/day) — club/league verification
- **CEMLA** — Argentine immigration archives (static fallback, CAPTCHA-blocked)
- **Ellis Island** — US immigration records (static fallback, SPA-blocked)
- **FamilySearch** — Genealogical records for SM emigration verification
- **Cognomix** — Italian surname geographic distribution
