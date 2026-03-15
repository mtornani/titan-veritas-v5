"""TITAN VERITAS v6 — Main orchestration CLI.

Commands:
    titan.py init-db       Initialise SQLite DB and seed surnames + clusters
    titan.py search        Run full pipeline: Wikidata + BDFA + API-Football
    titan.py enrich        Enrich existing candidates with OSINT (CEMLA + Ellis Island)
    titan.py score         Recalculate TITAN scores for all candidates
    titan.py export        Export to JSON (React HUD) and CSV
    titan.py stats         Show database statistics
"""

from __future__ import annotations

import json
import logging
import sys
import time

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from tqdm import tqdm

from titan_veritas.config import (
    ALL_SURNAMES,
    TIER1_SURNAMES,
    TIER2_SURNAMES,
    ARGENTINA_LEAGUES,
)
from titan_veritas.db.connection import Database
from titan_veritas.db.schema import init_db, seed_surnames, seed_clusters
from titan_veritas.db.repository import CandidateRepo, CacheRepo, SurnameRepo
from titan_veritas.core.models import PlayerProfile
from titan_veritas.core.scoring import score_player
from titan_veritas.export.exporter import export_json, export_csv

console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("titan")


# ─── CLI Group ─────────────────────────────────────────────────────────────

@click.group()
@click.option("--db", default=None, help="Path to SQLite database")
@click.pass_context
def cli(ctx, db):
    """TITAN VERITAS v6 — San Marino Diaspora Football Intelligence."""
    ctx.ensure_object(dict)
    ctx.obj["db"] = Database.get_instance(db)


# ─── init-db ───────────────────────────────────────────────────────────────

@cli.command("init-db")
@click.option("--seed/--no-seed", default=True, help="Seed surnames and clusters")
@click.option("--offline", is_flag=True, default=False, help="Import candidates from seed JSON (no internet needed)")
@click.option("--seed-file", default="data/seed_candidates.json", help="Path to seed JSON")
@click.pass_context
def cmd_init_db(ctx, seed, offline, seed_file):
    """Initialise the database schema and optionally seed reference data."""
    db = ctx.obj["db"]
    init_db(db)
    console.print("[green]✓[/] Database schema created")

    if seed:
        n_surnames = seed_surnames(db)
        n_clusters = seed_clusters(db)
        console.print(f"[green]✓[/] Seeded {n_surnames} surnames, {n_clusters} geographic clusters")

    if offline:
        import os
        if not os.path.exists(seed_file):
            console.print(f"[red]✗[/] Seed file not found: {seed_file}")
            console.print("  Run 'titan.py seed-export' first to generate it from a populated DB")
            return
        repo = CandidateRepo(db)
        n = repo.import_from_seed(seed_file)
        console.print(f"[green]✓[/] Imported {n} candidates from offline seed")


# ─── search ────────────────────────────────────────────────────────────────

@cli.command("search")
@click.option("--surnames", "-s", default=None, help="Comma-separated surnames (default: all)")
@click.option("--source", type=click.Choice(["all", "wikidata", "bdfa", "api-football"]), default="all")
@click.option("--tier", type=int, default=None, help="Only search tier N surnames (1 or 2)")
@click.pass_context
def cmd_search(ctx, surnames, source, tier):
    """Search for candidates across data sources."""
    db = ctx.obj["db"]
    repo = CandidateRepo(db)

    # Determine which surnames to search
    if surnames:
        name_list = [s.strip() for s in surnames.split(",")]
    elif tier == 1:
        name_list = TIER1_SURNAMES
    elif tier == 2:
        name_list = TIER2_SURNAMES
    else:
        name_list = ALL_SURNAMES

    console.print(f"[bold]Searching {len(name_list)} surnames via: {source}[/]")
    total_found = 0

    for surname in tqdm(name_list, desc="Surnames"):
        players: list[PlayerProfile] = []

        # ── Wikidata ──
        if source in ("all", "wikidata"):
            try:
                from titan_veritas.scrapers.wikidata import search_surname
                wp = search_surname(surname)
                players.extend(wp)
                logger.info(f"Wikidata: {len(wp)} for '{surname}'")
            except Exception as e:
                logger.warning(f"Wikidata error for '{surname}': {e}")

        # ── BDFA ──
        if source in ("all", "bdfa"):
            try:
                from titan_veritas.scrapers.bdfa import search_and_scrape
                bp = search_and_scrape(surname)
                players.extend(bp)
                logger.info(f"BDFA: {len(bp)} for '{surname}'")
            except Exception as e:
                logger.warning(f"BDFA error for '{surname}': {e}")

        # ── API-Football ──
        if source in ("all", "api-football"):
            try:
                from titan_veritas.scrapers.api_football import APIFootballClient
                afc = APIFootballClient(db)
                ap = afc.search_players_by_surname(surname)
                players.extend(ap)
                afc.close()
                logger.info(f"API-Football: {len(ap)} for '{surname}'")
            except Exception as e:
                logger.warning(f"API-Football error for '{surname}': {e}")

        # Score and persist
        for p in players:
            p = score_player(p)
            try:
                repo.upsert(p)
                total_found += 1
            except Exception as e:
                logger.debug(f"Upsert error for {p.full_name}: {e}")

    console.print(f"\n[green]✓[/] Search complete: {total_found} candidates found/updated")
    _print_quick_stats(repo)


# ─── enrich ────────────────────────────────────────────────────────────────

@cli.command("enrich")
@click.option("--tier", type=int, default=1, help="Enrich candidates with tier N surnames first")
@click.pass_context
def cmd_enrich(ctx, tier):
    """Run OSINT enrichment (CEMLA + Ellis Island) on existing candidates."""
    db = ctx.obj["db"]
    repo = CandidateRepo(db)

    # Get surnames to check — prioritize by tier
    surname_repo = SurnameRepo(db)
    target_surnames = [s["name"] for s in surname_repo.get_all(tier=tier)]

    if not target_surnames:
        target_surnames = TIER1_SURNAMES

    console.print(f"[bold]OSINT enrichment for {len(target_surnames)} Tier-{tier} surnames[/]")

    # ── CEMLA ──
    console.print("\n[cyan]Phase 1: CEMLA (Argentine immigration archives)[/]")
    try:
        from titan_veritas.osint.cemla import search_surnames_sync
        cemla_results = search_surnames_sync(target_surnames)
        cemla_map = {}
        for r in cemla_results:
            if r.has_san_marino_connection:
                cemla_map[r.surname.lower()] = {
                    "total_hits": r.total_hits,
                    "sm_hits": r.san_marino_hits,
                    "records": len(r.records),
                }
                console.print(f"  [green]✓[/] {r.surname}: {r.san_marino_hits} SM connections")
            elif r.total_hits > 0:
                console.print(f"  [yellow]○[/] {r.surname}: {r.total_hits} records (no SM)")
        console.print(f"  CEMLA: {len(cemla_map)} surnames with San Marino connections")
    except Exception as e:
        logger.warning(f"CEMLA enrichment error: {e}")
        cemla_map = {}

    # ── Ellis Island ──
    console.print("\n[cyan]Phase 2: Ellis Island (US immigration records)[/]")
    try:
        from titan_veritas.osint.ellis_island import search_surnames_sync
        ellis_results = search_surnames_sync(target_surnames)
        ellis_map = {}
        for r in ellis_results:
            if r.has_san_marino_connection:
                ellis_map[r.surname.lower()] = {
                    "total_hits": r.total_hits,
                    "sm_hits": r.san_marino_hits,
                    "search_url": r.search_url,
                }
                console.print(f"  [green]✓[/] {r.surname}: {r.san_marino_hits} SM connections")
            elif r.total_hits > 0:
                console.print(f"  [yellow]○[/] {r.surname}: {r.total_hits} records (no SM)")
        console.print(f"  Ellis Island: {len(ellis_map)} surnames with San Marino connections")
    except Exception as e:
        logger.warning(f"Ellis Island enrichment error: {e}")
        ellis_map = {}

    # ── Update candidates in DB ──
    console.print("\n[cyan]Phase 3: Updating candidate records[/]")
    candidates = repo.get_all(include_filtered=True)
    updated = 0

    for c in candidates:
        last_lower = c["last_name"].lower()
        changed = False

        if last_lower in cemla_map:
            db.execute(
                "UPDATE candidate SET cemla_hit = 1, updated_at = datetime('now') WHERE id = ?",
                (c["id"],),
            )
            changed = True

        if last_lower in ellis_map:
            db.execute(
                "UPDATE candidate SET ellis_island_hit = 1, updated_at = datetime('now') WHERE id = ?",
                (c["id"],),
            )
            changed = True

        if changed:
            updated += 1

    db.commit()
    console.print(f"[green]✓[/] Updated {updated} candidates with OSINT data")
    console.print("[dim]Run 'titan.py score' to recalculate scores with OSINT multipliers[/]")


# ─── score ─────────────────────────────────────────────────────────────────

@cli.command("score")
@click.pass_context
def cmd_score(ctx):
    """Recalculate TITAN scores for all candidates."""
    db = ctx.obj["db"]
    repo = CandidateRepo(db)
    candidates = repo.get_all(include_filtered=True)

    console.print(f"[bold]Recalculating scores for {len(candidates)} candidates[/]")

    for c in tqdm(candidates, desc="Scoring"):
        p = PlayerProfile(
            first_name=c["first_name"],
            last_name=c["last_name"],
            wikidata_qid=c["wikidata_qid"],
            bdfa_id=c["bdfa_id"],
            api_football_id=c["api_football_id"],
            date_of_birth=None,  # Will parse below
            age=c["age"],
            birth_place=c["birth_place"],
            birth_country=c["birth_country"],
            nationalities=json.loads(c["nationalities"]) if c["nationalities"] else [],
            current_club=c["current_club"],
            current_league=c["current_league"],
            position=c["position"],
            career_start_year=c["career_start_year"],
            cemla_hit=bool(c["cemla_hit"]),
            ellis_island_hit=bool(c["ellis_island_hit"]),
        )

        # Parse DOB
        if c["date_of_birth"]:
            try:
                from datetime import date
                p.date_of_birth = date.fromisoformat(c["date_of_birth"])
            except (ValueError, TypeError):
                pass

        p = score_player(p)
        repo.upsert(p)

    console.print("[green]✓[/] Scoring complete")
    _print_quick_stats(repo)


# ─── export ────────────────────────────────────────────────────────────────

@cli.command("export")
@click.option("--json-out", default="titan-hud/src/data.json", help="JSON output path")
@click.option("--csv-out", default="data/candidates.csv", help="CSV output path")
@click.pass_context
def cmd_export(ctx, json_out, csv_out):
    """Export candidates to JSON (React HUD) and CSV."""
    db = ctx.obj["db"]
    n_json = export_json(db, json_out)
    n_csv = export_csv(db, csv_out)
    console.print(f"[green]✓[/] Exported {n_json} candidates to JSON: {json_out}")
    console.print(f"[green]✓[/] Exported {n_csv} candidates to CSV: {csv_out}")


# ─── seed-export ───────────────────────────────────────────────────────────

@cli.command("seed-export")
@click.option("--output", "-o", default="data/seed_candidates.json", help="Output seed file path")
@click.pass_context
def cmd_seed_export(ctx, output):
    """Export current DB candidates to a seed JSON for offline use."""
    db = ctx.obj["db"]
    repo = CandidateRepo(db)
    candidates = repo.get_all(include_filtered=False)

    records = []
    for c in candidates:
        rec = {
            "first_name": c["first_name"],
            "last_name": c["last_name"],
            "date_of_birth": c["date_of_birth"],
            "age": c["age"],
            "birth_place": c["birth_place"],
            "birth_country": c["birth_country"],
            "nationalities": json.loads(c["nationalities"]) if c["nationalities"] else [],
            "current_club": c["current_club"],
            "current_league": c["current_league"],
            "position": c["position"],
            "career_start_year": c["career_start_year"],
            "wikidata_qid": c["wikidata_qid"],
            "bdfa_id": c["bdfa_id"],
            "api_football_id": c["api_football_id"],
        }
        records.append(rec)

    from pathlib import Path
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    console.print(f"[green]✓[/] Seed exported: {len(records)} candidates → {output}")
    console.print("  Use 'titan.py init-db --offline' to import on any machine")


# ─── stats ─────────────────────────────────────────────────────────────────

@cli.command("stats")
@click.pass_context
def cmd_stats(ctx):
    """Display database statistics."""
    db = ctx.obj["db"]
    repo = CandidateRepo(db)
    _print_quick_stats(repo)

    # Score distribution
    console.print("\n[bold]Score Distribution:[/]")
    for threshold in [90, 80, 70, 60, 50, 40, 30]:
        count = db.execute(
            "SELECT COUNT(*) as c FROM candidate WHERE titan_score >= ? AND is_filtered_out = 0",
            (threshold,),
        ).fetchone()["c"]
        console.print(f"  ≥ {threshold}: {count}")

    # Top 10 candidates
    console.print("\n[bold]Top 10 Candidates:[/]")
    table = Table()
    table.add_column("Name", style="cyan")
    table.add_column("Score", style="green")
    table.add_column("Tier")
    table.add_column("Age")
    table.add_column("Club")
    table.add_column("Country")
    table.add_column("OSINT")

    top = db.execute(
        "SELECT * FROM candidate WHERE is_filtered_out = 0 ORDER BY titan_score DESC LIMIT 10"
    ).fetchall()
    for c in top:
        osint = []
        if c["cemla_hit"]:
            osint.append("CEMLA")
        if c["ellis_island_hit"]:
            osint.append("Ellis")
        table.add_row(
            f"{c['first_name']} {c['last_name']}",
            str(c["titan_score"]),
            str(c["tier"]),
            str(c["age"] or "?"),
            c["current_club"] or "?",
            c["birth_country"] or "?",
            ", ".join(osint) if osint else "-",
        )
    console.print(table)


# ─── Helper ────────────────────────────────────────────────────────────────

def _print_quick_stats(repo: CandidateRepo):
    stats = repo.stats()
    console.print(f"\n[bold]Database Stats:[/]")
    console.print(f"  Total candidates: {stats['total']}")
    console.print(f"  Active (not filtered): {stats['active']}")
    console.print(f"  Filtered out: {stats['filtered_out']}")
    console.print(f"  DOB coverage: {stats['dob_coverage']}")
    console.print(f"  Club coverage: {stats['club_coverage']}")
    console.print(f"  Average score: {stats['avg_score']}")


# ─── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli()
