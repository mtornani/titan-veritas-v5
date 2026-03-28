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
    console.print("[green]OK[/] Database schema created")

    if seed:
        n_surnames = seed_surnames(db)
        n_clusters = seed_clusters(db)
        console.print(f"[green]OK[/] Seeded {n_surnames} surnames, {n_clusters} geographic clusters")

    if offline:
        import os
        if not os.path.exists(seed_file):
            console.print(f"[red]FAIL[/] Seed file not found: {seed_file}")
            console.print("  Run 'titan.py seed-export' first to generate it from a populated DB")
            return
        repo = CandidateRepo(db)
        n = repo.import_from_seed(seed_file)
        console.print(f"[green]OK[/] Imported {n} candidates from offline seed")


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

    console.print(f"\n[green]OK[/] Search complete: {total_found} candidates found/updated")
    _print_quick_stats(repo)


# ─── enrich ────────────────────────────────────────────────────────────────

@cli.command("enrich")
@click.option("--tier", type=int, default=None, help="Enrich candidates with tier N surnames (default: all)")
@click.option("--try-live/--static-only", default=True, help="Try live scraping (default) or static-only fallback")
@click.pass_context
def cmd_enrich(ctx, tier, try_live):
    """Run OSINT enrichment (CEMLA + Ellis Island) on existing candidates.

    Default: live scraping via StealthyFetcher (CEMLA) / Fetcher (Ellis Island).
    Falls back to static OSINT (known SM emigrant surname database) on failure.
    Pass --static-only to skip live scraping entirely.
    """
    db = ctx.obj["db"]
    repo = CandidateRepo(db)

    # Get unique surnames from active candidates
    if tier:
        rows = db.execute(
            "SELECT DISTINCT last_name FROM candidate WHERE tier = ? AND is_filtered_out = 0",
            (tier,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT DISTINCT last_name FROM candidate WHERE is_filtered_out = 0"
        ).fetchall()

    target_surnames = [r["last_name"] for r in rows]
    console.print(f"[bold]OSINT enrichment for {len(target_surnames)} unique surnames[/]")
    if not try_live:
        console.print("[dim]Using static OSINT only (known SM emigration data).[/]")
    else:
        console.print("[dim]Live scraping enabled (StealthyFetcher + Fetcher). Falls back to static on failure.[/]")

    # ── CEMLA ──
    console.print("\n[cyan]Phase 1: CEMLA (Argentine immigration archives)[/]")
    try:
        from titan_veritas.osint.cemla import search_surnames_sync as cemla_search
        cemla_results = cemla_search(target_surnames, try_live=try_live)
        cemla_map = {}
        for r in cemla_results:
            if r.has_san_marino_connection:
                cemla_map[r.surname.lower()] = {
                    "total_hits": r.total_hits,
                    "sm_hits": r.san_marino_hits,
                    "method": r.method,
                }
                console.print(f"  [green]OK[/] {r.surname}: SM connection [{r.method}]")
        console.print(f"  CEMLA: {len(cemla_map)} surnames with San Marino connections")
    except Exception as e:
        logger.warning(f"CEMLA enrichment error: {e}")
        cemla_map = {}

    # ── Ellis Island ──
    console.print("\n[cyan]Phase 2: Ellis Island (US immigration records)[/]")
    try:
        from titan_veritas.osint.ellis_island import search_surnames_sync as ellis_search
        ellis_results = ellis_search(target_surnames, try_live=try_live)
        ellis_map = {}
        for r in ellis_results:
            if r.has_san_marino_connection:
                ellis_map[r.surname.lower()] = {
                    "total_hits": r.total_hits,
                    "sm_hits": r.san_marino_hits,
                    "method": r.method,
                }
                console.print(f"  [green]OK[/] {r.surname}: SM connection [{r.method}]")
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
    console.print(f"[green]OK[/] Updated {updated} candidates with OSINT data")
    console.print("[dim]Run 'titan.py score' to recalculate scores with OSINT multipliers[/]")


# ─── dedupe ───────────────────────────────────────────────────────────────

@cli.command("dedupe")
@click.option("--dry-run", is_flag=True, default=False, help="Show duplicates without merging")
@click.pass_context
def cmd_dedupe(ctx, dry_run):
    """Find and merge duplicate candidate records using fuzzy matching."""
    from titan_veritas.core.deduplication import find_duplicates, merge_duplicates

    db = ctx.obj["db"]

    console.print("[bold]Deduplication Analysis[/]")
    groups = find_duplicates(db, include_filtered=False)

    if not groups:
        console.print("[green]OK[/] No duplicates found")
        return

    console.print(f"\nFound [yellow]{len(groups)}[/] duplicate groups:")
    for g in groups[:30]:
        console.print(f"  Primary #{g.primary_id} absorbs {g.duplicate_ids} ({g.similarity:.0f}%) - {g.reason}")

    if dry_run:
        total_dupes = sum(len(g.duplicate_ids) for g in groups)
        console.print(f"\n[yellow]DRY RUN[/] - would merge {total_dupes} duplicates into {len(groups)} primaries")
        return

    merged = merge_duplicates(db, groups)
    console.print(f"\n[green]OK[/] Merged {merged} duplicate records")
    console.print("[dim]Run 'titan.py score' to recalculate scores[/]")


# ─── tier3-cutoff ─────────────────────────────────────────────────────────

@cli.command("tier3-cutoff")
@click.option("--min-score", default=15, help="Minimum score to keep Tier 3 candidates (default: 15)")
@click.option("--keep-club", is_flag=True, default=True, help="Keep Tier 3 with known club (default: yes)")
@click.option("--keep-dob", is_flag=True, default=True, help="Keep Tier 3 with known DOB (default: yes)")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would be filtered without filtering")
@click.pass_context
def cmd_tier3_cutoff(ctx, min_score, keep_club, keep_dob, dry_run):
    """Filter out low-value Tier 3 candidates (W_name=0).

    Keeps Tier 3 candidates if they have:
    - Score >= min-score, OR
    - A known current club (if --keep-club), OR
    - A known DOB (if --keep-dob)
    """
    db = ctx.obj["db"]

    # Count before
    t3_active = db.execute(
        "SELECT COUNT(*) as c FROM candidate WHERE tier = 3 AND is_filtered_out = 0"
    ).fetchone()["c"]

    console.print(f"[bold]Tier 3 Cutoff Analysis[/]")
    console.print(f"  Active Tier 3 candidates: {t3_active}")
    console.print(f"  Min score threshold: {min_score}")

    # Build the filter query - find candidates to FILTER OUT
    conditions = ["tier = 3", "is_filtered_out = 0", f"titan_score < {min_score}"]
    if keep_club:
        conditions.append("current_club IS NULL")
    if keep_dob:
        conditions.append("date_of_birth IS NULL")

    where = " AND ".join(conditions)
    to_filter = db.execute(f"SELECT COUNT(*) as c FROM candidate WHERE {where}").fetchone()["c"]
    to_keep = t3_active - to_filter

    console.print(f"  Would filter: [red]{to_filter}[/]")
    console.print(f"  Would keep: [green]{to_keep}[/]")

    if dry_run:
        console.print("\n[yellow]DRY RUN[/] - no changes made")
        return

    if to_filter == 0:
        console.print("[green]OK[/] No Tier 3 candidates to filter")
        return

    db.execute(
        f"UPDATE candidate SET is_filtered_out = 1, filter_reason = 'tier3_cutoff' "
        f"WHERE {where}"
    )
    db.commit()
    console.print(f"\n[green]OK[/] Filtered {to_filter} low-value Tier 3 candidates")
    console.print("[dim]Run 'titan.py score' to recalculate remaining scores[/]")


# ─── api-queue ────────────────────────────────────────────────────────

@cli.command("api-queue")
@click.option("--populate", is_flag=True, default=False, help="Discover teams and populate the queue")
@click.option("--process", is_flag=True, default=False, help="Process pending queue entries")
@click.option("--max-calls", default=95, help="Max API calls per run (default: 95)")
@click.option("--status", "show_status", is_flag=True, default=False, help="Show queue status")
@click.pass_context
def cmd_api_queue(ctx, populate, process, max_calls, show_status):
    """Manage API-Football queue for lower league scanning.

    Usage:
        titan.py api-queue --populate   # Discover teams, fill queue
        titan.py api-queue --process    # Process up to 95 teams from queue
        titan.py api-queue --status     # Show queue progress
    """
    from titan_veritas.scrapers.api_football import APIFootballClient

    db = ctx.obj["db"]
    afc = APIFootballClient(db)

    if show_status or (not populate and not process):
        stats = afc.queue_stats()
        console.print(f"[bold]API-Football Queue Status[/]")
        console.print(f"  Total teams: {stats['total']}")
        console.print(f"  Processed: {stats['done']}")
        console.print(f"  Pending: {stats['pending']}")
        console.print(f"  Matches found: {stats['matches_total']}")
        afc.close()
        return

    if populate:
        console.print("[bold]Populating API-Football queue...[/]")
        added = afc.populate_queue()
        stats = afc.queue_stats()
        console.print(f"[green]OK[/] Added {added} teams to queue")
        console.print(f"  Total pending: {stats['pending']}")
        console.print(f"  API calls used: {afc._calls_today}")

    if process:
        stats_before = afc.queue_stats()
        console.print(f"[bold]Processing queue ({stats_before['pending']} pending, max {max_calls} calls)...[/]")
        result = afc.process_queue(max_calls=max_calls)
        console.print(f"[green]OK[/] Queue processing complete:")
        console.print(f"  Teams processed: {result['processed']}")
        console.print(f"  Remaining: {result['remaining']}")
        console.print(f"  Matches found: {result['matches_found']}")
        console.print(f"  API calls used: {result['api_calls_used']}")
        if result['remaining'] > 0:
            console.print(f"[dim]Run again tomorrow to process remaining {result['remaining']} teams[/]")

    afc.close()


# ─── bdfa-enrich ──────────────────────────────────────────────────────

@cli.command("bdfa-enrich")
@click.option("--limit", "-n", default=0, help="Max profiles to scrape (0 = all)")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would be scraped without scraping")
@click.pass_context
def cmd_bdfa_enrich(ctx, limit, dry_run):
    """Enrich candidates that have BDFA IDs by scraping their profile pages.

    Extracts DOB, current club, position, and career_start_year from
    individual BDFA profile pages (which still work, unlike search).
    """
    import random
    from datetime import date as _date

    from titan_veritas.config import BDFA_BASE, DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX
    from titan_veritas.scrapers.bdfa import scrape_profile

    db = ctx.obj["db"]
    repo = CandidateRepo(db)

    # Find candidates with BDFA IDs that are missing DOB
    rows = db.execute(
        "SELECT id, first_name, last_name, bdfa_id, date_of_birth, current_club, "
        "career_start_year, position FROM candidate WHERE bdfa_id IS NOT NULL"
    ).fetchall()

    # Prioritise those missing DOB
    needs_enrichment = [r for r in rows if not r["date_of_birth"]]
    already_have = len(rows) - len(needs_enrichment)

    console.print(f"[bold]BDFA Profile Enrichment[/]")
    console.print(f"  Candidates with BDFA ID: {len(rows)}")
    console.print(f"  Already have DOB: {already_have}")
    console.print(f"  Need enrichment: {len(needs_enrichment)}")

    if not needs_enrichment:
        console.print("[green]OK[/] All BDFA candidates already enriched")
        return

    targets = needs_enrichment[:limit] if limit > 0 else needs_enrichment

    if dry_run:
        console.print(f"\n[yellow]DRY RUN[/] — would scrape {len(targets)} profiles:")
        for r in targets[:20]:
            console.print(f"  {r['first_name']} {r['last_name']} (bdfa_id={r['bdfa_id']})")
        if len(targets) > 20:
            console.print(f"  ... and {len(targets) - 20} more")
        return

    console.print(f"\n[cyan]Scraping {len(targets)} BDFA profiles...[/]")

    enriched = 0
    errors = 0

    for r in tqdm(targets, desc="BDFA profiles"):
        bdfa_id = r["bdfa_id"]
        name = f"{r['first_name']}-{r['last_name']}".upper().replace(" ", "-")
        url = f"{BDFA_BASE}/jugadores-{name}-{bdfa_id}.html"

        try:
            data = scrape_profile(url)

            updates = []
            params = []

            if data.get("date_of_birth") and not r["date_of_birth"]:
                updates.append("date_of_birth = ?")
                params.append(data["date_of_birth"].isoformat())
                # Also compute age
                today = _date.today()
                dob = data["date_of_birth"]
                age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                updates.append("age = ?")
                params.append(age)

            if data.get("current_club") and not r["current_club"]:
                updates.append("current_club = ?")
                params.append(data["current_club"])

            if data.get("career_start_year") and not r["career_start_year"]:
                updates.append("career_start_year = ?")
                params.append(data["career_start_year"])

            if data.get("position") and not r["position"]:
                updates.append("position = ?")
                params.append(data["position"])

            if updates:
                updates.append("updated_at = datetime('now')")
                sql = f"UPDATE candidate SET {', '.join(updates)} WHERE id = ?"
                params.append(r["id"])
                db.execute(sql, tuple(params))
                db.commit()
                enriched += 1

        except Exception as e:
            errors += 1
            logger.debug(f"BDFA profile error for {r['last_name']} ({bdfa_id}): {e}")

        # Polite delay
        time.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))

    console.print(f"\n[green]OK[/] BDFA enrichment complete:")
    console.print(f"  Enriched: {enriched}")
    console.print(f"  Errors: {errors}")
    console.print(f"  Skipped (no new data): {len(targets) - enriched - errors}")
    console.print("[dim]Run 'titan.py score' to recalculate scores with new data[/]")


# ─── score ─────────────────────────────────────────────────────────────────

@cli.command("score")
@click.pass_context
def cmd_score(ctx):
    """Recalculate TITAN scores for all candidates.

    Preserves manual filters (tier3_cutoff, duplicate_of:*) that were
    applied outside of the scoring engine.
    """
    db = ctx.obj["db"]
    repo = CandidateRepo(db)
    candidates = repo.get_all(include_filtered=True)

    # Save manually-applied filters so we can restore them after scoring
    MANUAL_FILTER_PREFIXES = (
        "tier3_cutoff", "duplicate_of", "surname_not_sm", "age_over_32",
        "italian_club", "insufficient_data", "non_diaspora", "national_team_entry",
    )
    manual_filters: dict[int, str] = {}
    for c in candidates:
        reason = c.get("filter_reason") or ""
        if any(reason.startswith(p) for p in MANUAL_FILTER_PREFIXES):
            manual_filters[c["id"]] = reason

    console.print(f"[bold]Recalculating scores for {len(candidates)} candidates[/]")
    console.print(f"[dim]Preserving {len(manual_filters)} manual filters (tier3_cutoff, duplicates)[/]")

    for c in tqdm(candidates, desc="Scoring"):
        # Skip manually filtered candidates — don't rescore them
        cid = c["id"]
        if cid in manual_filters:
            continue

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
            is_filtered_out=bool(c["is_filtered_out"]),
            filter_reason=c["filter_reason"],
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

    console.print("[green]OK[/] Scoring complete")
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
    console.print(f"[green]OK[/] Exported {n_json} candidates to JSON: {json_out}")
    console.print(f"[green]OK[/] Exported {n_csv} candidates to CSV: {csv_out}")


# ─── export-html ──────────────────────────────────────────────────────

@cli.command("export-html")
@click.option("--output", "-o", default="report.html", help="Output HTML file path")
@click.option("--top", "-n", default=50, help="Number of top candidates to include (default: 50)")
@click.pass_context
def cmd_export_html(ctx, output, top):
    """Generate an executive HTML report for FSGC directors.

    Mobile-first responsive design with Tailwind CSS.
    Includes tutorial section explaining the TITAN formula
    and clean player cards for Top N candidates.
    """
    from titan_veritas.export.exporter import export_html

    db = ctx.obj["db"]
    n = export_html(db, output, top_n=top)
    console.print(f"[green]OK[/] Executive report generated: {output}")
    console.print(f"  Top {n} candidates included")
    console.print(f"  Open in browser to view")


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
    console.print(f"[green]OK[/] Seed exported: {len(records)} candidates -> {output}")
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
        console.print(f"  >= {threshold}: {count}")

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
