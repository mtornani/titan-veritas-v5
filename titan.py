"""TITAN VERITAS v5.0 — San Marino Oriundi Intelligence Platform.

CLI entry point with subcommands for search, OSINT, youth league scanning, and outreach.
"""

import os
import asyncio
import logging

import click
from rich.console import Console
from rich.table import Table

console = Console()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)


def _load_env():
    """Load .env file if python-dotenv is available."""
    try:
        from dotenv import load_dotenv
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        if os.path.exists(env_path):
            load_dotenv(env_path)
    except ImportError:
        pass


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """TITAN VERITAS v5.0 - San Marino Oriundi Intelligence Platform"""
    _load_env()
    if ctx.invoked_subcommand is None:
        console.print("\n[bold blue]TITAN VERITAS v5.0[/bold blue] - [bold gold3]San Marino Oriundi Intelligence Platform[/bold gold3]\n")
        console.print("Usa [bold cyan]python titan.py --help[/bold cyan] per vedere i comandi disponibili.")
        console.print("  [cyan]search[/cyan]    - Ricerca giocatori (pipeline originale)")
        console.print("  [cyan]init-db[/cyan]   - Inizializza database SQLite")
        console.print("  [cyan]rosetta[/cyan]   - Pipeline OSINT Rosetta Stone (varianti cognomi)")
        console.print("  [cyan]youth[/cyan]     - Scan leghe giovanili regionali")
        console.print("  [cyan]outreach[/cyan]  - Pipeline Active Scouting (email/telegram)")
        console.print("  [cyan]db[/cyan]        - Query database\n")


# ─── SEARCH (Original Pipeline) ──────────────────────────────────────────────

@cli.command()
@click.option('--surnames', type=str, help='Lista cognomi target separati da virgola (es. "Gasperoni,Mularoni")')
@click.option('--offline-only', is_flag=True, help='Usa solo il database offline senza scraping web')
def search(surnames, offline_only):
    """Ricerca giocatori — pipeline originale TITAN VERITAS."""
    from titan_veritas.scrapers.wikidata import WikidataScraper
    from titan_veritas.scrapers.bdfa import BDFAScraper
    from titan_veritas.scrapers.regional import RegionalScraper
    from titan_veritas.core.offline_search import OfflineSearchEngine
    from titan_veritas.core.rosetta import RosettaStone
    from titan_veritas.core.scoring import apply_filters_and_score
    from titan_veritas.export.report import export_csv, export_html, export_json

    console.print("\n[bold blue]TITAN VERITAS v5.0[/bold blue] - [bold gold3]Target: Ricerca Oriundi (Jus Sanguinis)[/bold gold3]\n")

    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'global_players.csv')
    offline_engine = OfflineSearchEngine(csv_path)
    all_players = []

    if not surnames:
        console.print("[bold yellow]Inizializzazione Funnel Inverso (Global Pool Matching)...[/bold yellow]")
        with console.status("[bold cyan]Analisi offline di 17k+ giocatori...[/bold cyan]"):
            offline_suspects = offline_engine.search_globals()

        console.print(f"[green]✔[/green] Identificati [bold]{len(offline_suspects)}[/bold] sospetti iniziali per cognome.")

        if offline_only:
            for p in offline_suspects:
                scored_p = apply_filters_and_score(p)
                if not scored_p.is_lethal_filtered:
                    all_players.append(scored_p)
        else:
            console.print("[yellow]Avvio Micro-Targeting Regionale (U14-U20)...[/yellow]")
            rosetta = RosettaStone(offline_engine.target_surnames)
            reg_scraper = RegionalScraper(rosetta)

            regional_players = asyncio.run(reg_scraper.scrape_pergamino())
            regional_players += asyncio.run(reg_scraper.scrape_michigan_youth())

            console.print(f"[green]✔[/green] Trovati [bold]{len(regional_players)}[/bold] talenti nelle leghe regionali.")

            for p in offline_suspects + regional_players:
                scored_p = apply_filters_and_score(p)
                if not scored_p.is_lethal_filtered:
                    all_players.append(scored_p)
    else:
        surnames_list = [s.strip() for s in surnames.split(",")]
        scraper_wiki = WikidataScraper()
        scraper_bdfa = BDFAScraper()

        for sur in surnames_list:
            with console.status(f"[bold cyan]Estrazione Dati per {sur}...[/bold cyan]"):
                players_w = scraper_wiki.search_by_surname(sur)
                players_b = scraper_bdfa.search_by_surname(sur)
                players = players_w + players_b

            console.print(f"[green]✔[/green] Trovati {len(players_w)} in Wikidata e {len(players_b)} in BDFA per [bold]{sur}[/bold].")

            for p in players:
                scored_p = apply_filters_and_score(p)
                if not scored_p.is_lethal_filtered:
                    all_players.append(scored_p)

    if not all_players:
        console.print("[bold red]Nessun candidato trovato dopo l'applicazione dei filtri letali.[/bold red]")

    console.print(f"[bold green]Ricerca completata![/bold green] Candidati validi: {len(all_players)}")

    # Table output
    table = Table(title="Top 10 Sospettati (Ordinati per TITAN Score)", show_header=True, header_style="bold magenta")
    table.add_column("Nome Sospetto", style="cyan", width=25)
    table.add_column("Età", justify="center")
    table.add_column("Club Corrente", style="green", width=30)
    table.add_column("Score", justify="right", style="bold red")

    sorted_players = sorted(all_players, key=lambda x: x.titan_score, reverse=True)
    for p in sorted_players[:10]:
        table.add_row(
            p.known_as,
            str(p.age) if p.age else "??",
            p.current_club[:30] if p.current_club else "Sconosciuto",
            str(p.titan_score),
        )
    console.print(table)

    # Export
    export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports')
    os.makedirs(export_dir, exist_ok=True)
    csv_out = os.path.join(export_dir, 'titan_report.csv')
    html_out = os.path.join(export_dir, 'titan_report.html')
    json_out = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'titan-hud', 'src', 'data.json')

    export_csv(all_players, csv_out)
    export_html(all_players, html_out)
    export_json(all_players, json_out)

    console.print(f"\n[bold gold3]Export completato.[/bold gold3]")
    console.print(f"  CSV: [underline]{csv_out}[/underline]")
    console.print(f"  HTML: [underline]{html_out}[/underline]\n")


# ─── INIT-DB ─────────────────────────────────────────────────────────────────

@cli.command("init-db")
@click.option('--seed', is_flag=True, help='Popola il DB con cognomi Tier 1/2 e cluster noti')
def init_db_cmd(seed):
    """Inizializza il database SQLite."""
    from titan_veritas.db.connection import get_db
    from titan_veritas.db.schema import init_db, seed_surnames, seed_clusters

    db_path = os.environ.get("TITAN_DB_PATH", "titan_veritas.db")
    console.print(f"[bold cyan]Inizializzazione database:[/bold cyan] {db_path}")

    conn = get_db(db_path)
    init_db(conn)
    console.print("[green]OK[/green] Schema creato.")

    if seed:
        surname_count = seed_surnames(conn)
        cluster_count = seed_clusters(conn)
        console.print(f"[green]OK[/green] Seeded {surname_count} cognomi originali.")
        console.print(f"[green]OK[/green] Seeded {cluster_count} cluster geografici (Fratellanze).")

    console.print("[bold green]Database pronto.[/bold green]\n")


# ─── ROSETTA (OSINT Surname Discovery) ───────────────────────────────────────

@cli.command()
@click.option('--source', type=click.Choice(['all', 'fratellanze', 'ellis', 'cemla']),
              default='all', help='Fonte OSINT da interrogare')
@click.option('--dry-run', is_flag=True, help='Mostra cosa verrebbe estratto senza scrivere al DB')
def rosetta(source, dry_run):
    """Pipeline OSINT Rosetta Stone — scoperta varianti cognomi."""
    from titan_veritas.db.connection import get_db
    from titan_veritas.db.schema import init_db
    from titan_veritas.osint.variant_engine import VariantEngine

    console.print("\n[bold blue]ROSETTA STONE[/bold blue] - [bold gold3]OSINT Surname Discovery Pipeline[/bold gold3]\n")

    conn = get_db()
    init_db(conn)  # Ensure schema exists

    engine = VariantEngine(conn)

    if dry_run:
        console.print("[yellow]DRY RUN — nessuna scrittura al database.[/yellow]")
        from titan_veritas.db.repository import SurnameRepo
        repo = SurnameRepo(conn)
        stats = repo.get_stats()
        console.print(f"  Cognomi originali: {stats['originals']}")
        console.print(f"  Varianti note: {stats['variants']}")
        console.print(f"  Tier 1: {stats['tier1']}, Tier 2: {stats['tier2']}")
        console.print(f"\n  [cyan]Fonte selezionata:[/cyan] {source}")
        console.print("  Esegui senza --dry-run per avviare la discovery.\n")
        return

    console.print(f"[cyan]Avvio discovery da fonte:[/cyan] {source}")
    stats = asyncio.run(engine.run_full_discovery(sources=source))

    table = Table(title="Rosetta Stone — Risultati Discovery", show_header=True)
    table.add_column("Metrica", style="cyan")
    table.add_column("Valore", justify="right", style="bold green")

    table.add_row("Record Fratellanze", str(stats.get("fratellanze_records", 0)))
    table.add_row("Record Ellis Island", str(stats.get("ellis_records", 0)))
    table.add_row("Record CEMLA", str(stats.get("cemla_records", 0)))
    table.add_row("Varianti Generate", str(stats.get("variants_generated", 0)))
    table.add_row("Contatti Trovati", str(stats.get("contacts_found", 0)))
    console.print(table)
    console.print("")


# ─── YOUTH (Regional League Scanning) ────────────────────────────────────────

@cli.command()
@click.option('--country', type=click.Choice(['all', 'usa', 'argentina']),
              default='all', help='Filtra per nazione')
@click.option('--age-min', type=int, default=14, help='Età minima')
@click.option('--age-max', type=int, default=20, help='Età massima')
def youth(country, age_min, age_max):
    """Scan leghe giovanili regionali per candidati U14-U20."""
    from titan_veritas.db.connection import get_db
    from titan_veritas.db.repository import SurnameRepo
    from titan_veritas.scrapers.youth_leagues.registry import create_default_registry
    from titan_veritas.core.scoring import apply_filters_and_score

    console.print("\n[bold blue]YOUTH LEAGUE SCANNER[/bold blue] - [bold gold3]Micro-Targeting Regionale[/bold gold3]\n")

    conn = get_db()
    repo = SurnameRepo(conn)

    tier1 = [s["name"] for s in repo.get_all_originals(tier=1)]
    if not tier1:
        console.print("[red]Nessun cognome nel DB. Esegui prima: python titan.py init-db --seed[/red]")
        return

    registry = create_default_registry()
    country_filter = None if country == "all" else country

    console.print(f"[cyan]Searching {len(tier1)} Tier 1 surnames across {country or 'all'} leagues...[/cyan]")
    players = asyncio.run(registry.search_all(
        tier1[:15],
        age_range=(age_min, age_max),
        country=country_filter,
    ))

    scored = []
    for p in players:
        scored_p = apply_filters_and_score(p)
        if not scored_p.is_lethal_filtered:
            scored.append(scored_p)

    console.print(f"[green]✔[/green] Trovati [bold]{len(scored)}[/bold] candidati nelle leghe giovanili.\n")

    if scored:
        table = Table(title="Youth League Candidates", show_header=True, header_style="bold magenta")
        table.add_column("Nome", style="cyan", width=25)
        table.add_column("Età", justify="center")
        table.add_column("Club", style="green", width=30)
        table.add_column("Lega", width=25)
        table.add_column("Score", justify="right", style="bold red")

        for p in sorted(scored, key=lambda x: x.titan_score, reverse=True)[:20]:
            table.add_row(
                p.known_as or f"{p.first_name} {p.last_name}",
                str(p.age) if p.age else "??",
                (p.current_club or "?")[:30],
                (p.current_league or "?")[:25],
                str(p.titan_score),
            )
        console.print(table)
    console.print("")


# ─── OUTREACH (Active Scouting Pipeline) ─────────────────────────────────────

@cli.command()
@click.option('--mode', type=click.Choice(['full', 'send', 'poll', 'status']),
              default='status', help='Modalità operativa')
@click.option('--community', type=str, help='Target specifico fratellanza (per nome)')
def outreach(mode, community):
    """Pipeline Active Scouting — email, LLM processing, Telegram alerts."""
    from titan_veritas.db.connection import get_db
    from titan_veritas.db.schema import init_db

    console.print("\n[bold blue]ACTIVE SCOUTING[/bold blue] - [bold gold3]Outreach Pipeline[/bold gold3]\n")

    conn = get_db()
    init_db(conn)

    if mode == "status":
        from titan_veritas.db.repository import OutreachRepo
        repo = OutreachRepo(conn)
        stats = repo.get_stats()
        if not stats:
            console.print("[yellow]Nessuna attività di outreach registrata.[/yellow]")
        else:
            table = Table(title="Outreach Status", show_header=True)
            table.add_column("Status", style="cyan")
            table.add_column("Count", justify="right", style="bold")
            for status, count in stats.items():
                table.add_row(status, str(count))
            console.print(table)
        console.print("")
        return

    # Initialize services
    gmail_creds = os.environ.get("GMAIL_CREDENTIALS_FILE", "credentials.json")
    gmail_token = os.environ.get("GMAIL_TOKEN_FILE", "token.json")
    gemini_key = os.environ.get("GEMINI_API_KEY")
    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    tg_chat = os.environ.get("TELEGRAM_CHAT_ID")

    if not gemini_key:
        console.print("[red]GEMINI_API_KEY non configurata. Imposta in .env[/red]")
        return
    if not tg_token or not tg_chat:
        console.print("[red]TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID richiesti. Imposta in .env[/red]")
        return

    from titan_veritas.core.outreach import GmailService
    from titan_veritas.outreach.intel_processor import IntelProcessor
    from titan_veritas.outreach.telegram_router import TelegramRouter
    from titan_veritas.outreach.coordinator import OutreachCoordinator

    gmail = GmailService(gmail_token, gmail_creds)
    intel = IntelProcessor(gemini_key)
    telegram = TelegramRouter(tg_token, tg_chat)
    coordinator = OutreachCoordinator(gmail, intel, telegram, conn)

    if mode == "full" or mode == "send":
        console.print("[cyan]Avvio ciclo outreach completo...[/cyan]")
        stats = asyncio.run(coordinator.run_outreach_cycle(target_community=community))
        console.print(f"[green]✔[/green] Email inviate: {stats.emails_sent}, Errori: {stats.errors}")

    if mode == "full" or mode == "poll":
        console.print("[cyan]Avvio polling risposte Gmail...[/cyan]")
        stats = asyncio.run(coordinator.run_poll_only(max_iterations=1))
        console.print(f"[green]✔[/green] Risposte: {stats.replies_received}, Intel: {stats.intel_extracted}")

    console.print("")


# ─── DB (Database Query) ─────────────────────────────────────────────────────

@cli.command()
@click.option('--surname', type=str, help='Mostra varianti per un cognome specifico')
@click.option('--stats', is_flag=True, help='Mostra statistiche database')
@click.option('--clusters', is_flag=True, help='Mostra cluster geografici')
def db(surname, stats, clusters):
    """Query il database TITAN VERITAS."""
    from titan_veritas.db.connection import get_db
    from titan_veritas.db.repository import SurnameRepo, ClusterRepo

    conn = get_db()
    surname_repo = SurnameRepo(conn)
    cluster_repo = ClusterRepo(conn)

    if stats:
        s = surname_repo.get_stats()
        console.print("\n[bold cyan]Database Statistics[/bold cyan]")
        console.print(f"  Cognomi originali: {s['originals']}")
        console.print(f"  Varianti fonetiche: {s['variants']}")
        console.print(f"  Tier 1 (rari): {s['tier1']}")
        console.print(f"  Tier 2 (comuni): {s['tier2']}\n")

    if surname:
        original = surname_repo.get_original_by_name(surname)
        if not original:
            console.print(f"[red]Cognome '{surname}' non trovato nel database.[/red]")
            return

        console.print(f"\n[bold cyan]{original['name']}[/bold cyan] (Tier {original['tier']})")
        variants = surname_repo.get_variants(original["id"])
        if variants:
            table = Table(title=f"Varianti di {original['name']}", show_header=True)
            table.add_column("Variante", style="cyan")
            table.add_column("Confidenza", justify="right", style="bold")
            table.add_column("Metodo", style="green")
            table.add_column("Fonte")
            for v in variants:
                table.add_row(
                    v["variant"],
                    f"{v['confidence']:.1f}%",
                    v["method"],
                    v["source"] or "-",
                )
            console.print(table)
        else:
            console.print("  Nessuna variante scoperta ancora. Esegui: python titan.py rosetta")
        console.print("")

    if clusters:
        all_clusters = cluster_repo.get_all()
        table = Table(title="Cluster Geografici (Fratellanze)", show_header=True)
        table.add_column("Città", style="cyan")
        table.add_column("Paese", style="green")
        table.add_column("Fratellanza")
        table.add_column("Email", style="dim")
        for c in all_clusters:
            table.add_row(
                c["city"],
                c["country"],
                c.get("fratellanza_name") or "-",
                c.get("contact_email") or "-",
            )
        console.print(table)
        console.print("")

    if not stats and not surname and not clusters:
        console.print("[yellow]Specifica --stats, --surname <nome>, o --clusters[/yellow]")


if __name__ == "__main__":
    cli()
