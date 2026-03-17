"""Export pipeline — generates JSON for React HUD, CSV, and summary stats."""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path

from titan_veritas.db.connection import Database
from titan_veritas.db.repository import CandidateRepo

logger = logging.getLogger(__name__)


def _bdfa_url(c) -> str:
    """Build BDFA profile URL from candidate record."""
    name = f"{c['first_name']}-{c['last_name']}".upper().replace(" ", "-")
    return f"https://www.bdfa.com.ar/jugadores-{name}-{c['bdfa_id']}.html"


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
            "bdfa_id": c["bdfa_id"],
            "bdfa_url": _bdfa_url(c) if c["bdfa_id"] else None,
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


def export_html(
    db: Database,
    output_path: str | Path,
    top_n: int = 50,
) -> int:
    """Generate an executive HTML report for FSGC directors.

    Static single-page report with Tailwind CSS (CDN), mobile-first,
    showing Top N candidates as clean player cards.
    Returns count of candidates in report.
    """
    from datetime import datetime

    repo = CandidateRepo(db)
    candidates = repo.get_all(include_filtered=False)
    top_candidates = candidates[:top_n]
    stats = repo.stats()

    # Build OSINT status text for each candidate
    def osint_status(c):
        cemla = bool(c["cemla_hit"])
        ellis = bool(c["ellis_island_hit"])
        if cemla and ellis:
            return "Verificato", "text-emerald-400", "bg-emerald-400/10"
        if cemla or ellis:
            return "Parziale", "text-amber-400", "bg-amber-400/10"
        return "In Attesa", "text-slate-400", "bg-slate-400/10"

    def tier_label(t):
        return {1: "Tier 1 — Endemico", 2: "Tier 2 — Probabile", 3: "Tier 3 — Esplorativo"}.get(t, f"Tier {t}")

    def tier_color(t):
        return {1: "text-emerald-400", 2: "text-blue-400", 3: "text-slate-400"}.get(t, "text-slate-400")

    # Build player cards HTML
    cards_html = []
    for i, c in enumerate(top_candidates, 1):
        status_text, status_color, status_bg = osint_status(c)
        age_str = str(c["age"]) + " anni" if c["age"] else "N/D"
        club = c["current_club"] or "Sconosciuto"
        league = c["current_league"] or ""
        position = c["position"] or "N/D"
        score = c["titan_score"]
        tier = c["tier"]

        # Score color
        if score >= 80:
            score_color = "text-emerald-400"
        elif score >= 60:
            score_color = "text-blue-400"
        elif score >= 40:
            score_color = "text-amber-400"
        else:
            score_color = "text-slate-300"

        # External links
        links = []
        if c["wikidata_qid"]:
            links.append(f'<a href="https://www.wikidata.org/wiki/{c["wikidata_qid"]}" target="_blank" class="text-blue-400 hover:text-blue-300 text-xs">Wikidata</a>')
        if c["bdfa_id"]:
            burl = _bdfa_url(c)
            links.append(f'<a href="{burl}" target="_blank" class="text-blue-400 hover:text-blue-300 text-xs">BDFA</a>')
        links_html = " &middot; ".join(links) if links else ""

        card = f'''
        <div class="bg-slate-800/60 backdrop-blur-sm rounded-xl border border-slate-700/50 p-5 hover:border-slate-600 transition-all duration-200">
          <div class="flex items-start justify-between mb-3">
            <div class="flex items-center gap-3">
              <span class="text-slate-500 text-sm font-mono">#{i}</span>
              <div>
                <h3 class="text-white font-semibold text-lg leading-tight">{c["first_name"]} {c["last_name"]}</h3>
                <p class="text-slate-400 text-sm">{club}{(" &mdash; " + league) if league else ""}</p>
              </div>
            </div>
            <div class="text-right">
              <span class="{score_color} text-2xl font-bold">{score}</span>
              <p class="text-slate-500 text-xs">TITAN Score</p>
            </div>
          </div>
          <div class="grid grid-cols-3 gap-3 text-sm mb-3">
            <div>
              <span class="text-slate-500 text-xs block">Eta</span>
              <span class="text-slate-200">{age_str}</span>
            </div>
            <div>
              <span class="text-slate-500 text-xs block">Ruolo</span>
              <span class="text-slate-200">{position}</span>
            </div>
            <div>
              <span class="text-slate-500 text-xs block">Cognome</span>
              <span class="{tier_color(tier)} text-xs">{tier_label(tier)}</span>
            </div>
          </div>
          <div class="flex items-center justify-between">
            <span class="{status_color} {status_bg} text-xs font-medium px-2.5 py-1 rounded-full">{status_text}</span>
            <span class="text-slate-500 text-xs">{links_html}</span>
          </div>
        </div>'''
        cards_html.append(card)

    cards_block = "\n".join(cards_html)
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    html = f'''<!DOCTYPE html>
<html lang="it" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TITAN VERITAS v6 — Executive Report</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body {{ background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%); }}
    .glass {{ background: rgba(30, 41, 59, 0.5); backdrop-filter: blur(12px); }}
    details summary {{ cursor: pointer; list-style: none; }}
    details summary::-webkit-details-marker {{ display: none; }}
    details[open] .chevron {{ transform: rotate(180deg); }}
  </style>
</head>
<body class="min-h-screen text-slate-200 font-sans">

  <!-- Header -->
  <header class="sticky top-0 z-50 glass border-b border-slate-700/50">
    <div class="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between">
      <div>
        <h1 class="text-xl font-bold text-white">TITAN VERITAS <span class="text-blue-400">v6</span></h1>
        <p class="text-slate-400 text-xs">San Marino Diaspora Football Intelligence</p>
      </div>
      <div class="text-right">
        <p class="text-slate-400 text-xs">Report generato il</p>
        <p class="text-white text-sm font-medium">{generated_at}</p>
      </div>
    </div>
  </header>

  <main class="max-w-6xl mx-auto px-4 py-8 space-y-8">

    <!-- KPI Cards -->
    <section class="grid grid-cols-2 md:grid-cols-4 gap-4">
      <div class="bg-slate-800/60 rounded-xl border border-slate-700/50 p-4 text-center">
        <p class="text-3xl font-bold text-white">{stats['active']}</p>
        <p class="text-slate-400 text-xs mt-1">Candidati Attivi</p>
      </div>
      <div class="bg-slate-800/60 rounded-xl border border-slate-700/50 p-4 text-center">
        <p class="text-3xl font-bold text-emerald-400">{top_candidates[0]['titan_score'] if top_candidates else 0}</p>
        <p class="text-slate-400 text-xs mt-1">Score Massimo</p>
      </div>
      <div class="bg-slate-800/60 rounded-xl border border-slate-700/50 p-4 text-center">
        <p class="text-3xl font-bold text-blue-400">{stats['avg_score']}</p>
        <p class="text-slate-400 text-xs mt-1">Score Medio</p>
      </div>
      <div class="bg-slate-800/60 rounded-xl border border-slate-700/50 p-4 text-center">
        <p class="text-3xl font-bold text-amber-400">{stats['dob_coverage']}</p>
        <p class="text-slate-400 text-xs mt-1">Copertura DOB</p>
      </div>
    </section>

    <!-- Tutorial Accordion -->
    <section>
      <details class="bg-slate-800/60 rounded-xl border border-slate-700/50 overflow-hidden">
        <summary class="px-5 py-4 flex items-center justify-between hover:bg-slate-700/30 transition-colors">
          <div class="flex items-center gap-3">
            <svg class="w-5 h-5 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
            <span class="text-white font-semibold">Come leggere questo report &mdash; La Formula TITAN</span>
          </div>
          <svg class="w-5 h-5 text-slate-400 chevron transition-transform duration-200" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
        </summary>
        <div class="px-5 pb-5 space-y-4 text-sm text-slate-300 border-t border-slate-700/50 pt-4">
          <p>
            <strong class="text-white">TITAN VERITAS</strong> e un sistema di intelligence sportiva progettato per
            identificare calciatori della diaspora sammarinese che potrebbero avere diritto alla
            cittadinanza per <em>Jure Sanguinis</em> (diritto di sangue).
          </p>
          <div class="bg-slate-900/60 rounded-lg p-4 space-y-3">
            <h4 class="text-white font-semibold">L'approccio Bayesiano semplificato</h4>
            <p>
              Il sistema non valuta solo le statistiche sportive. Combina tre livelli di evidenza:
            </p>
            <ol class="list-decimal list-inside space-y-2 text-slate-300">
              <li>
                <strong class="text-blue-400">Probabilita di base (Prior):</strong>
                Quanto e comune il cognome a San Marino? Un <em>Gasperoni</em> o <em>Gualandi</em>
                ha una probabilita endemica altissima (Tier 1); un <em>Rossi</em> e comune sia a
                San Marino che in tutta Italia (Tier 2).
              </li>
              <li>
                <strong class="text-emerald-400">Evidenza geografica:</strong>
                Il giocatore e nato o gioca in Argentina, USA o Brasile &mdash; paesi con
                comunita sammarinesi storicamente documentate? Questo aumenta la probabilita.
              </li>
              <li>
                <strong class="text-amber-400">Conferma documentale (OSINT):</strong>
                I registri navali dell'epoca (CEMLA per l'Argentina, Ellis Island per gli USA)
                confermano che famiglie con quel cognome sono emigrate da San Marino?
                Questa e l'evidenza piu forte e funge da moltiplicatore.
              </li>
            </ol>
            <p>
              Il <strong class="text-white">Punteggio TITAN</strong> finale sintetizza tutti questi fattori.
              Un punteggio alto indica un'alta probabilita che il giocatore abbia diritto alla
              cittadinanza sammarinese e meriti un approfondimento genealogico.
            </p>
          </div>
          <div class="bg-slate-900/60 rounded-lg p-4 space-y-2">
            <h4 class="text-white font-semibold">Status OSINT</h4>
            <ul class="space-y-1">
              <li><span class="text-emerald-400 font-medium">Verificato:</span> Cognome confermato nei registri navali CEMLA + Ellis Island</li>
              <li><span class="text-amber-400 font-medium">Parziale:</span> Confermato in una sola fonte (CEMLA o Ellis Island)</li>
              <li><span class="text-slate-400 font-medium">In Attesa:</span> Non ancora verificato nei registri &mdash; richiede ricerca manuale</li>
            </ul>
          </div>
        </div>
      </details>
    </section>

    <!-- Player Cards Grid -->
    <section>
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-xl font-bold text-white">Top {len(top_candidates)} Candidati</h2>
        <p class="text-slate-400 text-sm">Ordinati per Punteggio TITAN</p>
      </div>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        {cards_block}
      </div>
    </section>

    <!-- Footer -->
    <footer class="text-center py-8 border-t border-slate-700/50">
      <p class="text-slate-500 text-xs">
        TITAN VERITAS v6 &mdash; Federazione Sammarinese Giuoco Calcio
      </p>
      <p class="text-slate-600 text-xs mt-1">
        Report automatico &middot; Dati: Wikidata, BDFA, API-Football, CEMLA, Ellis Island
      </p>
    </footer>

  </main>

</body>
</html>'''

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")

    logger.info(f"Exported HTML report with {len(top_candidates)} candidates to {output}")
    return len(top_candidates)
