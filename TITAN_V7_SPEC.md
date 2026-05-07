# TITAN VERITAS v7 — Specifica Tecnica
**Versione spec:** 1.0 | **Data:** maggio 2026 | **Autore:** Claude per FSGC
**Status:** In attesa di approvazione Mirko Tornani

---

## 0. Analisi del punto di partenza

Questa sezione documenta ciò che il codice fa realmente (non ciò che il README dichiara).

### 0.1 Cosa funziona e va tenuto

| Componente | File | Stato | Note |
|---|---|---|---|
| BDFA scraper Argentina | `scrapers/bdfa.py` | **Funzionante** | Scrapling + pandas, robusto |
| Schema DB base | `db/schema.py` | **Riusabile** | Da estendere, non riscrivere |
| CLI orchestration | `titan.py` | **Riusabile** | Struttura Click OK |
| Modello PlayerProfile | `core/models.py` | **Riusabile** | Da estendere |
| Deduplication | `core/deduplication.py` | **Funzionante** | Non toccare |
| Exporter JSON/CSV | `export/exporter.py` | **Funzionante** | Da estendere per narrativa |
| DB connection + repo | `db/connection.py`, `db/repository.py` | **Funzionante** | Non toccare |
| Config struttura | `config.py` | **Riusabile** | Contenuto da aggiornare |

### 0.2 Problemi critici trovati nel codice (non nell'audit)

**Problema 1 — Filtro letale che rigetta l'obiettivo principale:**
In `core/scoring.py`, `_is_in_italian_league()` e `_is_in_italian_club()` sono
**LETHAL FILTERS**: se un giocatore gioca in Serie C italiana viene **eliminato prima
ancora di essere scorato**. Con il pivot a "giocatori di livello Serie C", questo filtro
uccide l'intero bacino target. È il bug più grave del sistema.

**Problema 2 — Campi DB mancanti per OSINT:**
`PlayerProfile` in `models.py` ha i campi `familysearch_hit` e `cognomix_hit`.
La tabella `candidate` in `schema.py` ha solo `cemla_hit` e `ellis_island_hit`.
I due campi aggiuntivi non vengono mai scritti su DB. Il moltiplicatore OSINT
li legge ma sono sempre `False`: effetto zero senza errore visibile.

**Problema 3 — CEMLA e FamilySearch sono lookup su lista statica:**
Quando il live scraping fallisce (quasi sempre, per CAPTCHA/SPA), il "fallback"
controlla se il cognome è in `KNOWN_SM_EMIGRANT_SURNAMES` o `KNOWN_SM_FAMILYSEARCH`.
Queste liste sono identiche alla lista cognomi Tier 1/2 del sistema.
Risultato: `cemla_hit=1` NON significa "trovato nell'archivio CEMLA". Significa
"il cognome è nella lista che abbiamo già". Il moltiplicatore OSINT moltiplica per
dati circolari. Il nome del campo è fuorviante per il Federal Council.

**Problema 4 — `surname_variant` table è un'isola:**
La tabella esiste nello schema e viene seeded, ma `_surname_score()` in `scoring.py`
fa solo exact match (`== s.lower()`). Le varianti (Cecchetti/Cechetti, Gasperoni/
Gasparon) non vengono mai usate nello scoring.

**Problema 5 — Italia assente da DIASPORA_HUBS:**
`DIASPORA_HUBS` in `config.py` contiene Argentina, USA, Brazil, France, Belgium.
L'Italia — area geografica #1 per il nuovo obiettivo — non esiste come peso geografico.
Un giocatore nato a Rimini ha W_geo = 0.

**Problema 6 — Athletic score ignora i campionati target:**
`_athletic_score()` riconosce solo keyword argentine ("proyección", "reserva",
"federal", "primera b"). Un giocatore di Serie C italiana ottiene M_athletic = 15
(fallback "club noto, qualsiasi lega"), identico a un giocatore di quinta divisione
argentina con club noto.

### 0.3 Cosa va riscritto vs aggiunto vs rimosso

| Azione | File | Motivo |
|---|---|---|
| **RIMUOVERE** | `scoring.py: _is_in_italian_league`, `_is_in_italian_club` | Sono filtri letali che eliminano il target principale |
| **RISCRIVERE** | `scoring.py` intero | Nuova formula Christie, nuovi pesi, Italia inclusa |
| **RISCRIVERE** | `config.py` (contenuto) | Aggiungere Italia a DIASPORA_HUBS, comuni Romagna, pesi campionati |
| **AGGIUNGERE COLONNE** | `db/schema.py` | familysearch_hit, cognomix_hit, candidate_status |
| **AGGIUNGERE TABELLA** | `db/schema.py` | evidence (indizi convergenti per candidato) |
| **RINOMINARE CAMPI** | `db/schema.py`, `models.py` | cemla_hit → surname_in_cemla_archive, etc. |
| **AGGIUNGERE** | `scrapers/transfermarkt.py` | Scraper Italia per birthplace+cognome |
| **AGGIUNGERE** | `scrapers/lega_pro.py` | Parser PDF tesseramenti Lega Pro |
| **RISCRIVERE** | `osint/familysearch.py` | API reale invece di lista statica |
| **AGGIUNGERE** | `export/narrative.py` | Generazione storia narrativa per candidato |
| **AGGIUNGERE** | `titan.py` comandi | `lega-pro-scan`, `tm-scan`, `narrative` |

---

## 1. Obiettivo del sistema v7

TITAN v7 identifica giocatori con **due caratteristiche convergenti**:

- **CARATTERISTICA 1 (Livello tecnico):** Giocatori che giocano stabilmente in Serie C
  italiana o equivalente europeo/sudamericano (Primera Nacional AR, Brasileirão Série B,
  USL Championship USA).
- **CARATTERISTICA 2 (Eleggibilità):** Giocatori con cognome statisticamente sammarinese
  E almeno un secondo indizio indipendente che suggerisca ascendenza sammarinese
  documentabile.

Un candidato non viene segnalato se soddisfa solo una delle due caratteristiche.

---

## 2. Framework Christie — Regola degli indizi convergenti

Ogni candidato deve avere **almeno 2 indizi indipendenti** per essere segnalato.
Un solo indizio = solo tracking interno, mai segnalazione.

### 2.1 Categorie di indizi

**Indizi forti** (peso 3):
- Luogo di nascita in comune target sammarinese o Romagna (lista definita in §4)
- Cognome Tier 1 sammarinese (endemico, <500 occorrenze in Italia)
- Riscontro verificato in archivio genealogico (FamilySearch API con risposta positiva reale)

**Indizi medi** (peso 2):
- Cognome Tier 2 sammarinese (alta probabilità, anche diffuso in Romagna)
- Concentrazione cognome >40% nella provincia di Rimini/Pesaro-Urbino (Cognomix)
- Cognome padre/madre in lista T1 sammarinese (quando disponibile da Wikidata)

**Indizi deboli** (peso 1):
- Cognome in lista storicamente emigrato (CEMLA/KNOWN_SM — dichiarato come tale)
- Cluster geografico (nazione diaspora: Argentina, Brasile, USA)
- Onomastica (nome "Marino", "Savino", "Leo" — nomi storicamente sammarinesi)

### 2.2 Soglie di segnalazione

| Soglia | Stato | Significato |
|---|---|---|
| Peso totale ≥ 1 | `tracked` | Solo nel database, non segnalato |
| Peso totale ≥ 4 (≥2 indizi indipendenti) | `flagged` | Segnalato per revisione umana |
| Peso totale ≥ 6 + verifica genealogica | `verified` | Pronto per valutazione FSGC |
| Contatto avviato | `contacted` | In pipeline outreach |
| Eleggibilità documentata | `eligible` | Documentazione completa per FIFA/UEFA |

---

## 3. Schema database v7

### 3.1 Tabella `candidate` — modifiche rispetto a v6

Aggiungere colonne:
```
candidate_status   TEXT NOT NULL DEFAULT 'tracked'
                   -- valori: tracked | flagged | verified | contacted | eligible
familysearch_hit   INTEGER NOT NULL DEFAULT 0   -- era solo in models.py, mai in DB
cognomix_hit       INTEGER NOT NULL DEFAULT 0   -- idem
cognomix_pct_rimini REAL                        -- % concentrazione cognome prov. Rimini
birth_place_verified INTEGER NOT NULL DEFAULT 0  -- luogo nascita confermato (non da TM)
source             TEXT NOT NULL DEFAULT 'unknown'
                   -- bdfa | transfermarkt | lega_pro | wikidata | api_football | manual
```

Rinominare (migration SQL):
```
cemla_hit      → surname_in_cemla_list    (dichiarativo: è nella lista, non verificato in archivio)
ellis_island_hit → surname_in_ellis_list  (idem)
```

### 3.2 Nuova tabella `evidence`

```sql
CREATE TABLE IF NOT EXISTS evidence (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id    INTEGER NOT NULL REFERENCES candidate(id) ON DELETE CASCADE,
    evidence_type   TEXT NOT NULL,
    -- valori: birthplace_target | surname_tier1 | surname_tier2 | genealogy_verified
    --         cognomix_cluster | cemla_list | ellis_list | diaspora_hub | onomastica
    source          TEXT NOT NULL,
    -- es: "Lega Pro PDF 2025-26", "FamilySearch API", "Cognomix.it", "BDFA"
    description     TEXT NOT NULL,
    -- es: "Nato a Rimini (RM), a 8km dal confine SM"
    weight          INTEGER NOT NULL DEFAULT 1,
    -- 1=debole, 2=medio, 3=forte
    verified_at     TEXT,
    -- NULL = non verificato da umano, data = verificato
    created_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_evidence_candidate ON evidence(candidate_id);
CREATE INDEX IF NOT EXISTS idx_evidence_type ON evidence(evidence_type);
```

### 3.3 Nuova tabella `scan_log`

```sql
CREATE TABLE IF NOT EXISTS scan_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    run_at      TEXT NOT NULL DEFAULT (datetime('now')),
    candidates_found INTEGER NOT NULL DEFAULT 0,
    candidates_new   INTEGER NOT NULL DEFAULT 0,
    notes       TEXT
);
```

### 3.4 Schema completo v7 — tabelle invariate

`surname`, `surname_variant`, `geographic_cluster`, `api_cache`, `api_queue`
restano identiche a v6. Non toccare.

---

## 4. Configurazione geography (config.py)

### 4.1 Comuni target Italia/Romagna

```python
ROMAGNA_TARGET_COMUNI = [
    # Provincia Rimini — confine diretto con SM
    "San Marino", "Rimini", "Riccione", "Cattolica", "Bellaria-Igea Marina",
    "Santarcangelo di Romagna", "Coriano", "Misano Adriatico", "Verucchio",
    "Novafeltria", "Pennabilli", "San Leo", "Talamello", "Casteldelci",
    # Provincia Pesaro-Urbino — confinante
    "Pesaro", "Urbino", "Fano", "Morciano di Romagna",
    # Provincia Forlì-Cesena — diaspora storica
    "Forlì", "Cesena", "Cesenatico",
]
```

### 4.2 DIASPORA_HUBS aggiornato

```python
DIASPORA_HUBS = {
    "Italy": 30,        # AGGIUNTO — area confine Romagna, massima densità
    "Argentina": 25,    # invariato
    "United States": 20, # invariato
    "Brazil": 20,       # invariato
    "France": 15,       # invariato
    "Belgium": 15,      # invariato
}
```

### 4.3 Campionati target per livello tecnico

```python
TARGET_LEAGUES_TIER_A = {
    # Campionati equivalenti a Serie C italiana o superiori
    "serie c": "italy",
    "primera nacional": "argentina",  # División B argentina
    "primeira liga": "portugal",
    "liga portugal 2": "portugal",
    "brasileirao serie b": "brazil",
    "usl championship": "usa",
    "mls next pro": "usa",
}

TARGET_LEAGUES_TIER_B = {
    # Inferiori ma accettabili (da esaminare caso per caso)
    "serie d": "italy",
    "eccellenza": "italy",
    "federal a": "argentina",
    "brasileirao serie c": "brazil",
    "usl league one": "usa",
}
```

---

## 5. Nuovo scoring engine (scoring.py)

### 5.1 Formula v7

La formula v6 (`S_base = (W_geo + W_name + M_athletic) × V_osint + A_bonus`) viene
**abbandonata** perché usa un moltiplicatore OSINT basato su dati circolari e non
distingue il livello tecnico in modo utile.

**Formula v7 — Score Christie:**

```
evidence_score = somma dei pesi di tutti gli indizi convergenti
level_bonus    = bonus per livello tecnico verificato (vedi §5.2)
titan_score_v7 = evidence_score + level_bonus
```

Il candidato è **segnalabile** solo se:
- evidence_score ≥ 4 (almeno 2 indizi indipendenti)
- level_bonus > 0 (gioca a livello minimo accettabile)

### 5.2 Level bonus

```python
def _level_bonus(league: str | None) -> tuple[float, str]:
    if not league:
        return 0.0, "unknown"
    l = league.lower()
    if any(kw in l for kw in TARGET_LEAGUES_TIER_A):
        return 20.0, "tier_a"   # Serie C o equivalente
    if any(kw in l for kw in TARGET_LEAGUES_TIER_B):
        return 8.0, "tier_b"    # Serie D o equivalente
    return 2.0, "unknown_league"  # Club noto ma lega non classificata
```

### 5.3 Lethal filters da RIMUOVERE

- `_is_in_italian_league` → **ELIMINATO**
- `_is_in_italian_club` → **ELIMINATO**

Lethal filters da **MANTENERE** (logicamente corretti):
- `_is_elite_noise` (Messi, Ronaldo, etc.)
- `_has_sm_or_italian_nationality` (già sammarinese)
- `_is_in_sm_club` (già nel sistema SM)
- `_age_out_of_range` (<16 o >38)

### 5.4 Score breakdown v7

```python
p.score_breakdown = {
    "evidence_score": evidence_score,
    "evidence_count": len(evidences),
    "evidence_list": [{"type": e.type, "weight": e.weight, "source": e.source} for e in evidences],
    "level_bonus": level_bonus,
    "level_tier": level_tier,
    "titan_score_v7": titan_score,
    "status": candidate_status,
    "is_flagged": evidence_score >= 4 and level_bonus > 0,
}
```

---

## 6. Nuovi moduli da costruire

### 6.1 `scrapers/transfermarkt.py` — ROADMAP A, PRIORITÀ CRITICA

**Cosa fa:** Estrae giocatori dalla ricerca avanzata di Transfermarkt filtrati per:
- Competizione: Serie C (tutte le gironi IT), con possibilità di estendere
- Luogo di nascita: lista comuni target Romagna + "San Marino"

**Stack:** Python, `requests` + `BeautifulSoup`, crawl delay 5s (rispetto robots.txt),
User-Agent rotation già disponibile in `config.py`.

**Input:** lista `ROMAGNA_TARGET_COMUNI` da config.py
**Output:** lista `PlayerProfile` con source="transfermarkt"

**Attenzione:** Il filtro birthplace di Transfermarkt è una ricerca testuale nel campo
"città di nascita". Non è garantita la completezza — è un filtro di primo livello,
non esaustivo. Documentare questo limite nell'output.

**Ore stimate:** 12–16 ore

---

### 6.2 `scrapers/lega_pro.py` — ROADMAP A, PRIORITÀ CRITICA

**Cosa fa:** Scarica i comunicati ufficiali di tesseramento di Lega Pro dalla sezione
"Comunicati Ufficiali" di legapro.com, li parsifica con `pdfplumber`, estrae:
- Nome, cognome, data nascita, luogo nascita, società, categoria

Applica filtro automatico su `ROMAGNA_TARGET_COMUNI` e `ALL_SURNAMES` per produrre
candidati iniziali.

**Stack:** Python, `requests` per download PDF, `pdfplumber` per parsing
(aggiungere a requirements.txt), `rapidfuzz` per matching cognomi.

**Input:** URL sezione comunicati Lega Pro (da verificare in fase di sviluppo)
**Output:** lista `PlayerProfile` con source="lega_pro", data_source="PDF ufficiale Lega Pro"

**Copertura dichiarata:** Solo giocatori tesserati professionisti Serie C stagione corrente.
Non copre Serie D, Eccellenza, dilettanti.

**Ore stimate:** 14–20 ore (il parsing PDF varia molto per struttura del documento)

---

### 6.3 `osint/familysearch.py` — RISCRITTURA — ROADMAP A, PRIORITÀ ALTA

**Stato attuale:** Pseudo-API — in pratica è un lookup su `KNOWN_SM_FAMILYSEARCH` (lista statica).

**Obiettivo v7:** Integrazione con FamilySearch API ufficiale (gratuita, richiede
registrazione account sviluppatore su familysearch.org/developers).

**Stack:** `familysearch-python-sdk` se disponibile su PyPI, altrimenti `requests` su
endpoint REST `/platform/records/search` con parametri `q.surname` e `q.birthLikePlace`.
Autenticazione: OAuth2 (token via client_id + client_secret da .env).

**CAMBIO DI COMPORTAMENTO CRITICO:**
- Se API call ha successo → `familysearch_hit = 1` (reale)
- Se API call fallisce → `familysearch_hit = 0` (non fare fallback su lista statica)
- La lista statica viene rinominata `KNOWN_SM_FAMILYSEARCH_SURNAMES` e usata SOLO per
  documentazione, mai per settare il flag

**Aggiungere a .env.example:**
```
FAMILYSEARCH_CLIENT_ID=
FAMILYSEARCH_CLIENT_SECRET=
```

**Ore stimate:** 16–24 ore (OAuth2 + rate limiting FamilySearch + test su cognomi SM)

---

### 6.4 `osint/cognomix.py` — REVIEW + FIX — ROADMAP A, PRIORITÀ MEDIA

**Verifica se il file esiste già:** `titan_veritas/osint/cognomix.py` è presente nella
struttura del repo. Leggere il contenuto prima di decidere se riscrivere o estendere.

**Obiettivo v7:**
- Dato un cognome, restituire la percentuale di distribuzione nelle province target
  (Rimini, Pesaro-Urbino, Forlì-Cesena) rispetto al totale nazionale
- Se % > 40% nelle province target → indizio medio (peso 2), tipo `cognomix_cluster`
- Risultato persistito in `candidate.cognomix_pct_rimini`

**Nota:** Cognomix non ha API ufficiale. Il parsing HTML richiede delay conservativo
(1 req/3s) e può rompersi se il sito cambia struttura.

**Ore stimate:** 6–10 ore

---

### 6.5 `export/narrative.py` — NUOVO — ROADMAP A, PRIORITÀ ALTA

**Cosa fa:** Per ogni candidato con `candidate_status ≥ flagged`, genera una storia
narrativa di 3-5 righe in italiano che sintetizza gli indizi convergenti trovati.

**Formato output:**
```
"Cognome Zanotti (Tier 1 sammarinese, 11 famiglie documentate in SM, 
concentrazione 73% in prov. Rimini da Cognomix). Nato a Santarcangelo 
di Romagna (RN), comune confinante con SM. Attaccante, Almirante Brown 
(Primera Nacional Argentina) — livello Serie B/C italiano. Due indizi 
forti convergenti (birthplace_target + surname_tier1). Score: 22/30."
```

**Stack:** Python puro, template string. Non usare LLM per questo — il testo deve
essere deterministico e auditabile dal Federal Council.

**Output:** aggiunto a `score_breakdown["narrative"]` e visibile nella HUD

**Ore stimate:** 6–8 ore

---

### 6.6 Dashboard HUD — AGGIORNAMENTO — ROADMAP A, PRIORITÀ MEDIA

**Modifiche necessarie:**
- Aggiungere filtro per `candidate_status` (tracked/flagged/verified/contacted/eligible)
- Mostrare narrative box per candidati flagged
- Colonna "Livello tecnico" con badge (Serie C / Equivalente / Da verificare)
- Rimuovere il badge "CEMLA" e "Ellis" (campi rinominati) — sostituire con
  numero di indizi convergenti e loro peso totale

**Stack:** React esistente, modifica `App.jsx` e `data.json` structure

**Ore stimate:** 8–12 ore

---

## 7. Sequenza di costruzione raccomandata

### Sprint 1 — Fix bloccanti (settimana 1, ~10 ore)

Questi fix devono essere fatti prima di qualsiasi nuovo sviluppo perché il sistema
attuale produce dati parzialmente scorretti.

1. **`scoring.py`:** Rimuovere `_is_in_italian_league` e `_is_in_italian_club` dai lethal filters
2. **`config.py`:** Aggiungere "Italy": 30 a DIASPORA_HUBS, aggiungere ROMAGNA_TARGET_COMUNI,
   aggiungere TARGET_LEAGUES_TIER_A e TARGET_LEAGUES_TIER_B
3. **`db/schema.py`:** Aggiungere le nuove colonne (`familysearch_hit`, `cognomix_hit`,
   `cognomix_pct_rimini`, `candidate_status`, `source`), rinominare `cemla_hit` →
   `surname_in_cemla_list`, `ellis_island_hit` → `surname_in_ellis_list`,
   aggiungere tabella `evidence`, aggiungere tabella `scan_log`
4. **`models.py`:** Aggiungere nuovi campi corrispondenti alle colonne DB
5. **`osint/familysearch.py`:** Rimuovere il fallback su lista statica. Se non c'è
   API key configurata → skip silenzioso (non settare flag)
6. **`scoring.py`:** Riscrivere con formula Christie e nuovi pesi

**Checkpoint 1:** Dopo Sprint 1, rieseguire `titan.py score` — i candidati in
Serie C italiana non devono più essere filtrati. Verificare con `titan.py stats`.

---

### Sprint 2 — Nuove sorgenti (settimane 2-3, ~30 ore)

7. **`scrapers/transfermarkt.py`:** Nuovo scraper Transfermarkt per Serie C + birthplace
8. **`scrapers/lega_pro.py`:** Parser PDF tesseramenti Lega Pro
9. **`titan.py`:** Aggiungere comandi `tm-scan` e `lega-pro-scan`
10. **Eseguire i nuovi scraper** e verificare output con `titan.py stats`

**Checkpoint 2:** Il database deve contenere candidati da fonte italiana (source="transfermarkt"
o source="lega_pro"). Se non ci sono → problema nel parsing, blocco per revisione.

---

### Sprint 3 — Pipeline evidenze e narrativa (settimana 4, ~20 ore)

11. **`osint/cognomix.py`:** Fix/estensione per restituire percentuale Romagna
12. **Sistema evidenze:** Implementare popolazione automatica tabella `evidence`
    durante scoring — ogni scoring run deve creare/aggiornare i record evidence
13. **`export/narrative.py`:** Generatore testo narrativo
14. **`titan.py`:** Comando `narrative` per generare narrative per tutti i flagged
15. **HUD update:** Aggiungere filtro status + narrative box

**Checkpoint 3 (finale pre-Federal Council):**
- `titan.py stats` mostra candidati con source italiana
- `titan.py narrative` produce testi leggibili per ogni flagged candidate
- HUD aggiornato mostra lo stato per ogni candidato
- Almeno 10 candidati con status `flagged` e livello tecnico Serie C documentato

---

## 8. Stop points per checkpoint con Mirko

**STOP 1 — Prima di Sprint 1:**
Conferma questa spec. In particolare:
- Sei d'accordo con la rinominazione di `cemla_hit` → `surname_in_cemla_list`?
  (Cambia il significato visibile nel HUD e nella presentazione)
- Vuoi che lo sprint 1 includa anche la registrazione account FamilySearch Developer?
  (Richiede 1-2 settimane per approvazione — iniziare subito se priorità alta)
- La lista TIER1_SURNAMES da 28 cognomi è corretta o vuoi espandere/ridurre?

**STOP 2 — Dopo Checkpoint 1:**
Revisione risultati Sprint 1 prima di aggiungere nuove sorgenti.
Domanda: il BDFA scraper esistente funziona ancora dopo i fix? Rieseguire `titan.py search --source bdfa`.

**STOP 3 — Dopo Checkpoint 2:**
Revisione candidati italiani trovati. Quanti? Da quali fonti?
Decisione: la qualità del dato Lega Pro PDF è sufficiente o richiede aggiustamenti?

**STOP 4 — Pre Federal Council:**
Revisione narrativa dei candidati flagged. Il testo narrativo è presentabile?
Revisione HUD aggiornata — funziona su mobile?

---

## 9. Cosa NON è in scope per v7

- FamilySearch API reale (richiede approvazione — da avviare richiesta ora, disponibile per Sprint 3 se approvata in tempo)
- Wyscout API (Roadmap B, post-approvazione budget)
- Ancestry/ProQuest (Roadmap B)
- Accordo CCSRE (azione diplomatica, non tecnica)
- Copertura Brasile ex novo (bassa priorità, nessun database pubblico scrapabile utile)
- Copertura Australia (esclusa per ROI insufficiente)
- GDPR DPIA formale (da scrivere parallelamente, non bloccante per v7 pre-approvazione)

---

## 10. Requirements.txt aggiornamenti necessari

```
pdfplumber>=0.11    # AGGIUNGERE — per Lega Pro PDF parsing
```

Invariati: `httpx`, `scrapling`, `rapidfuzz`, `pandas`, `lxml`, `click`, `rich`,
`tqdm`, `python-dotenv`.

---

## 11. CLAUDE.md da creare per il progetto

Dopo l'approvazione della spec, creare `CLAUDE.md` nella root del progetto con:
- Descrizione del sistema e obiettivo
- Lista delle invarianti del codice (lethal filter non ripristinare, ecc.)
- Nota esplicita: "I flag `surname_in_cemla_list` non indicano verifica nell'archivio"
- Istruzioni per aggiungere nuovi scraper (dove, naming convention)
- Crawl delay policy (non negoziabile per ogni scraper)

---

*Spec prodotta dopo lettura integrale del codice. I problemi in §0.2 sono verificati
direttamente nel codice, non inferiti dall'audit.*

**Prossimo passo:** Mirko approva questa spec (o chiede modifiche), poi si inizia Sprint 1.
