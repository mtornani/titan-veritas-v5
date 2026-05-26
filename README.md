# TITAN VERITAS — Dashboard v6

Ricerca indipendente sulla diaspora sammarinese nel mondo.
Dashboard pubblica con mappa interattiva delle comunità e dei lead calcistici.

**Live:** https://mtornani.github.io/titan-veritas-v5/

## Stack

- HTML + CSS + Vanilla JS (no framework, no bundler, no CDN)
- Mappa SVG mondiale con id ISO 3166-1 alpha-2 per paese
- Dati statici in `data/leads.json`
- Deploy: GitHub Pages

## Sviluppo locale

```bash
python -m http.server 8080
# poi apri http://localhost:8080
```

Nessun build step. Il sito funziona offline una volta caricato.

## Struttura

```
/
├── index.html          # Homepage con mappa interattiva
├── leads.html          # Database lead con filtri
├── methodology.html    # Metodologia ricerca
├── about.html          # Chi sono
├── data/
│   └── leads.json      # Tutti i dati (leads + communities + origin)
├── assets/
│   ├── css/
│   │   ├── main.css    # Design system + componenti
│   │   └── map.css     # Stili mappa + side panel
│   ├── js/
│   │   ├── main.js     # Utilities condivise (badge, formatters)
│   │   ├── map.js      # TitanMap class — SVG map engine
│   │   └── panel.js    # PanelController — side panel
│   └── img/
│       ├── world.svg   # Mappa SVG mondiale (generata da gen_world_svg.py)
│       └── sm-logo.svg # Logo
└── gen_world_svg.py    # Script generazione world.svg
```

## Aggiungere un nuovo lead

Apri `data/leads.json` e aggiungi un oggetto all'array `leads`:

```json
{
  "id": "lead_007",
  "alias": "X.Y.",
  "year_of_birth": 2005,
  "country_code": "US",
  "country": "United States",
  "city": "New York",
  "lat": 40.7128,
  "lng": -74.0060,
  "club": "Nome Club",
  "position": "attaccante",
  "citizenship_sm": "to_verify",
  "fifa_eligibility": "to_verify",
  "ancestor": {
    "name": "Nome Cognome",
    "born_in": "San Marino",
    "year_emigration": 1950,
    "relation_to_player": "great-grandfather"
  },
  "source": "Comunità X",
  "first_contact_date": "2026-06-01",
  "status": "data_pending"
}
```

Poi aggiorna `metadata.total_leads` e `metadata.last_updated`.

**Valori `status`:** `data_pending` | `registered` | `verified` | `inactive_football` | `not_eligible` | `archived`

**Valori `citizenship_sm`:** `confirmed` | `to_verify`

**Valori `fifa_eligibility`:** `verified` | `to_verify` | `not_eligible`

## Aggiungere una comunità

Aggiungi all'array `communities` in `data/leads.json`:

```json
{
  "id": "com_010",
  "name": "Comunità Sammarinese Melbourne",
  "country_code": "AU",
  "city": "Melbourne",
  "lat": -37.8136,
  "lng": 144.9631,
  "president": "Nome Presidente",
  "status": "pending_outreach",
  "members_estimated": 300,
  "first_contact": null
}
```

**Valori `status`:** `active_dialogue` | `primary_amplifier` | `pending_outreach` | `informed`

## Rigenerare world.svg

Se il file SVG è corrotto o si vuole aggiornare il dataset geografico:

```bash
python3 gen_world_svg.py
```

Richiede accesso a `cdn.jsdelivr.net` per scaricare il dataset world-atlas@2.

## Deploy GitHub Pages

1. Push su `master` (o il branch configurato in Settings → Pages)
2. Settings → Pages → Source: seleziona branch e root `/`
3. Il sito è disponibile su `https://mtornani.github.io/titan-veritas-v5/`

---

**Disclaimer:** Ricerca indipendente condotta a titolo personale.
Non affiliata con FSGC né con organi istituzionali della Repubblica di San Marino.
Nessun nome completo di minorenni pubblicato.
