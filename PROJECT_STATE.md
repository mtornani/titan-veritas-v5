# TITAN VERITAS v5.0 - Unified Task List (Shared State)

- [x] Fase 1: Progettazione e Setup
- [x] Fase 2: Sviluppo Moduli Dati (Sorgenti)
- [x] Fase 3: Logica di Core e Scoring
- [x] Fase 4: Export e UI
- [x] Fase 5: Test
- [x] Fase 6: Architettura Imbuto Inverso
- [x] Fase 7: Trasparenza e UI Enrichment
- [x] Fase 8: TITAN HUD (Web App)
- [x] Fase 9: Technical Deep Dive (Karpathy Style)
- [x] Fase 10: TITAN VERITAS v5.0 - Active Scouting
  - [x] Modulo 1: OSINT Surname Generator (Rosetta Stone)
  - [x] Modulo 2: Micro-Targeting Regionale (Leghe U14-U20)
  - [x] Modulo 3: Automated Active Outreach (Gmail/Gemini/Telegram)

## Core Tasks In-Progress / Next
- [x] **[Antigravity]** Ottimizzazione TITAN HUD (Filtri avanzati, Virtual Scrolling per 14k record).
- [x] **[Antigravity]** Raffinamento Scoring Geografico (Cross-reference con porti storici).
- [x] **[Claude]** Database persistence layer (SQLite schema 5 tables, repositories, connection).
- [x] **[Claude]** CLI Restructure (click.group: search, init-db, rosetta, youth, outreach, db).
- [x] **[Claude]** Module 1 OSINT Infrastructure (FratellanzeScraper, EllisIslandScraper, CEMLAScraper, VariantEngine).
- [x] **[Claude]** Module 2 Youth Leagues (AsyncLeagueScraper base, LeagueRegistry, Michigan/Pergamino/Cordoba scaffolds).
- [x] **[Claude]** Module 3 Full Implementation (ContactExtractor, EmailComposer 3-lang, GmailPoller, IntelProcessor 3-tier fallback, TelegramRouter, OutreachCoordinator).
- [x] **[Claude]** Scoring expansion (load_expanded_surnames from DB with variants).
- [x] **[Claude]** Rate limiter infrastructure (per-domain semaphore + jitter).
- [x] **[Claude]** DB seeded: 241 original surnames (93 T1 + 148 T2) + 25 Fratellanze clusters.
- [ ] Integration test tra i moduli di entrambi gli agenti.
- [ ] **[Antigravity]** Premium HUD Design (React frontend con i nuovi dati dal DB).
