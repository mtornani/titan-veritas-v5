# Ricognizione Strumenti — Sistema TITAN v5 Pivot
## Dossier Tecnico per FSGC — Doppia Eleggibilità Sammarinese
**Versione:** 2.0 | **Data:** maggio 2026 | **Classificazione:** Uso Istituzionale FSGC

---

## Executive Summary

Il sistema TITAN v5 ha dimostrato la fattibilità tecnica dell'identificazione automatizzata di giocatori con doppia eleggibilità sammarinese. La presente ricognizione estende l'analisi a tutti gli strumenti disponibili e definisce due roadmap operative con costi verificati.

**Stato attuale (già funzionante):** Transfermarkt (free tier) copre parzialmente Serie C/D italiana e Primera División argentina; BDFA Argentina è pubblicamente accessibile per categorie fino a Federal A; FamilySearch API è gratuita previa approvazione; Wikidata SPARQL è operativo per analisi distribuzione cognomi; Cognomix.it è già integrato in TITAN v5. Il sistema esistente produce candidati grezzi ma manca di validazione tecnica del livello di gioco e di verifica genealogica certificata.

**Gap critici identificati:** (1) assenza di accesso a dati statistici di performance (Wyscout/InStat richiedono contratti commerciali); (2) nessuna copertura sistematica dei campionati brasiliani e australiani; (3) nessun accesso alle anagrafiche federali FIGC/Lega Pro; (4) mancanza di modulo outreach strutturato per contatto agenti.

**Costo stimato Roadmap A** (MVP, 4 settimane): €0–300/anno operativi, ~80 ore sviluppo serale.
**Costo stimato Roadmap B** (sistema completo): €12.000–28.000/anno, con accesso Wyscout come voce principale (€5.000–15.000/anno per singola lega).

L'area geografica con il più alto rapporto segnale/rumore rimane l'Italia centro-nord (Romagna, Marche, Umbria): giocatori nati nei comuni confinanti (Rimini, Pesaro-Urbino, Forlì-Cesena) con cognomi endemici sammarinesi rappresentano il cluster a maggiore probabilità di eleggibilità verificabile in tempi brevi. L'Argentina costituisce il secondo bacino per volume storico di emigrazione. Australia e Brasile sono bacini secondari con minore densità documentata.

---

## §1 — Strumenti per Identificazione Giocatori per Area Geografica

### ITALIA / ROMAGNA

**1a. Database federali FIGC**

Il Portale Servizi FIGC (portaleservizi.figc.it) e l'Anagrafe Federale (anagrafefederale.figc.it) sono sistemi chiusi ad accesso societario. Non esistono endpoint pubblici o CSV con elenchi tesserati per categoria. La Lega Pro (legapro.com) pubblica comunicati ufficiali di tesseramento in formato PDF stagionale — scrapabili con crawl delay rispettoso — contenenti nome, cognome, società, data e luogo di nascita. Tali dati sono riferiti a professionisti sportivi nell'esercizio dell'attività (GDPR Considerando 160; Art. 6(1)(e)) e non richiedono consenso per elaborazione da parte di federazioni sportive riconosciute.

**1b. Aggregatori commerciali**

- **Transfermarkt (gratuito):** Copertura confermata per Serie C (60 club, ~1.800 giocatori), Serie D (principale), Eccellenza (parziale). Il filtro "luogo di nascita" è disponibile nella ricerca avanzata giocatori. Accuracy del campo birthplace per "San Marino" e comuni confinanti (Rimini, Pesaro-Urbino): stimata al 70-80% per giocatori con profilo aggiornato, inferiore per categorie dilettantistiche. I ToS di Transfermarkt vietano scraping automatico a scopo commerciale ma tollerano accesso non commerciale con crawl delay (robots.txt: 5 secondi). Per uso istituzionale FSGC (non commerciale, senza redistribuzione) il rischio legale è basso se si rispetta il crawl delay e si evita il login automatizzato.
- **Wyscout (Hudl):** Prezzo verificato maggio 2026 — piano base individuale da €299/anno (70 min/mese video, no dati statistici esportabili). Accesso API dati: non pubblico, richiede contratto commerciale. Una quotazione documentata: £5.000/anno per 1 lega (campionato inglese minore). Stima per Serie C italiana: **€5.000–10.000/anno** (richiede contatto commerciale). Copertura Serie C: confermata (Wyscout dichiara 200+ competizioni, Italia Serie C inclusa). Roadmap B.
- **InStat Scout:** Piano base €35/mese (€420/anno) — include video su propria squadra e top 5 leghe europee + UCL. Piano completo €140/mese (€1.680/anno) — accesso video su tutti i campionati (limite 3 ore/mese). Copertura Serie C: da verificare con vendor — il catalogo completo non è pubblico. Prezzo API bulk: **richiede contatto commerciale — stima plausibile: €3.000–8.000/anno**. Roadmap B.

**1c. Note specifiche — Cluster Romagna/Marche**

I giocatori nati nei comuni di Rimini, Santarcangelo di Romagna, Novafeltria, Pennabilli, San Leo (tutti entro 30 km dal confine sammarinese) con cognomi Tier 1 sammarinesi (Bollini, Ceccoli, Della Valle, Gasperoni, Selva, Tura, Macina, Mazza, Sartini, Bucci) rappresentano il cluster a più alta probabilità per la Caratteristica 1 (livello tecnico Serie C). La concentrazione storica di famiglie sammarinesi nel Riminese è documentata dal Museo dell'Emigrante di San Marino.

| Strumento | Tipo | Prezzo | Copertura confermata | Roadmap A/B | Note ToS |
|---|---|---|---|---|---|
| Transfermarkt | Aggregatore free | €0 | Serie C/D, filtro birthplace | A | Crawl delay 5s, no commerciale |
| Lega Pro PDF ufficiali | Fonte federale | €0 | Serie C tesserati stagionali | A | Dati pubblici, sportivi professionisti |
| FIGC Portale Servizi | Database federale | Accesso societario | Tutti i livelli | Fuori scope | Accesso chiuso |
| Wyscout | Piattaforma scout | €299/anno base; API €5k-10k/anno | Serie C confermata | B | Contratto commerciale |
| InStat Scout | Piattaforma scout | €420–1.680/anno | Serie C da verificare | B | Contratto commerciale |

---

### ARGENTINA

**Gap TITAN v5:** BDFA (bdfa.com.ar) copre campionati argentini fino a Federal A con 60.000+ profili giocatori, pubblicamente accessibile e scrapabile (nessun ToS restrittivo rilevato). Copertura Federal B e Torneo Regional Amateur: incompleta (~40-60% dei giocatori). L'AFA non pubblica banche dati tesserati — accesso richiede accordo bilaterale federazione-federazione.

Livello tecnico target: Primera Nacional (División B argentina, ~equivalente Serie B/C italiana) e Primera División. Primera Nacional: 37 club, giocatori a contratto semi-professionale.

| Strumento | Tipo | Prezzo | Copertura confermata | Roadmap A/B | Note ToS |
|---|---|---|---|---|---|
| BDFA Argentina | Aggregatore free | €0 | Federal A, Primera Nacional, PD | A | Pubblico, non commerciale |
| Transfermarkt | Aggregatore free | €0 | Primera División, Primera Nacional (parziale) | A | Crawl delay 5s |
| AFA tesserati ufficiali | Database federale | Accordo istituzionale | Tutti i livelli | B | Richiede accordo FIFA/CONMEBOL |

---

### BRASILE

**CBF database:** La CBF non pubblica banche dati tesserati pubbliche. Il portale cbf.com.br offre tabelle campionati (risultati, non dati anagrafici). La piattaforma **Base dos Dados** (basedosdados.org) offre dataset aperti sul Brasileirão Série A dal 2003, Série B in parte — ma riguardano statistiche di partita, non anagrafiche complete.

Livello target: Brasileirão Série B (20 club professionistici) e Série C (64 club). Campionati statali Paulista/Carioca rilevanti per giocatori fuori dai roster principali.

OSINT genealogica: comunità sammarinese documentata nello Stato di São Paulo (immigrazione 1900–1950, da Serravalle e Borgo Maggiore — Museo dell'Emigrante San Marino). Ricerca genealogica: Portale Antenati + FamilySearch (registri parrocchiali São Paulo).

| Strumento | Tipo | Prezzo | Copertura confermata | Roadmap A/B | Note ToS |
|---|---|---|---|---|---|
| Base dos Dados | Dataset aperto | €0 | Brasileirão Série A/B (statistiche) | A | Licenza aperta, no dati anagrafici |
| Transfermarkt | Aggregatore free | €0 | Série A, B (parziale C) | A | Crawl delay 5s |
| FamilySearch | Genealogia | €0 (API gratuita) | Registri São Paulo 1900-1960 | A | Approvazione richiesta, API free |
| Wyscout | Piattaforma scout | API €5k-15k/anno | Série A, B confermata | B | Contratto commerciale |
| CBF database | Database federale | Accordo istituzionale | Tutti i livelli | Fuori scope | Accesso chiuso |

---

### USA

**US Soccer tesseramento:** US Soccer Federation non pubblica banche dati tesserati pubblici — accesso richiede accordo istituzionale.

**Livelli tecnici:** USL Championship (Div. II professionale USA, ~equivalente Serie B/C italiana) è il livello target. USL League One (Div. III, ~Serie C bassa). MLS Next Pro (~Serie C). NCAA Div. I rilevante come bacino pre-professionale (18-22 anni, equivalente Primavera/Serie D).

Transfermarkt: copertura USL Championship ~80% per giocatori con >20 presenze stagionali; MLS Next Pro ampiamente coperta.

| Strumento | Tipo | Prezzo | Copertura confermata | Roadmap A/B | Note ToS |
|---|---|---|---|---|---|
| Transfermarkt | Aggregatore free | €0 | USL Championship, MLS Next Pro | A | Crawl delay 5s |
| Wyscout | Piattaforma scout | API €5k-15k/anno | MLS, USL Championship confermata | B | Contratto commerciale |
| NCAA Soccer stats | Database istituzionale | €0 | NCAA Div. I/II | A | Dati pubblici |
| US Soccer DB | Database federale | Accordo istituzionale | Tutti i livelli | Fuori scope | Accesso chiuso |

---

### AUSTRALIA

**Football Australia:** Play Football (playfootball.com.au) gestisce le registrazioni nazionali ma non è una banca dati pubblica consultabile.

**Livelli tecnici:** A-League Men (~equivalente Serie B italiana) e NPL National Premier Leagues (~Serie C). Copertura Transfermarkt: A-League confermata, NPL molto parziale (<30%).

**Comunità sammarinese in Australia:** Presenza storicamente minima — ~14 cittadini sammarinesi documentati in Oceania (dato 2007, MAE San Marino). Nessuna comunità riconosciuta (soglia minima: 30 residenti). Australia classificata come area a bassa priorità: da escludere dalla Roadmap A, inserire opzionalmente nella Roadmap B.

| Strumento | Tipo | Prezzo | Copertura confermata | Roadmap A/B | Note ToS |
|---|---|---|---|---|---|
| Transfermarkt | Aggregatore free | €0 | A-League (buona), NPL (parziale) | B | Crawl delay 5s |
| Wyscout | Piattaforma scout | API da verificare | A-League da verificare | B | Contratto commerciale |
| Football Australia Play Football | Database federale | Accesso chiuso | Tutti i livelli | Fuori scope | Non pubblico |

---

## §2 — Strumenti Cross-Reference Cognome

Per determinare se un cognome è endemico sammarinese o semplicemente diffuso in Romagna/Emilia-Romagna, sono necessari strumenti di distribuzione geografica dei cognomi con granularità comunale.

- **Forebears.io (OnoGraph):** Offre API (OnoGraph) per predizione nazionalità da nome. Accuracy dichiarata: 85,1% in top-1, 93,4% in top-3. Copertura San Marino: la piattaforma include dati su ~55,5% della popolazione mondiale ma San Marino (34.000 abitanti) potrebbe avere campione statisticamente insufficiente per cognomi rari. Prezzo API: non pubblico sul sito principale — il sistema di pagamento accetta SEPA, PayPal, bonifico internazionale. **Stima plausibile basata su struttura documentata: €50–500/anno per uso istituzionale a basso volume** (richiede contatto con vendor per conferma). L'API è disponibile per batch fino a 1.000 nomi per chiamata. GDPR: classificazione etnica/nazionale da nome rientra potenzialmente in Art. 9 GDPR (dati particolari) se il risultato è utilizzato per prendere decisioni su individui; per uso di screening preliminare non decisionale il rischio è mitigato ma va documentato nel DPIA della FSGC.
- **Cognomix.it:** Già integrato in TITAN v5. Fornisce distribuzione regionale e provinciale dei cognomi italiani con buona granularità. Nessuna API ufficiale — scraping HTML. Limite richieste: non documentato pubblicamente, comportamento conservativo raccomandato (1 req/3s). Robustezza per cognomi rari: buona per cognomi con almeno 50 occorrenze nel database italiano; cognomi ultra-rari (<20 portatori totali in Italia) possono avere risultati non affidabili. Gratuito. Roadmap A confermata.
- **Geneanet (analisi cognomi):** Free tier: distribuzione cognomi per regione francese, dati internazionali limitati. Premium (~€39/anno): accesso esteso agli alberi genealogici ma non cambia la copertura per cognomi italiani. Per il caso d'uso FSGC il free tier è sufficiente per validazione incrociata di cognomi sammarinesi vs italiani generici. Roadmap A.
- **Wikidata SPARQL:** Query funzionante per distribuzione geografica persone per cognome (tramite wikidata.org/sparql). Copertura limitata a persone "notabili" con scheda Wikipedia — non rappresentativa per cognomi comuni ma utile per identificare cluster geografici di famiglie storicamente documentate. Gratuito. Roadmap A confermata.
- **Dataset accademici GitHub/Zenodo:** Il repository `philipperemy/name-dataset` (GitHub, licenza MIT) contiene 983K cognomi estratti da dump Facebook (533M utenti) con distribuzione geografica. Copertura Italia confermata ma granularità a livello nazionale, non provinciale/comunale. Utile come filtro primario. Il repository `opendatasicilia/comuni-italiani` contiene codici ISTAT e dati demografici per tutti i comuni italiani — complementare per mappatura geografica ma non contiene dati cognomi. Roadmap A.

| Strumento | Costo | Copertura SM | API disponibile | Roadmap |
|---|---|---|---|---|
| Cognomix.it | €0 | Buona (distribuzione prov.) | No (scraping) | A |
| Forebears.io OnoGraph | Stima €50–500/anno | Da verificare (piccolo campione) | Sì (batch 1000) | A/B |
| Geneanet | €0 (free) / €39 Premium | Parziale | No | A |
| Wikidata SPARQL | €0 | Limitata (notabili) | Sì (SPARQL) | A |
| name-dataset GitHub | €0 | Nazionale (no prov.) | Sì (Python lib) | A |

---

## §3 — Strumenti AI/LLM Emergenti (post-2024)

**API classificazione etnica/origine cognome:**
Oltre a Forebears OnoGraph (già citato), Namsor (namsor.com) offre pricing pubblico ~$0,001/nome (~€100/anno per 100K nomi) — costo marginale per il volume FSGC (500–5.000 nomi/anno).

**Pacchetti Python rilevanti (2024-2025):** `name-dataset` (PyPI, 2024) — distribuzione nomi/cognomi per paese, filtraggio iniziale. Nessun pacchetto specifico per "football player eligibility" su PyPI. Scraper Transfermarkt mantenuto: `dcaribou/transfermarkt-scraper` (GitHub, 2024) — adattabile per estrazione sistematica campi birthplace e nationality.

**Tool LLM-based per entity matching:** GPT-4/Claude API sono utilizzabili per fuzzy matching tra database (Transfermarkt vs BDFA, varianti ortografiche). Costo stimato: €0–50/anno per il volume FSGC.

**Valutazione critica — Affidabilità per cognomi sammarinesi Tier 1:**
OnoGraph e Namsor classificano l'origine da distribuzioni statistiche globali. Per cognomi come **Bollini, Ceccoli, Selva, Gasperoni** (<500 portatori in Italia) restituiranno "italiano" o "italiano centro-nord" ma non distingueranno sammarinese da romagnolo — limitazione attesa e non risolvibile con questi strumenti. La distinzione richiede un secondo livello (Cognomix distribuzione provinciale, FamilySearch). Per cognomi Tier 2 come **Della Valle, Mazza, Bucci** (diffusi in tutta Italia) l'affidabilità è ulteriormente ridotta.

**GDPR — Nota obbligatoria:** Classificazione di origine etnica/nazionale da nome è soggetta a GDPR Art. 9. La FSGC deve includere nel proprio DPIA l'utilizzo di questi strumenti, garantendo che la classificazione sia usata come filtro preliminare non decisionale e che il contatto diretto con i candidati abbia base giuridica documentata.

| Strumento | Tipo | Costo | Affidabilità cognomi SM Tier 1 | GDPR Art. 9 | Roadmap |
|---|---|---|---|---|---|
| OnoGraph (Forebears) | API classificazione | €50–500/anno | Bassa (no distinzione SM/IT) | Rilevante | A/B |
| Namsor | API classificazione | ~€100/anno (100K nomi) | Bassa (stessa limitazione) | Rilevante | A/B |
| GPT-4/Claude API | LLM entity matching | €0–50/anno | N/A (matching, non classificazione) | No | A |
| transfermarkt-scraper (GitHub) | Scraper open source | €0 | N/A | No | A |

---

## §4 — Strumenti Outreach (dopo identificazione candidati)

**Transfermarkt:** I profili giocatori a volte riportano il nome dell'agente nella sezione "Informazioni" — ma non email o telefono direttamente. Utile per identificare il nome dell'agente; la ricerca email va poi condotta separatamente. Gratuito. Limitazione: ~50% dei giocatori Serie C ha agente visibile su Transfermarkt.

**LinkedIn Sales Navigator:** Prezzo verificato maggio 2026 — piano Core €119,99/mese (€1.079,88/anno per seat). Per agenti di Serie C italiana su LinkedIn: la copertura è limitata. Gli agenti calcistici italiani di livello Serie C hanno spesso profilo LinkedIn presente ma non sempre aggiornato. LinkedIn Sales Navigator è sovradimensionato e troppo costoso per il caso d'uso FSGC (volume massimo stimato: 20-50 contatti agenti/anno). **Valutazione: non raccomandato per Roadmap A; valutare Roadmap B solo se il volume di candidati supera 200/anno.**

**Hunter.io:** Piano Starter €34/mese (€408/anno) — 500 ricerche email/mese. Piano Free: 25 ricerche/mese — **sufficiente per il volume FSGC in Roadmap A** (stima: 5-15 nuovi contatti/mese). Hunter.io non è specializzato per agenti sportivi italiani ma funziona per indirizzi email professionali di agenti con sito web o presenza digitale. Roadmap A (free tier).

**Apollo.io:** Piano Free: 900 crediti/anno (250 email send giornalieri). Piano Basic: €49/mese annuale. Per FSGC il free tier è sufficiente per volume ridotto. Database: 270M+ contatti con dati di settore — ma la copertura di agenti calcistici italiani di livello semi-professionistico è non verificata. Da testare prima di attivare piano a pagamento.

**CRM per sport:** Non esistono CRM specializzati per federazioni sportive a livello UEFA-minore a prezzo accessibile verificato. **HubSpot CRM free tier** (hubspot.com) è adeguato per il volume FSGC: fino a 1M contatti, pipeline visuale, tracking email — gratuito senza limiti di tempo per funzionalità base. Roadmap A confermata.

| Strumento | Prezzo | Caso d'uso FSGC specifico | Roadmap |
|---|---|---|---|
| Transfermarkt | €0 | Identificazione nome agente | A |
| Hunter.io | €0 (free 25/mese) / €408/anno | Ricerca email agenti con sito web | A (free) / B |
| Apollo.io | €0 (free 900cr/anno) / €588/anno | Database contatti agenti | A (free) |
| HubSpot CRM | €0 (free tier) | CRM candidati e outreach | A |
| LinkedIn Sales Navigator | €1.080/anno/seat | Troppo costoso per volume FSGC | Fuori scope (A) |

---

## §5 — Doppia Roadmap: Cosa Costruire

### ROADMAP A — MVP (€0–500/anno, 4 settimane sviluppo serale)

**Modulo A1 — Scraper Transfermarkt Birthplace**
Funzione: estrazione automatica giocatori Serie C/D con birthplace "San Marino" o comuni confinanti target.
Stack: Python 3, `requests` + `BeautifulSoup`, rispetto crawl delay 5s, output CSV.
Ore stimate: 12–16 ore (scraper + normalizzazione dati).
Costo operativo: €0/anno.
Rischio principale: modifiche HTML di Transfermarkt richiedono manutenzione periodica.
Priorità: **critica**.

**Modulo A2 — Matcher Cognomi Sammarinesi Tier 1/2**
Funzione: dato un elenco di giocatori (da Transfermarkt o Lega Pro PDF), applica fuzzy matching contro lista cognomi sammarinesi Tier 1 (alta confidenza) e Tier 2 (media confidenza), con score di probabilità.
Stack: Python, `rapidfuzz`, lista cognomi curata manualmente da Cognomix + Forebears.
Ore stimate: 8–12 ore.
Costo operativo: €0/anno (Cognomix scraping, Forebears free tier).
Rischio principale: falsi positivi per cognomi Tier 2 diffusi in tutta Italia.
Priorità: **critica**.

**Modulo A3 — Pipeline OSINT Genealogica**
Funzione: dato nome+cognome+data di nascita, esegue ricerca automatica su FamilySearch API e Portale Antenati (antenati.san.beniculturali.it) per trovare collegamento genealogico con famiglie sammarinesi documentate.
Stack: Python, FamilySearch SDK ufficiale (API gratuita previa approvazione), requests per Antenati.
Ore stimate: 20–24 ore (integrazione API + parsing risultati + logica di scoring).
Costo operativo: €0/anno (FamilySearch API gratuita).
Rischio principale: copertura genealogica limitata per rami emigrati post-1950; FamilySearch richiede approvazione (tempo stimato: 1–2 settimane).
Priorità: **alta**.

**Modulo A4 — Dashboard Aggiornabile (upgrade HUD esistente)**
Funzione: interfaccia web statica (upgrade del titan-hud esistente) con visualizzazione candidati per area geografica, score di probabilità eleggibilità, stato outreach.
Stack: HTML/CSS/JavaScript, dati JSON aggiornati manualmente o da script Python, hosting locale o GitHub Pages.
Ore stimate: 10–16 ore.
Costo operativo: €0/anno.
Rischio principale: nessuno tecnico — rischio di obsolescenza dati se non aggiornato.
Priorità: **media** (utile per presentazione Federal Council).

**Riepilogo Roadmap A:**
| Modulo | Stack | Ore | Costo/anno | Rischio | Priorità |
|---|---|---|---|---|---|
| A1 Scraper Transfermarkt | Python, BS4 | 12–16 | €0 | Manutenzione HTML | Critica |
| A2 Matcher Cognomi | Python, rapidfuzz | 8–12 | €0 | Falsi positivi T2 | Critica |
| A3 Pipeline FamilySearch | Python, FS API | 20–24 | €0 | Copertura limitata | Alta |
| A4 Dashboard HUD | HTML/JS | 10–16 | €0 | Obsolescenza dati | Media |
| **Totale** | | **50–68 ore** | **€0–300** | | |

---

### ROADMAP B — Sistema Completo (€5K–30K/anno)

**Modulo B1 — Wyscout API per Validazione Tecnica**
Funzione: dati statistici di performance (passaggi, dribbling, gol, assist, minuti giocati) per candidati già identificati da Roadmap A, per validare il livello tecnico sufficiente (soglia: ≥20 partite/stagione a livello Serie C o equivalente).
Strumento: Wyscout API (Hudl).
Costo: €5.000–10.000/anno per Serie C italiana; aggiungere €3.000–5.000/anno per ogni campionato extra (Primera Nacional AR, USL Championship USA). Stima totale per copertura 4 aree: **€8.000–20.000/anno**.
Valore aggiunto vs Roadmap A: riduce drasticamente i falsi positivi tecnici (un giocatore potrebbe avere ascendenza sammarinese ma non aver giocato a livello sufficiente — Wyscout consente verifica oggettiva in 5 minuti per candidato).
Priorità: **alta** (condizionata ad approvazione budget federale).

**Modulo B2 — Ancestry Library Edition per Verifica Genealogica Certificata**
Funzione: accesso a 30 miliardi di documenti storici (registri parrocchiali, liste di emigrazione, censimenti) per produrre traccia documentale certificabile ai fini FIFA/UEFA di eleggibilità.
Strumento: Ancestry Library Edition (distribuito da ProQuest), accessibile tramite accordo istituzionale con biblioteca o direttamente. Prezzo per istituzioni: **non pubblico — richiede contatto ProQuest — stima plausibile: €1.500–4.000/anno** per accesso istituzionale singolo utente.
Valore aggiunto vs Roadmap A: FamilySearch copre principalmente registri LDS e alcuni archivi parrocchiali; Ancestry aggiunge censimenti USA, liste passeggeri navi, archivi argentini e brasiliani non presenti altrove.
Priorità: **alta**.

**Modulo B3 — Database Agenti Serie C**
Non esiste una banca dati professionale dedicata agli agenti di Serie C italiana verificata e disponibile commercialmente. Le alternative pratiche sono: (a) lista agenti licenziati FIFA pubblicata annualmente dalla FIGC (pubblica, PDF, ~800 agenti italiani); (b) Apollo.io/Hunter.io per ricerca email su base nome agente; (c) contatto diretto tramite Transfermarkt. Nessun prodotto commerciale giustifica il costo per il volume FSGC.
Strumento consigliato: lista agenti FIGC (gratuita) + Hunter.io Starter (€408/anno).
Costo: €408/anno.
Priorità: **media**.

**Modulo B4 — Integrazione CCSRE (Consulta Sammarinesi all'Estero)**
Funzione: la CCSRE (Consulta dei Cittadini Sammarinesi all'Estero) è un organo istituzionale della Repubblica di San Marino con rappresentanti nelle comunità in Argentina, Francia, Italia, USA. Non ha una banca dati pubblica dei cittadini, ma i Consolati sammarinesi tengono registri dei cittadini all'estero. Un accordo formale FSGC-Segreteria di Stato per gli Affari Esteri di San Marino potrebbe sbloccare l'accesso a queste liste (cittadini sammarinesi residenti all'estero, potenzialmente con dati anagrafici). Questo è il percorso a più alto rendimento ma richiede azione diplomatica, non tecnica.
Costo tecnico: €0 (se l'accordo è raggiunto); il costo è politico-istituzionale.
Priorità: **critica** (percorso B, non tecnico).

**Riepilogo Roadmap B:**
| Modulo | Strumento | Costo/anno | Valore aggiunto | Priorità |
|---|---|---|---|---|
| B1 Wyscout API | Wyscout (Hudl) | €8.000–20.000 | Validazione tecnica oggettiva | Alta |
| B2 Ancestry Institutional | ProQuest/Ancestry | €1.500–4.000 | Traccia documentale certificata | Alta |
| B3 Database Agenti | Hunter.io Starter + FIGC list | €408 | Outreach sistematico agenti | Media |
| B4 CCSRE Integration | Accordo istituzionale | €0 (tecnico) | Accesso diretto a cittadini SM | Critica |
| **Totale stimato** | | **€10.000–25.000/anno** | | |

---

## §6 — Quick Wins (entro 7 giorni, budget zero)

**Quick Win 1 — Filtro Transfermarkt Serie C / Birthplace Rimini + San Marino**
Azione: accedere manualmente a Transfermarkt.com → ricerca avanzata giocatori → filtri: Competizione = "Serie C" (tutte e tre le gironali), Luogo di nascita = "San Marino"; ripetere con "Rimini", "Pesaro", "Forlì". Esportare i risultati (200-400 profili stimati) in foglio di calcolo applicando filtro manuale su cognomi Tier 1/2 sammarinesi. Registrare: nome, cognome, data di nascita, club attuale, categoria.
Strumento: Transfermarkt.com (interfaccia web, accesso manuale).
Output atteso: 15–40 candidati potenziali da filtrare ulteriormente.
Ore necessarie: 3–5 ore.

**Quick Win 2 — Scraping PDF Comunicati Ufficiali Lega Pro (stagione 2025-26)**
Azione: scaricare i comunicati ufficiali di tesseramento della stagione corrente da legapro.com/comunicati-ufficiali (sezione "Tesseramenti"); estrarre con `pdfplumber` (Python) i campi nome, cognome, luogo di nascita, data; applicare filtro su comuni target (San Marino, Rimini, Pesaro-Urbino). I comunicati Lega Pro includono sistematicamente il luogo di nascita nei moduli di tesseramento.
Strumento: Python + `pdfplumber`, legapro.com (documenti pubblici).
Output atteso: lista strutturata di tutti i tesseramenti Serie C con luogo di nascita — 600–1.200 giocatori per stagione; dopo filtro geografico: stima 30–80 candidati da esaminare.
Ore necessarie: 4–6 ore (script + parsing + revisione manuale).

**Quick Win 3 — Incrocio BDFA Argentina con Lista Cognomi Tier 1**
Azione: da BDFA Argentina (bdfa.com.ar/jugadores.asp) estrarre i giocatori della Primera Nacional con cognome corrispondente alla lista Tier 1 sammarinese (Bollini, Ceccoli, Gasperoni, Selva, Tura, Macina, Della Valle, Sartini, Bucci, Mazza — lista di 25 cognomi). BDFA ha una funzione di ricerca per cognome accessibile via GET request scrapabile. Per ogni match: verificare data di nascita, luogo di nascita (se disponibile), curriculum giocatore.
Strumento: Python (requests), BDFA Argentina (pubblico), lista cognomi Tier 1 curata.
Output atteso: 5–20 candidati argentini da verificare genealogicamente.
Ore necessarie: 3–4 ore (script di ricerca sequenziale per cognome + revisione manuale).

---

## §7 — Strumenti Fuori Scope

| Strumento/Categoria | Motivo esclusione |
|---|---|
| LinkedIn Sales Navigator (€1.080/anno) | Costo sproporzionato per volume FSGC (<50 contatti/anno); free tier LinkedIn è sufficiente per ricerca manuale |
| Ancestry.com piano individuale (€120-300/anno) | Funzionalità sovrapposte con FamilySearch (gratuito) per il caso d'uso; valore aggiunto marginale rispetto a costo per Roadmap A |
| Data broker commerciali (es. Pipl, Spokeo) | Violazione GDPR Art. 5 (minimizzazione dati) e Art. 6 (mancanza base giuridica) per profilazione di individui privati senza consenso; incompatibile con uso istituzionale UEFA |
| Strumenti di "ethnic profiling" autonomi (es. NamePrism) | Non distinguono micro-nazionalità regionali italiane; classificazione "etnica" da nome senza verifica genealogica è metodologicamente inaffidabile per il caso sammarinese e potenzialmente soggetta a GDPR Art. 9 senza base giuridica adeguata |
| Wyscout piano base individuale (€299/anno) | Fornisce solo accesso video (70 min/mese), nessun dato statistico esportabile, nessun accesso API — non utile per screening automatizzato |
| InStat piano base (€420/anno) | Stesso problema: accesso video limitato, no dati bulk per screening |
| Piattaforme >€50K/anno (es. STATS Perform, Opta) | Costo assolutamente sproporzionato per budget FSGC; copertura e profondità eccedono i bisogni del sistema |
| CBF, AFA, FIGC database interni | Accesso riservato a federazioni nazionali — richiede accordo bilaterale FIFA. Non realisticamente ottenibile in Roadmap A; valutare per Roadmap B tramite canali UEFA |
| Football Australia Play Football | Nessun accesso pubblico; comunità sammarinese in Australia stimata <20 persone — ROI insufficiente per investire in accesso |
| NCAA registrations database | Non pubblico; rilevante solo come pipeline futura (3-5 anni), non per identificazione immediata |

---

## §8 — Struttura Presentazione FSGC al Federal Council

### Atto 1 — "Cosa esiste già funzionante" (5 minuti)

Elementi dimostrabili immediatamente:
- TITAN v5 demo: mostrare il HUD esistente con candidati già identificati nelle prime settimane di operatività del sistema.
- Quick Win 1 in diretta (opzionale): aprire Transfermarkt con filtro Serie C + birthplace San Marino durante la presentazione — mostra approccio riproducibile senza infrastruttura.
- KPI baseline (da popolare prima della presentazione):
  - X candidati identificati tramite Transfermarkt e BDFA
  - Y candidati con corrispondenza cognome Tier 1
  - Z candidati con collegamento genealogico preliminare confermato

Il messaggio chiave dell'Atto 1 è che il sistema produce candidati reali con un investimento di ore-lavoro, non di budget.

### Atto 2 — "Cosa il budget federale sblocca" (10 minuti)

Presentare la Roadmap B come abilitatore di scalabilità, non come prerequisito:
- **Wyscout API (€8.000–20.000/anno):** riduce i falsi positivi tecnici dal ~65% (filtraggio manuale) al ~15% (verifica statistica). Tempo per candidato: da 2-4 ore a 15 minuti.
- **Ancestry Institutional (€1.500–4.000/anno):** produce traccia documentale genealogica utilizzabile per presentazione formale eleggibilità a UEFA/FIFA.
- **Accordo CCSRE (€0 tecnico):** sblocca accesso ai registri consolari di ~7.000 cittadini sammarinesi all'estero — risorsa non acquistabile sul mercato.
- Costo annuo totale Roadmap B: **€10.000–25.000/anno** (variabile con numero di campionati Wyscout).
- ROI: ogni giocatore di livello Serie C stabile in rosa impatta direttamente il ranking UEFA — il ranking attuale di San Marino (fondo classifica) lascia ampio margine con l'aggiunta di 3-5 giocatori di qualità.

### Atto 3 — "KPI misurabili" (5 minuti)

Obiettivi concreti e verificabili:
- **6 mesi (Roadmap A operativa):**
  - ≥100 candidati nel database con scheda completa
  - ≥30 candidati con corrispondenza cognome Tier 1 e livello tecnico Serie C/equivalente
  - ≥10 candidati contattati tramite agente o canale diretto
- **12 mesi (con integrazione Roadmap B parziale):**
  - ≥5 candidati in fase di valutazione formale eleggibilità (documentazione genealogica in corso)
  - ≥1 candidato con eleggibilità confermata e contatto avanzato con staff tecnico FSGC
- **24 mesi (sistema in produzione):**
  - Sistema con aggiornamenti mensili automatici (moduli A1-A3 in produzione)
  - ≥3 giocatori eleggibili confermati in rosa o in trattativa attiva
  - Database di 500+ candidati profilati con scoring aggiornato

**Metrica di successo principale:** Non il numero di candidati trovati, ma il numero di candidati con eleggibilità documentata e livello tecnico verificato (Serie C o equivalente) disposti a rappresentare San Marino. Questa metrica distingue il sistema TITAN da un semplice database anagrafico.

---

*Documento prodotto per uso interno FSGC. I prezzi riportati sono verificati a maggio 2026 tramite fonti pubbliche e ricerca web diretta. Le stime per strumenti senza pricing pubblico sono indicate esplicitamente come tali. Il documento va aggiornato prima di ogni presentazione istituzionale con verifica dei prezzi commerciali aggiornati.*
