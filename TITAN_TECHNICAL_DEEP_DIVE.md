# TITAN VERITAS v4.0: Engineering the Search for the "Hidden Sammarinese"

### The Problem Space
San Marino is the lowest-ranked FIFA member. To compete, they must leverage **Jus Sanguinis** (citizenship by descent). However, there is no "San Marino Heritage" checkbox in global databases. Information about ancestry is latent—hidden in plain sight within surnames and geographic migration patterns.

### Pipeline Architecture: The Inverse Funnel
Previous iterations relied on standard web scraping (BDFA, Transfermarkt). This approach is bottlenecked by search engine indexing and bot-detection. v4.0 pivots to an **Inverse Funnel** architecture.

#### 1. The Global Pool (High Latency -> Low Latency)
We ingest massive, multi-megabyte datasets (FIFA/FM database dumps) containing ~20k-400k players. 
- **Tech Stack:** Python + Regex Engine + Pandas.
- **Process:** We perform an offline, case-insensitive Regex match against a dictionary of 230+ "San Marino-specific" surnames. We don't just search for "Rossi"; we search for `\b(Gasperoni|Zonzini|...)\b` across `FullName` and `KnownAs`.

#### 2. The Heuristic Scoring Engine (TITAN Score)
Matching a surname is just a "Suspect Signal." We need a probability score.
- **Tier Weights:** Surnames are categorized by uniqueness. A "Gasperoni" (San Marino core) starts with a base score of 45. A "Rossi" (Common in Italy/San Marino) starts with 15.
- **Geographic Cluster Mapping:** Migration hubs like **Detroit (USA)** and **Pergamino (Argentina)** are hardcoded as high-probability clusters. Birth in these hubs triggers a +15 score multiplier.
- **Lethal Filters:** To minimize noise, we immediately discard anyone playing in the FIGC (Italian) or San Marino internal leagues. These players are already "known" or "within reach."

#### 3. Enrichment (Targeted Deep-Scraping)
Once the "Pool of Suspects" is narrow (~100 candidates), we launch targeted **Async Spiders** using the `Scrapling` framework. We don't spiderman across the web; we snipe. 
- **DuckDuckGo Dorking:** We use search-engine-as-a-proxy to find the exact BDFA/Transfermarkt URL for a specific name to bypass front-door bot detection.

### Why it Works
This isn't just a scraper; it's a **Latent Signal Detector**. By combining massive offline matching with surgical online enrichment, we transform San Marino's scouting from "hearsay and word of mouth" to a data-driven operation center.

---
*Authored by Antigravity AI for FSGC (Federazione Sammarinese Giuoco Calcio)* 🇸🇲⚽
