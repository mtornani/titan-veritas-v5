import os
import sqlite3
from typing import List, Tuple, Optional

from .models import PlayerProfile


def load_expanded_surnames(db_path: Optional[str] = None) -> Tuple[List[str], List[str]]:
    """Load surname lists from DB, including high-confidence variants.

    Falls back to hardcoded lists if DB is not available.
    Returns (tier1_names, tier2_names).
    """
    path = db_path or os.environ.get("TITAN_DB_PATH", "titan_veritas.db")
    if not os.path.exists(path):
        return TIER_1_NAMES, TIER_2_NAMES

    try:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row

        tier1 = []
        tier2 = []

        for row in conn.execute("SELECT name, tier FROM original_surname"):
            if row["tier"] == 1:
                tier1.append(row["name"].lower())
            else:
                tier2.append(row["name"].lower())

        for row in conn.execute(
            """SELECT sv.variant, os.tier FROM surname_variant sv
               JOIN original_surname os ON sv.original_surname_id = os.id
               WHERE sv.confidence >= 75"""
        ):
            name = row["variant"].lower()
            target = tier1 if row["tier"] == 1 else tier2
            if name not in target:
                target.append(name)

        conn.close()

        if tier1 or tier2:
            return tier1, tier2
    except Exception:
        pass

    return TIER_1_NAMES, TIER_2_NAMES


TIER_1_NAMES = [
    "gasperoni", "mularoni", "zonzini", "zafferani", "vitaioli", "bacciocchi", 
    "capicchioni", "tamagnini", "simoncini", "podeschi", "muraccini", "stolfi", 
    "cioncolini", "volpinari", "marquisio", "carchia", "guidi", "giardi", 
    "ghiotti", "maiani", "francini", "ercolani", "matteoni", "raschi", 
    "stefanelli", "benedettini", "tura", "mazzotti", "michelotti", "renzetti", 
    "salvini", "graziani", "zoli", "arcangeletti", "babboni", "bacchini", 
    "baffoni", "barberini", "bassanelli", "berdini", "biolini", "bufalini", 
    "bulletti", "busilacchi", "caciagli", "cagnoli", "calanca", "caporossi", 
    "caratu", "carosella", "carraresi", "cenerelli", "chiocci", "ciarlanti", 
    "cocchioni", "corgnoli", "cornacchia", "corvatta", "dall'olmo", "d'annibale", 
    "de biagi", "della balda", "della chiesa", "fagli", "falcinelli", "fancelli", 
    "focaccia", "forcellini", "galeotti", "ganganelli", "garantini", "ghinelli", 
    "grazzini", "gremigni", "gubinelli", "guagnelli", "leardini", "maniconi", 
    "mengucci", "mercadini", "molari", "muccioli", "pelliccioni", "pensi", 
    "petriccioli", "sancisi", "sassolini", "soncini", "stacchini", 
    "tomasucci", "vandi", "vignali", "zavoli"
]

TIER_2_NAMES = [
    "casadei", "zanotti", "selva", "rossi", "bianchi", "valentini", "ceci", 
    "della valle", "felici", "nanni", "pasolini", "berardi", "lombardi", 
    "riva", "amati", "angelini", "antonioli", "arlotti", "babbi", "bacci", 
    "balducci", "ballarini", "bartolini", "bellini", "beltrami", "benedetti", 
    "benvenuti", "bernardi", "berti", "bertoni", "bettini", "biagini", 
    "bolognesi", "bonelli", "boni", "bonini", "borghesi", "bortolotti", 
    "boschetti", "brandi", "brunelli", "calzolari", "capelli", "cardelli", 
    "cardinali", "carli", "carloni", "carnevali", "carpentieri", "carpi", 
    "carretti", "casali", "casini", "cassani", "castelli", "cattani", 
    "cavallini", "cenci", "cesarini", "cimatti", "cipriani", "cittadini", 
    "clementi", "cola", "coli", "colonna", "conti", "cordelli", "costantini", 
    "crespi", "crescenzi", "cugini", "d'addario", "damiani", "d'angelo", 
    "de carolis", "de luigi", "de marini", "de mattia", "de nittis", 
    "de santis", "dei", "del bianco", "demin", "dini", "donati", "donini", 
    "fabbri", "fabrizi", "falconi", "fattori", "federici", "fenucci", "fermi", 
    "ferracci", "ferrari", "ferretti", "filippi", "fiorini", "folli", "fonti", 
    "foschi", "franchi", "franco", "frisoni", "fuselli", "galassi", "gambetti", 
    "gambi", "gandolfi", "gatti", "giannini", "gobbi", "gori", "graziosi", 
    "guerrini", "lazzarini", "levi", "lisi", "lombardini", "lombardo", 
    "manuzzi", "marchetti", "marconi", "migani", "montanari", "morri", 
    "paolini", "pazzini", "proietti", "righi", "rinaldi", "santi", "sarzetto", 
    "sereni", "severi", "signorini", "sole", "sparta", "succi", "terenzi", 
    "tinti", "ugolini", "urbinati", "valenti", "venturini", "villa", "zaccaria"
]

# Blacklist di nomi che generano rumore OSINT (Giocatori Elite o non compatibili)
ELITE_NOISE_BLACKLIST = [
    "mbappe", "salah", "neymar", "haaland", "kane", "messi", "ronaldo", 
    "vinicius", "lewandowski", "de bruyne", "modric", "bellingham", "yamal",
    "osemhen", "gakpo", "xhaka", "lukaku", "pedri", "gavi"
]

def apply_filters_and_score(player: PlayerProfile) -> PlayerProfile:
    # 0. Filtro Elite Noise (Anti-Karpathy Noise)
    full_name = player.known_as.lower()
    if any(noise in full_name for noise in ELITE_NOISE_BLACKLIST):
        player.is_lethal_filtered = True
        player.filter_reason = "Noise Filter: Giocatore Elite (Non Eligibile)"
        return player

    # 1. Filtri Letali
    nats = [n.lower() for n in player.nationalities]
    if "italy" in nats or "italia" in nats or "italian" in nats:
        player.is_lethal_filtered = True
        player.filter_reason = "Nazionalità Italiana"
        return player
        
    if "san marino" in nats or "sammarinese" in nats:
        player.is_lethal_filtered = True
        player.filter_reason = "Già Sammarinese"
        return player
        
    if player.age is not None:
        if player.age < 16 or player.age > 32:
            player.is_lethal_filtered = True
            player.filter_reason = f"Età fuori stratosfera ({player.age})"
            return player

    # Controllo lega sammarinese
    club = (player.current_club or "").lower()
    smr_clubs = ["tre penne", "la fiorita", "tre fiori", "folgore", "murata", "virtus", "pennarossa", "faetano", "domagnano", "fiorentino", "juvenes", "san giovanni", "cosmos", "cailungo", "victor san marino"]
    if any(c in club for c in smr_clubs):
        player.is_lethal_filtered = True
        player.filter_reason = f"Club Sammarinese/Limitrofo ({club})"
        return player
        
    # Controllo lega italiana
    league = (player.current_league or "").lower()
    if "italy" in league or "italia" in league or "serie a" in league or "serie b" in league or "serie c" in league or "serie d" in league or "eccellenza" in league or "promozione" in league:
        player.is_lethal_filtered = True
        player.filter_reason = f"Campionato Italiano ({league})"
        return player

    # 2. Scoring
    score = 0
    player.score_breakdown = []
    ln = player.last_name.lower().strip()
    
    # Tier Cognome
    if ln in TIER_1_NAMES:
        score += 45
        player.tier = 1
        player.score_breakdown.append(f"Cognome Sammarinese Raro (Tier 1): +45pt")
    elif ln in TIER_2_NAMES:
        score += 15
        player.tier = 2
        player.score_breakdown.append(f"Cognome Diffuso (Tier 2): +15pt")
    else:
        player.tier = 3
        
    # Geografia (Nazione e Nascita)
    country = (player.birth_country or "").lower()
    birth_place = (player.birth_city or "").lower()
    geo_string = f"{country} {birth_place}"
    
    # Cluster storici Sammarinesi (Bonus +15)
    historical_hubs = ["detroit", "troy", "buenos aires", "pergamino", "cordoba", "san nicolas", "viedma"]
    matched_hubs = [h for h in historical_hubs if h in geo_string]
    if matched_hubs:
        score += 15
        player.score_breakdown.append(f"Cluster storico emigrazione ({', '.join(matched_hubs).capitalize()}): +15pt")
    
    if "argentina" in geo_string:
        score += 25
        player.score_breakdown.append(f"Nato in Argentina: +25pt")
    elif any(c in geo_string for c in ["usa", "united states", "brazil", "brasil", "france", "belgium"]):
        score += 20
        player.score_breakdown.append(f"Nato in nazione target (USA/BR/FR/BE): +20pt")
    elif country and country != "unknown":
        score += 10
        player.score_breakdown.append(f"Nato all'estero: +10pt")
        
    # Età
    age = player.age
    if age:
        if 16 <= age <= 21:
            score += 15
            player.score_breakdown.append(f"Fascia Prospetto (16-21 anni): +15pt")
        elif 22 <= age <= 26:
            score += 10
            player.score_breakdown.append(f"Fascia Prime (22-26 anni): +10pt")
        elif 27 <= age <= 32:
            score += 5
            player.score_breakdown.append(f"Fascia Esperto (27-32 anni): +5pt")

    if "div 1" in league or "premier" in league or "ncaa" in league:
        score += 15
        player.score_breakdown.append(f"Livello Sportivo Elevato: +15pt")
    elif league:
        score += 5
        player.score_breakdown.append(f"Campionato Attivo Schedato: +5pt")
        
    player.titan_score = score
    return player
