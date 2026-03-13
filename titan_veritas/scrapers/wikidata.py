import httpx
from typing import List
import datetime
import logging
from ..core.models import PlayerProfile

class WikidataScraper:
    def __init__(self):
        self.endpoint_url = "https://query.wikidata.org/sparql"
        self.headers = {
            "User-Agent": "TitanVeritas/1.0 (mirkotornani@fsgc.sm) Python/httpx Wikidata",
            "Accept": "application/sparql-results+json"
        }

    def search_by_surname(self, surname: str) -> List[PlayerProfile]:
        query = f"""
        SELECT ?item ?itemLabel ?birthDate ?birthPlaceLabel ?countryLabel ?clubLabel WHERE {{
          ?item wdt:P31 wd:Q5;
                wdt:P106 wd:Q937857.
          
          ?item rdfs:label ?itemLabel.
          FILTER(LANG(?itemLabel) = "en" || LANG(?itemLabel) = "es" || LANG(?itemLabel) = "it" || LANG(?itemLabel) = "pt" || LANG(?itemLabel) = "de")
          FILTER(REGEX(?itemLabel, "{surname}$", "i"))
          
          OPTIONAL {{ ?item wdt:P569 ?birthDate. }}
          OPTIONAL {{ ?item wdt:P19 ?birthPlace. ?birthPlace rdfs:label ?birthPlaceLabel. FILTER(LANG(?birthPlaceLabel) = "en") }}
          OPTIONAL {{ ?item wdt:P27 ?country. ?country rdfs:label ?countryLabel. FILTER(LANG(?countryLabel) = "en") }}
          OPTIONAL {{ ?item wdt:P54 ?club. ?club rdfs:label ?clubLabel. FILTER(LANG(?clubLabel) = "en") }}
          
          FILTER(YEAR(?birthDate) >= 1992 && YEAR(?birthDate) <= 2010)
        }} LIMIT 50
        """
        
        profiles = []
        try:
            r = httpx.get(self.endpoint_url, params={'query': query}, headers=self.headers, timeout=20.0)
            r.raise_for_status()
            data = r.json()
            
            players_map = {}
            for row in data.get("results", {}).get("bindings", []):
                item_url = row.get("item", {}).get("value", "")
                
                if item_url not in players_map:
                    bd_str = row.get("birthDate", {}).get("value", "")
                    age = None
                    if bd_str:
                        try:
                            year = int(bd_str.split("-")[0])
                            age = datetime.datetime.now().year - year
                        except:
                            pass
                            
                    players_map[item_url] = PlayerProfile(
                        first_name="", 
                        last_name=surname.capitalize(),
                        known_as=row.get("itemLabel", {}).get("value", "Unknown"),
                        birth_date=bd_str.split("T")[0] if bd_str else None,
                        age=age,
                        birth_city=row.get("birthPlaceLabel", {}).get("value", ""),
                        nationalities=[row.get("countryLabel", {}).get("value", "")] if row.get("countryLabel") else [],
                        current_club=row.get("clubLabel", {}).get("value", ""),
                        birth_country=row.get("countryLabel", {}).get("value", ""),
                        source="Wikidata",
                        source_url=item_url
                    )
                else:
                    club = row.get("clubLabel", {}).get("value", "")
                    if club and club not in (players_map[item_url].current_club or ""):
                        if players_map[item_url].current_club:
                            players_map[item_url].current_club += f", {club}"
                        else:
                            players_map[item_url].current_club = club
            
            for p in players_map.values():
                parts = p.known_as.split()
                if len(parts) > 1:
                    p.first_name = " ".join(parts[:-1])
                    p.last_name = parts[-1]
                else:
                    p.first_name = p.known_as
                profiles.append(p)
                
        except Exception as e:
            logging.error(f"Wikidata error for {surname}: {e}")
            
        return profiles
