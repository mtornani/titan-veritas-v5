import httpx
from bs4 import BeautifulSoup
import time
import random
import logging
from typing import List, Optional
from datetime import datetime
from ..core.models import PlayerProfile
import re

class BDFAScraper:
    def __init__(self):
        self.client = httpx.Client(timeout=15.0)
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "es-AR,es;q=0.9,en-US;q=0.8,en;q=0.7",
        }

    def _dork_search(self, surname: str) -> List[str]:
        """Usa DuckDuckGo HTML per trovare le URL BDFA del giocatore."""
        query = f"site:bdfa.com.ar {surname}"
        url = "https://html.duckduckgo.com/html/"
        data = {"q": query}
        
        urls = []
        try:
            # DuckDuckGo HTML accetta POST per via form
            r = self.client.post(url, data=data, headers=self.headers)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')
            
            for a in soup.select('a.result__url'):
                href = a.get('href', '')
                # Pulizia URL dal redirect di DDG
                if 'bdfa.com.ar/jugadores' in href or 'bdfa.com.ar/jugador' in href:
                    clean_url = "https://" + href.split('//')[-1].strip()
                    if clean_url not in urls:
                        urls.append(clean_url)
                        
        except Exception as e:
            logging.error(f"Errore Dorking DuckDuckGo per {surname}: {e}")
            
        time.sleep(random.uniform(1.5, 3.0))
        return urls

    def parse_profile(self, url: str, target_surname: str) -> Optional[PlayerProfile]:
        try:
            r = self.client.get(url, headers=self.headers)
            if r.status_code != 200:
                return None
                
            soup = BeautifulSoup(r.text, 'html.parser')
            
            # BDFA ha il nome in un tag h1 o div specifico. Cerchiamo la testata
            header = soup.find('h1')
            if not header:
                header = soup.find("div", class_="titulo")
                
            full_name = header.text.strip() if header else ""
            
            # Sicurezza: controlliamo che il cognome cercato sia effettivamente nel nome della pagina
            if target_surname.lower() not in full_name.lower():
                return None
                
            # Info di base in tabelle o span
            # BDFA usa spesso una struttura testuale con <b>Etichetta:</b> Valore
            text_content = soup.get_text(separator=' ')
            
            # Estrazione Età/Nascita
            birth_date = None
            age = None
            birth_city = None
            birth_country = "Argentina" # Default ragionevole per BDFA, ma da verificare
            
            # Cerchiamo pattern di data: dd/mm/yyyy
            date_match = re.search(r'Fecha de [Nn]acimiento[^\d]*(\d{2}/\d{2}/\d{4})', r.text)
            if date_match:
                birth_date = date_match.group(1)
                try:
                    year = int(birth_date.split("/")[-1])
                    age = datetime.datetime.now().year - year
                except:
                    pass
            
            # Cerchiamo il luogo di nascita
            lugar_match = re.search(r'Lugar de [Nn]acimiento[^\w]*([A-Za-z\s\,]+)', soup.get_text())
            if lugar_match:
                place_raw = lugar_match.group(1).split('\n')[0].strip()
                if place_raw:
                    birth_city = place_raw
            
            # Estrazione Club Attuale
            current_club = ""
            club_match = re.search(r'Club [Aa]ctual[^\w]*([A-Za-z\s]+)', soup.get_text())
            if not club_match:
                # Alternativa: cerchiamo la tabella della trayectoria e prendiamo l'ultima squadra
                tables = soup.find_all('table')
                for t in tables:
                    if 'Equipo' in t.text or 'Club' in t.text:
                        rows = t.find_all('tr')
                        if len(rows) > 1:
                            last_row_cols = rows[-1].find_all('td')
                            if len(last_row_cols) > 1:
                                current_club = last_row_cols[1].text.strip()
            else:
                current_club = club_match.group(1).strip()
            
            # Nazione
            nacionalidad_match = re.search(r'Nacionalidad[^\w]*([A-Za-z\s]+)', soup.get_text())
            nationalities = ["Argentina"]
            if nacionalidad_match:
                nat = nacionalidad_match.group(1).split('\n')[0].strip()
                if nat:
                    nationalities = [nat]
            
            return PlayerProfile(
                first_name="",
                last_name=target_surname.capitalize(),
                known_as=full_name,
                birth_date=birth_date,
                age=age,
                birth_city=birth_city,
                birth_country=nationalities[0] if nationalities else "Argentina",
                nationalities=nationalities,
                current_club=current_club,
                source="BDFA",
                source_url=url
            )
            
        except Exception as e:
            logging.error(f"Errore parsing BDFA URL {url}: {e}")
            return None

    def search_by_surname(self, surname: str) -> List[PlayerProfile]:
        urls = self._dork_search(surname)
        profiles = []
        
        for url in urls[:10]: # Limitiamo a 10 per evitare ban e lunghi tempi di attesa
            p = self.parse_profile(url, surname)
            if p:
                profiles.append(p)
            time.sleep(random.uniform(1.0, 2.5))
            
        return profiles
