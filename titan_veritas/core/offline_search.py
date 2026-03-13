import csv
import re
import os
from typing import List
from .models import PlayerProfile
from .scoring import TIER_1_NAMES, TIER_2_NAMES, load_expanded_surnames
from .rosetta import RosettaStone

class OfflineSearchEngine:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        # Carichiamo i cognomi espansi (DB + varianti) se possibile
        t1, t2 = load_expanded_surnames()
        self.target_surnames = list(set(t1 + t2))
        self.rosetta = RosettaStone(self.target_surnames)

    def search_globals(self) -> List[PlayerProfile]:
        profiles = []
        if not os.path.exists(self.csv_path):
            print(f"Dataset non trovato in {self.csv_path}")
            return []

        # Compiliamo una regex gigante per tutti i cognomi per efficienza
        # Cerchiamo il cognome come parola intera \b per evitare falsi positivi parziali
        regex_pattern = r'\b(' + '|'.join(map(re.escape, self.target_surnames)) + r')\b'
        pattern = re.compile(regex_pattern, re.IGNORECASE)

        try:
            with open(self.csv_path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    full_name = row.get("FullName", "")
                    # Anche second name o middle name possono essere cognomi materni (v5.0)
                    search_string = f"{full_name} {row.get('Name', '')} {row.get('KnownAs', '')}"
                    nationality = row.get("Nationality", "")
                    
                    # Filtro preliminare: se è già ITA o SMR lo saltiamo subito
                    if nationality.lower() in ["italy", "san marino"]:
                        continue

                    # Split della stringa espansa per analizzare ogni parola come potenziale cognome
                    name_parts = search_string.replace("-", " ").split()
                    for part in name_parts:
                        is_match, confidence, reason = self.rosetta.is_likely_sammarinese(part)
                        
                        if is_match:
                            profiles.append(PlayerProfile(
                                first_name="", 
                                last_name=part.capitalize(),
                                known_as=full_name,
                                age=int(row.get("Age")) if row.get("Age") else None,
                                birth_country=nationality,
                                nationalities=[nationality],
                                current_club=row.get("Club", ""),
                                source="Offline DB (v5.0 Fuzzy)",
                                source_url=f"Offline ID: {row.get('ID')}",
                                titan_score=int(confidence / 2), # Base score basato sulla confidenza
                                score_breakdown=[f"Identificazione tramite {reason}"]
                            ))
                            break # Evitiamo di aggiungere lo stesso giocatore più volte se ha più parti matchanti
        except Exception as e:
            print(f"Errore durante la ricerca offline: {e}")

        return profiles
