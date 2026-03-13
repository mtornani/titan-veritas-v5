import jellyfish
from thefuzz import fuzz, process
from typing import List, Dict, Tuple
from .archive_data import get_historical_variants

class RosettaStone:
    """Motore di espansione fonetica e fuzzy per cognomi sammarinesi"""
    
    def __init__(self, original_surnames: List[str]):
        self.originals = [s.lower() for s in original_surnames]
        self.historical_variants = get_historical_variants()

    def generate_phonetic_variants(self, target: str) -> Dict[str, str]:
        """Genera i codici fonetici per un cognome"""
        return {
            "soundex": jellyfish.soundex(target),
            "metaphone": jellyfish.metaphone(target),
            "match_rating": jellyfish.match_rating_codex(target)
        }

    def find_fuzzy_matches(self, candidate: str, threshold: int = 90) -> List[Tuple[str, int]]:
        """Trova i cognomi originali più vicini a un candidato (Soglia alzata a 90 per v5.0)"""
        if len(candidate) < 4:
            threshold = 95 # Molto più severo su nomi brevi (es. Sema, Salah)
        matches = process.extract(candidate.lower(), self.originals, limit=3)
        return [match for match in matches if match[1] >= threshold]

    def is_likely_sammarinese(self, candidate: str) -> Tuple[bool, float, str]:
        """
        Analisi combinata: Ground Truth + Fonetica + Fuzzy.
        """
        candidate = candidate.lower()
        
        # 1. Match esatto
        if candidate in self.originals:
            return True, 100.0, "Match Esatto"

        # 2. Ground Truth (Archive Records)
        for record in self.historical_variants:
            if candidate == record['variant'].lower():
                return True, 95.0, f"Documentato in {record['context']} ({record['location']})"

        # 3. Match Fuzzy (Distanza Levenshtein)
        fuzzy_matches = self.find_fuzzy_matches(candidate)
        if fuzzy_matches:
            best_match, score = fuzzy_matches[0]
            return True, float(score), f"Fuzzy Match con {best_match.capitalize()}"

        # 4. Match Fonetico (Metaphone) - Solo per nomi di lunghezza > 4 per evitare falsi positivi
        if len(candidate) > 4:
            cand_meta = jellyfish.metaphone(candidate)
            for original in self.originals:
                if len(original) > 4 and jellyfish.metaphone(original) == cand_meta:
                    return True, 75.0, f"Match Fonetico (Metaphone) con {original.capitalize()}"

        return False, 0.0, "Nessuna correlazione trovata"

if __name__ == "__main__":
    rosetta = RosettaStone(["Gasperoni", "Mularoni", "Zonzini"])
    test_cases = ["Gasparony", "Mularony", "Zonzinis", "Smith"]
    
    for tc in test_cases:
        match, conf, reason = rosetta.is_likely_sammarinese(tc)
        print(f"Candidate: {tc} -> Match: {match} (Conf: {conf}%) | Reason: {reason}")
