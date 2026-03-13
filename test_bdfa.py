from titan_veritas.scrapers.bdfa import BDFAScraper

def main():
    print("Testing BDFA Scraper...")
    scraper = BDFAScraper()
    players = scraper.search_by_surname("Zonzini")
    print(f"Trovati {len(players)} giocatori:")
    for p in players:
        print(f"- {p.known_as} | Nascita: {p.birth_city} ({p.birth_date}) | Club: {p.current_club} | Età: {p.age}")
        
if __name__ == "__main__":
    main()
