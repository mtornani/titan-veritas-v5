# Database storico delle migrazioni sammarinesi documentate (v5.0 Ground Truth)
# Dati estratti da archivi storici, registri delle Fratellanze e manifesti di sbarco.

HISTORICAL_RECORDS = [
    {"original": "Gasperoni", "variant": "Gasparony", "location": "Detroit", "year": 1912, "context": "Ellis Island Manifest"},
    {"original": "Mularoni", "variant": "Mularony", "location": "Pergamino", "year": 1895, "context": "CEMLA Argentina"},
    {"original": "Zonzini", "variant": "Sonzini", "location": "Buenos Aires", "year": 1905, "context": "CEMLA Argentina"},
    {"original": "Beccari", "variant": "Beckary", "location": "Detroit", "year": 1920, "context": "US Census 1920"},
    {"original": "Selva", "variant": "Silvas", "location": "San Paolo", "year": 1888, "context": "Registros de Imigração BR"},
    {"original": "Francini", "variant": "Franchini", "location": "Francia", "year": 1930, "context": "Fratellanza Parigi"},
    {"original": "Valentini", "variant": "Valentine", "location": "USA", "year": 1910, "context": "Ellis Island"},
    {"original": "Tura", "variant": "Thura", "location": "Belgio", "year": 1947, "context": "Minatori Charleroi"},
]

def get_historical_variants():
    return HISTORICAL_RECORDS
