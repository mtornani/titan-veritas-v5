import httpx

query = """
SELECT ?item ?itemLabel ?birthDate ?birthPlaceLabel ?countryLabel ?clubLabel WHERE {
  ?item wdt:P31 wd:Q5;
        wdt:P106 wd:Q937857.
  
  ?item rdfs:label ?itemLabel.
  FILTER(LANG(?itemLabel) = "en" || LANG(?itemLabel) = "es" || LANG(?itemLabel) = "it" || LANG(?itemLabel) = "pt" || LANG(?itemLabel) = "de")
  FILTER(REGEX(?itemLabel, "Zonzini", "i"))
  
  OPTIONAL { ?item wdt:P569 ?birthDate. }
  OPTIONAL { ?item wdt:P19 ?birthPlace. ?birthPlace rdfs:label ?birthPlaceLabel. FILTER(LANG(?birthPlaceLabel) = "en") }
  OPTIONAL { ?item wdt:P27 ?country. ?country rdfs:label ?countryLabel. FILTER(LANG(?countryLabel) = "en") }
  OPTIONAL { ?item wdt:P54 ?club. ?club rdfs:label ?clubLabel. FILTER(LANG(?clubLabel) = "en") }
} LIMIT 10
"""

r = httpx.get("https://query.wikidata.org/sparql", params={'query': query}, headers={"Accept": "application/sparql-results+json"})
print(r.status_code)
data = r.json()
print("Results:", len(data.get("results", {}).get("bindings", [])))
for row in data.get("results", {}).get("bindings", []):
    print(row.get("itemLabel", {}).get("value"))
    print(row.get("birthDate", {}).get("value"))
