import csv
import json
from jinja2 import Environment, FileSystemLoader
import os
from datetime import datetime
from ..core.models import PlayerProfile

def export_csv(players, filepath):
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["Name", "Age", "Nationality", "Birthplace", "Club", "Score", "Tier", "URL"])
        for p in players:
            writer.writerow([
                p.known_as or f"{p.first_name} {p.last_name}",
                p.age,
                ",".join(p.nationalities),
                p.birth_city,
                p.current_club,
                p.titan_score,
                p.tier,
                p.source_url
            ])

def export_html(players, filepath):
    env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')))
    template = env.get_template('report.html')
    
    sorted_players = sorted(players, key=lambda x: x.titan_score, reverse=True)
    
    html_content = template.render(
        players=sorted_players,
        date=datetime.now().strftime("%Y-%m-%d %H:%M")
    )
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html_content)

def export_json(players, filepath):
    # Convertiamo gli oggetti PlayerProfile in dizionari semplici per il JSON
    data = []
    for p in players:
        data.append({
            "name": p.known_as or f"{p.first_name} {p.last_name}",
            "last_name": p.last_name,
            "age": p.age,
            "nationalities": p.nationalities,
            "birth_country": p.birth_country,
            "birth_city": p.birth_city,
            "current_club": p.current_club,
            "score": p.titan_score,
            "tier": p.tier,
            "source": p.source,
            "source_url": p.source_url,
            "breakdown": p.score_breakdown
        })
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
