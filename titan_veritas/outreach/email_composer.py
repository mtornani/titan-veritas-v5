"""EmailComposer: Generates localized outreach emails using Jinja2 templates.

Supports Italian, Spanish, and English. Auto-detects language from country.
"""

import logging
from typing import List, Tuple

from jinja2 import Template

from ..core.models import CommunityContact

logger = logging.getLogger(__name__)

# Language detection by country
COUNTRY_LANGUAGE = {
    "argentina": "es",
    "uruguay": "es",
    "usa": "en",
    "uk": "en",
    "france": "fr",
    "belgium": "fr",
    "brazil": "pt",
    "germany": "de",
    "switzerland": "it",
    "italy": "it",
}

# Email templates per language
TEMPLATES = {
    "it": Template("""Gentile {{ contact_name }},

mi permetto di contattarLa in qualità di referente tecnico della Federazione Sammarinese Giuoco Calcio (FSGC), il cui mandato include l'identificazione di giovani talenti di origine sammarinese nel mondo.

La Comunità {{ fratellanza_name }} rappresenta un punto di riferimento fondamentale per la diaspora sammarinese a {{ city }}, e ci rivolgiamo a Voi con una richiesta specifica: siamo alla ricerca di ragazzi e ragazze di discendenza sammarinese (anche per linea materna), di età compresa tra i 14 e i 25 anni, che siano attualmente tesserati in campionati locali di calcio.

In particolare, siamo interessati a persone con i seguenti cognomi di origine sammarinese:
{% for surname in target_surnames %}- {{ surname }}
{% endfor %}
Anche varianti ortografiche dei suddetti cognomi sono di nostro interesse (es. Gasperoni/Gasparony, Mularoni/Mularony).

La eleggibilità per la Nazionale di San Marino avviene tramite Jus Sanguinis e non richiede la rinuncia alla cittadinanza attuale.

Se nella Vostra comunità ci sono giovani che corrispondono a questo profilo, Vi saremmo estremamente grati se poteste segnalarceli, anche semplicemente indicandoci un nome e un contatto.

Cordiali saluti,
Settore Tecnico FSGC
Federazione Sammarinese Giuoco Calcio"""),

    "es": Template("""Estimado/a {{ contact_name }},

Me dirijo a usted en calidad de referente técnico de la Federación de Fútbol de San Marino (FSGC), cuyo mandato incluye la identificación de jóvenes talentos de origen sanmarinense en el mundo.

La Comunidad {{ fratellanza_name }} es un referente fundamental para la diáspora sanmarinense en {{ city }}, y nos dirigimos a ustedes con un pedido específico: estamos buscando jóvenes de ascendencia sanmarinense (también por línea materna), de entre 14 y 25 años, que estén actualmente registrados en ligas locales de fútbol.

En particular, nos interesan personas con los siguientes apellidos de origen sanmarinense:
{% for surname in target_surnames %}- {{ surname }}
{% endfor %}
También nos interesan variantes ortográficas de estos apellidos (ej. Gasperoni/Gasparony, Mularoni/Mularony).

La elegibilidad para la Selección de San Marino se obtiene por Jus Sanguinis y no requiere renunciar a la ciudadanía actual.

Si en su comunidad hay jóvenes que correspondan a este perfil, les estaríamos muy agradecidos si pudieran indicarnos un nombre y un contacto.

Cordiales saludos,
Sector Técnico FSGC
Federación Sanmarinense de Fútbol"""),

    "en": Template("""Dear {{ contact_name }},

I am writing to you as a technical representative of the San Marino Football Federation (FSGC), whose mandate includes identifying young talents of San Marinese heritage worldwide.

The {{ fratellanza_name }} community is a vital reference point for the San Marinese diaspora in {{ city }}, and we reach out with a specific request: we are looking for young people of San Marinese descent (including through maternal lineage), aged 14 to 25, who are currently registered in local football/soccer leagues.

In particular, we are interested in individuals with the following surnames of San Marinese origin:
{% for surname in target_surnames %}- {{ surname }}
{% endfor %}
We are also interested in spelling variations of these surnames (e.g., Gasperoni/Gasparony, Mularoni/Mularony).

Eligibility for the San Marino National Team is through Jus Sanguinis and does not require renouncing current citizenship.

If there are young people in your community who match this profile, we would be extremely grateful if you could share a name and contact.

Best regards,
Technical Department FSGC
San Marino Football Federation"""),
}

SUBJECT_TEMPLATES = {
    "it": "Scouting FSGC - Ricerca Giovani Talenti Sammarinesi a {city}",
    "es": "Scouting FSGC - Búsqueda de Jóvenes Talentos Sanmarinenses en {city}",
    "en": "FSGC Scouting - Searching for San Marinese Youth Talent in {city}",
}


class EmailComposer:
    """Generates localized outreach emails."""

    def compose(self, contact: CommunityContact,
                target_surnames: List[str],
                language: str = None) -> Tuple[str, str]:
        """Generate a localized outreach email. Returns (subject, body)."""
        lang = language or self._detect_language(contact.country)

        template = TEMPLATES.get(lang, TEMPLATES["en"])
        subject_tmpl = SUBJECT_TEMPLATES.get(lang, SUBJECT_TEMPLATES["en"])

        # Use top 10 surnames to keep email focused
        top_surnames = [s.capitalize() for s in target_surnames[:10]]

        body = template.render(
            contact_name=contact.name or "Referente della Comunità",
            fratellanza_name=contact.fratellanza_name or f"Comunità Sammarinese di {contact.city}",
            city=contact.city,
            target_surnames=top_surnames,
        )

        subject = subject_tmpl.format(city=contact.city)

        return subject, body

    def _detect_language(self, country: str) -> str:
        """Detect the most appropriate language for a country."""
        return COUNTRY_LANGUAGE.get(country.lower(), "en")
