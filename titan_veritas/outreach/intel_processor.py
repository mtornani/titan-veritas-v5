"""IntelProcessor: Gemini LLM response parsing with 3-tier fallback.

Tier 1: Parse JSON from LLM response (schema-validated)
Tier 2: Retry with constrained prompt (up to 3x, tenacity backoff)
Tier 3: Regex fallback extraction (name/email/phone patterns)
"""

import re
import json
import logging
from typing import Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ..core.models import IntelResult

logger = logging.getLogger(__name__)

# Regex patterns for fallback extraction
NAME_PATTERN = re.compile(
    r'\b([A-Z][a-zà-ú]{1,20}\s+[A-Z][a-zà-ú]{1,30})\b'
)
EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
)
PHONE_PATTERN = re.compile(
    r'[\+]?\d[\d\s\-\(\)]{6,18}\d'
)
CLUB_KEYWORDS = [
    "club", "fc", "cf", "ac", "sc", "asd", "u17", "u15", "u19", "u20",
    "youth", "juvenil", "infantil", "cadete", "reserve",
]

EXTRACTION_PROMPT = """Analizza la seguente risposta email ricevuta da una comunità sammarinese all'estero.
Estrai le seguenti informazioni e rispondi ESCLUSIVAMENTE in formato JSON valido:

{
  "names": ["Lista di nomi completi di calciatori/ragazzi menzionati"],
  "contacts": ["email, telefoni o altri contatti menzionati"],
  "clubs_mentioned": ["Nomi di club, squadre o leghe menzionate"],
  "confidence": 0.0-1.0
}

Se non trovi informazioni per un campo, usa una lista vuota [].
Se l'email non contiene informazioni rilevanti sullo scouting, imposta confidence a 0.0.

IMPORTANTE: Rispondi SOLO con il JSON, senza testo aggiuntivo.

--- INIZIO EMAIL ---
{email_text}
--- FINE EMAIL ---"""

CONSTRAINED_PROMPT = """You MUST respond with ONLY valid JSON. No explanations, no markdown.
Extract player names, contacts, and club names from this email reply.

Required JSON format:
{{"names": [], "contacts": [], "clubs_mentioned": [], "confidence": 0.0}}

Email:
{email_text}"""


class IntelProcessor:
    """Processes email responses using Gemini LLM with robust fallback."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.api_url = (
            f"https://generativelanguage.googleapis.com/v1beta/"
            f"models/gemini-pro:generateContent?key={self.api_key}"
        )

    async def extract_intelligence(self, email_text: str) -> IntelResult:
        """Extract structured intelligence from an email reply.

        Uses 3-tier fallback: LLM JSON → constrained retry → regex.
        """
        # Tier 1: Standard LLM extraction
        result = await self._try_llm_extraction(email_text, EXTRACTION_PROMPT)
        if result:
            logger.info(f"[IntelProcessor] Tier 1 success: {len(result.names)} names extracted")
            return result

        # Tier 2: Constrained prompt retry
        result = await self._try_llm_extraction(email_text, CONSTRAINED_PROMPT)
        if result:
            logger.info(f"[IntelProcessor] Tier 2 success: {len(result.names)} names extracted")
            return result

        # Tier 3: Regex fallback
        logger.warning("[IntelProcessor] LLM extraction failed, using regex fallback")
        return self._fallback_regex_extraction(email_text)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _call_gemini(self, prompt: str) -> str:
        """Call Gemini API with retry logic."""
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.1,
                "maxOutputTokens": 1024,
            },
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(self.api_url, json=payload)
            response.raise_for_status()
            data = response.json()

        # Extract text from Gemini response
        candidates = data.get("candidates", [])
        if not candidates:
            raise ValueError("No candidates in Gemini response")

        parts = candidates[0].get("content", {}).get("parts", [])
        if not parts:
            raise ValueError("No parts in Gemini response")

        return parts[0].get("text", "")

    async def _try_llm_extraction(self, email_text: str,
                                  prompt_template: str) -> Optional[IntelResult]:
        """Attempt LLM extraction with a given prompt template."""
        try:
            prompt = prompt_template.format(email_text=email_text[:3000])
            raw_response = await self._call_gemini(prompt)
            return self._validate_llm_response(raw_response)
        except Exception as e:
            logger.warning(f"[IntelProcessor] LLM extraction failed: {e}")
            return None

    def _validate_llm_response(self, raw_text: str) -> Optional[IntelResult]:
        """Validate and parse the LLM JSON response."""
        # Strip markdown code blocks if present
        text = raw_text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # Try to find JSON within the text
            json_match = re.search(r'\{[\s\S]*\}', text)
            if json_match:
                try:
                    data = json.loads(json_match.group())
                except json.JSONDecodeError:
                    return None
            else:
                return None

        # Validate structure
        names = data.get("names", [])
        contacts = data.get("contacts", [])
        clubs = data.get("clubs_mentioned", [])
        confidence = float(data.get("confidence", 0.5))

        if not isinstance(names, list):
            names = []
        if not isinstance(contacts, list):
            contacts = []
        if not isinstance(clubs, list):
            clubs = []

        return IntelResult(
            names=[str(n) for n in names],
            contacts=[str(c) for c in contacts],
            clubs_mentioned=[str(c) for c in clubs],
            confidence=min(1.0, max(0.0, confidence)),
            raw_text=raw_text,
        )

    def _fallback_regex_extraction(self, email_text: str) -> IntelResult:
        """Extract intelligence using regex patterns when LLM fails."""
        # Extract potential names (Capitalized First Last patterns)
        names = NAME_PATTERN.findall(email_text)
        # Filter out common false positives
        names = [n for n in names if not any(
            w.lower() in n.lower() for w in [
                "Dear", "Gentile", "Estimado", "Cordial", "Saluti",
                "San Marino", "Best Regards", "Cordiali",
            ]
        )]

        # Extract emails and phones
        contacts = EMAIL_PATTERN.findall(email_text)
        phones = PHONE_PATTERN.findall(email_text)
        contacts.extend(phones)

        # Extract club mentions
        clubs = []
        text_lower = email_text.lower()
        for keyword in CLUB_KEYWORDS:
            # Find the keyword and grab surrounding words
            for match in re.finditer(rf'\b\w*{keyword}\w*\b', text_lower):
                start = max(0, match.start() - 30)
                end = min(len(email_text), match.end() + 30)
                context = email_text[start:end].strip()
                clubs.append(context)

        confidence = 0.3 if names else 0.1

        return IntelResult(
            names=list(set(names))[:10],
            contacts=list(set(contacts))[:10],
            clubs_mentioned=list(set(clubs))[:5],
            confidence=confidence,
            raw_text=email_text[:500],
        )
