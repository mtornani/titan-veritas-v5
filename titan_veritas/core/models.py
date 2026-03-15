"""Domain models for TITAN VERITAS."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import date
from typing import Optional


@dataclass
class PlayerProfile:
    """Canonical representation of a candidate player."""

    first_name: str
    last_name: str
    # Identifiers
    wikidata_qid: Optional[str] = None
    bdfa_id: Optional[str] = None
    api_football_id: Optional[int] = None
    # Bio
    date_of_birth: Optional[date] = None
    age: Optional[int] = None
    birth_place: Optional[str] = None
    birth_country: Optional[str] = None
    nationalities: list[str] = field(default_factory=list)
    # Football
    current_club: Optional[str] = None
    current_league: Optional[str] = None
    position: Optional[str] = None
    career_start_year: Optional[int] = None
    # Scoring
    titan_score: float = 0.0
    tier: int = 3
    score_breakdown: dict = field(default_factory=dict)
    is_filtered_out: bool = False
    filter_reason: Optional[str] = None
    # OSINT
    cemla_hit: bool = False
    ellis_island_hit: bool = False
    osint_details: dict = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"

    @property
    def estimated_age(self) -> Optional[int]:
        """Age from DOB or proxy estimation from career start."""
        if self.date_of_birth:
            today = date.today()
            return today.year - self.date_of_birth.year - (
                (today.month, today.day) < (self.date_of_birth.month, self.date_of_birth.day)
            )
        if self.career_start_year:
            # Assume debut at ~18
            return date.today().year - self.career_start_year + 18
        return self.age

    def to_dict(self) -> dict:
        d = asdict(self)
        if self.date_of_birth:
            d["date_of_birth"] = self.date_of_birth.isoformat()
        d["estimated_age"] = self.estimated_age
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


@dataclass
class SurnameEntry:
    """A San Marino endemic surname with tier classification."""

    name: str
    tier: int  # 1 = endemic, 2 = high probability, 3 = variant
    incidence: int = 0
    variants: list[str] = field(default_factory=list)


@dataclass
class GeographicCluster:
    """A known San Marino diaspora community."""

    city: str
    country: str
    fratellanza_name: Optional[str] = None
    contact_info: Optional[str] = None
