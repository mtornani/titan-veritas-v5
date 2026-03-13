from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class PlayerProfile:
    first_name: str
    last_name: str
    known_as: Optional[str] = None
    birth_date: Optional[str] = None
    age: Optional[int] = None
    birth_city: Optional[str] = None
    birth_country: Optional[str] = None
    nationalities: List[str] = field(default_factory=list)
    current_club: Optional[str] = None
    current_league: Optional[str] = None
    international_caps: List[str] = field(default_factory=list)
    source: str = ""
    source_url: str = ""

    titan_score: int = 0
    tier: int = 0
    is_lethal_filtered: bool = False
    filter_reason: str = ""
    score_breakdown: List[str] = field(default_factory=list)


# --- v5.0 Models ---

@dataclass
class SurnameVariant:
    original: str
    variant: str
    confidence: float
    method: str  # 'metaphone', 'soundex', 'fuzzy', 'exact', 'archive_record'
    source: str = ""
    source_url: str = ""


@dataclass
class RawArchiveRecord:
    surname: str
    origin: str  # place of origin (e.g. "San Marino")
    destination: str  # destination city/country
    year: Optional[int] = None
    source_url: str = ""


@dataclass
class CommunityContact:
    name: str
    email: str
    city: str
    country: str
    fratellanza_name: str = ""
    cluster_id: Optional[int] = None


@dataclass
class IntelResult:
    names: List[str] = field(default_factory=list)
    contacts: List[str] = field(default_factory=list)
    clubs_mentioned: List[str] = field(default_factory=list)
    confidence: float = 0.0
    raw_text: str = ""


@dataclass
class PipelineStats:
    emails_sent: int = 0
    replies_received: int = 0
    intel_extracted: int = 0
    candidates_found: int = 0
    errors: int = 0
