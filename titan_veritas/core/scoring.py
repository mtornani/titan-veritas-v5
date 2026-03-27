"""Dynamic scoring engine with Bayesian-inspired weighting.

Formula:
    S_base = (W_geo + W_name + M_athletic) × V_osint
    A_bonus = exact age bonus if DOB known, else proxy estimation
    S_total = S_base + A_bonus

Proxy age estimation: if DOB unknown, deduce approximate age from
career_start_year (assuming debut at ~18) or league level.
"""

from __future__ import annotations

from datetime import date

from titan_veritas.config import TIER1_SURNAMES, TIER2_SURNAMES, DIASPORA_HUBS
from titan_veritas.core.models import PlayerProfile

# ─── Lethal filters (immediate rejection) ──────────────────────────────────

ELITE_NOISE = {
    "messi", "ronaldo", "mbappé", "mbappe", "neymar", "haaland",
    "salah", "de bruyne", "modric", "benzema", "lewandowski",
    "vinicius", "bellingham", "saka", "palmer",
}

SM_CLUBS = {
    "tre penne", "la fiorita", "tre fiori", "folgore", "libertas",
    "virtus", "murata", "faetano", "domagnano", "pennarossa",
    "cosmos", "cailungo", "fiorentino", "juvenes/dogana",
    "san giovanni",
}

ITALIAN_LEAGUES = {
    "serie a", "serie b", "serie c", "serie d",
    "eccellenza", "promozione", "prima categoria",
    "seconda categoria", "terza categoria",
}


def _is_elite_noise(p: PlayerProfile) -> str | None:
    full = p.full_name.lower()
    for name in ELITE_NOISE:
        if name in full:
            return f"Elite noise: {name}"
    return None


def _has_sm_or_italian_nationality(p: PlayerProfile) -> str | None:
    for nat in p.nationalities:
        nat_l = nat.lower()
        if "san marino" in nat_l or "sammarinese" in nat_l:
            return "Already has San Marino nationality"
    return None


def _is_in_sm_club(p: PlayerProfile) -> str | None:
    if p.current_club:
        for club in SM_CLUBS:
            if club in p.current_club.lower():
                return f"Already in SM club: {p.current_club}"
    return None


def _is_in_italian_league(p: PlayerProfile) -> str | None:
    if p.current_league:
        for league in ITALIAN_LEAGUES:
            if league in p.current_league.lower():
                return f"Italian league: {p.current_league}"
    return None


def _age_out_of_range(p: PlayerProfile) -> str | None:
    age = p.estimated_age
    if age is not None and (age < 16 or age > 38):
        return f"Age out of range: {age}"
    return None


LETHAL_FILTERS = [
    _is_elite_noise,
    _has_sm_or_italian_nationality,
    _is_in_sm_club,
    _is_in_italian_league,
    _age_out_of_range,
]


# ─── Scoring components ───────────────────────────────────────────────────

def _surname_score(p: PlayerProfile) -> tuple[float, int]:
    """Returns (W_name, tier)."""
    last = p.last_name.strip()
    for s in TIER1_SURNAMES:
        if s.lower() == last.lower():
            return 45.0, 1
    for s in TIER2_SURNAMES:
        if s.lower() == last.lower():
            return 30.0, 2
    return 0.0, 3


def _geographic_score(p: PlayerProfile) -> float:
    """W_geo — based on birth country or nationalities."""
    # Check birth country
    if p.birth_country:
        for country, weight in DIASPORA_HUBS.items():
            if country.lower() in p.birth_country.lower():
                return float(weight)
    # Check nationalities
    for nat in p.nationalities:
        for country, weight in DIASPORA_HUBS.items():
            if country.lower() in nat.lower():
                return float(weight)
    return 0.0


def _athletic_score(p: PlayerProfile) -> float:
    """M_athletic — based on club/league verification."""
    if p.current_club:
        if p.current_league:
            league_l = p.current_league.lower()
            # Youth/development leagues → higher athletic interest
            if any(kw in league_l for kw in ("proyección", "reserva", "primera d", "primera c")):
                return 25.0
            if any(kw in league_l for kw in ("federal", "primera b", "primera nacional")):
                return 20.0
            return 15.0  # Known club, any league
        return 10.0  # Club but no league info
    return 0.0  # Unknown club status


def _osint_multiplier(p: PlayerProfile) -> float:
    """V_osint — CEMLA/Ellis Island confirmation multiplier."""
    if p.cemla_hit and p.ellis_island_hit:
        return 1.8  # Both sources confirm
    if p.cemla_hit or p.ellis_island_hit:
        return 1.5  # One source confirms
    return 1.0  # No OSINT confirmation


def _age_bonus(p: PlayerProfile) -> tuple[float, str]:
    """A_bonus — exact if DOB known, proxy if estimated.

    Returns (bonus, method_description).
    """
    age = p.estimated_age
    if age is None:
        # No age data at all — use league-level proxy
        return _league_proxy_bonus(p), "league_proxy"

    # Determine if this is exact or proxy-estimated
    method = "exact" if p.date_of_birth else "career_proxy"

    if 16 <= age <= 21:
        return 20.0, method
    elif 22 <= age <= 26:
        return 10.0, method
    elif 27 <= age <= 31:
        return 0.0, method
    elif age >= 32:
        return -10.0, method
    return 0.0, method


def _league_proxy_bonus(p: PlayerProfile) -> float:
    """When no age data exists, estimate youth probability from league level."""
    if p.current_league:
        league_l = p.current_league.lower()
        # Youth/reserve leagues → likely young
        if any(kw in league_l for kw in ("proyección", "reserva", "youth", "juvenil")):
            return 15.0
        # Lower divisions → slightly more likely to be young
        if any(kw in league_l for kw in ("primera d", "primera c")):
            return 8.0
    return 0.0  # Can't estimate — no penalty


# ─── Main scoring function ─────────────────────────────────────────────────

def score_player(p: PlayerProfile) -> PlayerProfile:
    """Apply lethal filters and dynamic scoring to a player.

    Modifies and returns the same PlayerProfile with score fields populated.
    """
    # Step 1: Lethal filters
    for filter_fn in LETHAL_FILTERS:
        reason = filter_fn(p)
        if reason:
            p.is_filtered_out = True
            p.filter_reason = reason
            p.titan_score = 0.0
            return p

    # Step 1b: Preserve manual filters (tier3_cutoff, duplicate_of)
    _MANUAL_PREFIXES = ("tier3_cutoff", "duplicate_of")
    existing_reason = getattr(p, "filter_reason", None) or ""
    if existing_reason.startswith(_MANUAL_PREFIXES):
        # Keep the manual filter — only update the score, don't un-filter
        pass
    else:
        p.is_filtered_out = False
        p.filter_reason = None

    # Step 2: Calculate score components
    w_name, tier = _surname_score(p)
    w_geo = _geographic_score(p)
    m_athletic = _athletic_score(p)
    v_osint = _osint_multiplier(p)
    a_bonus, age_method = _age_bonus(p)

    # S_base = (W_geo + W_name + M_athletic) × V_osint
    s_base = (w_geo + w_name + m_athletic) * v_osint
    s_total = s_base + a_bonus

    p.titan_score = round(max(0, s_total), 1)
    p.tier = tier
    p.score_breakdown = {
        "W_name": w_name,
        "W_geo": w_geo,
        "M_athletic": m_athletic,
        "V_osint": v_osint,
        "A_bonus": a_bonus,
        "age_method": age_method,
        "S_base": round(s_base, 1),
        "S_total": round(s_total, 1),
    }

    return p
