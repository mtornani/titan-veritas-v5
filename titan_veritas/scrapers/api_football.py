"""API-Football integration — optimised for free tier (100 req/day).

Uses /players/squads endpoint to fetch entire rosters in a single call.
Implements SQLite caching to never waste calls on already-processed teams.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Optional

import httpx

from titan_veritas.config import (
    API_FOOTBALL_KEY,
    API_FOOTBALL_BASE,
    API_FOOTBALL_DAILY_LIMIT,
    ARGENTINA_LEAGUES,
)
from titan_veritas.core.models import PlayerProfile
from titan_veritas.db.connection import Database
from titan_veritas.db.repository import CacheRepo

logger = logging.getLogger(__name__)

CACHE_SOURCE = "api_football"


def _headers() -> dict:
    return {
        "x-apisports-key": API_FOOTBALL_KEY,
        "Accept": "application/json",
    }


def _check_api_key() -> bool:
    if not API_FOOTBALL_KEY:
        logger.warning("API_FOOTBALL_KEY not set — skipping API-Football queries")
        return False
    return True


class APIFootballClient:
    """Wrapper around API-Football with built-in caching."""

    def __init__(self, db: Database):
        self.db = db
        self.cache = CacheRepo(db)
        self.client = httpx.Client(
            base_url=API_FOOTBALL_BASE,
            headers=_headers(),
            timeout=30,
        )
        self._calls_today = 0

    @property
    def can_call(self) -> bool:
        return self._calls_today < API_FOOTBALL_DAILY_LIMIT

    def _get(self, endpoint: str, params: dict) -> Optional[dict]:
        """Make a cached API call."""
        cache_key = f"{endpoint}|{json.dumps(params, sort_keys=True)}"

        # Check cache first
        cached = self.cache.get(CACHE_SOURCE, cache_key)
        if cached:
            logger.debug(f"Cache hit: {cache_key}")
            return json.loads(cached)

        if not self.can_call:
            logger.warning("API-Football daily limit reached — skipping")
            return None

        if not _check_api_key():
            return None

        try:
            resp = self.client.get(endpoint, params=params)
            resp.raise_for_status()
            data = resp.json()
            self._calls_today += 1

            # Cache the response
            self.cache.put(CACHE_SOURCE, cache_key, json.dumps(data, ensure_ascii=False))
            logger.info(f"API-Football call #{self._calls_today}: {endpoint} {params}")
            return data
        except httpx.HTTPStatusError as e:
            logger.warning(f"API-Football HTTP {e.response.status_code} for {endpoint}")
            return None
        except Exception as e:
            logger.warning(f"API-Football error: {e}")
            return None

    def get_teams(self, league_id: int, season: int = 2025) -> list[dict]:
        """Get all teams in a league/season."""
        data = self._get("/teams", {"league": league_id, "season": season})
        if not data:
            return []
        return [t["team"] for t in data.get("response", [])]

    def get_squad(self, team_id: int) -> list[dict]:
        """Get full roster for a team via /players/squads — single API call.

        Returns list of player dicts with: id, name, age, number, position.
        """
        data = self._get("/players/squads", {"team": team_id})
        if not data:
            return []
        response = data.get("response", [])
        if response:
            return response[0].get("players", [])
        return []

    def search_players_by_surname(
        self,
        surname: str,
        target_leagues: list[int] | None = None,
    ) -> list[PlayerProfile]:
        """Search for players with a given surname across Argentine leagues.

        Strategy: iterate league → teams → squad roster, filter by surname.
        Uses caching so repeated runs don't waste API calls.
        """
        if target_leagues is None:
            target_leagues = list(ARGENTINA_LEAGUES.values())

        players = []

        for league_id in target_leagues:
            teams = self.get_teams(league_id)
            if not self.can_call and not teams:
                break

            for team_info in teams:
                team_id = team_info.get("id")
                team_name = team_info.get("name", "Unknown")

                squad = self.get_squad(team_id)
                for p in squad:
                    p_name = p.get("name", "")
                    if surname.lower() in p_name.lower():
                        # Split name (API-Football: "First Last" or "Last First")
                        parts = p_name.rsplit(" ", 1)
                        first_name = parts[0] if len(parts) > 1 else ""
                        last_name = parts[-1]

                        player = PlayerProfile(
                            first_name=first_name,
                            last_name=last_name,
                            api_football_id=p.get("id"),
                            age=p.get("age"),
                            position=p.get("position"),
                            current_club=team_name,
                            current_league=self._league_name(league_id),
                            birth_country="Argentina",
                        )
                        players.append(player)

                if not self.can_call:
                    logger.warning("Daily limit reached mid-scan — results may be partial")
                    break

        logger.info(f"API-Football: found {len(players)} matching '{surname}'")
        return players

    @staticmethod
    def _league_name(league_id: int) -> str:
        for name, lid in ARGENTINA_LEAGUES.items():
            if lid == league_id:
                return name
        return f"League {league_id}"

    def close(self):
        self.client.close()
