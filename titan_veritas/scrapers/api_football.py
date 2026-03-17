"""API-Football integration — optimised for free tier (100 req/day).

Uses /players/squads endpoint to fetch entire rosters in a single call.
Implements SQLite caching to never waste calls on already-processed teams.

Queue system:
    - `populate_queue()` discovers teams for all target leagues and saves them
      to the `api_queue` table.
    - `process_queue()` reads pending entries, processes up to `max_calls`
      squads, and marks them done. Run daily until the queue is empty.
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
    ALL_SURNAMES,
)
from titan_veritas.core.models import PlayerProfile
from titan_veritas.db.connection import Database
from titan_veritas.db.repository import CacheRepo

logger = logging.getLogger(__name__)

CACHE_SOURCE = "api_football"

# Correct league IDs for Argentine lower divisions (verified on API-Football)
ARGENTINA_LOWER_LEAGUES = {
    "Liga Profesional": 128,
    "Primera Nacional": 96,
    "Primera B Metropolitana": 233,
    "Primera C": 234,
    "Primera D": 235,
    "Torneo Federal A": 232,
    "Torneo Proyeccion": 521,
}


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
    """Wrapper around API-Football with built-in caching and queue support."""

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

    def get_teams(self, league_id: int, season: int = 2024) -> list[dict]:
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

    # ─── Queue management ─────────────────────────────────────────────────

    def populate_queue(self, leagues: dict[str, int] | None = None) -> int:
        """Discover teams for target leagues and add them to api_queue.

        Returns the number of new entries added.
        """
        if leagues is None:
            leagues = ARGENTINA_LOWER_LEAGUES

        # Ensure table exists
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS api_queue (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                league_id   INTEGER NOT NULL,
                league_name TEXT    NOT NULL,
                team_id     INTEGER NOT NULL,
                team_name   TEXT    NOT NULL DEFAULT '',
                status      TEXT    NOT NULL DEFAULT 'pending',
                processed_at TEXT,
                players_found INTEGER NOT NULL DEFAULT 0,
                UNIQUE(league_id, team_id)
            )
        """)
        self.db.commit()

        added = 0
        for league_name, league_id in leagues.items():
            teams = self.get_teams(league_id)
            for team in teams:
                tid = team.get("id")
                tname = team.get("name", "Unknown")
                try:
                    self.db.execute(
                        "INSERT OR IGNORE INTO api_queue "
                        "(league_id, league_name, team_id, team_name) VALUES (?, ?, ?, ?)",
                        (league_id, league_name, tid, tname),
                    )
                    added += 1
                except Exception:
                    pass

            if not self.can_call:
                logger.warning("Daily limit reached during queue population")
                break

        self.db.commit()
        logger.info(f"Queue: added {added} teams")
        return added

    def process_queue(self, max_calls: int = 95) -> dict:
        """Process pending queue entries up to max_calls API calls.

        Scans each team's squad for SM-surname matches, saves candidates.
        Returns summary dict with counts.
        """
        from titan_veritas.core.scoring import score_player
        from titan_veritas.db.repository import CandidateRepo

        repo = CandidateRepo(self.db)
        surname_set = {s.lower() for s in ALL_SURNAMES}

        # Get pending queue entries
        pending = self.db.execute(
            "SELECT * FROM api_queue WHERE status = 'pending' ORDER BY league_id"
        ).fetchall()

        processed = 0
        matches_found = 0
        calls_used = 0

        for entry in pending:
            if calls_used >= max_calls or not self.can_call:
                logger.info(f"Queue: stopping at {calls_used} calls (limit: {max_calls})")
                break

            team_id = entry["team_id"]
            team_name = entry["team_name"]
            league_name = entry["league_name"]

            # Check if already cached (free call)
            cache_key = f"/players/squads|{json.dumps({'team': team_id}, sort_keys=True)}"
            is_cached = self.cache.has(CACHE_SOURCE, cache_key)

            squad = self.get_squad(team_id)

            if not is_cached:
                calls_used += 1

            team_matches = 0
            for p_data in squad:
                p_name = p_data.get("name", "")
                p_name_lower = p_name.lower()

                for surname in ALL_SURNAMES:
                    if surname.lower() in p_name_lower:
                        parts = p_name.rsplit(" ", 1)
                        first_name = parts[0] if len(parts) > 1 else ""
                        last_name = parts[-1]

                        player = PlayerProfile(
                            first_name=first_name,
                            last_name=last_name,
                            api_football_id=p_data.get("id"),
                            age=p_data.get("age"),
                            position=p_data.get("position"),
                            current_club=team_name,
                            current_league=league_name,
                            birth_country="Argentina",
                        )
                        player = score_player(player)
                        if not player.is_filtered_out:
                            try:
                                repo.upsert(player)
                                team_matches += 1
                                matches_found += 1
                            except Exception:
                                pass
                        break

            # Mark as done
            self.db.execute(
                "UPDATE api_queue SET status = 'done', processed_at = datetime('now'), "
                "players_found = ? WHERE id = ?",
                (team_matches, entry["id"]),
            )
            self.db.commit()
            processed += 1

            if team_matches > 0:
                logger.info(f"Queue: {team_name} ({league_name}) -> {team_matches} matches")

        return {
            "processed": processed,
            "remaining": len(pending) - processed,
            "matches_found": matches_found,
            "api_calls_used": calls_used,
        }

    def queue_stats(self) -> dict:
        """Get queue status summary."""
        try:
            total = self.db.execute("SELECT COUNT(*) as c FROM api_queue").fetchone()["c"]
            done = self.db.execute(
                "SELECT COUNT(*) as c FROM api_queue WHERE status = 'done'"
            ).fetchone()["c"]
            pending = total - done
            found = self.db.execute(
                "SELECT COALESCE(SUM(players_found), 0) as s FROM api_queue WHERE status = 'done'"
            ).fetchone()["s"]
            return {"total": total, "done": done, "pending": pending, "matches_total": found}
        except Exception:
            return {"total": 0, "done": 0, "pending": 0, "matches_total": 0}

    # ─── Legacy methods (kept for backward compat) ────────────────────────

    def search_players_by_surname(
        self,
        surname: str,
        target_leagues: list[int] | None = None,
    ) -> list[PlayerProfile]:
        """Search for players with a given surname across Argentine leagues."""
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
        # Check both old and new league maps
        for name, lid in ARGENTINA_LOWER_LEAGUES.items():
            if lid == league_id:
                return name
        for name, lid in ARGENTINA_LEAGUES.items():
            if lid == league_id:
                return name
        return f"League {league_id}"

    def close(self):
        self.client.close()
