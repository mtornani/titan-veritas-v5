"""TelegramRouter: Enhanced notifier with structured intelligence cards."""

import logging
from typing import Optional

import httpx

from ..core.models import IntelResult, PipelineStats

logger = logging.getLogger(__name__)


class TelegramRouter:
    """Enhanced Telegram bot for TITAN VERITAS notifications."""

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    async def send_intel_card(self, intel: IntelResult,
                              source_community: str,
                              outreach_id: int = None) -> bool:
        """Send a structured intelligence card to Telegram."""
        # Build markdown message
        lines = [
            f"*TITAN VERITAS - Intelligence Alert*",
            f"_Fonte: {source_community}_",
            "",
        ]

        if intel.names:
            lines.append("*Nomi Estratti:*")
            for name in intel.names:
                lines.append(f"  - {name}")
            lines.append("")

        if intel.contacts:
            lines.append("*Contatti:*")
            for contact in intel.contacts:
                lines.append(f"  - `{contact}`")
            lines.append("")

        if intel.clubs_mentioned:
            lines.append("*Club Menzionati:*")
            for club in intel.clubs_mentioned:
                lines.append(f"  - {club}")
            lines.append("")

        confidence_bar = self._confidence_bar(intel.confidence)
        lines.append(f"*Confidenza:* {confidence_bar} ({intel.confidence:.0%})")

        if outreach_id:
            lines.append(f"_Outreach ID: #{outreach_id}_")

        message = "\n".join(lines)
        return await self._send(message)

    async def send_pipeline_status(self, stats: PipelineStats) -> bool:
        """Send a pipeline status summary."""
        message = (
            f"*TITAN VERITAS - Pipeline Report*\n\n"
            f"Email Inviate: {stats.emails_sent}\n"
            f"Risposte Ricevute: {stats.replies_received}\n"
            f"Intelligence Estratta: {stats.intel_extracted}\n"
            f"Candidati Trovati: {stats.candidates_found}\n"
            f"Errori: {stats.errors}\n"
        )
        return await self._send(message)

    async def send_alert(self, message: str) -> bool:
        """Send a plain text alert."""
        return await self._send(message)

    async def send_error(self, error_msg: str, context: str = "") -> bool:
        """Send an error notification."""
        message = (
            f"*TITAN VERITAS - Errore*\n\n"
            f"Contesto: {context}\n"
            f"Errore: `{error_msg}`"
        )
        return await self._send(message)

    async def _send(self, message: str) -> bool:
        """Send a message via Telegram Bot API."""
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(url, json=payload)
                if response.status_code == 200:
                    return True
                else:
                    logger.warning(
                        f"[Telegram] HTTP {response.status_code}: {response.text[:200]}"
                    )
                    return False
        except Exception as e:
            logger.error(f"[Telegram] Send failed: {e}")
            return False

    @staticmethod
    def _confidence_bar(confidence: float) -> str:
        """Generate a visual confidence bar."""
        filled = int(confidence * 10)
        empty = 10 - filled
        return "█" * filled + "░" * empty
