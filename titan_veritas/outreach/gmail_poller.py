"""GmailPoller: Long-running poll loop for inbound email replies.

Checks Gmail threads that correspond to sent outreach emails,
detects new replies, and dispatches them for LLM processing.
"""

import logging
import sqlite3
import asyncio
from typing import Optional

from ..core.outreach import GmailService
from ..db.repository import OutreachRepo

logger = logging.getLogger(__name__)


class GmailPoller:
    """Polls Gmail for replies to outreach threads."""

    POLL_INTERVAL_SECONDS = 300  # 5 minutes

    def __init__(self, gmail_service: GmailService, conn: sqlite3.Connection):
        self.gmail = gmail_service
        self.conn = conn
        self.outreach_repo = OutreachRepo(conn)

    async def poll_loop(self, callback=None, max_iterations: int = None):
        """Long-running poll loop. Checks for new replies periodically.

        Args:
            callback: async callable(outreach_record, reply_text) invoked for each new reply
            max_iterations: stop after N iterations (None = run forever)
        """
        iteration = 0
        while max_iterations is None or iteration < max_iterations:
            try:
                replies = self._check_all_pending()
                for outreach_record, reply_text in replies:
                    logger.info(
                        f"[GmailPoller] New reply for outreach #{outreach_record['id']} "
                        f"(thread: {outreach_record['gmail_thread_id']})"
                    )
                    self.outreach_repo.mark_replied(outreach_record["id"])

                    if callback:
                        await callback(outreach_record, reply_text)

                if replies:
                    logger.info(f"[GmailPoller] Processed {len(replies)} new replies")

            except Exception as e:
                logger.error(f"[GmailPoller] Error in poll cycle: {e}")

            iteration += 1
            if max_iterations is None or iteration < max_iterations:
                await asyncio.sleep(self.POLL_INTERVAL_SECONDS)

    def _check_all_pending(self) -> list:
        """Check all pending outreach threads for new replies.

        Returns list of (outreach_record, reply_text) tuples.
        """
        pending = self.outreach_repo.get_pending_threads()
        replies = []

        for record in pending:
            thread_id = record.get("gmail_thread_id")
            if not thread_id:
                continue

            reply_text = self._check_thread_for_reply(thread_id, record.get("gmail_message_id"))
            if reply_text:
                replies.append((record, reply_text))

        return replies

    def _check_thread_for_reply(self, thread_id: str,
                                original_message_id: str = None) -> Optional[str]:
        """Check a Gmail thread for new messages beyond the original.

        Returns the text body of the most recent reply, or None if no new messages.
        """
        try:
            thread = self.gmail.service.users().threads().get(
                userId="me", id=thread_id, format="full"
            ).execute()

            messages = thread.get("messages", [])
            if len(messages) <= 1:
                # Only our original message exists — no reply yet
                return None

            # Get the most recent message that isn't ours
            for msg in reversed(messages):
                msg_id = msg.get("id")
                if msg_id == original_message_id:
                    continue

                # Check if this is an inbound message (not sent by us)
                headers = {h["name"].lower(): h["value"]
                           for h in msg.get("payload", {}).get("headers", [])}

                # Extract the text body
                body_text = self._extract_body(msg)
                if body_text:
                    return body_text

        except Exception as e:
            logger.warning(f"[GmailPoller] Error checking thread {thread_id}: {e}")

        return None

    def _extract_body(self, message: dict) -> str:
        """Extract plain text body from a Gmail message."""
        import base64

        payload = message.get("payload", {})

        # Simple message with body directly
        if payload.get("body", {}).get("data"):
            data = payload["body"]["data"]
            return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")

        # Multipart message — look for text/plain
        parts = payload.get("parts", [])
        for part in parts:
            if part.get("mimeType") == "text/plain":
                data = part.get("body", {}).get("data")
                if data:
                    return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")

            # Nested multipart
            for subpart in part.get("parts", []):
                if subpart.get("mimeType") == "text/plain":
                    data = subpart.get("body", {}).get("data")
                    if data:
                        return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")

        # Fallback: snippet
        return message.get("snippet", "")
