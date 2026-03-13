"""OutreachCoordinator: Full pipeline orchestrator for Active Scouting.

Pipeline: extract contacts → compose emails → send → poll replies → LLM process → Telegram notify
"""

import json
import hashlib
import logging
import sqlite3
import asyncio
from typing import List, Optional

from ..core.models import CommunityContact, IntelResult, PipelineStats
from ..core.outreach import GmailService
from ..db.repository import OutreachRepo, SurnameRepo, ClusterRepo
from .contact_extractor import ContactExtractor
from .email_composer import EmailComposer
from .gmail_poller import GmailPoller
from .intel_processor import IntelProcessor
from .telegram_router import TelegramRouter

logger = logging.getLogger(__name__)


class OutreachCoordinator:
    """Orchestrates the complete Active Scouting outreach pipeline."""

    def __init__(self, gmail: GmailService, intel: IntelProcessor,
                 telegram: TelegramRouter, conn: sqlite3.Connection):
        self.gmail = gmail
        self.intel = intel
        self.telegram = telegram
        self.conn = conn
        self.outreach_repo = OutreachRepo(conn)
        self.surname_repo = SurnameRepo(conn)
        self.cluster_repo = ClusterRepo(conn)
        self.composer = EmailComposer()
        self.extractor = ContactExtractor(conn)
        self.poller = GmailPoller(gmail, conn)

    async def run_outreach_cycle(self,
                                 target_community: str = None) -> PipelineStats:
        """Run the complete outreach cycle.

        1. Extract contacts from fratellanza websites
        2. Compose localized emails
        3. Send emails via Gmail API
        4. Report via Telegram
        """
        stats = PipelineStats()

        # 1. Get contacts
        logger.info("[Outreach] Phase 1: Extracting contacts...")
        if target_community:
            clusters = self.cluster_repo.get_all()
            matching = [c for c in clusters
                        if target_community.lower() in
                        (c.get("fratellanza_name") or "").lower()]
            contacts = []
            for cluster in matching:
                extracted = await self.extractor.extract_from_cluster(cluster["id"])
                contacts.extend(extracted)
        else:
            contacts = await self.extractor.extract_from_all_clusters()

        # Also include contacts already stored in DB
        all_clusters = self.cluster_repo.get_all()
        for cluster in all_clusters:
            email = cluster.get("contact_email")
            if email and not any(c.email == email for c in contacts):
                contacts.append(CommunityContact(
                    name=cluster.get("contact_name") or cluster.get("fratellanza_name", ""),
                    email=email,
                    city=cluster["city"],
                    country=cluster["country"],
                    fratellanza_name=cluster.get("fratellanza_name", ""),
                    cluster_id=cluster["id"],
                ))

        # Deduplicate
        contacts = self._deduplicate_contacts(contacts)
        logger.info(f"[Outreach] Found {len(contacts)} unique contacts")

        # 2. Get target surnames (Tier 1 prioritized)
        tier1_surnames = [s["name"] for s in self.surname_repo.get_all_originals(tier=1)]

        # 3. Compose and send
        logger.info("[Outreach] Phase 2: Composing and sending emails...")
        for contact in contacts:
            try:
                subject, body = self.composer.compose(contact, tier1_surnames)

                # Check for duplicates via hash
                body_hash = hashlib.sha256(body.encode()).hexdigest()
                existing = self.conn.execute(
                    "SELECT id FROM outreach_log WHERE email_body_hash = ? AND target_email = ?",
                    (body_hash, contact.email),
                ).fetchone()
                if existing:
                    logger.info(f"[Outreach] Skipping duplicate for {contact.email}")
                    continue

                # Create draft in DB
                outreach_id = self.outreach_repo.create_draft(
                    target_email=contact.email,
                    subject=subject,
                    body=body,
                    cluster_id=contact.cluster_id,
                    target_name=contact.name,
                )

                # Send via Gmail
                success = self.gmail.send_localized_email(contact.email, subject, body)
                if success:
                    # Get the sent message ID for thread tracking
                    # Search for the most recent sent message to this address
                    msg_id, thread_id = self._get_last_sent_message_ids(contact.email)
                    if msg_id:
                        self.outreach_repo.mark_sent(outreach_id, msg_id, thread_id)
                    else:
                        self.outreach_repo.mark_status(outreach_id, "SENT",
                                                       notes="thread_id_not_found")
                    stats.emails_sent += 1

                    await self.telegram.send_alert(
                        f"📧 Outreach inviato a *{contact.fratellanza_name}* ({contact.city})"
                    )
                else:
                    self.outreach_repo.mark_status(outreach_id, "BOUNCED",
                                                   notes="send_failed")
                    stats.errors += 1

            except Exception as e:
                logger.error(f"[Outreach] Error sending to {contact.email}: {e}")
                stats.errors += 1

        # 4. Report
        await self.telegram.send_pipeline_status(stats)
        logger.info(f"[Outreach] Cycle complete: {stats}")
        return stats

    async def run_poll_only(self, max_iterations: int = 1) -> PipelineStats:
        """Run only the inbound processing loop.

        Checks for replies, processes them with LLM, sends Telegram alerts.
        """
        stats = PipelineStats()

        async def on_reply(record, reply_text):
            try:
                # Process with LLM
                intel = await self.intel.extract_intelligence(reply_text)
                stats.replies_received += 1

                # Update DB
                self.outreach_repo.mark_validated(
                    record["id"],
                    llm_extraction=json.dumps({
                        "names": intel.names,
                        "contacts": intel.contacts,
                        "clubs": intel.clubs_mentioned,
                    }),
                    confidence=intel.confidence,
                )

                if intel.names:
                    stats.intel_extracted += len(intel.names)

                # Get community name for the alert
                source = record.get("target_name") or record.get("target_email", "Unknown")

                # Send Telegram alert
                await self.telegram.send_intel_card(
                    intel, source_community=source, outreach_id=record["id"]
                )

            except Exception as e:
                logger.error(f"[Outreach] Error processing reply for #{record['id']}: {e}")
                self.outreach_repo.mark_status(
                    record["id"], "PROCESSING",
                    notes=f"LLM_ERROR: {str(e)[:200]}"
                )
                await self.telegram.send_error(
                    str(e)[:200],
                    context=f"Processing reply for outreach #{record['id']}"
                )
                stats.errors += 1

        await self.poller.poll_loop(callback=on_reply, max_iterations=max_iterations)
        return stats

    async def get_status(self) -> dict:
        """Get current outreach pipeline status."""
        return self.outreach_repo.get_stats()

    def _deduplicate_contacts(self, contacts: List[CommunityContact]) -> List[CommunityContact]:
        """Remove duplicate contacts by email address."""
        seen = set()
        unique = []
        for c in contacts:
            if c.email.lower() not in seen:
                seen.add(c.email.lower())
                unique.append(c)
        return unique

    def _get_last_sent_message_ids(self, to_email: str):
        """Get the message ID and thread ID of the last sent email to an address."""
        try:
            results = self.gmail.service.users().messages().list(
                userId="me",
                q=f"to:{to_email} in:sent",
                maxResults=1,
            ).execute()

            messages = results.get("messages", [])
            if messages:
                msg = self.gmail.service.users().messages().get(
                    userId="me", id=messages[0]["id"], format="metadata"
                ).execute()
                return msg.get("id"), msg.get("threadId")
        except Exception as e:
            logger.warning(f"[Outreach] Could not get message IDs for {to_email}: {e}")

        return None, None
