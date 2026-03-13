# TITAN VERITAS - Multi-Agent Collaboration Protocol

This project is currently being developed by two AI agents: **Antigravity (Google)** and **Claude Code (Anthropic)**. To ensure smooth progress and avoid architectural conflicts, we will follow these rules.

## 1. Task Management
- The source of truth for high-level progress is the `task.md` located in the brain/artifacts directory (or mirrored in the root if necessary).
- Each agent should mark their active task in `task.md` (e.g., `- [/] [Agent Name] Working on X`).

## 2. File Ownership & Locks
- Before making significant architectural changes to a core module (e.g., `core/rosetta.py` or `core/outreach.py`), check if the other agent is currently working on it.
- Use comments at the top of a file if you are performing a complex refactor: `# LOCK: [Agent Name] is refactoring this file. Expected completion: [Time/Task]`.

## 3. Communication Channel
- We communicate through the **Active Implementation Plan** (`implementation_plan_v5.md`).
- If one agent proposes a change that affects the other's module, add a "Cross-Agent Note" in the plan.
- Use `AGENTS_MEMO.md` for non-code communication about project status.

## 4. Technical Strategy
- **Antigravity Focus:** High-level OSINT architecture, Data Modeling (Rosetta Stone), Active Outreach (Gmail/Gemini), and Premium HUD Design.
- **Claude Focus:** Database persistence layer (SQLite schema, repositories, connection management), CLI restructure (click.group with subcommands), Module 3 full implementation (Gmail poller, IntelProcessor with 3-tier LLM fallback, TelegramRouter, OutreachCoordinator), Module 2 youth league scaffolding (abstract base + registry pattern), requirements.txt management, and end-to-end integration wiring.

---
*Let's build the ultimate scouting engine for San Marino.* 🇸🇲⚽
