#!/usr/bin/env python3
"""
agent-crm — a local-first CRM for humans and AI agents.

SQLite + Python. No SaaS, no seats, no API limits.
Your data stays on your machine. Any agent can read and write to it.

Usage:
    python crm.py add "Jane Doe" --email jane@co.com --company Acme --status prospect
    python crm.py ls
    python crm.py pipeline
    python crm.py view jane@co.com
    python crm.py update jane@co.com --status active_customer --deal '$5K/mo'
    python crm.py log jane@co.com email "Sent proposal, she's interested"
    python crm.py activity jane@co.com
    python crm.py search "acme"
    python crm.py export --format csv
    python crm.py import contacts.csv
    python crm.py markdown
    python crm.py stats
"""

import sqlite3
import argparse
import csv
import sys
import os
import json
import re
from datetime import datetime, date, timedelta
from pathlib import Path

DEFAULT_DB = os.environ.get("CRM_DB", "crm.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE,
    name TEXT NOT NULL,
    company TEXT,
    title TEXT,
    deal_size TEXT,
    status TEXT NOT NULL DEFAULT 'prospect',
    source TEXT,
    notes TEXT,
    tags TEXT,
    last_contacted DATE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    type TEXT NOT NULL,
    summary TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (contact_id) REFERENCES contacts(id)
);

CREATE TABLE IF NOT EXISTS deals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    value TEXT,
    stage TEXT NOT NULL DEFAULT 'prospect',
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    closed_at DATETIME,
    FOREIGN KEY (contact_id) REFERENCES contacts(id)
);

CREATE TABLE IF NOT EXISTS facts (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    entity TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    source TEXT,
    observed_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_facts_entity ON facts(entity);
CREATE INDEX IF NOT EXISTS idx_facts_key ON facts(entity, key);
CREATE INDEX IF NOT EXISTS idx_facts_source ON facts(source);
CREATE INDEX IF NOT EXISTS idx_facts_value ON facts(value);
CREATE UNIQUE INDEX IF NOT EXISTS idx_facts_dedup ON facts(entity, key, value, source);
"""


class CRM:
    def __init__(self, db_path=DEFAULT_DB):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def _init_schema(self):
        """Initialize schema, deduplicating existing facts if needed for the unique index."""
        try:
            self.conn.executescript(SCHEMA)
        except sqlite3.IntegrityError:
            # Existing DB has duplicate facts — deduplicate before retrying.
            # Keep the row with the latest observed_at for each (entity, key, value, source).
            self.conn.execute(
                """DELETE FROM facts WHERE rowid NOT IN (
                       SELECT MAX(rowid) FROM facts
                       GROUP BY entity, key, value, source
                   )"""
            )
            self.conn.commit()
            self.conn.executescript(SCHEMA)

    # --- Contacts ---

    def add_contact(self, name, email=None, company=None, title=None,
                    deal_size=None, status="prospect", source=None,
                    notes=None, tags=None, warn_duplicate=False):
        if warn_duplicate:
            existing = self.conn.execute(
                "SELECT * FROM contacts WHERE LOWER(name) = LOWER(?)", (name,)
            ).fetchone()
            if existing:
                return {"id": None, "duplicate_of": dict(existing)}
        self.conn.execute(
            """INSERT INTO contacts (name, email, company, title, deal_size, status, source, notes, tags)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, email, company, title, deal_size, status, source, notes, tags)
        )
        self.conn.commit()
        return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def update_contact(self, identifier, **fields):
        contact = self.get_contact(identifier)
        if not contact:
            return None
        allowed = {"name", "company", "title", "deal_size", "status", "source", "notes", "tags", "last_contacted"}
        updates = {k: v for k, v in fields.items() if k in allowed and v is not None}
        if not updates:
            return contact
        sets = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values())
        vals.append(contact["id"])
        self.conn.execute(f"UPDATE contacts SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE id = ?", vals)
        self.conn.commit()
        return self.get_contact(contact.get("email") or contact["name"])

    def get_contact(self, identifier):
        """Get contact by email or name (partial match)."""
        row = self.conn.execute("SELECT * FROM contacts WHERE email = ?", (identifier,)).fetchone()
        if row:
            return dict(row)
        row = self.conn.execute("SELECT * FROM contacts WHERE name LIKE ?", (f"%{identifier}%",)).fetchone()
        return dict(row) if row else None

    def list_contacts(self, status=None, company=None):
        query = "SELECT * FROM contacts WHERE 1=1"
        params = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if company:
            query += " AND company LIKE ?"
            params.append(f"%{company}%")
        query += " ORDER BY last_contacted DESC NULLS LAST, created_at DESC"
        return [dict(r) for r in self.conn.execute(query, params).fetchall()]

    def search(self, term):
        query = """SELECT * FROM contacts WHERE
                   name LIKE ? OR company LIKE ? OR email LIKE ? OR notes LIKE ? OR tags LIKE ?
                   ORDER BY last_contacted DESC NULLS LAST"""
        t = f"%{term}%"
        return [dict(r) for r in self.conn.execute(query, (t, t, t, t, t)).fetchall()]

    def unified_search(self, term):
        """Search across contacts, facts, and activity. Returns dict with sections."""
        t = f"%{term}%"
        contacts = self.conn.execute(
            """SELECT * FROM contacts WHERE
               name LIKE ? OR company LIKE ? OR email LIKE ? OR notes LIKE ? OR tags LIKE ?
               ORDER BY last_contacted DESC NULLS LAST""",
            (t, t, t, t, t)
        ).fetchall()
        facts = self.conn.execute(
            """SELECT entity, key, value, source, observed_at FROM facts
               WHERE entity LIKE ? OR key LIKE ? OR value LIKE ? OR source LIKE ?
               ORDER BY observed_at DESC""",
            (t, t, t, t)
        ).fetchall()
        activities = self.conn.execute(
            """SELECT a.type, a.summary, a.created_at, c.name as contact_name
               FROM activity a JOIN contacts c ON a.contact_id = c.id
               WHERE a.summary LIKE ?
               ORDER BY a.created_at DESC LIMIT 20""",
            (t,)
        ).fetchall()
        return {
            "contacts": [dict(r) for r in contacts],
            "facts": [dict(r) for r in facts],
            "activities": [dict(r) for r in activities],
        }

    def delete_contact(self, email):
        contact = self.get_contact(email)
        if not contact:
            return False
        name = contact["name"]
        self.conn.execute("DELETE FROM activity WHERE contact_id = ?", (contact["id"],))
        self.conn.execute("DELETE FROM deals WHERE contact_id = ?", (contact["id"],))
        self.conn.execute("DELETE FROM contacts WHERE id = ?", (contact["id"],))
        # Cascade delete facts for all contact entity variants
        entity_variants = self._contact_entity_keys(contact)
        placeholders = ",".join("?" * len(entity_variants))
        self.conn.execute(
            f"DELETE FROM facts WHERE entity LIKE 'contact:%' AND entity IN ({placeholders})",
            entity_variants
        )
        self.conn.commit()
        return True

    # --- Activity ---

    def log_activity(self, identifier, activity_type, summary):
        contact = self.get_contact(identifier)
        if not contact:
            return None
        self.conn.execute(
            "INSERT INTO activity (contact_id, type, summary) VALUES (?, ?, ?)",
            (contact["id"], activity_type, summary)
        )
        self.conn.execute(
            "UPDATE contacts SET last_contacted = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (date.today().isoformat(), contact["id"])
        )
        self.conn.commit()
        return True

    def get_activity(self, identifier, limit=20):
        contact = self.get_contact(identifier)
        if not contact:
            return []
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM activity WHERE contact_id = ? ORDER BY created_at DESC LIMIT ?",
            (contact["id"], limit)
        ).fetchall()]

    # --- Deals ---

    def add_deal(self, identifier, name, value=None, stage="prospect", notes=None):
        contact = self.get_contact(identifier)
        if not contact:
            return None
        self.conn.execute(
            "INSERT INTO deals (contact_id, name, value, stage, notes) VALUES (?, ?, ?, ?, ?)",
            (contact["id"], name, value, stage, notes)
        )
        self.conn.commit()
        return True

    def list_deals(self, stage=None):
        query = "SELECT d.*, c.name as contact_name, c.company FROM deals d JOIN contacts c ON d.contact_id = c.id"
        params = []
        if stage:
            query += " WHERE d.stage = ?"
            params.append(stage)
        query += " ORDER BY d.updated_at DESC"
        return [dict(r) for r in self.conn.execute(query, params).fetchall()]

    # --- Views ---

    def pipeline(self):
        rows = self.conn.execute(
            """SELECT status, COUNT(*) as count, GROUP_CONCAT(name, ', ') as names
               FROM contacts GROUP BY status ORDER BY count DESC"""
        ).fetchall()
        return [dict(r) for r in rows]

    def stats(self):
        total = self.conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
        by_status = self.conn.execute(
            "SELECT status, COUNT(*) as n FROM contacts GROUP BY status ORDER BY n DESC"
        ).fetchall()
        recent = self.conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE last_contacted >= date('now', '-7 days')"
        ).fetchone()[0]
        stale = self.conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE last_contacted < date('now', '-14 days') AND status NOT IN ('active_customer', 'churned', 'lost')"
        ).fetchone()[0]
        return {
            "total_contacts": total,
            "contacted_last_7d": recent,
            "stale_14d": stale,
            "by_status": [dict(r) for r in by_status],
        }

    def markdown(self):
        """Dump entire CRM as markdown — paste into agent context."""
        contacts = self.list_contacts()
        lines = ["# CRM Pipeline", ""]
        lines.append(f"| Name | Company | Status | Deal | Last Contacted |")
        lines.append(f"|------|---------|--------|------|----------------|")
        for c in contacts:
            lines.append(f"| {c['name']} | {c['company'] or '-'} | {c['status']} | {c['deal_size'] or '-'} | {c['last_contacted'] or 'never'} |")
        lines.append("")
        stats = self.stats()
        lines.append(f"**Total:** {stats['total_contacts']} · **Active last 7d:** {stats['contacted_last_7d']} · **Stale (14d+):** {stats['stale_14d']}")
        return "\n".join(lines)

    # --- Import/Export ---

    def export_csv(self, path=None, enrich=False):
        """Export contacts to CSV.

        If enrich=True, append activity_count, deal_count, and score columns
        so the spreadsheet gives a complete picture of each contact.
        """
        contacts = self.list_contacts()
        if not contacts:
            return ""
        if enrich:
            for c in contacts:
                cid = c["id"]
                c["activity_count"] = self.conn.execute(
                    "SELECT COUNT(*) FROM activity WHERE contact_id = ?", (cid,)
                ).fetchone()[0]
                c["deal_count"] = self.conn.execute(
                    "SELECT COUNT(*) FROM deals WHERE contact_id = ?", (cid,)
                ).fetchone()[0]
                sc = self.score_contact(c.get("email") or c["name"])
                c["score"] = sc["score"] if sc else 0
        output = path or sys.stdout
        should_close = False
        if isinstance(output, str):
            output = open(output, "w", newline="")
            should_close = True
        writer = csv.DictWriter(output, fieldnames=contacts[0].keys())
        writer.writeheader()
        writer.writerows(contacts)
        if should_close:
            output.close()
        return path or "stdout"

    def import_csv(self, path):
        count = 0
        with open(path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    self.add_contact(
                        name=row.get("name", ""),
                        email=row.get("email"),
                        company=row.get("company"),
                        title=row.get("title"),
                        deal_size=row.get("deal_size"),
                        status=row.get("status", "prospect"),
                        source=row.get("source"),
                        notes=row.get("notes"),
                        tags=row.get("tags"),
                    )
                    count += 1
                except sqlite3.IntegrityError:
                    pass  # skip duplicates
        return count

    # --- Context Graph ---

    def observe(self, entity, key, value, source="manual"):
        """Record a fact. Any entity, any key, any value, any source.

        Deduplicates: if the exact (entity, key, value, source) tuple already
        exists, refreshes observed_at instead of creating a duplicate row.
        """
        if not entity or not key:
            raise ValueError("entity and key must be non-empty")
        self.conn.execute(
            """INSERT INTO facts (entity, key, value, source) VALUES (?, ?, ?, ?)
               ON CONFLICT(entity, key, value, source)
               DO UPDATE SET observed_at = CURRENT_TIMESTAMP""",
            (entity, key, value, source)
        )
        self.conn.commit()
        return True

    def observe_many(self, facts, source="manual"):
        """Bulk insert facts in a single transaction.

        facts: iterable of (entity, key, value) or (entity, key, value, source) tuples.
        Deduplicates: exact (entity, key, value, source) matches refresh
        observed_at instead of creating duplicate rows.
        Returns the number of facts processed (inserted or refreshed).
        """
        rows = []
        for f in facts:
            if len(f) == 3:
                entity, key, value = f
                src = source
            else:
                entity, key, value, src = f
            if not entity or not key:
                raise ValueError("entity and key must be non-empty")
            rows.append((entity, key, value, src))
        self.conn.executemany(
            """INSERT INTO facts (entity, key, value, source) VALUES (?, ?, ?, ?)
               ON CONFLICT(entity, key, value, source)
               DO UPDATE SET observed_at = CURRENT_TIMESTAMP""",
            rows
        )
        self.conn.commit()
        return len(rows)

    def facts_about(self, entity):
        """Latest fact per key for an entity, using rowid for tiebreaking."""
        rows = self.conn.execute(
            """SELECT key, value, source, observed_at FROM facts
               WHERE entity = ? ORDER BY observed_at DESC, rowid DESC""",
            (entity,)
        ).fetchall()
        seen = {}
        for r in rows:
            if r["key"] not in seen:
                seen[r["key"]] = dict(r)
        return seen

    def facts_as_of(self, entity, date_str):
        """Latest fact per key for an entity, only considering facts observed on or before date_str (YYYY-MM-DD)."""
        rows = self.conn.execute(
            """SELECT key, value, source, observed_at FROM facts
               WHERE entity = ? AND date(observed_at) <= date(?)
               ORDER BY observed_at DESC, rowid DESC""",
            (entity, date_str)
        ).fetchall()
        seen = {}
        for r in rows:
            if r["key"] not in seen:
                seen[r["key"]] = dict(r)
        return seen

    def history_of(self, entity, key=None):
        """Full history of an entity (or one key). Every observation, chronological."""
        if key:
            return [dict(r) for r in self.conn.execute(
                "SELECT * FROM facts WHERE entity = ? AND key = ? ORDER BY observed_at ASC",
                (entity, key)
            ).fetchall()]
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM facts WHERE entity = ? ORDER BY observed_at ASC",
            (entity,)
        ).fetchall()]

    def stale_facts(self, days=7):
        """Facts not observed in N days. What needs re-verification."""
        return [dict(r) for r in self.conn.execute(
            """SELECT entity, key, value, source, observed_at FROM facts
               WHERE observed_at < date('now', ?)
               GROUP BY entity, key HAVING observed_at = MAX(observed_at)
               ORDER BY observed_at ASC""",
            (f"-{days} days",)
        ).fetchall()]

    def related(self, entity):
        """Find all entities related to this one via facts."""
        rows = self.conn.execute(
            """SELECT DISTINCT value as related_entity, key as relation FROM facts
               WHERE entity = ? AND value LIKE '%:%'
               ORDER BY observed_at DESC""",
            (entity,)
        ).fetchall()
        return [dict(r) for r in rows]

    def reverse_lookup(self, entity):
        """Find all entities that reference this entity as a fact value."""
        rows = self.conn.execute(
            """SELECT DISTINCT entity as referencing_entity, key as relation, source, observed_at
               FROM facts WHERE value = ?
               ORDER BY observed_at DESC""",
            (entity,)
        ).fetchall()
        return [dict(r) for r in rows]

    def reachable(self, entity, hops=2, max_hops=None):
        """Find all entities reachable from this entity within N hops via fact values.

        Returns a dict mapping each reachable entity to its hop distance.
        """
        if max_hops is not None:
            hops = max_hops
        visited = {entity: 0}
        frontier = {entity}
        for hop in range(1, hops + 1):
            if not frontier:
                break
            next_frontier = set()
            # Forward: values that look like entity references (contain ':')
            placeholders = ",".join("?" * len(frontier))
            rows = self.conn.execute(
                f"""SELECT DISTINCT value FROM facts
                    WHERE entity IN ({placeholders}) AND value LIKE '%:%'""",
                list(frontier)
            ).fetchall()
            for r in rows:
                target = r[0]
                if target not in visited:
                    visited[target] = hop
                    next_frontier.add(target)
            # Reverse: entities that reference any entity in frontier
            rows = self.conn.execute(
                f"""SELECT DISTINCT entity FROM facts
                    WHERE value IN ({placeholders})""",
                list(frontier)
            ).fetchall()
            for r in rows:
                target = r[0]
                if target not in visited:
                    visited[target] = hop
                    next_frontier.add(target)
            frontier = next_frontier
        # Remove the starting entity itself
        visited.pop(entity, None)
        return visited

    def conflicts(self, entity=None):
        """Find fact conflicts: same entity+key with different values from different sources.

        If entity is given, restrict to that entity. Returns list of conflict dicts.
        """
        base = """
            SELECT f1.entity, f1.key,
                   f1.value as value1, f1.source as source1, f1.observed_at as observed_at1,
                   f2.value as value2, f2.source as source2, f2.observed_at as observed_at2
            FROM facts f1
            JOIN facts f2
              ON f1.entity = f2.entity
             AND f1.key = f2.key
             AND f1.value != f2.value
             AND f1.source != f2.source
             AND f1.rowid < f2.rowid
        """
        params = []
        if entity:
            base += " WHERE f1.entity = ?"
            params.append(entity)
        base += " ORDER BY f1.entity, f1.key"
        return [dict(r) for r in self.conn.execute(base, params).fetchall()]

    def find_by_fact(self, key, value=None):
        """Find entities that have a specific fact."""
        if value:
            return [dict(r) for r in self.conn.execute(
                "SELECT DISTINCT entity, value, source, observed_at FROM facts WHERE key = ? AND value = ?",
                (key, value)
            ).fetchall()]
        return [dict(r) for r in self.conn.execute(
            "SELECT DISTINCT entity, value, source, observed_at FROM facts WHERE key = ?",
            (key,)
        ).fetchall()]

    def from_source(self, source):
        """Everything derived from a specific source. For cascade updates."""
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM facts WHERE source = ? ORDER BY observed_at DESC",
            (source,)
        ).fetchall()]

    def count_by_fact(self, key):
        """Count entities by their latest value for a given key.

        Returns dict mapping value -> count of entities with that value.
        """
        # Get the latest fact per entity for this key, then count by value
        rows = self.conn.execute(
            """SELECT value, COUNT(*) as cnt FROM (
                   SELECT entity, value
                   FROM facts
                   WHERE key = ?
                   GROUP BY entity
                   HAVING rowid = MAX(rowid)
               ) GROUP BY value""",
            (key,)
        ).fetchall()
        return {r["value"]: r["cnt"] for r in rows}

    def recent_changes(self, days=7):
        """Return facts observed within the last N days."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        return [dict(r) for r in self.conn.execute(
            """SELECT entity, key, value, source, observed_at FROM facts
               WHERE observed_at >= ?
               ORDER BY observed_at DESC""",
            (cutoff,)
        ).fetchall()]

    def graph_stats(self):
        """Return basic stats about the context graph."""
        entities = self.conn.execute("SELECT COUNT(DISTINCT entity) FROM facts").fetchone()[0]
        facts = self.conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        return {"entities": entities, "facts": facts}

    def network_summary(self):
        """High-level summary of relationship network based on all ingested data.

        Returns a dict with total_contacts, total_graph_entities, total_facts,
        sources breakdown, top_contacts by iMessage volume, companies,
        pipeline_value, and resolution_rate.
        """
        total_contacts = self.conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
        total_graph_entities = self.conn.execute("SELECT COUNT(DISTINCT entity) FROM facts").fetchone()[0]
        total_facts = self.conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]

        # Sources breakdown
        source_names = ["macos_contacts", "imessage", "macos_calendar", "macos_mail"]
        sources = {}
        for src in source_names:
            cnt = self.conn.execute(
                "SELECT COUNT(*) FROM facts WHERE source = ?", (src,)
            ).fetchone()[0]
            sources[src] = cnt

        # Top 10 contacts by iMessage volume
        # imessage_total facts are on phone: or contact: entities
        rows = self.conn.execute(
            """SELECT entity, CAST(value AS INTEGER) as vol
               FROM facts
               WHERE key = 'imessage_total' AND source = 'imessage'
               ORDER BY CAST(value AS INTEGER) DESC"""
        ).fetchall()
        top_contacts_map = {}  # name -> highest message count
        for r in rows:
            entity = r["entity"]
            vol = r["vol"]
            # Try to resolve to a name
            display = entity
            if entity.startswith("phone:"):
                name_fact = self.conn.execute(
                    "SELECT value FROM facts WHERE entity = ? AND key = 'name' AND source = 'macos_contacts' LIMIT 1",
                    (entity,)
                ).fetchone()
                if name_fact:
                    display = name_fact[0]
                else:
                    display = entity.replace("phone:", "")
            elif entity.startswith("contact:"):
                display = entity.replace("contact:", "").title()
            # Deduplicate by name, keeping highest message count
            if display not in top_contacts_map or vol > top_contacts_map[display]:
                top_contacts_map[display] = vol
        top_contacts = [{"name": n, "messages": v} for n, v in
                        sorted(top_contacts_map.items(), key=lambda x: x[1], reverse=True)][:10]

        # Companies from contacts table and facts
        contact_companies = self.conn.execute(
            "SELECT DISTINCT company FROM contacts WHERE company IS NOT NULL AND company != ''"
        ).fetchall()
        companies_raw = {r[0] for r in contact_companies}
        fact_companies = self.conn.execute(
            "SELECT DISTINCT value FROM facts WHERE key = 'company' AND value IS NOT NULL AND value != ''"
        ).fetchall()
        for r in fact_companies:
            companies_raw.add(r[0])
        # Also look for company: entities
        company_entities = self.conn.execute(
            "SELECT DISTINCT entity FROM facts WHERE entity LIKE 'company:%'"
        ).fetchall()
        for r in company_entities:
            companies_raw.add(r[0].replace("company:", "").title())
        # Normalize: replace hyphens with spaces, title case, deduplicate
        companies_set = set()
        for c in companies_raw:
            normalized = c.replace("-", " ").strip()
            # Title case, but preserve if already mixed case (e.g. "OpenAI")
            normalized = normalized.title()
            companies_set.add(normalized)
        companies = sorted(companies_set)

        # Pipeline value: sum of deal_size from contacts
        all_contacts = self.conn.execute(
            "SELECT deal_size FROM contacts WHERE deal_size IS NOT NULL AND deal_size != ''"
        ).fetchall()
        pipeline_value = sum(self._parse_deal_size(r[0]) for r in all_contacts)

        # Resolution rate: % of iMessage phone: handles resolved to names
        phone_handles = self.conn.execute(
            "SELECT COUNT(DISTINCT entity) FROM facts WHERE entity LIKE 'phone:%' AND source = 'imessage'"
        ).fetchone()[0]
        resolved_handles = 0
        if phone_handles > 0:
            resolved_handles = self.conn.execute(
                """SELECT COUNT(DISTINCT f.entity) FROM facts f
                   WHERE f.entity LIKE 'phone:%%' AND f.source = 'imessage'
                   AND EXISTS (
                       SELECT 1 FROM facts f2
                       WHERE f2.entity = f.entity AND f2.key = 'name' AND f2.source = 'macos_contacts'
                   )"""
            ).fetchone()[0]
        resolution_rate = (resolved_handles / phone_handles * 100) if phone_handles > 0 else 0.0

        return {
            "total_contacts": total_contacts,
            "total_graph_entities": total_graph_entities,
            "total_facts": total_facts,
            "sources": sources,
            "top_contacts": top_contacts,
            "companies": companies,
            "pipeline_value": pipeline_value,
            "resolution_rate": round(resolution_rate, 1),
        }

    def merge_entities(self, source_entity, target_entity):
        """Merge all facts from target_entity into source_entity.

        After merge, facts_about(source_entity) has facts from both.
        target_entity facts are re-attributed to source_entity.
        """
        self.conn.execute(
            "UPDATE facts SET entity = ? WHERE entity = ?",
            (source_entity, target_entity)
        )
        self.conn.commit()
        return True

    def compact_markdown(self):
        """Shorter summary of the context graph with key insights."""
        rows = self.conn.execute(
            """SELECT entity, COUNT(DISTINCT key) as fact_count
               FROM facts GROUP BY entity ORDER BY fact_count DESC"""
        ).fetchall()
        stats = self.graph_stats()
        lines = ["# Graph Summary", ""]
        # Show top entities (cap at 10 to stay compact)
        shown = rows[:10]
        for r in shown:
            lines.append(f"- **{r['entity']}**: {r['fact_count']} facts")
        if len(rows) > 10:
            lines.append(f"- _...and {len(rows) - 10} more entities_")
        lines.append("")
        # Source breakdown
        sources = self.conn.execute(
            """SELECT source, COUNT(*) as cnt
               FROM facts GROUP BY source ORDER BY cnt DESC"""
        ).fetchall()
        if sources:
            for s in sources:
                lines.append(f"- _{s['source']}_: {s['cnt']} facts")
            lines.append("")
        lines.append(f"_{stats['entities']} entities, {stats['facts']} total facts_")
        return "\n".join(lines)

    def graph_markdown(self):
        """Dump the context graph as markdown."""
        # Single query to get all facts, then group in Python — avoids N+1 queries
        rows = self.conn.execute(
            """SELECT entity, key, value, source, observed_at
               FROM facts
               ORDER BY entity, key, observed_at DESC, rowid DESC"""
        ).fetchall()

        # Group by entity, keeping only the latest value per key
        entities = {}
        for r in rows:
            entity = r["entity"]
            key = r["key"]
            if entity not in entities:
                entities[entity] = {}
            if key not in entities[entity]:
                entities[entity][key] = dict(r)

        lines = ["# Context Graph", ""]
        for entity in sorted(entities.keys()):
            facts = entities[entity]
            lines.append(f"## {entity}")
            for key, fact in facts.items():
                age = fact["observed_at"][:10]
                lines.append(f"- **{key}**: {fact['value']} _(via {fact['source']}, {age})_")
            lines.append("")
        return "\n".join(lines)

    def to_json(self):
        """Full CRM dump as JSON — for agent consumption."""
        # Single query for all facts to avoid N+1
        all_facts_rows = self.conn.execute(
            "SELECT entity, key, value, source, observed_at FROM facts ORDER BY entity, observed_at DESC, rowid DESC"
        ).fetchall()
        graph = {}
        for r in all_facts_rows:
            entity = r["entity"]
            key = r["key"]
            if entity not in graph:
                graph[entity] = {}
            if key not in graph[entity]:
                graph[entity][key] = {
                    "key": key,
                    "value": r["value"],
                    "source": r["source"],
                    "observed_at": r["observed_at"],
                }
        return {
            "contacts": self.list_contacts(),
            "stats": self.stats(),
            "graph": graph,
            "exported_at": datetime.now().isoformat(),
        }

    # --- Lead Intelligence ---

    STATUS_ORDER = ["prospect", "contacted", "met", "proposal_drafted", "verbal_yes", "active_customer"]
    STATUS_SCORES = {"prospect": 0, "contacted": 5, "met": 8, "proposal_drafted": 12, "verbal_yes": 16, "active_customer": 20}
    STAGE_PROBABILITIES = {"prospect": 0.10, "contacted": 0.20, "met": 0.30, "proposal_drafted": 0.50, "verbal_yes": 0.75, "active_customer": 1.0}

    @staticmethod
    def _normalize_phone(raw):
        """Normalize a phone number: strip formatting, ensure +country code.

        Strips spaces, dashes, parens, dots — keeps digits and leading +.
        Adds +1 prefix for bare US 10-digit or 11-digit-with-leading-1 numbers.
        Returns the normalized string, or '' if nothing useful remains.
        """
        if not raw:
            return ""
        cleaned = "".join(c for c in str(raw).strip() if c.isdigit() or c == "+")
        if not cleaned:
            return ""
        if not cleaned.startswith("+") and len(cleaned) == 10:
            cleaned = "+1" + cleaned
        elif not cleaned.startswith("+") and len(cleaned) == 11 and cleaned.startswith("1"):
            cleaned = "+" + cleaned
        return cleaned

    @staticmethod
    def _parse_deal_size(val):
        """Parse deal size strings into annual dollar value. Returns 0 if unparseable."""
        if not val:
            return 0
        s = str(val).strip().lower().replace(",", "").replace("$", "")
        m = re.match(r'([\d.]+)\s*(k|m)?\s*(?:/\s*(mo|yr|month|year))?', s)
        if not m:
            return 0
        num = float(m.group(1))
        mult = m.group(2)
        period = m.group(3)
        if mult == 'k':
            num *= 1_000
        elif mult == 'm':
            num *= 1_000_000
        if period in ('mo', 'month'):
            num *= 12
        return num

    def score_contact(self, identifier):
        """Score a contact 0-100 based on engagement signals."""
        contact = self.get_contact(identifier)
        if not contact:
            return None
        score = 0
        factors = []
        today = date.today()

        # Recency of last contact (30 pts)
        lc = contact.get("last_contacted")
        if lc:
            try:
                last_date = date.fromisoformat(str(lc)[:10])
                days_ago = (today - last_date).days
                if days_ago <= 3:
                    pts = 30
                elif days_ago <= 7:
                    pts = 24
                elif days_ago <= 14:
                    pts = 18
                elif days_ago <= 30:
                    pts = 10
                else:
                    pts = 3
                score += pts
                factors.append(f"Last contacted {days_ago}d ago (+{pts}pts)")
            except (ValueError, TypeError):
                factors.append("No valid last_contacted date (+0pts)")
        else:
            factors.append("Never contacted (+0pts)")

        # Activity count in last 30d (25 pts)
        cutoff = (today - timedelta(days=30)).isoformat()
        act_count = self.conn.execute(
            "SELECT COUNT(*) FROM activity WHERE contact_id = ? AND created_at >= ?",
            (contact["id"], cutoff)
        ).fetchone()[0]
        act_pts = min(25, act_count * 5)
        score += act_pts
        factors.append(f"{act_count} activities in 30d (+{act_pts}pts)")

        # Deal size exists (15 pts)
        if contact.get("deal_size"):
            score += 15
            factors.append(f"Has deal size: {contact['deal_size']} (+15pts)")

        # Status advancement (20 pts)
        status = contact.get("status", "prospect")
        status_pts = self.STATUS_SCORES.get(status, 0)
        score += status_pts
        factors.append(f"Status '{status}' (+{status_pts}pts)")

        # Facts richness + graph engagement (10 pts)
        entity_keys = self._contact_entity_keys(contact)
        total_facts = 0
        graph_engagement_pts = 0
        for ek in entity_keys:
            ef = self.facts_about(ek)
            total_facts += len(ef)
            # iMessage engagement signal — high volume = strong relationship
            imsg = ef.get("imessage_total")
            if imsg:
                try:
                    vol = int(imsg["value"])
                    if vol > 100:
                        graph_engagement_pts = max(graph_engagement_pts, 5)
                    elif vol > 20:
                        graph_engagement_pts = max(graph_engagement_pts, 3)
                    elif vol > 0:
                        graph_engagement_pts = max(graph_engagement_pts, 1)
                except (ValueError, TypeError):
                    pass
            # Email engagement signal
            email_f = ef.get("email_total")
            if email_f:
                try:
                    vol = int(email_f["value"])
                    if vol > 50:
                        graph_engagement_pts = max(graph_engagement_pts, 4)
                    elif vol > 10:
                        graph_engagement_pts = max(graph_engagement_pts, 2)
                    elif vol > 0:
                        graph_engagement_pts = max(graph_engagement_pts, 1)
                except (ValueError, TypeError):
                    pass
        facts_pts = min(10, total_facts * 2 + graph_engagement_pts)
        score += facts_pts
        engagement_detail = f", graph engagement +{graph_engagement_pts}" if graph_engagement_pts else ""
        factors.append(f"{total_facts} facts (+{facts_pts}pts{engagement_detail})")

        return {"score": min(score, 100), "factors": factors}

    def prioritize(self, limit=10):
        """Return contacts sorted by score, highest first."""
        contacts = self.list_contacts()
        scored = []
        for c in contacts:
            identifier = c.get("email") or c["name"]
            result = self.score_contact(identifier)
            if result:
                scored.append({
                    "name": c["name"],
                    "email": c.get("email"),
                    "company": c.get("company"),
                    "status": c.get("status"),
                    "score": result["score"],
                    "top_factor": result["factors"][0] if result["factors"] else "",
                })
        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored[:limit]

    def health_check(self):
        """Categorize contacts into healthy/at_risk/cold with recommended actions."""
        contacts = self.list_contacts()
        healthy, at_risk, cold, actions = [], [], [], []
        today = date.today()

        for c in contacts:
            identifier = c.get("email") or c["name"]
            result = self.score_contact(identifier)
            if not result:
                continue
            sc = result["score"]
            lc = c.get("last_contacted")
            days_stale = None
            if lc:
                try:
                    days_stale = (today - date.fromisoformat(str(lc)[:10])).days
                except (ValueError, TypeError):
                    pass

            entry = {"name": c["name"], "email": c.get("email"), "score": sc, "status": c.get("status")}

            if sc < 30 or (days_stale is not None and days_stale > 14):
                cold.append(entry)
                if days_stale is not None:
                    actions.append(f"Re-engage {c['name']} (last contacted {days_stale} days ago)")
                else:
                    actions.append(f"Initiate contact with {c['name']} (never contacted)")
            elif sc <= 60 or (days_stale is not None and days_stale > 7):
                at_risk.append(entry)
                if days_stale is not None:
                    actions.append(f"Follow up with {c['name']} (last contacted {days_stale} days ago)")
                else:
                    actions.append(f"Schedule first contact with {c['name']}")
            else:
                healthy.append(entry)

        return {"healthy": healthy, "at_risk": at_risk, "cold": cold, "actions": actions}

    def conversion_funnel(self):
        """Pipeline conversion funnel — count, avg days, and conversion rate per stage."""
        funnel = {}
        today_str = date.today().isoformat()
        for status in self.STATUS_ORDER:
            rows = self.conn.execute(
                "SELECT created_at, updated_at, status FROM contacts WHERE status = ?",
                (status,)
            ).fetchall()
            # Also count contacts that passed through this status (now in a later stage)
            idx = self.STATUS_ORDER.index(status)
            later_statuses = self.STATUS_ORDER[idx + 1:]
            passed_through = 0
            if later_statuses:
                placeholders = ",".join("?" * len(later_statuses))
                passed_through = self.conn.execute(
                    f"SELECT COUNT(*) FROM contacts WHERE status IN ({placeholders})",
                    later_statuses
                ).fetchone()[0]

            current_count = len(rows)
            total_entered = current_count + passed_through

            # Average days in this status (use created_at to updated_at for current)
            total_days = 0
            for r in rows:
                try:
                    created = datetime.fromisoformat(r["created_at"]).date()
                    updated = datetime.fromisoformat(r["updated_at"]).date()
                    total_days += max((updated - created).days, 0)
                except (ValueError, TypeError):
                    pass

            avg_days = round(total_days / current_count, 1) if current_count else 0
            conversion_rate = round((passed_through / total_entered) * 100, 1) if total_entered > 0 else 0

            funnel[status] = {
                "count": current_count,
                "avg_days": avg_days,
                "conversion_rate": conversion_rate,
            }
        return funnel

    def forecast(self):
        """Weighted pipeline forecast based on stage probabilities and deal sizes."""
        by_stage = []
        weighted_total = 0

        for status in self.STATUS_ORDER:
            rows = self.conn.execute(
                "SELECT deal_size FROM contacts WHERE status = ?", (status,)
            ).fetchall()
            prob = self.STAGE_PROBABILITIES.get(status, 0)
            count = len(rows)
            total_value = sum(self._parse_deal_size(r["deal_size"]) for r in rows)
            weighted = total_value * prob
            weighted_total += weighted
            by_stage.append({
                "stage": status,
                "count": count,
                "total_value": round(total_value, 2),
                "probability": prob,
                "weighted_value": round(weighted, 2),
            })

        return {"weighted_pipeline": round(weighted_total, 2), "by_stage": by_stage}

    def find_duplicates(self):
        """Find potential duplicate contact pairs based on email, name similarity, or company+title."""
        contacts = self.list_contacts()
        dupes = []
        seen = set()

        for i, a in enumerate(contacts):
            for b in contacts[i + 1:]:
                pair_key = (min(a["id"], b["id"]), max(a["id"], b["id"]))
                if pair_key in seen:
                    continue
                reasons = []

                # Same email (non-null)
                if a.get("email") and b.get("email") and a["email"].lower() == b["email"].lower():
                    reasons.append("same email")

                # Similar name: first 4 chars match + same company
                a_name = (a.get("name") or "").lower().strip()
                b_name = (b.get("name") or "").lower().strip()
                if (len(a_name) >= 4 and len(b_name) >= 4
                        and a_name[:4] == b_name[:4]
                        and a.get("company") and b.get("company")
                        and a["company"].lower() == b["company"].lower()):
                    reasons.append("similar name + same company")

                # Same company + same title (non-null)
                if (a.get("company") and b.get("company")
                        and a.get("title") and b.get("title")
                        and a["company"].lower() == b["company"].lower()
                        and a["title"].lower() == b["title"].lower()):
                    reasons.append("same company + title")

                if reasons:
                    seen.add(pair_key)
                    dupes.append({
                        "contact_a": {"id": a["id"], "name": a["name"], "email": a.get("email")},
                        "contact_b": {"id": b["id"], "name": b["name"], "email": b.get("email")},
                        "reasons": reasons,
                    })
        return dupes

    def stale_contacts(self, days=14):
        """Contacts not contacted in N days, sorted by deal size (biggest first). Excludes terminal statuses.

        Cross-references the knowledge graph: if a contact has recent facts
        (e.g. iMessage activity, calendar events) observed within the window,
        they are NOT considered stale even if last_contacted is old.
        """
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        rows = self.conn.execute(
            """SELECT * FROM contacts
               WHERE status NOT IN ('active_customer', 'churned', 'lost')
               AND (last_contacted IS NULL OR last_contacted < ?)
               ORDER BY last_contacted ASC NULLS FIRST""",
            (cutoff,)
        ).fetchall()
        contacts = []
        for r in rows:
            c = dict(r)
            # Check knowledge graph for recent activity on this contact
            entity_keys = self._contact_entity_keys(c)
            placeholders = ",".join("?" * len(entity_keys))
            recent_fact = self.conn.execute(
                f"""SELECT 1 FROM facts
                    WHERE entity IN ({placeholders})
                    AND date(observed_at) >= date(?)
                    LIMIT 1""",
                entity_keys + [cutoff]
            ).fetchone()
            if recent_fact:
                continue  # has recent graph activity — not truly stale
            contacts.append(c)
        contacts.sort(key=lambda c: self._parse_deal_size(c.get("deal_size")), reverse=True)
        return contacts

    # --- Agent Superpowers (Wave 3) ---

    VALID_STATUSES = {"prospect", "contacted", "met", "proposal_drafted", "verbal_yes", "active_customer", "lost", "churned"}

    def query(self, q):
        """Natural language-ish query. Supports status keywords, 'high value',
        'most messaged', graph fact search, tag/name/company search."""
        q = q.strip()
        q_lower = q.lower().replace(" ", "_").replace("-", "_")
        # Strip trailing 's' for plural tolerance (e.g. "active_customers" -> "active_customer")
        q_stripped = q_lower.rstrip("s") if q_lower.endswith("s") else q_lower

        # Check for status keyword match
        for status in self.VALID_STATUSES:
            status_nound = status.replace("_", "")
            if q_lower == status or q_lower == status_nound or q_stripped == status or q_stripped == status_nound:
                contacts = self.list_contacts(status=status)
                return self._sort_by_score(contacts)

        # "high value" / "high_value" → contacts with parseable deal_size > 0
        if q_lower in ("high_value", "highvalue"):
            all_c = self.list_contacts()
            contacts = [c for c in all_c if self._parse_deal_size(c.get("deal_size")) > 0]
            return self._sort_by_score(contacts)

        # "most messaged" / "most_messaged" — rank contacts by iMessage total from graph
        if q_lower in ("most_messaged", "mostmessaged", "most_contacted", "mostcontacted"):
            return self._query_most_messaged()

        # Fall through: search tags first, then general search
        tag_results = self.conn.execute(
            "SELECT * FROM contacts WHERE tags LIKE ?", (f"%{q}%",)
        ).fetchall()
        if tag_results:
            return self._sort_by_score([dict(r) for r in tag_results])

        # Contact-table search
        contact_results = self.search(q)
        if contact_results:
            return self._sort_by_score(contact_results)

        # Graph fallback: search facts for the query term and resolve to contacts
        return self._query_graph_fallback(q)

    def _query_most_messaged(self):
        """Return contacts ranked by iMessage total (highest first) from graph facts."""
        rows = self.conn.execute(
            "SELECT entity, value FROM facts WHERE key = 'imessage_total' ORDER BY CAST(value AS INTEGER) DESC"
        ).fetchall()
        seen_ids = set()
        contacts = []
        for r in rows:
            c = self._resolve_entity_to_contact(r["entity"])
            if c and c["id"] not in seen_ids:
                seen_ids.add(c["id"])
                contacts.append(c)
        return contacts  # already ranked by message count, skip score re-sort

    def _query_graph_fallback(self, q):
        """Search the knowledge graph for q and resolve matching entities to contacts."""
        t = f"%{q}%"
        rows = self.conn.execute(
            """SELECT DISTINCT entity FROM facts
               WHERE entity LIKE ? OR key LIKE ? OR value LIKE ?
               ORDER BY observed_at DESC""",
            (t, t, t)
        ).fetchall()
        seen_ids = set()
        contacts = []
        for r in rows:
            c = self._resolve_entity_to_contact(r["entity"])
            if c and c["id"] not in seen_ids:
                seen_ids.add(c["id"])
                contacts.append(c)
        return self._sort_by_score(contacts) if contacts else []

    def _resolve_entity_to_contact(self, entity):
        """Resolve a graph entity key (e.g. 'contact:alice_smith') to a contact dict."""
        if not entity.startswith("contact:"):
            return None
        identifier = entity[len("contact:"):]
        # Try direct lookup: the identifier might be an email or a name fragment
        return self.get_contact(identifier.replace("_", " ")) or self.get_contact(identifier)

    def _sort_by_score(self, contacts):
        """Sort contacts by score descending. Used by query/segment."""
        scored = []
        for c in contacts:
            identifier = c.get("email") or c["name"]
            result = self.score_contact(identifier)
            sc = result["score"] if result else 0
            scored.append((sc, c))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [c for _, c in scored]

    def segment(self, tags=None, status=None, min_score=None, company=None,
                fact_key=None, fact_value=None):
        """Filter contacts by multiple AND-combined criteria.

        fact_key/fact_value: filter by knowledge graph data. When both are
        given, only contacts whose graph entity has a fact matching key=value
        are included. When only fact_key is given, contacts with any value
        for that key are included. Supports graph-powered segments like
        "contacts with high iMessage intensity" or "contacts at fintech companies".
        """
        query = "SELECT * FROM contacts WHERE 1=1"
        params = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if company:
            query += " AND company LIKE ?"
            params.append(f"%{company}%")
        if tags:
            query += " AND tags LIKE ?"
            params.append(f"%{tags}%")
        contacts = [dict(r) for r in self.conn.execute(query, params).fetchall()]

        if min_score is not None:
            filtered = []
            for c in contacts:
                identifier = c.get("email") or c["name"]
                result = self.score_contact(identifier)
                if result and result["score"] >= min_score:
                    filtered.append(c)
            contacts = filtered

        if fact_key is not None:
            filtered = []
            for c in contacts:
                entity_keys = self._contact_entity_keys(c)
                matched = False
                for ek in entity_keys:
                    facts = self.facts_about(ek)
                    if fact_key in facts:
                        if fact_value is None or facts[fact_key].get("value") == fact_value:
                            matched = True
                            break
                if matched:
                    filtered.append(c)
            contacts = filtered

        return self._sort_by_score(contacts)

    def timeline(self, identifier):
        """Merged chronological timeline of activities and facts for a contact."""
        contact = self.get_contact(identifier)
        if not contact:
            return []

        events = []

        # Activities
        activities = self.conn.execute(
            "SELECT type, summary, created_at FROM activity WHERE contact_id = ? ORDER BY created_at ASC",
            (contact["id"],)
        ).fetchall()
        for a in activities:
            events.append({
                "type": "activity",
                "detail": f"{a['type']}: {a['summary']}",
                "timestamp": a["created_at"],
            })

        # Facts — look up all entity key variants for this contact
        entity_keys = self._contact_entity_keys(contact)

        for ek in entity_keys:
            facts = self.conn.execute(
                "SELECT key, value, source, observed_at FROM facts WHERE entity = ? ORDER BY observed_at ASC",
                (ek,)
            ).fetchall()
            for f in facts:
                events.append({
                    "type": "fact",
                    "detail": f"{f['key']} = {f['value']} (via {f['source']})",
                    "timestamp": f["observed_at"],
                })

        # Sort chronologically (oldest first)
        events.sort(key=lambda e: e.get("timestamp", ""))
        return events

    def context_for_agent(self, identifier=None):
        """Generate optimized LLM context. Single contact or executive summary."""
        if identifier:
            return self._context_single(identifier)
        return self._context_summary()

    def _context_single(self, identifier):
        contact = self.get_contact(identifier)
        if not contact:
            return f"Contact not found: {identifier}"

        lines = []
        lines.append(f"# {contact['name']}")
        if contact.get("company"):
            lines.append(f"**Company:** {contact['company']}")
        lines.append(f"**Status:** {contact['status']}")
        if contact.get("deal_size"):
            lines.append(f"**Deal:** {contact['deal_size']}")
        if contact.get("email"):
            lines.append(f"**Email:** {contact['email']}")
        if contact.get("title"):
            lines.append(f"**Title:** {contact['title']}")
        if contact.get("tags"):
            lines.append(f"**Tags:** {contact['tags']}")

        # Score
        sc = self.score_contact(contact.get("email") or contact["name"])
        if sc:
            lines.append(f"**Score:** {sc['score']}/100")

        # Recent activity (last 5)
        acts = self.get_activity(contact.get("email") or contact["name"], limit=5)
        if acts:
            lines.append("\n## Recent Activity")
            for a in acts:
                lines.append(f"- [{a['created_at'][:10]}] {a['type']}: {a['summary']}")

        # Facts — match all entity key variants for this contact
        entity_keys = self._contact_entity_keys(contact)
        all_facts = {}
        for ek in entity_keys:
            all_facts.update(self.facts_about(ek))
        if all_facts:
            lines.append("\n## Known Facts")
            for key, f in all_facts.items():
                lines.append(f"- **{key}:** {f['value']} (via {f['source']})")

        # Related entities
        for ek in entity_keys:
            rels = self.related(ek)
            if rels:
                lines.append("\n## Related Entities")
                for r in rels:
                    lines.append(f"- {r['relation']} -> {r['related_entity']}")
                break

        result = "\n".join(lines)
        return result[:5000]

    def _context_summary(self):
        lines = []
        lines.append("# CRM Executive Summary")
        lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")

        # Key metrics
        rev = self.revenue_report()
        stats = self.stats()
        lines.append("## Metrics")
        lines.append(f"- **Contacts:** {stats['total_contacts']}")
        lines.append(f"- **MRR:** ${rev['mrr']:,.0f}")
        lines.append(f"- **Pipeline:** ${rev['pipeline_value']:,.0f}")
        lines.append(f"- **Active (7d):** {stats['contacted_last_7d']}")
        lines.append(f"- **Stale (14d+):** {stats['stale_14d']}")

        # Health summary
        hc = self.health_check()
        lines.append(f"- **Health:** {len(hc['healthy'])} healthy, {len(hc['at_risk'])} at risk, {len(hc['cold'])} cold")

        # Deal count by stage
        funnel = self.conversion_funnel()
        stage_parts = []
        for status in self.STATUS_ORDER:
            if status in funnel and funnel[status]["count"] > 0:
                stage_parts.append(f"{status}: {funnel[status]['count']}")
        if stage_parts:
            lines.append(f"\n## Pipeline by Stage")
            for sp in stage_parts:
                lines.append(f"- {sp}")

        # Graph stats
        gs = self.graph_stats()
        if gs["entities"] > 0 or gs["facts"] > 0:
            lines.append(f"\n## Knowledge Graph")
            lines.append(f"- **Entities:** {gs['entities']}")
            lines.append(f"- **Facts:** {gs['facts']}")

        # Top 5 priority contacts
        pri = self.prioritize(limit=5)
        if pri:
            lines.append("\n## Top Priority Contacts")
            for p in pri:
                lines.append(f"- **{p['name']}** ({p.get('company') or '-'}) — {p['status']}, score {p['score']}")

        # Relationship health highlights (most actionable)
        rh = self.relationship_health()
        actionable = [r for r in rh if r["status"] in ("one-sided-in", "fading")][:3]
        if actionable:
            lines.append("\n## Relationship Alerts")
            for r in actionable:
                lines.append(f"- **{r['name']}** ({r['status']}): {r['suggestion']}")

        # Recent changes (last 7 days)
        changes = self.diff(since=(datetime.now() - timedelta(days=7)).isoformat())
        if changes:
            lines.append(f"\n## Recent Changes ({min(len(changes), 10)} of {len(changes)})")
            for ch in changes[:10]:
                lines.append(f"- {ch.get('detail', '')}")

        result = "\n".join(lines)
        return result[:8000]

    def add_tag(self, identifier, tag):
        """Add a tag to a contact's comma-separated tags field. No duplicates."""
        contact = self.get_contact(identifier)
        if not contact:
            return None
        existing = contact.get("tags") or ""
        tags = [t.strip() for t in existing.split(",") if t.strip()]
        if tag.strip() not in tags:
            tags.append(tag.strip())
        new_tags = ",".join(tags)
        self.conn.execute(
            "UPDATE contacts SET tags = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (new_tags, contact["id"])
        )
        self.conn.commit()
        return True

    def remove_tag(self, identifier, tag):
        """Remove a tag from a contact's tags field."""
        contact = self.get_contact(identifier)
        if not contact:
            return None
        existing = contact.get("tags") or ""
        tags = [t.strip() for t in existing.split(",") if t.strip()]
        tags = [t for t in tags if t != tag.strip()]
        new_tags = ",".join(tags) if tags else None
        self.conn.execute(
            "UPDATE contacts SET tags = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (new_tags, contact["id"])
        )
        self.conn.commit()
        return True

    def list_by_tag(self, tag):
        """Return contacts that have this tag in their tags field."""
        rows = self.conn.execute(
            "SELECT * FROM contacts WHERE tags LIKE ?",
            (f"%{tag}%",)
        ).fetchall()
        # Exact tag match within comma-separated list
        results = []
        for r in rows:
            tags = [t.strip() for t in (r["tags"] or "").split(",")]
            if tag.strip() in tags:
                results.append(dict(r))
        return results

    # --- Pipeline Analytics (Wave 2) ---

    def win_loss_analysis(self):
        wins_rows = self.conn.execute(
            "SELECT * FROM contacts WHERE status = 'active_customer'"
        ).fetchall()
        losses_rows = self.conn.execute(
            "SELECT * FROM contacts WHERE status IN ('lost', 'churned')"
        ).fetchall()

        wins = []
        for r in wins_rows:
            r = dict(r)
            try:
                created = datetime.fromisoformat(r["created_at"])
                updated = datetime.fromisoformat(r["updated_at"])
                days_to_close = max((updated - created).days, 0)
            except (ValueError, TypeError):
                days_to_close = 0
            wins.append({
                "name": r["name"],
                "company": r.get("company"),
                "deal_size": self._parse_deal_size(r.get("deal_size")),
                "source": r.get("source"),
                "days_to_close": days_to_close,
            })

        losses = []
        for r in losses_rows:
            r = dict(r)
            losses.append({
                "name": r["name"],
                "company": r.get("company"),
                "deal_size": self._parse_deal_size(r.get("deal_size")),
                "source": r.get("source"),
            })

        total_closed = len(wins) + len(losses)
        win_rate = round((len(wins) / total_closed) * 100, 1) if total_closed else 0.0
        avg_deal_size = round(sum(w["deal_size"] for w in wins) / len(wins), 2) if wins else 0
        avg_days = round(sum(w["days_to_close"] for w in wins) / len(wins), 1) if wins else 0

        source_counts = {}
        for w in wins:
            src = w.get("source")
            if src:
                source_counts[src] = source_counts.get(src, 0) + 1
        top_source = max(source_counts, key=source_counts.get) if source_counts else None

        return {
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "avg_deal_size": avg_deal_size,
            "avg_days_to_close": avg_days,
            "top_source": top_source,
        }

    def cohort_analysis(self, period='month'):
        rows = self.conn.execute("SELECT created_at, status FROM contacts").fetchall()
        cohorts = {}
        for r in rows:
            try:
                dt = datetime.fromisoformat(r["created_at"])
            except (ValueError, TypeError):
                continue
            if period == 'week':
                iso = dt.isocalendar()
                label = f"{iso[0]}-W{iso[1]:02d}"
            else:
                label = f"{dt.year}-{dt.month:02d}"
            if label not in cohorts:
                cohorts[label] = {"added": 0, "converted": 0}
            cohorts[label]["added"] += 1
            if r["status"] == "active_customer":
                cohorts[label]["converted"] += 1

        result = {}
        for label in sorted(cohorts.keys()):
            c = cohorts[label]
            result[label] = {
                "added": c["added"],
                "converted": c["converted"],
                "conversion_rate": round(c["converted"] / c["added"], 4) if c["added"] else 0.0,
            }
        return result

    def activity_summary(self, days=30):
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        rows = self.conn.execute(
            "SELECT a.type, a.summary, a.created_at, c.name FROM activity a JOIN contacts c ON a.contact_id = c.id WHERE a.created_at >= ?",
            (cutoff,)
        ).fetchall()

        by_type = {}
        by_contact = {}
        for r in rows:
            t = r["type"]
            by_type[t] = by_type.get(t, 0) + 1
            name = r["name"]
            by_contact[name] = by_contact.get(name, 0) + 1

        top_contacts = sorted(by_contact.items(), key=lambda x: x[1], reverse=True)[:5]

        total = len(rows)
        daily_avg = round(total / max(days, 1), 2)

        return {
            "total_activities": total,
            "by_type": by_type,
            "by_contact": [{"name": n, "count": c} for n, c in top_contacts],
            "daily_avg": daily_avg,
        }

    def revenue_report(self):
        active_rows = self.conn.execute(
            "SELECT deal_size FROM contacts WHERE status = 'active_customer'"
        ).fetchall()
        pipeline_rows = self.conn.execute(
            "SELECT deal_size FROM contacts WHERE status NOT IN ('active_customer', 'lost', 'churned')"
        ).fetchall()

        arr = sum(self._parse_deal_size(r["deal_size"]) for r in active_rows)
        mrr = round(arr / 12, 2)
        arr = round(arr, 2)
        pipeline_value = round(sum(self._parse_deal_size(r["deal_size"]) for r in pipeline_rows), 2)
        active_count = len(active_rows)
        avg_rev = round(arr / active_count, 2) if active_count else 0

        return {
            "mrr": mrr,
            "arr": arr,
            "pipeline_value": pipeline_value,
            "avg_revenue_per_customer": avg_rev,
        }

    def diff(self, since=None):
        if since:
            cutoff = since
        else:
            cutoff = (datetime.now() - timedelta(hours=24)).isoformat()

        changes = []

        contacts = self.conn.execute(
            "SELECT name, company, created_at FROM contacts WHERE created_at >= ? ORDER BY created_at DESC",
            (cutoff,)
        ).fetchall()
        for r in contacts:
            changes.append({
                "type": "new_contact",
                "entity": r["name"],
                "name": r["name"],
                "company": r["company"],
                "detail": f"New contact: {r['name']}",
                "timestamp": r["created_at"],
            })

        activities = self.conn.execute(
            """SELECT a.type, a.summary, a.created_at, c.name
               FROM activity a JOIN contacts c ON a.contact_id = c.id
               WHERE a.created_at >= ? ORDER BY a.created_at DESC""",
            (cutoff,)
        ).fetchall()
        for r in activities:
            changes.append({
                "type": "activity",
                "entity": r["name"],
                "contact": r["name"],
                "activity_type": r["type"],
                "summary": r["summary"],
                "detail": f"Activity on {r['name']}: {r['type']}",
                "timestamp": r["created_at"],
            })

        facts = self.conn.execute(
            "SELECT entity, key, value, observed_at FROM facts WHERE observed_at >= ? ORDER BY observed_at DESC",
            (cutoff,)
        ).fetchall()
        for r in facts:
            changes.append({
                "type": "fact",
                "entity": r["entity"],
                "key": r["key"],
                "value": r["value"],
                "detail": f"Fact: {r['entity']} {r['key']}={r['value']}",
                "timestamp": r["observed_at"],
            })

        changes.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        return changes

    def snapshot(self):
        status_rows = self.conn.execute(
            "SELECT status, COUNT(*) as n FROM contacts GROUP BY status"
        ).fetchall()
        contacts_by_status = {r["status"]: r["n"] for r in status_rows}
        total = sum(contacts_by_status.values())

        rev = self.revenue_report()

        top_deals_rows = self.conn.execute(
            "SELECT name, company, deal_size, status FROM contacts WHERE deal_size IS NOT NULL ORDER BY id"
        ).fetchall()
        top_deals_list = []
        for r in top_deals_rows:
            top_deals_list.append({
                "name": r["name"],
                "company": r["company"],
                "deal_size": self._parse_deal_size(r["deal_size"]),
                "status": r["status"],
            })
        top_deals_list.sort(key=lambda x: x["deal_size"], reverse=True)
        top_deals = top_deals_list[:5]

        hc = self.health_check()

        all_contacts = self.list_contacts()
        high = medium = low = 0
        for c in all_contacts:
            identifier = c.get("email") or c["name"]
            result = self.score_contact(identifier)
            if result:
                s = result["score"]
                if s > 70:
                    high += 1
                elif s >= 30:
                    medium += 1
                else:
                    low += 1

        return {
            "timestamp": datetime.now().isoformat(),
            "contacts": contacts_by_status,
            "total_contacts": total,
            "pipeline_value": rev["pipeline_value"],
            "mrr": rev["mrr"],
            "top_deals": top_deals,
            "health": {
                "healthy": len(hc["healthy"]),
                "at_risk": len(hc["at_risk"]),
                "cold": len(hc["cold"]),
            },
            "score_distribution": {"high": high, "medium": medium, "low": low},
        }

    # --- Automation & Intelligence (Wave 4) ---

    def next_actions(self, limit=10):
        """Recommended actions prioritized by urgency and deal value.

        Cross-references iMessage reciprocity and velocity data so that
        contacts who are actively reaching out to you (one-sided-in) or
        whose relationships are decaying surface as actionable items.
        """
        contacts = self.list_contacts()
        actions = []
        today = date.today()
        priority_rank = {"high": 0, "medium": 1, "low": 2}
        contacts_with_actions = set()

        # --- Pre-compute iMessage reciprocity data for all contacts ---
        # Build a lookup: lowercased contact name -> {sent, received, total, intensity}
        imsg_rows = self.conn.execute(
            """SELECT entity, key, value FROM facts
               WHERE source = 'imessage'
               AND key IN ('imessage_sent', 'imessage_received', 'imessage_total', 'message_intensity')"""
        ).fetchall()
        imsg_by_entity = {}
        for r in imsg_rows:
            e = r["entity"]
            if e not in imsg_by_entity:
                imsg_by_entity[e] = {}
            imsg_by_entity[e][r["key"]] = r["value"]

        def _get_imsg_data(contact):
            """Resolve iMessage data for a CRM contact across entity key variants."""
            variants = self._contact_entity_keys(contact)
            best = None
            for v in variants:
                data = imsg_by_entity.get(v)
                if data and "imessage_total" in data:
                    total = int(data.get("imessage_total", 0))
                    if best is None or total > best.get("total", 0):
                        best = {
                            "sent": int(data.get("imessage_sent", 0)),
                            "received": int(data.get("imessage_received", 0)),
                            "total": total,
                            "intensity": data.get("message_intensity", "low"),
                        }
            return best

        for c in contacts:
            identifier = c.get("email") or c["name"]
            name = c["name"]
            status = c.get("status", "prospect")
            deal_val = self._parse_deal_size(c.get("deal_size"))
            lc = c.get("last_contacted")
            days_since = None
            if lc:
                try:
                    days_since = (today - date.fromisoformat(str(lc)[:10])).days
                except (ValueError, TypeError):
                    pass

            acts = self.get_activity(identifier, limit=50)
            act_types = [a["type"] for a in acts]

            # Check for unresolved conflicts
            entity_keys = self._contact_entity_keys(c)
            has_conflicts = False
            for ek in entity_keys:
                if self.conflicts(entity=ek):
                    has_conflicts = True
                    break

            # Verbal yes -> Send contract (high) — check before stale so it takes priority
            if status == "verbal_yes":
                actions.append({"contact": name, "action": "Send contract", "reason": "Verbal yes — close the deal", "priority": "high", "deal_value": deal_val})
                contacts_with_actions.add(name)
                continue

            # iMessage reciprocity: they message you heavily, you barely reply (high)
            imsg = _get_imsg_data(c)
            if imsg and imsg["received"] > 5 and imsg["sent"] > 0 and imsg["received"] > imsg["sent"] * 5:
                actions.append({
                    "contact": name,
                    "action": "Reply",
                    "reason": f"They've sent {imsg['received']} messages, you've sent {imsg['sent']} — relationship is one-sided",
                    "priority": "high",
                    "deal_value": deal_val,
                })
                contacts_with_actions.add(name)
                continue

            # Stale high-value contacts -> Follow up (high)
            if deal_val > 0 and (days_since is None or days_since > 14) and status not in ("active_customer", "lost", "churned"):
                actions.append({"contact": name, "action": "Follow up", "reason": "High-value contact gone stale", "priority": "high", "deal_value": deal_val})
                contacts_with_actions.add(name)
                continue

            # Proposal drafted > 7 days -> Follow up on proposal (medium)
            if status == "proposal_drafted" and days_since is not None and days_since > 7:
                actions.append({"contact": name, "action": "Follow up on proposal", "reason": "Proposal sent over 7 days ago", "priority": "medium", "deal_value": deal_val})
                contacts_with_actions.add(name)
                continue

            # Met but no proposal -> Draft proposal (medium)
            if status == "met":
                actions.append({"contact": name, "action": "Draft proposal", "reason": "Met but no proposal yet", "priority": "medium", "deal_value": deal_val})
                contacts_with_actions.add(name)
                continue

            # Decaying velocity on pipeline contacts -> Re-engage (medium)
            if status not in ("active_customer", "lost", "churned") and len(acts) >= 2:
                vel = self.velocity(identifier)
                if vel and vel["trend"] == "decaying" and vel["days_until_cold"] is not None:
                    actions.append({
                        "contact": name,
                        "action": "Re-engage",
                        "reason": f"Engagement decaying — projected cold in {vel['days_until_cold']} days",
                        "priority": "medium",
                        "deal_value": deal_val,
                    })
                    contacts_with_actions.add(name)
                    continue

            # Prospects with no activity -> Initial outreach (low)
            if status == "prospect" and len(acts) == 0:
                actions.append({"contact": name, "action": "Initial outreach", "reason": "Prospect with no activity", "priority": "low", "deal_value": deal_val})
                contacts_with_actions.add(name)
                continue

            # Unresolved conflicts -> Verify (low)
            if has_conflicts:
                actions.append({"contact": name, "action": "Verify conflicting info", "reason": "Conflicting facts detected", "priority": "low", "deal_value": deal_val})
                contacts_with_actions.add(name)
                continue

        # Sort: priority first (high < medium < low), then deal_value descending
        actions.sort(key=lambda a: (priority_rank.get(a["priority"], 9), -a["deal_value"]))
        return actions[:limit]

    def suggest_status(self, identifier):
        """Suggest what status a contact should be based on activity patterns."""
        contact = self.get_contact(identifier)
        if not contact:
            return None

        current = contact.get("status", "prospect")
        acts = self.get_activity(identifier, limit=50)
        act_types = [a["type"] for a in acts]
        act_count = len(acts)

        # Check deals
        deals = self.conn.execute(
            "SELECT stage FROM deals WHERE contact_id = ?", (contact["id"],)
        ).fetchall()
        deal_stages = [d["stage"] for d in deals]

        suggested = current
        reason = "Current status appears correct"
        confidence = "low"

        # If last activity was meeting and status is still contacted -> suggest met
        if act_types and act_types[0] == "meeting" and current == "contacted":
            suggested = "met"
            reason = "Last activity was a meeting"
            confidence = "high"
        # If activity count > 3 and status is prospect -> suggest contacted
        elif act_count > 3 and current == "prospect":
            suggested = "contacted"
            reason = f"{act_count} activities logged but still marked as prospect"
            confidence = "high"
        # If deal exists at proposal stage -> suggest proposal_drafted
        elif "proposal" in deal_stages and current in ("prospect", "contacted", "met"):
            suggested = "proposal_drafted"
            reason = "Deal at proposal stage"
            confidence = "high"
        # If deal at closed_won -> suggest active_customer
        elif "closed_won" in deal_stages and current != "active_customer":
            suggested = "active_customer"
            reason = "Deal marked as closed won"
            confidence = "high"
        # If any activity and status is prospect -> suggest contacted
        elif act_count > 0 and current == "prospect":
            suggested = "contacted"
            reason = "Has activity but still marked as prospect"
            confidence = "medium"
        # If meeting in activity history and status < met
        elif "meeting" in act_types and current in ("prospect", "contacted"):
            suggested = "met"
            reason = "Meeting recorded in activity history"
            confidence = "medium"

        return {
            "current": current,
            "suggested": suggested,
            "reason": reason,
            "confidence": confidence,
        }

    def auto_observe(self, identifier):
        """Auto-generate facts from contact data and activity. Returns list of created facts."""
        contact = self.get_contact(identifier)
        if not contact:
            return []

        name = contact["name"]
        name_lower = name.lower()
        entity = f"contact:{name_lower.replace(' ', '_')}"
        created = []

        # Company
        if contact.get("company"):
            self.observe(entity, "company", contact["company"], source="auto")
            created.append({"entity": entity, "key": "company", "value": contact["company"]})

        # Title
        if contact.get("title"):
            self.observe(entity, "title", contact["title"], source="auto")
            created.append({"entity": entity, "key": "title", "value": contact["title"]})

        # Email
        if contact.get("email"):
            self.observe(entity, "email", contact["email"], source="auto")
            created.append({"entity": entity, "key": "email", "value": contact["email"]})

        # Status
        if contact.get("status"):
            self.observe(entity, "status", contact["status"], source="auto")
            created.append({"entity": entity, "key": "status", "value": contact["status"]})

        # Deal size
        if contact.get("deal_size"):
            self.observe(entity, "deal_size", contact["deal_size"], source="auto")
            created.append({"entity": entity, "key": "deal_size", "value": contact["deal_size"]})

        # Source
        if contact.get("source"):
            self.observe(entity, "source", contact["source"], source="auto")
            created.append({"entity": entity, "key": "source", "value": contact["source"]})

        # Last meeting from activity
        acts = self.get_activity(identifier, limit=50)
        for a in acts:
            if a["type"] == "meeting":
                meeting_date = a["created_at"][:10] if a.get("created_at") else "unknown"
                self.observe(entity, "last_meeting", meeting_date, source="auto")
                created.append({"entity": entity, "key": "last_meeting", "value": meeting_date})
                break

        # Last activity date
        if acts:
            last_date = acts[0]["created_at"][:10] if acts[0].get("created_at") else "unknown"
            self.observe(entity, "last_activity", last_date, source="auto")
            created.append({"entity": entity, "key": "last_activity", "value": last_date})

        return created

    def enrich(self, identifier):
        """Comprehensive contact profile assembled from all sources."""
        contact = self.get_contact(identifier)
        if not contact:
            return None

        name = contact["name"]
        entity_keys = self._contact_entity_keys(contact)

        # Gather all facts across entity key variants
        all_facts = {}
        for ek in entity_keys:
            all_facts.update(self.facts_about(ek))

        # Activities
        acts = self.get_activity(identifier, limit=50)
        activities = [{"type": a["type"], "summary": a["summary"], "date": a["created_at"][:10] if a.get("created_at") else None} for a in acts]

        # Score
        sc = self.score_contact(identifier)
        score = sc["score"] if sc else 0

        # Related entities
        related = []
        for ek in entity_keys:
            for r in self.related(ek):
                if r not in related:
                    related.append(r)

        # Deals
        deals = self.conn.execute(
            "SELECT name, value, stage, created_at FROM deals WHERE contact_id = ?",
            (contact["id"],)
        ).fetchall()
        deal_list = [{"name": d["name"], "value": d["value"], "stage": d["stage"]} for d in deals]

        # Timeline highlights (last 5 events)
        tl = self.timeline(identifier)
        highlights = tl[-5:] if len(tl) > 5 else tl

        # Flatten facts to simple key->value dict for easy consumption
        facts_flat = {k: v["value"] for k, v in all_facts.items()}

        return {
            "name": name,
            "email": contact.get("email"),
            "company": contact.get("company"),
            "title": contact.get("title"),
            "status": contact.get("status"),
            "deal_size": contact.get("deal_size"),
            "score": score,
            "facts": facts_flat,
            "activities": activities,
            "related": related,
            "deals": deal_list,
            "timeline_highlights": highlights,
        }

    def bulk_update(self, updates):
        """Apply multiple contact updates in a single transaction. Returns count of successful updates."""
        count = 0
        for upd in updates:
            upd = dict(upd)
            identifier = upd.pop("identifier", None)
            if not identifier:
                continue
            result = self.update_contact(identifier, **upd)
            if result is not None:
                count += 1
        return count

    def find_intros(self, target):
        """Find warm intro paths to a target company, person, or email.

        Uses the iMessage/contacts knowledge graph to find people you know
        who are connected to the target. Ranks by relationship warmth
        (message intensity, recency).

        Returns a list of dicts with: connector, relationship_to_you,
        connection_to_target, and suggested_action.
        """
        target_lower = target.strip().lower()
        results = []
        seen_connectors = set()

        # --- Step 1: Find all entities related to target ---

        # Find contacts who work at the target company (via contacts table)
        company_matches = self.conn.execute(
            "SELECT * FROM contacts WHERE LOWER(company) LIKE ?",
            (f"%{target_lower}%",)
        ).fetchall()

        # Also match by email domain: @acme.com matches target "acme"
        # This catches contacts whose company field is empty but email reveals affiliation
        domain_matches = self.conn.execute(
            "SELECT * FROM contacts WHERE email IS NOT NULL AND LOWER(email) LIKE ?",
            (f"%@{target_lower}%",)
        ).fetchall()
        # Merge domain matches into company_matches (avoid duplicates by id)
        company_match_ids = {r["id"] for r in company_matches}
        company_matches = list(company_matches) + [r for r in domain_matches if r["id"] not in company_match_ids]

        # Find contacts whose name matches the target
        name_matches = self.conn.execute(
            "SELECT * FROM contacts WHERE LOWER(name) LIKE ?",
            (f"%{target_lower}%",)
        ).fetchall()

        # Find contacts by email match
        email_matches = self.conn.execute(
            "SELECT * FROM contacts WHERE LOWER(email) LIKE ?",
            (f"%{target_lower}%",)
        ).fetchall()

        # Also search the facts graph for company references
        graph_company_entities = self.conn.execute(
            """SELECT DISTINCT entity FROM facts
               WHERE key = 'company' AND LOWER(value) LIKE ?""",
            (f"%{target_lower}%",)
        ).fetchall()

        # Search for any entity or value matching target in the graph
        graph_entity_matches = self.conn.execute(
            """SELECT DISTINCT entity FROM facts
               WHERE LOWER(entity) LIKE ? OR LOWER(value) LIKE ?""",
            (f"%{target_lower}%", f"%{target_lower}%")
        ).fetchall()

        # Collect all target-related entity names (from contact:xyz format)
        target_people = set()
        for row in company_matches:
            target_people.add(row["name"].lower())
        for row in name_matches:
            target_people.add(row["name"].lower())
        for row in email_matches:
            target_people.add(row["name"].lower())
        for row in graph_company_entities:
            entity = row["entity"]
            if entity.startswith("contact:"):
                target_people.add(entity[len("contact:"):])
        for row in graph_entity_matches:
            entity = row["entity"]
            if entity.startswith("contact:"):
                target_people.add(entity[len("contact:"):])

        # --- Step 2: For each target person, find connectors ---
        # A connector is someone YOU message frequently who also knows the target person.

        # Get all contacts with iMessage data (your warm relationships)
        imessage_entities = self.conn.execute(
            """SELECT entity, key, value FROM facts
               WHERE key IN ('imessage_total', 'message_intensity')
               AND source = 'imessage'
               ORDER BY entity"""
        ).fetchall()

        # Build a map: entity -> {imessage_total, message_intensity}
        imessage_data = {}
        for row in imessage_entities:
            entity = row["entity"]
            if entity not in imessage_data:
                imessage_data[entity] = {}
            imessage_data[entity][row["key"]] = row["value"]

        # Get all contacts with their companies from facts
        company_facts = self.conn.execute(
            """SELECT entity, value FROM facts WHERE key = 'company'"""
        ).fetchall()
        entity_companies = {}
        for row in company_facts:
            entity_companies[row["entity"]] = row["value"]

        # Also map from contacts table
        all_contacts = self.conn.execute(
            "SELECT name, email, company FROM contacts"
        ).fetchall()
        contact_by_name = {}
        for c in all_contacts:
            contact_by_name[c["name"].lower()] = dict(c)
            entity_key = f"contact:{c['name'].lower()}"
            if c["company"] and entity_key not in entity_companies:
                entity_companies[entity_key] = c["company"]

        # --- Step 3: Match connectors to target ---
        # Strategy A: Find your contacts who work at the target company
        for entity, imsg in imessage_data.items():
            person_name = None
            if entity.startswith("contact:"):
                person_name = entity[len("contact:"):]
            elif entity.startswith("phone:"):
                # Resolve phone to name
                name_fact = self.conn.execute(
                    "SELECT value FROM facts WHERE entity = ? AND key = 'name' LIMIT 1",
                    (entity,)
                ).fetchone()
                if name_fact:
                    person_name = name_fact[0].lower()

            if not person_name or person_name in seen_connectors:
                continue

            # Check if this person works at the target company
            company = entity_companies.get(f"contact:{person_name}", "")
            contact_info = contact_by_name.get(person_name, {})
            if not company:
                company = contact_info.get("company") or ""

            imessage_total = int(imsg.get("imessage_total", 0))
            intensity = imsg.get("message_intensity", "low")

            # Also check email domain for company affiliation
            email = contact_info.get("email") or ""
            email_domain = email.split("@")[1].split(".")[0].lower() if "@" in email else ""

            # Direct match: connector works at target company (by company field or email domain)
            matched_company = ""
            if company and target_lower in company.lower():
                matched_company = company
            elif email_domain and target_lower in email_domain:
                matched_company = email_domain.title()

            if matched_company:
                seen_connectors.add(person_name)
                display_name = person_name.title()
                results.append({
                    "connector": display_name,
                    "connector_email": contact_info.get("email"),
                    "relationship_to_you": {
                        "imessage_total": imessage_total,
                        "intensity": intensity,
                    },
                    "connection_to_target": f"Works at {matched_company}",
                    "warmth_score": self._intro_warmth_score(imessage_total, intensity),
                    "suggested_action": self._intro_suggested_action(display_name, intensity, matched_company, "employee"),
                })

        # Strategy B: Find your contacts who know someone at the target company
        # (i.e., they share a company in the graph, or are linked via facts)
        for target_person in target_people:
            # Find who references this target person in the graph
            referrers = self.conn.execute(
                """SELECT DISTINCT entity FROM facts
                   WHERE LOWER(value) LIKE ?""",
                (f"%{target_person}%",)
            ).fetchall()

            for ref in referrers:
                ref_entity = ref["entity"]
                if not ref_entity.startswith("contact:"):
                    continue
                ref_name = ref_entity[len("contact:"):]
                if ref_name in seen_connectors or ref_name in target_people:
                    continue

                imsg = imessage_data.get(ref_entity, {})
                imessage_total = int(imsg.get("imessage_total", 0))
                intensity = imsg.get("message_intensity", "low")

                if imessage_total > 0:
                    seen_connectors.add(ref_name)
                    display_name = ref_name.title()
                    contact_info = contact_by_name.get(ref_name, {})
                    results.append({
                        "connector": display_name,
                        "connector_email": contact_info.get("email"),
                        "relationship_to_you": {
                            "imessage_total": imessage_total,
                            "intensity": intensity,
                        },
                        "connection_to_target": f"Connected to {target_person.title()}",
                        "warmth_score": self._intro_warmth_score(imessage_total, intensity),
                        "suggested_action": self._intro_suggested_action(display_name, intensity, target, "connection"),
                    })

            # Also check reverse: entities that this target person references
            reverse = self.conn.execute(
                """SELECT DISTINCT value FROM facts
                   WHERE entity = ? AND value LIKE 'contact:%'""",
                (f"contact:{target_person}",)
            ).fetchall()
            for rev in reverse:
                rev_entity = rev["value"]
                rev_name = rev_entity[len("contact:"):]
                if rev_name in seen_connectors or rev_name in target_people:
                    continue

                imsg = imessage_data.get(rev_entity, {})
                imessage_total = int(imsg.get("imessage_total", 0))
                intensity = imsg.get("message_intensity", "low")

                if imessage_total > 0:
                    seen_connectors.add(rev_name)
                    display_name = rev_name.title()
                    contact_info = contact_by_name.get(rev_name, {})
                    results.append({
                        "connector": display_name,
                        "connector_email": contact_info.get("email"),
                        "relationship_to_you": {
                            "imessage_total": imessage_total,
                            "intensity": intensity,
                        },
                        "connection_to_target": f"Known by {target_person.title()}",
                        "warmth_score": self._intro_warmth_score(imessage_total, intensity),
                        "suggested_action": self._intro_suggested_action(display_name, intensity, target, "connection"),
                    })

        # Strategy C: Contacts at the target company who don't have iMessage data
        # (still useful, just lower warmth)
        for row in company_matches:
            person_name = row["name"].lower()
            if person_name in seen_connectors:
                continue
            seen_connectors.add(person_name)
            display_name = row["name"]
            imsg = imessage_data.get(f"contact:{person_name}", {})
            imessage_total = int(imsg.get("imessage_total", 0))
            intensity = imsg.get("message_intensity", "low")
            # Use company field if set, otherwise infer from email domain
            company_label = row["company"]
            if not company_label and row["email"] and "@" in row["email"]:
                domain = row["email"].split("@")[1].split(".")[0]
                company_label = domain.title()
            results.append({
                "connector": display_name,
                "connector_email": row["email"],
                "relationship_to_you": {
                    "imessage_total": imessage_total,
                    "intensity": intensity,
                },
                "connection_to_target": f"Works at {company_label}",
                "warmth_score": self._intro_warmth_score(imessage_total, intensity),
                "suggested_action": self._intro_suggested_action(display_name, intensity, company_label or target, "employee"),
            })

        # --- Step 4: Sort by warmth score (highest first) ---
        results.sort(key=lambda x: x["warmth_score"], reverse=True)

        return results

    @staticmethod
    def _intro_warmth_score(imessage_total, intensity):
        """Calculate a warmth score 0-100 for ranking intro paths."""
        score = 0
        # Message volume (up to 60 points)
        if imessage_total > 200:
            score += 60
        elif imessage_total > 100:
            score += 50
        elif imessage_total > 50:
            score += 40
        elif imessage_total > 20:
            score += 30
        elif imessage_total > 5:
            score += 15
        elif imessage_total > 0:
            score += 5

        # Intensity bonus (up to 40 points)
        intensity_scores = {"high": 40, "medium": 25, "low": 10}
        score += intensity_scores.get(intensity, 0)

        return min(score, 100)

    @staticmethod
    def _intro_suggested_action(connector_name, intensity, target, connection_type):
        """Generate a suggested action for the intro path."""
        if intensity == "high":
            if connection_type == "employee":
                return f"Text {connector_name} directly — you have a strong iMessage relationship. Ask for a warm intro to decision-makers at {target}."
            return f"Text {connector_name} — you message frequently. Ask them to introduce you to their contact at {target}."
        elif intensity == "medium":
            if connection_type == "employee":
                return f"Reach out to {connector_name} — moderate iMessage history. Mention shared context and ask about {target}."
            return f"Message {connector_name} and ask about their connection to {target}."
        else:
            if connection_type == "employee":
                return f"{connector_name} works at {target} — consider a LinkedIn message or email to re-establish contact first."
            return f"{connector_name} may know someone at {target} — reach out with context about why you're interested."

    def search_graph(self, query):
        """Search across all facts: entity, key, value, source."""
        q = f"%{query}%"
        rows = self.conn.execute(
            """SELECT entity, key, value, source, observed_at FROM facts
               WHERE entity LIKE ? OR key LIKE ? OR value LIKE ? OR source LIKE ?
               ORDER BY observed_at DESC""",
            (q, q, q, q)
        ).fetchall()
        return [dict(r) for r in rows]

    def export_json(self, path=None):
        """Full CRM + graph export as JSON with scores, health, and revenue."""
        data = self.to_json()
        # Enrich with Wave 1-4 data
        data["scores"] = {}
        for c in data["contacts"]:
            identifier = c.get("email") or c["name"]
            sc = self.score_contact(identifier)
            if sc:
                data["scores"][identifier] = sc

        data["health"] = self.health_check()
        data["revenue"] = self.revenue_report()

        json_str = json.dumps(data, indent=2, default=str)
        if path:
            with open(path, "w") as f:
                f.write(json_str)
            return path
        return json_str

    # --- Local Data Connectors (macOS) ---

    def ingest_macos_contacts(self):
        """Import contacts from macOS AddressBook into CRM + context graph.

        Reads ~/Library/Application Support/AddressBook/Sources/*/AddressBook-v22.abcddb
        Returns (contacts_added, facts_added) tuple.
        """
        import glob as globmod
        ab_pattern = os.path.expanduser(
            "~/Library/Application Support/AddressBook/Sources/*/AddressBook-v22.abcddb"
        )
        db_paths = globmod.glob(ab_pattern)
        if not db_paths:
            return (0, 0)

        contacts_added = 0
        facts = []

        for db_path in db_paths:
            try:
                ab = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
                ab.row_factory = sqlite3.Row
            except Exception as e:
                print(f"Warning: AddressBook open error ({db_path}): {e}", file=sys.stderr)
                continue

            try:
                # Get people with names (Z_PK is CoreData's ROWID)
                rows = ab.execute("""
                    SELECT
                        r.Z_PK as rowid,
                        r.ZFIRSTNAME as first,
                        r.ZLASTNAME as last,
                        r.ZORGANIZATION as company,
                        r.ZJOBTITLE as title,
                        r.ZNOTE as notes
                    FROM ZABCDRECORD r
                    WHERE r.ZFIRSTNAME IS NOT NULL OR r.ZLASTNAME IS NOT NULL
                """).fetchall()

                for row in rows:
                    first = (row["first"] or "").strip()
                    last = (row["last"] or "").strip()
                    name = f"{first} {last}".strip()
                    if not name:
                        continue

                    company = (row["company"] or "").strip() or None
                    title = (row["title"] or "").strip() or None
                    rowid = row["rowid"]
                    entity = f"contact:{name.lower()}"

                    # Get emails for this record
                    emails = ab.execute("""
                        SELECT ZADDRESSNORMALIZED as email
                        FROM ZABCDEMAILADDRESS
                        WHERE ZOWNER = ?
                    """, (rowid,)).fetchall()

                    email = emails[0]["email"] if emails else None

                    # Get phone numbers
                    phones = ab.execute("""
                        SELECT ZFULLNUMBER as phone
                        FROM ZABCDPHONENUMBER
                        WHERE ZOWNER = ?
                    """, (rowid,)).fetchall()

                    # Add as contact if they have an email AND a company
                    # (personal contacts without a company stay in the knowledge graph only)
                    if email and company:
                        try:
                            self.add_contact(
                                name=name,
                                email=email,
                                company=company,
                                title=title,
                                source="macos_contacts",
                                status="prospect"
                            )
                            contacts_added += 1
                        except sqlite3.IntegrityError:
                            pass  # email already exists

                    # Build facts — always add to knowledge graph
                    facts.append((entity, "source", "macos_contacts", "macos_contacts"))
                    # Map phone numbers to this contact name for iMessage resolution
                    for p in phones:
                        phone = str(p["phone"] or "").strip()
                        if phone:
                            normalized = self._normalize_phone(phone)
                            if normalized:
                                facts.append((f"phone:{normalized}", "name", name, "macos_contacts"))
                                if company:
                                    facts.append((f"phone:{normalized}", "company", company, "macos_contacts"))

                    # Build facts
                    if company:
                        facts.append((entity, "company", company, "macos_contacts"))
                    if title:
                        facts.append((entity, "title", title, "macos_contacts"))
                    for e in emails:
                        if e["email"]:
                            facts.append((entity, "email", e["email"], "macos_contacts"))
                    for p in phones:
                        if p["phone"]:
                            facts.append((entity, "phone", p["phone"], "macos_contacts"))
                    raw_notes = row["notes"]
                    if isinstance(raw_notes, str) and raw_notes.strip():
                        facts.append((entity, "notes", raw_notes.strip()[:500], "macos_contacts"))

            except Exception as e:
                print(f"Warning: AddressBook read error: {e}", file=sys.stderr)
            finally:
                ab.close()

        facts_added = 0
        if facts:
            facts_added = self.observe_many(facts, source="macos_contacts")

        return (contacts_added, facts_added)

    def ingest_macos_imessage(self, days=90):
        """Import iMessage conversation partners and message counts into context graph.

        Reads ~/Library/Messages/chat.db (requires Full Disk Access).
        Returns (contacts_found, facts_added) tuple.
        """
        chat_db = os.path.expanduser("~/Library/Messages/chat.db")
        if not os.path.exists(chat_db):
            return (0, 0)

        try:
            mdb = sqlite3.connect(f"file:{chat_db}?mode=ro", uri=True)
            mdb.row_factory = sqlite3.Row
        except Exception as e:
            print(f"Warning: iMessage DB open error: {e}", file=sys.stderr)
            return (0, 0)

        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        facts = []
        contacts_found = set()

        try:
            # Get message counts per handle (phone/email) in recent period
            # chat.db stores dates as Apple epoch (seconds since 2001-01-01) * 10^9
            apple_epoch_offset = 978307200  # seconds between Unix epoch and Apple epoch
            cutoff_apple = (datetime.strptime(cutoff, "%Y-%m-%d").timestamp() - apple_epoch_offset) * 1_000_000_000

            rows = mdb.execute("""
                SELECT
                    h.id as handle,
                    COUNT(*) as msg_count,
                    SUM(CASE WHEN m.is_from_me = 1 THEN 1 ELSE 0 END) as sent,
                    SUM(CASE WHEN m.is_from_me = 0 THEN 1 ELSE 0 END) as received,
                    MAX(m.date) as last_msg_date
                FROM message m
                JOIN handle h ON m.handle_id = h.ROWID
                WHERE m.date > ?
                GROUP BY h.id
                ORDER BY msg_count DESC
            """, (int(cutoff_apple),)).fetchall()

            for row in rows:
                handle = row["handle"]
                if not handle:
                    continue

                # Normalize: +1234567890 → phone entity, email → email entity
                if "@" in handle:
                    entity = f"contact:{handle.lower()}"
                else:
                    # Apply same normalization as ingest_macos_contacts so
                    # phone: entities match up for name resolution.
                    normalized_phone = self._normalize_phone(handle)
                    entity = f"phone:{normalized_phone}" if normalized_phone else f"phone:{handle}"

                contacts_found.add(handle)

                msg_count = row["msg_count"]
                sent = row["sent"]
                received = row["received"]

                facts.append((entity, "imessage_total", str(msg_count), "imessage"))
                facts.append((entity, "imessage_sent", str(sent), "imessage"))
                facts.append((entity, "imessage_received", str(received), "imessage"))
                facts.append((entity, "imessage_ratio", f"{sent}:{received}", "imessage"))

                # Classify relationship intensity
                if msg_count > 100:
                    intensity = "high"
                elif msg_count > 20:
                    intensity = "medium"
                else:
                    intensity = "low"
                facts.append((entity, "message_intensity", intensity, "imessage"))

                # Try to match to existing CRM contact by email or phone
                if "@" in handle:
                    existing = self.get_contact(handle)
                    if existing:
                        crm_entity = f"contact:{existing['name'].lower()}"
                        facts.append((crm_entity, "imessage_handle", handle, "imessage"))
                        facts.append((crm_entity, "imessage_total", str(msg_count), "imessage"))
                else:
                    # Check if this phone was resolved to a name via contacts ingest
                    name_fact = self.conn.execute(
                        "SELECT value FROM facts WHERE entity = ? AND key = 'name' AND source = 'macos_contacts' LIMIT 1",
                        (entity,)
                    ).fetchone()
                    if name_fact:
                        crm_entity = f"contact:{name_fact[0].lower()}"
                        facts.append((crm_entity, "imessage_handle", handle, "imessage"))
                        facts.append((crm_entity, "imessage_total", str(msg_count), "imessage"))
                        facts.append((crm_entity, "message_intensity", intensity, "imessage"))

        except Exception as e:
            print(f"Warning: iMessage read error: {e}", file=sys.stderr)
        finally:
            mdb.close()

        facts_added = 0
        if facts:
            facts_added = self.observe_many(facts, source="imessage")

        return (len(contacts_found), facts_added)

    def ingest_macos_calendar(self, days_back=30, days_forward=30):
        """Import calendar events into context graph as meeting facts.

        Reads ~/Library/Calendars/Calendar Cache (requires calendar access).
        Returns (events_found, facts_added) tuple.
        """
        cal_db = os.path.expanduser("~/Library/Calendars/Calendar Cache")
        if not os.path.exists(cal_db):
            return (0, 0)

        try:
            cdb = sqlite3.connect(f"file:{cal_db}?mode=ro", uri=True)
            cdb.row_factory = sqlite3.Row
        except Exception as e:
            print(f"Warning: Calendar DB open error: {e}", file=sys.stderr)
            return (0, 0)

        now = datetime.now()
        start = (now - timedelta(days=days_back)).timestamp()
        end = (now + timedelta(days=days_forward)).timestamp()
        # CoreData stores dates as seconds since 2001-01-01
        apple_epoch = 978307200
        start_apple = start - apple_epoch
        end_apple = end - apple_epoch

        facts = []
        events_found = 0

        try:
            rows = cdb.execute("""
                SELECT
                    ZSUMMARY as title,
                    ZLOCATION as location,
                    ZSTARTDATE as start_date,
                    ZENDDATE as end_date,
                    ZNOTES as notes
                FROM ZCALENDARITEM
                WHERE ZSTARTDATE BETWEEN ? AND ?
                  AND ZSUMMARY IS NOT NULL
                ORDER BY ZSTARTDATE
            """, (start_apple, end_apple)).fetchall()

            for row in rows:
                title = (row["title"] or "").strip()
                if not title:
                    continue

                events_found += 1
                # Convert Apple date to datetime
                ts = row["start_date"] + apple_epoch if row["start_date"] else None
                date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else "unknown"
                entity = f"meeting:{title.lower()[:80]}"

                facts.append((entity, "title", title, "macos_calendar"))
                facts.append((entity, "date", date_str, "macos_calendar"))
                if row["location"]:
                    facts.append((entity, "location", row["location"].strip(), "macos_calendar"))

                # Extract attendee names/emails from notes or title
                # Try to link to CRM contacts
                notes = (row["notes"] or "").strip()
                if notes:
                    facts.append((entity, "notes", notes[:500], "macos_calendar"))

                # Try matching title words to existing contacts
                for word in title.split():
                    if len(word) > 2 and word[0].isupper():
                        match = self.get_contact(word)
                        if match:
                            crm_entity = f"contact:{match['name'].lower()}"
                            facts.append((crm_entity, "meeting", title, "macos_calendar"))
                            facts.append((crm_entity, "last_meeting_date", date_str, "macos_calendar"))

            # Also try to get attendees from the attendee table
            try:
                attendees = cdb.execute("""
                    SELECT
                        a.ZCOMMONNAME as name,
                        a.ZADDRESS as address,
                        i.ZSUMMARY as event_title,
                        i.ZSTARTDATE as event_date
                    FROM ZATTENDEE a
                    JOIN ZCALENDARITEM i ON a.ZEVENT = i.Z_PK
                    WHERE i.ZSTARTDATE BETWEEN ? AND ?
                """, (start_apple, end_apple)).fetchall()

                for att in attendees:
                    att_name = (att["name"] or "").strip()
                    att_addr = (att["address"] or "").replace("mailto:", "").strip()
                    event_title = (att["event_title"] or "").strip()

                    if att_addr and "@" in att_addr:
                        att_entity = f"contact:{att_addr.lower()}"
                        facts.append((att_entity, "meeting", event_title, "macos_calendar"))
                        ts2 = att["event_date"] + apple_epoch if att["event_date"] else None
                        if ts2:
                            facts.append((att_entity, "last_meeting_date",
                                         datetime.fromtimestamp(ts2).strftime("%Y-%m-%d"), "macos_calendar"))
                    elif att_name:
                        att_entity = f"contact:{att_name.lower()}"
                        facts.append((att_entity, "meeting", event_title, "macos_calendar"))

            except Exception:
                pass  # Attendee table may not exist in all versions

        except Exception as e:
            print(f"Warning: Calendar read error: {e}", file=sys.stderr)
        finally:
            cdb.close()

        facts_added = 0
        if facts:
            facts_added = self.observe_many(facts, source="macos_calendar")

        return (events_found, facts_added)

    def ingest_all(self, imessage_days=90, cal_days_back=30, cal_days_forward=30):
        """Run all local data connectors. Returns summary dict."""
        results = {}

        c_added, c_facts = self.ingest_macos_contacts()
        results["contacts"] = {"added": c_added, "facts": c_facts}

        m_found, m_facts = self.ingest_macos_imessage(days=imessage_days)
        results["imessage"] = {"handles": m_found, "facts": m_facts}

        e_found, e_facts = self.ingest_macos_calendar(
            days_back=cal_days_back, days_forward=cal_days_forward
        )
        results["calendar"] = {"events": e_found, "facts": e_facts}

        ml_found, ml_facts = self.ingest_macos_mail(days=imessage_days)
        results["mail"] = {"threads": ml_found, "facts": ml_facts}

        total_facts = c_facts + m_facts + e_facts + ml_facts
        results["total_facts"] = total_facts
        return results

    # --- Smart CSV Import (Salesforce/HubSpot/Any CRM migration) ---

    # Column name mappings for known CRM exports
    _SALESFORCE_MAP = {
        "first name": "_first", "last name": "_last", "name": "name",
        "email": "email", "account name": "company", "company": "company",
        "title": "title", "lead status": "status", "status": "status",
        "annual revenue": "deal_size", "amount": "deal_size",
        "lead source": "source", "description": "notes", "phone": "_phone",
    }
    _HUBSPOT_MAP = {
        "first name": "_first", "last name": "_last",
        "email": "email", "company name": "company", "company": "company",
        "job title": "title", "lifecycle stage": "status",
        "annual revenue": "deal_size", "deal amount": "deal_size",
        "original source": "source", "notes": "notes", "phone number": "_phone",
    }
    _STATUS_MAP = {
        # Salesforce
        "new": "prospect", "open": "prospect", "working": "contacted",
        "qualified": "contacted", "nurturing": "contacted",
        "negotiation": "proposal_drafted", "closedwon": "active_customer",
        "closed-won": "active_customer", "closedlost": "lost",
        "closed-lost": "lost", "unqualified": "lost",
        # HubSpot
        "subscriber": "prospect", "lead": "prospect", "marketingqualifiedlead": "contacted",
        "salesqualifiedlead": "contacted", "opportunity": "proposal_drafted",
        "customer": "active_customer", "evangelist": "active_customer", "other": "prospect",
    }

    def import_smart(self, csv_path, field_map=None):
        """Auto-detect CSV format and import. Works with Salesforce, HubSpot, or any CRM export.

        Returns dict with contacts_added, facts_added, skipped, mapping_used.
        """
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = [h.strip() for h in (reader.fieldnames or [])]
            headers_lower = [h.lower() for h in headers]

            # Detect format
            if field_map:
                mapping = field_map
            else:
                mapping = self._auto_map_columns(headers_lower)

            contacts_added = 0
            facts_added_list = []
            skipped = 0

            for row in reader:
                # Normalize row keys
                normalized = {}
                for h, v in row.items():
                    h_lower = h.strip().lower()
                    if h_lower in mapping:
                        normalized[mapping[h_lower]] = (v or "").strip()

                # Handle first/last name split
                first = normalized.pop("_first", "")
                last = normalized.pop("_last", "")
                phone = normalized.pop("_phone", "")
                if "_first" in normalized:
                    del normalized["_first"]

                name = normalized.get("name", "").strip()
                if not name and (first or last):
                    name = f"{first} {last}".strip()
                if not name:
                    skipped += 1
                    continue
                normalized["name"] = name

                # Map status
                raw_status = (normalized.get("status", "") or "").lower().replace(" ", "")
                normalized["status"] = self._STATUS_MAP.get(raw_status, normalized.get("status", "prospect")) or "prospect"

                # Add contact
                try:
                    self.add_contact(
                        name=normalized.get("name", ""),
                        email=normalized.get("email"),
                        company=normalized.get("company"),
                        title=normalized.get("title"),
                        deal_size=normalized.get("deal_size"),
                        status=normalized.get("status", "prospect"),
                        source=normalized.get("source"),
                        notes=normalized.get("notes"),
                    )
                    contacts_added += 1
                except sqlite3.IntegrityError:
                    skipped += 1

                # Store unmapped columns as facts
                entity = f"contact:{name.lower()}"
                mapped_vals = set(mapping.values())
                for h, v in row.items():
                    h_lower = h.strip().lower()
                    if h_lower not in mapping and v and v.strip():
                        facts_added_list.append((entity, h.strip(), v.strip(), "csv_import"))
                if phone:
                    facts_added_list.append((entity, "phone", phone, "csv_import"))

            facts_count = 0
            if facts_added_list:
                facts_count = self.observe_many(facts_added_list, source="csv_import")

            return {
                "contacts_added": contacts_added,
                "facts_added": facts_count,
                "skipped": skipped,
                "mapping_used": {k: v for k, v in mapping.items() if v not in ("_first", "_last", "_phone")},
            }

    def import_salesforce(self, csv_path):
        """Import from Salesforce CSV export with auto-mapped columns."""
        return self.import_smart(csv_path, field_map=self._SALESFORCE_MAP)

    def import_hubspot(self, csv_path):
        """Import from HubSpot CSV export with auto-mapped columns."""
        return self.import_smart(csv_path, field_map=self._HUBSPOT_MAP)

    def _auto_map_columns(self, headers_lower):
        """Heuristic column mapping for any CSV."""
        mapping = {}
        # Try known maps first
        sf_hits = sum(1 for h in headers_lower if h in self._SALESFORCE_MAP)
        hs_hits = sum(1 for h in headers_lower if h in self._HUBSPOT_MAP)

        if sf_hits >= 3:
            base = self._SALESFORCE_MAP
        elif hs_hits >= 3:
            base = self._HUBSPOT_MAP
        else:
            base = {}

        for h in headers_lower:
            if h in base:
                mapping[h] = base[h]
                continue
            # Fuzzy matching
            if "email" in h:
                mapping[h] = "email"
            elif "first" in h and "name" in h:
                mapping[h] = "_first"
            elif "last" in h and "name" in h:
                mapping[h] = "_last"
            elif h in ("name", "full name", "contact name"):
                mapping[h] = "name"
            elif any(w in h for w in ("company", "organization", "org", "account")):
                mapping[h] = "company"
            elif any(w in h for w in ("title", "job title", "role", "position")):
                mapping[h] = "title"
            elif any(w in h for w in ("amount", "value", "deal", "revenue", "mrr", "arr")):
                mapping[h] = "deal_size"
            elif any(w in h for w in ("status", "stage", "lifecycle")):
                mapping[h] = "status"
            elif any(w in h for w in ("source", "origin", "channel", "referral")):
                mapping[h] = "source"
            elif any(w in h for w in ("note", "description", "comment")):
                mapping[h] = "notes"
            elif "phone" in h:
                mapping[h] = "_phone"

        return mapping

    # --- Email Thread Ingestion ---

    def ingest_macos_mail(self, days=90):
        """Import email threads from Mail.app into activity log + context graph.

        Reads ~/Library/Mail/V10/MailData/Envelope Index (SQLite).
        Returns (threads_found, facts_added) tuple.
        """
        envelope_db = os.path.expanduser("~/Library/Mail/V10/MailData/Envelope Index")
        if not os.path.exists(envelope_db):
            # Try older path
            envelope_db = os.path.expanduser("~/Library/Mail/V9/MailData/Envelope Index")
            if not os.path.exists(envelope_db):
                return (0, 0)

        try:
            mdb = sqlite3.connect(f"file:{envelope_db}?mode=ro", uri=True)
            mdb.row_factory = sqlite3.Row
        except Exception as e:
            print(f"Warning: Mail DB open error: {e}", file=sys.stderr)
            return (0, 0)

        cutoff = int((datetime.now() - timedelta(days=days)).timestamp())
        facts = []
        threads_found = 0
        email_counts = {}  # address -> {sent: int, received: int, last: str}

        try:
            # Query messages joined to sender address and recipient addresses.
            # messages.sender -> addresses.ROWID (sender email)
            # recipients.message -> messages.ROWID, recipients.address -> addresses.ROWID
            # messages.subject -> subjects.ROWID (subject text)
            # recipients.type: 0 = To, 1 = Cc, 2 = Bcc
            rows = mdb.execute("""
                SELECT
                    s.subject,
                    m.date_sent,
                    sa.address as from_addr,
                    ra.address as to_addr,
                    m.read as is_read
                FROM messages m
                LEFT JOIN addresses sa ON sa.ROWID = m.sender
                LEFT JOIN recipients r ON r.message = m.ROWID AND r.type = 0
                LEFT JOIN addresses ra ON ra.ROWID = r.address
                LEFT JOIN subjects s ON s.ROWID = m.subject
                WHERE m.date_sent > ?
                ORDER BY m.date_sent DESC
            """, (cutoff,)).fetchall()

            for row in rows:
                from_addr = (row["from_addr"] or "").strip().lower()
                to_addr = (row["to_addr"] or "").strip().lower()
                subject = (row["subject"] or "").strip()

                if not from_addr and not to_addr:
                    continue
                threads_found += 1

                # Determine if sent or received (heuristic: check if from_addr matches known emails)
                # We'll track both directions
                for addr in (from_addr, to_addr):
                    if addr and "@" in addr:
                        if addr not in email_counts:
                            email_counts[addr] = {"sent": 0, "received": 0, "last": ""}
                        email_counts[addr]["last"] = str(row["date_sent"] or "")

                if from_addr and "@" in from_addr:
                    email_counts[from_addr]["sent"] += 1
                if to_addr and "@" in to_addr:
                    email_counts[to_addr]["received"] += 1

        except Exception as e:
            print(f"Warning: Mail read error: {e}", file=sys.stderr)
        finally:
            mdb.close()

        # Build facts and link to CRM contacts
        for addr, counts in email_counts.items():
            entity = f"email:{addr}"
            total = counts["sent"] + counts["received"]
            facts.append((entity, "email_total", str(total), "macos_mail"))
            facts.append((entity, "email_sent", str(counts["sent"]), "macos_mail"))
            facts.append((entity, "email_received", str(counts["received"]), "macos_mail"))

            # Try to match to CRM contact
            existing = self.get_contact(addr)
            if existing:
                crm_entity = f"contact:{existing['name'].lower()}"
                facts.append((crm_entity, "email_total", str(total), "macos_mail"))
                facts.append((crm_entity, "email_frequency", "high" if total > 50 else "medium" if total > 10 else "low", "macos_mail"))

        facts_added = 0
        if facts:
            facts_added = self.observe_many(facts, source="macos_mail")

        return (threads_found, facts_added)

    def ingest_mbox(self, mbox_path, days=90):
        """Import from any mbox file (Gmail Takeout, Thunderbird, etc).

        Uses stdlib mailbox module. Cross-platform.
        Returns (messages_imported, facts_added) tuple.
        """
        import mailbox
        import email.utils

        if not os.path.exists(mbox_path):
            return (0, 0)

        cutoff = datetime.now() - timedelta(days=days)
        facts = []
        messages_imported = 0
        email_counts = {}

        try:
            mbox = mailbox.mbox(mbox_path)
            for msg in mbox:
                # Parse date
                date_str = msg.get("Date", "")
                try:
                    date_tuple = email.utils.parsedate_to_datetime(date_str)
                    if date_tuple < cutoff:
                        continue
                except (TypeError, ValueError):
                    continue

                from_addr = ""
                from_header = msg.get("From", "")
                parsed = email.utils.parseaddr(from_header)
                if parsed[1]:
                    from_addr = parsed[1].lower()

                to_addrs = []
                to_header = msg.get("To", "")
                for _, addr in email.utils.getaddresses([to_header]):
                    if addr:
                        to_addrs.append(addr.lower())

                subject = msg.get("Subject", "")
                messages_imported += 1

                for addr in [from_addr] + to_addrs:
                    if addr and "@" in addr:
                        if addr not in email_counts:
                            email_counts[addr] = {"sent": 0, "received": 0, "subjects": []}
                        email_counts[addr]["subjects"].append(subject[:100])

                if from_addr:
                    if from_addr in email_counts:
                        email_counts[from_addr]["sent"] += 1
                for addr in to_addrs:
                    if addr in email_counts:
                        email_counts[addr]["received"] += 1

            mbox.close()
        except Exception as e:
            print(f"Warning: mbox read error: {e}", file=sys.stderr)

        for addr, counts in email_counts.items():
            entity = f"email:{addr}"
            total = counts["sent"] + counts["received"]
            facts.append((entity, "email_total", str(total), "mbox_import"))
            facts.append((entity, "email_sent", str(counts["sent"]), "mbox_import"))
            facts.append((entity, "email_received", str(counts["received"]), "mbox_import"))

            existing = self.get_contact(addr)
            if existing:
                crm_entity = f"contact:{existing['name'].lower()}"
                facts.append((crm_entity, "email_total", str(total), "mbox_import"))

        facts_added = 0
        if facts:
            facts_added = self.observe_many(facts, source="mbox_import")

        return (messages_imported, facts_added)

    # --- Relationship Velocity ---

    def _contact_entity_keys(self, contact):
        """Return all entity key variants for a contact, used across graph lookups."""
        name_lower = contact["name"].lower()
        parts = name_lower.split()
        keys = list(set(
            [f"contact:{name_lower}",
             f"contact:{name_lower.replace(' ', '_')}"]
            + [f"contact:{p}" for p in parts]
            + ([f"contact:{contact['email'].lower()}"] if contact.get("email") else [])
        ))
        return keys

    def velocity(self, identifier, window_days=14):
        """Measure engagement velocity: acceleration or decay of interactions.

        Counts both explicit activity-table entries AND graph-sourced interactions
        (iMessage, calendar meetings, mail) from the facts table for a complete
        picture of engagement momentum.

        Returns dict with velocity ratio, trend, and projected days until cold.
        """
        contact = self.get_contact(identifier)
        if not contact:
            return None

        now = datetime.now()
        current_start = (now - timedelta(days=window_days)).strftime("%Y-%m-%d %H:%M:%S")
        prev_start = (now - timedelta(days=window_days * 2)).strftime("%Y-%m-%d %H:%M:%S")
        current_end = now.strftime("%Y-%m-%d %H:%M:%S")

        # Count activities in each period (activity table)
        current_count = self.conn.execute(
            """SELECT COUNT(*) FROM activity
               WHERE contact_id = ? AND created_at >= ?""",
            (contact["id"], current_start)
        ).fetchone()[0]

        prev_count = self.conn.execute(
            """SELECT COUNT(*) FROM activity
               WHERE contact_id = ? AND created_at >= ? AND created_at < ?""",
            (contact["id"], prev_start, current_start)
        ).fetchone()[0]

        # Also count graph-sourced interactions (iMessage, calendar, mail facts)
        entity_keys = self._contact_entity_keys(contact)
        if entity_keys:
            placeholders = ",".join("?" * len(entity_keys))
            graph_sources = ("imessage", "macos_calendar", "macos_mail")
            src_placeholders = ",".join("?" * len(graph_sources))

            current_graph = self.conn.execute(
                f"""SELECT COUNT(*) FROM facts
                    WHERE entity IN ({placeholders})
                    AND source IN ({src_placeholders})
                    AND observed_at >= ?""",
                list(entity_keys) + list(graph_sources) + [current_start]
            ).fetchone()[0]

            prev_graph = self.conn.execute(
                f"""SELECT COUNT(*) FROM facts
                    WHERE entity IN ({placeholders})
                    AND source IN ({src_placeholders})
                    AND observed_at >= ? AND observed_at < ?""",
                list(entity_keys) + list(graph_sources) + [prev_start, current_start]
            ).fetchone()[0]

            current_count += current_graph
            prev_count += prev_graph

        # Calculate velocity
        if prev_count == 0 and current_count == 0:
            vel = 0.0
            trend = "dead"
        elif prev_count == 0:
            vel = float("inf")
            trend = "accelerating"
        else:
            vel = current_count / prev_count
            if vel > 1.2:
                trend = "accelerating"
            elif vel > 0.8:
                trend = "stable"
            else:
                trend = "decaying"

        # Days until cold (extrapolate decay)
        days_until_cold = None
        if trend == "decaying" and current_count > 0 and vel > 0:
            # At current decay rate, how many periods until < 1 activity?
            import math
            try:
                periods = math.log(1 / current_count) / math.log(vel)
                days_until_cold = max(1, int(periods * window_days))
            except (ValueError, ZeroDivisionError):
                pass

        # Average response time (time between received activities and next sent)
        response_time = None
        acts = self.conn.execute(
            """SELECT type, created_at FROM activity
               WHERE contact_id = ? ORDER BY created_at""",
            (contact["id"],)
        ).fetchall()
        if len(acts) >= 2:
            gaps = []
            for i in range(1, len(acts)):
                try:
                    t1 = datetime.strptime(acts[i-1]["created_at"][:19], "%Y-%m-%d %H:%M:%S")
                    t2 = datetime.strptime(acts[i]["created_at"][:19], "%Y-%m-%d %H:%M:%S")
                    gap_hours = (t2 - t1).total_seconds() / 3600
                    if gap_hours > 0:
                        gaps.append(gap_hours)
                except (ValueError, TypeError):
                    pass
            if gaps:
                response_time = round(sum(gaps) / len(gaps), 1)

        return {
            "current_period": {"activities": current_count, "period_days": window_days},
            "previous_period": {"activities": prev_count, "period_days": window_days},
            "velocity": round(vel, 2) if vel != float("inf") else 999.0,
            "trend": trend,
            "days_until_cold": days_until_cold,
            "response_time_avg_hours": response_time,
        }

    def relationship_health(self):
        """Analyze iMessage + contacts data to surface relationship health insights.

        Returns a list of dicts sorted by actionability, each with:
        name, total_messages, intensity, reciprocity, status, in_pipeline, suggestion.
        """
        # Gather all entities with imessage facts
        rows = self.conn.execute(
            """SELECT entity, key, value FROM facts
               WHERE source = 'imessage' AND key IN
               ('imessage_total', 'imessage_sent', 'imessage_received', 'message_intensity')
               ORDER BY entity"""
        ).fetchall()

        # Build per-entity data
        entity_data = {}
        for r in rows:
            entity = r["entity"]
            if entity not in entity_data:
                entity_data[entity] = {}
            entity_data[entity][r["key"]] = r["value"]

        # Resolve entity names and check CRM pipeline status
        results = []
        seen_names = set()

        for entity, data in entity_data.items():
            total = int(data.get("imessage_total", 0))
            if total == 0:
                continue

            sent = int(data.get("imessage_sent", 0))
            received = int(data.get("imessage_received", 0))
            intensity = data.get("message_intensity", "low")

            # Derive display name from entity
            if entity.startswith("contact:"):
                raw_name = entity[len("contact:"):]
                name = " ".join(w.capitalize() for w in raw_name.split())
            elif entity.startswith("phone:"):
                name = entity[len("phone:"):]
            else:
                name = entity

            # Deduplicate — keep the entry with more messages
            name_key = name.lower()
            if name_key in seen_names:
                replaced = False
                for i, existing in enumerate(results):
                    if existing["name"].lower() == name_key:
                        if total > existing["total_messages"]:
                            results[i] = None
                            replaced = True
                        break
                if not replaced:
                    continue
                results = [r for r in results if r is not None]
            seen_names.add(name_key)

            # Compute reciprocity
            if received > 0 and sent > 0:
                ratio = sent / received
                if 0.5 <= ratio <= 2.0:
                    reciprocity = "balanced"
                elif ratio > 2.0:
                    reciprocity = "you-heavy"
                else:
                    reciprocity = "them-heavy"
            elif sent > 0 and received == 0:
                reciprocity = "you-only"
            elif received > 0 and sent == 0:
                reciprocity = "them-only"
            else:
                reciprocity = "none"

            # Determine relationship status
            if total >= 50 and reciprocity == "balanced":
                status = "strong"
            elif reciprocity in ("them-heavy", "them-only") and received > 10:
                status = "one-sided-in"
            elif reciprocity in ("you-heavy", "you-only") and sent > 10:
                status = "one-sided-out"
            elif total < 5:
                status = "dormant"
            elif intensity == "low" and total < 20:
                status = "fading"
            elif reciprocity == "balanced":
                status = "strong"
            else:
                status = "fading"

            # Check if in CRM with a deal
            in_pipeline = False
            crm_contact = self.get_contact(name)
            if crm_contact and crm_contact.get("deal_size"):
                in_pipeline = True
            if not crm_contact and entity.startswith("phone:"):
                name_fact = self.conn.execute(
                    "SELECT value FROM facts WHERE entity = ? AND key = 'name' LIMIT 1",
                    (entity,)
                ).fetchone()
                if name_fact:
                    crm_contact = self.get_contact(name_fact[0])
                    if crm_contact and crm_contact.get("deal_size"):
                        in_pipeline = True

            # Generate actionable suggestion
            if status == "one-sided-in" and received > sent * 5:
                suggestion = f"Reply - they've sent {received} msgs, you've sent {sent}"
            elif status == "one-sided-in":
                suggestion = f"Respond more - they message {received} vs your {sent}"
            elif status == "one-sided-out":
                suggestion = f"Ease off - you've sent {sent} msgs, they've sent {received}"
            elif status == "strong" and in_pipeline:
                suggestion = "Strong relationship - ask for intro"
            elif status == "strong":
                suggestion = "Strong relationship - consider adding to CRM"
            elif status == "dormant":
                suggestion = "Reconnect - send a quick check-in"
            elif status == "fading":
                suggestion = "Re-engage before they go cold"
            else:
                suggestion = "Monitor"

            results.append({
                "name": name,
                "total_messages": total,
                "intensity": intensity,
                "reciprocity": reciprocity,
                "status": status,
                "in_pipeline": in_pipeline,
                "suggestion": suggestion,
            })

        # Sort: one-sided-in first (most actionable), then by total messages desc
        status_priority = {
            "one-sided-in": 0,
            "fading": 1,
            "one-sided-out": 2,
            "dormant": 3,
            "strong": 4,
        }
        results.sort(key=lambda r: (status_priority.get(r["status"], 5), -r["total_messages"]))
        return results

    def relationship_health_report(self):
        """All contacts ranked by relationship health — decaying high-value first.

        Returns list sorted by urgency: who are you about to lose?
        """
        contacts = self.list_contacts()
        results = []

        for c in contacts:
            identifier = c.get("email") or c["name"]
            vel = self.velocity(identifier)
            if not vel:
                continue

            score_data = self.score_contact(identifier)
            score = score_data["score"] if score_data else 0
            deal = self._parse_deal_size(c.get("deal_size", ""))

            results.append({
                "name": c["name"],
                "email": c.get("email"),
                "company": c.get("company"),
                "status": c["status"],
                "deal_value": deal,
                "score": score,
                "velocity": vel["velocity"],
                "trend": vel["trend"],
                "days_until_cold": vel["days_until_cold"],
            })

        # Sort: decaying high-value first
        def sort_key(r):
            trend_priority = {"decaying": 0, "dead": 1, "stable": 2, "accelerating": 3}
            return (
                trend_priority.get(r["trend"], 2),
                -(r["deal_value"] or 0),
                -(r["score"] or 0),
            )

        results.sort(key=sort_key)
        return results

    # --- Saved Views (Smart Lists) ---

    def _ensure_views_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS views (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                query TEXT NOT NULL,
                last_result_ids TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def save_view(self, name, *, status=None, tags=None, company=None,
                  min_score=None, min_deal=None, max_stale_days=None,
                  fact_key=None, fact_value=None):
        """Save a named smart list. Runs live against current data."""
        self._ensure_views_table()
        query_data = {}
        if status:
            query_data["status"] = status
        if tags:
            query_data["tags"] = tags
        if company:
            query_data["company"] = company
        if min_score is not None:
            query_data["min_score"] = min_score
        if min_deal is not None:
            query_data["min_deal"] = min_deal
        if max_stale_days is not None:
            query_data["max_stale_days"] = max_stale_days
        if fact_key is not None:
            query_data["fact_key"] = fact_key
        if fact_value is not None:
            query_data["fact_value"] = fact_value

        query_json = json.dumps(query_data)
        self.conn.execute(
            """INSERT OR REPLACE INTO views (name, query) VALUES (?, ?)""",
            (name, query_json)
        )
        self.conn.commit()

        # Run it immediately and store result IDs
        results = self.run_view(name)
        result_ids = json.dumps([r.get("id") for r in results])
        self.conn.execute(
            "UPDATE views SET last_result_ids = ? WHERE name = ?",
            (result_ids, name)
        )
        self.conn.commit()
        return len(results)

    def run_view(self, name):
        """Execute a saved view and return matching contacts."""
        self._ensure_views_table()
        row = self.conn.execute(
            "SELECT query FROM views WHERE name = ?", (name,)
        ).fetchone()
        if not row:
            return []

        q = json.loads(row["query"])

        # Start with segment filters
        results = self.segment(
            status=q.get("status"),
            tags=q.get("tags"),
            company=q.get("company"),
            fact_key=q.get("fact_key"),
            fact_value=q.get("fact_value"),
        )

        # Apply additional filters
        if q.get("min_score"):
            scored = []
            for c in results:
                identifier = c.get("email") or c["name"]
                s = self.score_contact(identifier)
                if s and s["score"] >= q["min_score"]:
                    c["score"] = s["score"]
                    scored.append(c)
            results = scored

        if q.get("min_deal"):
            results = [c for c in results if self._parse_deal_size(c.get("deal_size", "")) >= q["min_deal"]]

        if q.get("max_stale_days"):
            cutoff = (datetime.now() - timedelta(days=q["max_stale_days"])).strftime("%Y-%m-%d")
            results = [c for c in results if not c.get("last_contacted") or c["last_contacted"] < cutoff]

        return results

    def list_views(self):
        """List all saved views with current match counts."""
        self._ensure_views_table()
        rows = self.conn.execute("SELECT name, query, created_at FROM views ORDER BY name").fetchall()
        views = []
        for r in rows:
            count = len(self.run_view(r["name"]))
            views.append({
                "name": r["name"],
                "query": json.loads(r["query"]),
                "count": count,
                "created_at": r["created_at"],
            })
        return views

    def delete_view(self, name):
        """Delete a saved view."""
        self._ensure_views_table()
        self.conn.execute("DELETE FROM views WHERE name = ?", (name,))
        self.conn.commit()
        return True

    def watch(self, view_name):
        """Compare view's current results to last-run results. What changed?"""
        self._ensure_views_table()
        row = self.conn.execute(
            "SELECT last_result_ids FROM views WHERE name = ?", (view_name,)
        ).fetchone()

        current_results = self.run_view(view_name)
        current_ids = set(c.get("id") for c in current_results)

        old_ids = set()
        if row and row["last_result_ids"]:
            old_ids = set(json.loads(row["last_result_ids"]))

        added = [c for c in current_results if c.get("id") not in old_ids]
        removed_ids = old_ids - current_ids

        # Update stored IDs
        result_ids = json.dumps(list(current_ids))
        self.conn.execute(
            "UPDATE views SET last_result_ids = ? WHERE name = ?",
            (result_ids, view_name)
        )
        self.conn.commit()

        return {
            "added": added,
            "removed_count": len(removed_ids),
            "total": len(current_results),
        }

    # --- Interaction Prompts (AI-native follow-up engine) ---

    def interaction_prompt(self, identifier, action_type="follow_up"):
        """Generate a structured prompt for an AI agent to craft the next interaction.

        action_type: follow_up, cold_outreach, proposal, check_in, close, save
        """
        contact = self.get_contact(identifier)
        if not contact:
            return None

        name = contact["name"]
        company = contact.get("company") or "unknown company"
        status = contact.get("status", "prospect")
        deal = contact.get("deal_size") or "no deal"
        entity = f"contact:{name.lower()}"

        # Gather all context
        facts = self.facts_about(entity)
        acts = self.get_activity(identifier, limit=10)
        vel = self.velocity(identifier)
        score_data = self.score_contact(identifier)
        score = score_data["score"] if score_data else 0

        # Build context sections
        parts = []
        parts.append(f"# Interaction Brief: {name}")
        parts.append(f"**Company:** {company}  |  **Status:** {status}  |  **Deal:** {deal}  |  **Score:** {score}/100")
        parts.append("")

        # Relationship velocity
        if vel:
            parts.append(f"**Engagement:** {vel['trend']} (velocity {vel['velocity']}x)")
            if vel["days_until_cold"]:
                parts.append(f"**Warning:** Projected to go cold in {vel['days_until_cold']} days")
            if vel["response_time_avg_hours"]:
                parts.append(f"**Avg gap between interactions:** {vel['response_time_avg_hours']:.0f}h")
            parts.append("")

        # Known facts
        if facts:
            parts.append("**Known facts:**")
            for k, v in facts.items():
                parts.append(f"- {k}: {v.get('value', v) if isinstance(v, dict) else v}")
            parts.append("")

        # Recent activity
        if acts:
            parts.append("**Recent activity:**")
            for a in acts[:5]:
                date = (a.get("created_at") or a.get("date", ""))[:10]
                parts.append(f"- [{date}] {a['type']}: {a['summary']}")
            parts.append("")

        # Warm intro paths — surface mutual connections from the knowledge graph
        if company and company != "unknown company":
            try:
                intros = self.find_intros(company)
                if intros:
                    parts.append("**Warm intro paths:**")
                    for intro in intros[:3]:
                        warmth = intro.get("warmth_score", 0)
                        connector = intro["connector"]
                        connection = intro["connection_to_target"]
                        parts.append(f"- {connector} ({connection}, warmth {warmth}/100)")
                        if intro.get("suggested_action"):
                            parts.append(f"  Suggested: {intro['suggested_action']}")
                    parts.append("")
            except Exception:
                pass  # Graph data may not exist; don't break the prompt

        # Action-specific instructions
        templates = {
            "follow_up": (
                "**Task:** Write a follow-up message.\n"
                "- Reference the last interaction specifically\n"
                "- Add value (insight, resource, or connection)\n"
                "- One clear next step\n"
                "- Keep it under 100 words"
            ),
            "cold_outreach": (
                "**Task:** Write a cold outreach message.\n"
                "- Lead with THEIR pain, not your product\n"
                "- Reference something specific about their business\n"
                "- Show proof it works (example, not pitch)\n"
                "- CTA: specific, not 'let's chat'\n"
                "- 3-4 sentences max"
            ),
            "proposal": (
                "**Task:** Draft a proposal outline.\n"
                "- Restate their problem in their words\n"
                "- Specific deliverables with timeline\n"
                "- Pricing tied to value, not hours\n"
                "- Include 'what happens if you don't act' urgency"
            ),
            "check_in": (
                "**Task:** Write a check-in message.\n"
                "- NOT 'just checking in'\n"
                "- Bring a specific insight or update\n"
                "- Ask about something they mentioned before\n"
                "- Light touch, no ask"
            ),
            "close": (
                "**Task:** Write a closing message.\n"
                "- Summarize the value they've seen\n"
                "- Create urgency (timeline, limited availability, price change)\n"
                "- Make saying yes easy (next step is tiny)\n"
                "- Address the likely objection preemptively"
            ),
            "save": (
                "**Task:** Write a re-engagement message for a decaying relationship.\n"
                "- Acknowledge the gap without being needy\n"
                "- Lead with something new and relevant to them\n"
                "- Low-friction CTA (share a resource, not book a call)\n"
                "- If the relationship is dead, consider whether to reach out at all"
            ),
        }

        parts.append(templates.get(action_type, templates["follow_up"]))

        return "\n".join(parts)

    def batch_prompts(self, view_name=None, action_type="follow_up"):
        """Generate interaction prompts for multiple contacts.

        Uses a saved view or next_actions() to select contacts.
        Returns list of {contact, action, prompt} dicts.
        """
        if view_name:
            contacts = self.run_view(view_name)
        else:
            actions = self.next_actions(limit=10)
            contacts = []
            for a in actions:
                c = self.get_contact(a["contact"])
                if c:
                    contacts.append(c)

        results = []
        for c in contacts:
            identifier = c.get("email") or c["name"]
            prompt = self.interaction_prompt(identifier, action_type=action_type)
            if prompt:
                results.append({
                    "contact": c["name"],
                    "email": c.get("email"),
                    "company": c.get("company"),
                    "prompt": prompt,
                })

        return results


# --- CLI ---

def fmt_table(rows, columns):
    if not rows:
        print("  (empty)")
        return
    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            widths[col] = max(widths[col], len(str(row.get(col, "") or "")))
    header = "  ".join(col.ljust(widths[col]) for col in columns)
    sep = "  ".join("-" * widths[col] for col in columns)
    print(header)
    print(sep)
    for row in rows:
        print("  ".join(str(row.get(col, "") or "").ljust(widths[col]) for col in columns))


def main():
    parser = argparse.ArgumentParser(description="agent-crm: local-first CRM for humans and AI agents")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to CRM database")
    sub = parser.add_subparsers(dest="command")

    # add
    p_add = sub.add_parser("add", help="Add a contact")
    p_add.add_argument("name")
    p_add.add_argument("--email", "-e")
    p_add.add_argument("--company", "-c")
    p_add.add_argument("--title", "-t")
    p_add.add_argument("--deal", "-d")
    p_add.add_argument("--status", "-s", default="prospect")
    p_add.add_argument("--source")
    p_add.add_argument("--notes", "-n")
    p_add.add_argument("--tags")
    p_add.add_argument("--force", "-f", action="store_true", help="Add even if a contact with the same name exists")

    # ls
    p_ls = sub.add_parser("ls", help="List contacts")
    p_ls.add_argument("--status", "-s")
    p_ls.add_argument("--company", "-c")

    # view
    p_view = sub.add_parser("view", help="View a contact")
    p_view.add_argument("identifier", help="Email or name")

    # update
    p_up = sub.add_parser("update", help="Update a contact")
    p_up.add_argument("identifier", help="Email or name")
    p_up.add_argument("--name")
    p_up.add_argument("--company", "-c")
    p_up.add_argument("--title", "-t")
    p_up.add_argument("--deal", "-d")
    p_up.add_argument("--status", "-s")
    p_up.add_argument("--source")
    p_up.add_argument("--notes", "-n")
    p_up.add_argument("--tags")

    # delete
    p_del = sub.add_parser("delete", help="Delete a contact")
    p_del.add_argument("identifier")

    # log
    p_log = sub.add_parser("log", help="Log activity on a contact")
    p_log.add_argument("identifier")
    p_log.add_argument("type", help="e.g. email, call, meeting, note")
    p_log.add_argument("summary")

    # activity
    p_act = sub.add_parser("activity", help="View activity for a contact")
    p_act.add_argument("identifier")
    p_act.add_argument("--limit", "-l", type=int, default=20)

    # search
    p_search = sub.add_parser("search", help="Search contacts")
    p_search.add_argument("term")

    # pipeline
    sub.add_parser("pipeline", help="View pipeline summary")

    # stats
    sub.add_parser("stats", help="CRM stats")

    # markdown
    sub.add_parser("markdown", help="Dump CRM as markdown (for agent context)")

    # json
    sub.add_parser("json", help="Dump CRM as JSON (for agent consumption)")

    # export
    p_export = sub.add_parser("export", help="Export contacts to CSV")
    p_export.add_argument("--output", "-o", help="Output file path")
    p_export.add_argument("--enrich", action="store_true", help="Include activity_count, deal_count, and score columns")

    # import
    p_import = sub.add_parser("import", help="Import contacts from CSV")
    p_import.add_argument("file", help="CSV file path")

    # observe
    p_obs = sub.add_parser("observe", help="Record a fact: entity key value [source]")
    p_obs.add_argument("entity", help="e.g. contact:maneet, company:interstate")
    p_obs.add_argument("key", help="e.g. status, deal_size, competes_with")
    p_obs.add_argument("value")
    p_obs.add_argument("--source", default="manual")

    # facts
    p_facts = sub.add_parser("facts", help="Show latest facts about an entity")
    p_facts.add_argument("entity")

    # graph history
    p_hist = sub.add_parser("history", help="Full history of an entity")
    p_hist.add_argument("entity")
    p_hist.add_argument("--key", "-k", help="Filter to one key")

    # graph
    sub.add_parser("graph", help="Dump context graph as markdown")

    # stale
    p_stale = sub.add_parser("stale", help="Facts older than N days")
    p_stale.add_argument("--days", "-d", type=int, default=7)

    # reverse-lookup
    p_rev = sub.add_parser("reverse-lookup", help="Find entities that reference a given entity")
    p_rev.add_argument("entity")

    # reachable
    p_reach = sub.add_parser("reachable", help="Find entities reachable within N hops")
    p_reach.add_argument("entity")
    p_reach.add_argument("--hops", type=int, default=2)

    # conflicts
    p_conf = sub.add_parser("conflicts", help="Show fact conflicts (same entity+key, different values)")
    p_conf.add_argument("--entity", help="Restrict to a specific entity")

    # --- Lead Intelligence CLI ---
    p_score = sub.add_parser("score", help="Score a contact (0-100)")
    p_score.add_argument("identifier", help="Email or name")

    sub.add_parser("prioritize", help="Rank contacts by score")

    sub.add_parser("health", help="Pipeline health check")

    sub.add_parser("funnel", help="Conversion funnel analysis")

    sub.add_parser("forecast", help="Weighted pipeline forecast")

    sub.add_parser("duplicates", help="Find potential duplicate contacts")

    p_stalec = sub.add_parser("stale-contacts", help="Contacts not contacted in N days")
    p_stalec.add_argument("--days", "-D", type=int, default=14)

    # --- Agent Superpowers (Wave 3) CLI ---
    p_query = sub.add_parser("query", help="Natural language query")
    p_query.add_argument("q", help="e.g. 'active customers', 'high value', 'enterprise'")

    p_segment = sub.add_parser("segment", help="Filter contacts by multiple criteria")
    p_segment.add_argument("--tag", help="Filter by tag")
    p_segment.add_argument("--status", "-s", help="Filter by status")
    p_segment.add_argument("--min-score", type=int, help="Minimum score")
    p_segment.add_argument("--company", "-c", help="Filter by company")
    p_segment.add_argument("--fact-key", help="Filter by graph fact key (e.g. message_intensity)")
    p_segment.add_argument("--fact-value", help="Filter by graph fact value (e.g. high)")

    p_timeline = sub.add_parser("timeline", help="Chronological timeline for a contact")
    p_timeline.add_argument("identifier", help="Email or name")

    p_context = sub.add_parser("context", help="Generate agent context")
    p_context.add_argument("identifier", nargs="?", help="Email or name (omit for summary)")

    p_tag = sub.add_parser("tag", help="Add a tag to a contact")
    p_tag.add_argument("identifier", help="Email or name")
    p_tag.add_argument("tag_name", help="Tag to add")

    p_untag = sub.add_parser("untag", help="Remove a tag from a contact")
    p_untag.add_argument("identifier", help="Email or name")
    p_untag.add_argument("tag_name", help="Tag to remove")

    p_tagged = sub.add_parser("tagged", help="List contacts with a specific tag")
    p_tagged.add_argument("tag_name", help="Tag to search for")

    # --- Pipeline Analytics (Wave 2) CLI ---
    sub.add_parser("win-loss", help="Win/loss analysis")

    p_cohorts = sub.add_parser("cohorts", help="Cohort analysis")
    p_cohorts.add_argument("--period", "-p", default="month", choices=["month", "week"])

    p_actreport = sub.add_parser("activity-report", help="Activity summary")
    p_actreport.add_argument("--days", "-D", type=int, default=30)

    sub.add_parser("revenue", help="Revenue report")

    p_diff = sub.add_parser("diff", help="Recent changes")
    p_diff.add_argument("--since", help="YYYY-MM-DD cutoff date")

    sub.add_parser("snapshot", help="Full CRM state snapshot")

    # --- Automation & Intelligence (Wave 4) CLI ---
    p_next = sub.add_parser("next-actions", help="Recommended next actions")
    p_next.add_argument("--limit", "-l", type=int, default=10)

    p_suggest = sub.add_parser("suggest-status", help="Suggest status for a contact")
    p_suggest.add_argument("identifier", help="Email or name")

    p_autoobs = sub.add_parser("auto-observe", help="Auto-generate facts from contact data")
    p_autoobs.add_argument("identifier", help="Email or name")

    p_enrich = sub.add_parser("enrich", help="Full enriched profile for a contact")
    p_enrich.add_argument("identifier", help="Email or name")

    p_sgraph = sub.add_parser("search-graph", help="Search across the context graph")
    p_sgraph.add_argument("query", help="Search term")

    # ingest
    p_ingest = sub.add_parser("ingest", help="Import from local data sources (macOS)")
    p_ingest.add_argument("source", nargs="?", default="all",
                          choices=["all", "contacts", "imessage", "calendar", "mail"],
                          help="Which source to ingest (default: all)")
    p_ingest.add_argument("--days", type=int, default=90, help="Days of iMessage history (default: 90)")

    # import-smart / import-salesforce / import-hubspot
    p_ismart = sub.add_parser("import-smart", help="Import from any CRM CSV export (auto-detect format)")
    p_ismart.add_argument("csv_path", help="Path to CSV file")

    p_isf = sub.add_parser("import-salesforce", help="Import from Salesforce CSV export")
    p_isf.add_argument("csv_path", help="Path to CSV file")

    p_ihs = sub.add_parser("import-hubspot", help="Import from HubSpot CSV export")
    p_ihs.add_argument("csv_path", help="Path to CSV file")

    # velocity
    p_vel = sub.add_parser("velocity", help="Measure engagement velocity for a contact")
    p_vel.add_argument("identifier", help="Email or name")
    p_vel.add_argument("--window", type=int, default=14, help="Window in days (default: 14)")

    # relationship-health
    sub.add_parser("relationship-health", help="All contacts ranked by relationship health")

    # views
    p_vs = sub.add_parser("view-save", help="Save a smart list/view")
    p_vs.add_argument("name", help="View name")
    p_vs.add_argument("--status", help="Filter by status")
    p_vs.add_argument("--tags", help="Filter by tag")
    p_vs.add_argument("--company", help="Filter by company")
    p_vs.add_argument("--min-score", type=int, help="Minimum score")
    p_vs.add_argument("--min-deal", type=float, help="Minimum deal value")
    p_vs.add_argument("--max-stale-days", type=int, help="Max days since last contact")
    p_vs.add_argument("--fact-key", help="Filter by graph fact key (e.g. message_intensity)")
    p_vs.add_argument("--fact-value", help="Filter by graph fact value (e.g. high)")

    p_vr = sub.add_parser("view-run", help="Run a saved view")
    p_vr.add_argument("name", help="View name")

    sub.add_parser("views", help="List all saved views")

    p_vd = sub.add_parser("view-delete", help="Delete a saved view")
    p_vd.add_argument("name", help="View name")

    p_vw = sub.add_parser("view-watch", help="See what changed in a view since last run")
    p_vw.add_argument("name", help="View name")

    # interaction prompts
    p_prompt = sub.add_parser("prompt", help="Generate AI interaction prompt for a contact")
    p_prompt.add_argument("identifier", help="Email or name")
    p_prompt.add_argument("--type", default="follow_up",
                          choices=["follow_up", "cold_outreach", "proposal", "check_in", "close", "save"],
                          help="Interaction type")

    p_batch = sub.add_parser("batch-prompts", help="Generate prompts for multiple contacts")
    p_batch.add_argument("--view", help="Saved view name (optional)")
    p_batch.add_argument("--type", default="follow_up",
                          choices=["follow_up", "cold_outreach", "proposal", "check_in", "close", "save"])

    # ingest-email
    p_ie = sub.add_parser("ingest-email", help="Import email threads from Mail.app")
    p_ie.add_argument("--days", type=int, default=90)

    p_imbox = sub.add_parser("ingest-mbox", help="Import from mbox file (Gmail Takeout, etc)")
    p_imbox.add_argument("mbox_path", help="Path to .mbox file")
    p_imbox.add_argument("--days", type=int, default=90)

    # network
    sub.add_parser("network", help="High-level relationship network summary")

    # warm intros
    p_intros = sub.add_parser("intros", help="Find warm intro paths to a target company/person")
    p_intros.add_argument("target", help="Company name, person name, or email to find intros for")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    crm = CRM(args.db)

    if args.command == "add":
        result = crm.add_contact(args.name, email=args.email, company=args.company,
                                  title=args.title, deal_size=args.deal, status=args.status,
                                  source=args.source, notes=args.notes, tags=args.tags,
                                  warn_duplicate=not args.force)
        if isinstance(result, dict):
            dup = result["duplicate_of"]
            print(f"Warning: contact '{dup['name']}' already exists (id={dup['id']}, email={dup.get('email')})")
            print(f"  Use --force to add anyway, or 'crm.py update {dup.get('email') or dup['name']}' to update the existing contact.")
        else:
            print(f"Added: {args.name} (id={result})")

    elif args.command == "ls":
        contacts = crm.list_contacts(status=args.status, company=args.company)
        fmt_table(contacts, ["name", "company", "status", "deal_size", "last_contacted"])

    elif args.command == "view":
        c = crm.get_contact(args.identifier)
        if not c:
            print(f"Not found: {args.identifier}")
            return
        for k, v in c.items():
            if v:
                print(f"  {k}: {v}")
        acts = crm.get_activity(args.identifier, limit=5)
        if acts:
            print("\n  Recent activity:")
            for a in acts:
                print(f"    [{a['created_at'][:10]}] {a['type']}: {a['summary']}")

    elif args.command == "update":
        fields = {}
        if args.name: fields["name"] = args.name
        if args.company: fields["company"] = args.company
        if args.title: fields["title"] = args.title
        if args.deal: fields["deal_size"] = args.deal
        if args.status: fields["status"] = args.status
        if args.source: fields["source"] = args.source
        if args.notes: fields["notes"] = args.notes
        if args.tags: fields["tags"] = args.tags
        c = crm.update_contact(args.identifier, **fields)
        if c:
            print(f"Updated: {c['name']} → {c['status']}")
        else:
            print(f"Not found: {args.identifier}")

    elif args.command == "delete":
        if crm.delete_contact(args.identifier):
            print(f"Deleted: {args.identifier}")
        else:
            print(f"Not found: {args.identifier}")

    elif args.command == "log":
        if crm.log_activity(args.identifier, args.type, args.summary):
            print(f"Logged: {args.type} on {args.identifier}")
        else:
            print(f"Not found: {args.identifier}")

    elif args.command == "activity":
        acts = crm.get_activity(args.identifier, limit=args.limit)
        if not acts:
            print(f"No activity for: {args.identifier}")
            return
        for a in acts:
            print(f"  [{a['created_at'][:10]}] {a['type']}: {a['summary']}")

    elif args.command == "search":
        results = crm.unified_search(args.term)
        if results["contacts"]:
            print("Contacts:")
            fmt_table(results["contacts"], ["name", "company", "status", "deal_size", "last_contacted"])
        if results["facts"]:
            print("\nFacts:")
            for f in results["facts"][:20]:
                print(f"  {f['entity']} → {f['key']} = {f['value']}  (via {f['source']}, {f['observed_at'][:10]})")
        if results["activities"]:
            print("\nActivity:")
            for a in results["activities"]:
                print(f"  [{a['created_at'][:10]}] {a['contact_name']} — {a['type']}: {a['summary']}")
        if not any(results.values()):
            print(f"  No results for: {args.term}")

    elif args.command == "pipeline":
        pipeline = crm.pipeline()
        for row in pipeline:
            print(f"  {row['status']}: {row['count']} — {row['names']}")

    elif args.command == "stats":
        s = crm.stats()
        print(f"  Total contacts: {s['total_contacts']}")
        print(f"  Contacted (7d): {s['contacted_last_7d']}")
        print(f"  Stale (14d+):   {s['stale_14d']}")
        print()
        for row in s["by_status"]:
            print(f"    {row['status']}: {row['n']}")

    elif args.command == "markdown":
        print(crm.markdown())

    elif args.command == "json":
        print(json.dumps(crm.to_json(), indent=2, default=str))

    elif args.command == "export":
        crm.export_csv(args.output, enrich=args.enrich)
        if args.output:
            print(f"Exported to: {args.output}")

    elif args.command == "import":
        count = crm.import_csv(args.file)
        print(f"Imported: {count} contacts")

    elif args.command == "observe":
        crm.observe(args.entity, args.key, args.value, args.source)
        print(f"Observed: {args.entity} → {args.key} = {args.value}")

    elif args.command == "facts":
        facts = crm.facts_about(args.entity)
        if not facts:
            print(f"No facts about: {args.entity}")
        else:
            for key, f in facts.items():
                print(f"  {key}: {f['value']}  (via {f['source']}, {f['observed_at'][:10]})")

    elif args.command == "history":
        hist = crm.history_of(args.entity, key=args.key)
        if not hist:
            print(f"No history for: {args.entity}")
        else:
            for h in hist:
                print(f"  [{h['observed_at'][:10]}] {h['key']}: {h['value']}  (via {h['source']})")

    elif args.command == "graph":
        print(crm.graph_markdown())

    elif args.command == "stale":
        stale = crm.stale_facts(args.days)
        if not stale:
            print(f"Nothing stale (>{args.days} days)")
        else:
            for s in stale:
                print(f"  {s['entity']} → {s['key']}: {s['value']}  (last: {s['observed_at'][:10]})")

    elif args.command == "reverse-lookup":
        refs = crm.reverse_lookup(args.entity)
        if not refs:
            print(f"No entities reference: {args.entity}")
        else:
            for r in refs:
                print(f"  {r['referencing_entity']} —[{r['relation']}]→ {args.entity}  (via {r['source']}, {r['observed_at'][:10]})")

    elif args.command == "reachable":
        reached = crm.reachable(args.entity, hops=args.hops)
        if not reached:
            print(f"No entities reachable from: {args.entity} within {args.hops} hops")
        else:
            for entity, hop in sorted(reached.items(), key=lambda x: (x[1], x[0])):
                print(f"  hop {hop}: {entity}")

    elif args.command == "conflicts":
        entity = getattr(args, "entity", None)
        conf = crm.conflicts(entity=entity)
        if not conf:
            print("No conflicts found")
        else:
            for c in conf:
                print(f"  {c['entity']} / {c['key']}:")
                print(f"    {c['value1']} (via {c['source1']}, {c['observed_at1'][:10]})")
                print(f"    {c['value2']} (via {c['source2']}, {c['observed_at2'][:10]})")

    # --- Lead Intelligence CLI handlers ---

    elif args.command == "score":
        result = crm.score_contact(args.identifier)
        if not result:
            print(f"Not found: {args.identifier}")
        else:
            print(f"  Score: {result['score']}/100")
            for f in result["factors"]:
                print(f"    • {f}")

    elif args.command == "prioritize":
        ranked = crm.prioritize()
        if not ranked:
            print("  No contacts to prioritize")
        else:
            fmt_table(ranked, ["name", "company", "status", "score", "top_factor"])

    elif args.command == "health":
        # Pipeline health
        h = crm.health_check()
        for category in ["healthy", "at_risk", "cold"]:
            items = h[category]
            print(f"\n  {category.upper()} ({len(items)}):")
            for entry in items:
                print(f"    {entry['name']} (score: {entry['score']}, status: {entry['status']})")
        if h["actions"]:
            print(f"\n  RECOMMENDED ACTIONS:")
            for a in h["actions"]:
                print(f"    -> {a}")

        # Relationship health from iMessage data
        rh = crm.relationship_health()
        if rh:
            print(f"\n  RELATIONSHIP HEALTH ({len(rh)} contacts with iMessage data):")
            print(f"  {'Name':<22} {'Msgs':>6} {'Intensity':<10} {'Reciprocity':<12} {'Status':<15} {'Pipeline':<9} Suggestion")
            print(f"  {'-'*22} {'-'*6} {'-'*10} {'-'*12} {'-'*15} {'-'*9} {'-'*30}")
            for r in rh:
                pipeline_flag = "Yes" if r["in_pipeline"] else "-"
                status_icon = "*" if r["status"] == "one-sided-in" else " "
                print(f" {status_icon}{r['name']:<22} {r['total_messages']:>6} {r['intensity']:<10} {r['reciprocity']:<12} {r['status']:<15} {pipeline_flag:<9} {r['suggestion']}")
            # Highlight most actionable
            actionable = [r for r in rh if r["status"] == "one-sided-in"]
            if actionable:
                print(f"\n  ATTENTION NEEDED:")
                for r in actionable:
                    print(f"    * {r['name']}: {r['suggestion']}")

    elif args.command == "funnel":
        funnel = crm.conversion_funnel()
        print(f"  {'Stage':<20} {'Count':>6} {'Avg Days':>9} {'Conv %':>7}")
        print(f"  {'-'*20} {'-'*6} {'-'*9} {'-'*7}")
        for stage in CRM.STATUS_ORDER:
            if stage in funnel:
                f = funnel[stage]
                print(f"  {stage:<20} {f['count']:>6} {f['avg_days']:>9} {f['conversion_rate']:>6.1f}%")

    elif args.command == "forecast":
        fc = crm.forecast()
        print(f"  Weighted Pipeline: ${fc['weighted_pipeline']:,.0f}\n")
        print(f"  {'Stage':<20} {'Count':>6} {'Total Value':>12} {'Prob':>6} {'Weighted':>12}")
        print(f"  {'-'*20} {'-'*6} {'-'*12} {'-'*6} {'-'*12}")
        for s in fc["by_stage"]:
            print(f"  {s['stage']:<20} {s['count']:>6} ${s['total_value']:>10,.0f} {s['probability']:>5.0%} ${s['weighted_value']:>10,.0f}")

    elif args.command == "duplicates":
        dupes = crm.find_duplicates()
        if not dupes:
            print("  No duplicates found")
        else:
            for d in dupes:
                a, b = d["contact_a"], d["contact_b"]
                reasons = ", ".join(d["reasons"])
                print(f"  {a['name']} ({a['email'] or 'no email'}) ↔ {b['name']} ({b['email'] or 'no email'}): {reasons}")

    elif args.command == "stale-contacts":
        stale = crm.stale_contacts(days=args.days)
        if not stale:
            print(f"  No stale contacts (>{args.days} days)")
        else:
            fmt_table(stale, ["name", "company", "status", "deal_size", "last_contacted"])

    # --- Agent Superpowers (Wave 3) CLI handlers ---

    elif args.command == "query":
        results = crm.query(args.q)
        if not results:
            print(f"  No results for: {args.q}")
        else:
            fmt_table(results, ["name", "company", "status", "deal_size", "tags"])

    elif args.command == "segment":
        results = crm.segment(tags=args.tag, status=args.status,
                              min_score=getattr(args, "min_score", None), company=args.company,
                              fact_key=getattr(args, "fact_key", None),
                              fact_value=getattr(args, "fact_value", None))
        if not results:
            print("  No matching contacts")
        else:
            fmt_table(results, ["name", "company", "status", "deal_size", "tags"])

    elif args.command == "timeline":
        events = crm.timeline(args.identifier)
        if not events:
            print(f"  No timeline for: {args.identifier}")
        else:
            for e in events:
                ts = (e.get("timestamp") or "")[:16]
                print(f"  [{ts}] {e['type']}: {e['detail']}")

    elif args.command == "context":
        identifier = getattr(args, "identifier", None)
        print(crm.context_for_agent(identifier))

    elif args.command == "tag":
        if crm.add_tag(args.identifier, args.tag_name):
            print(f"  Tagged {args.identifier} with '{args.tag_name}'")
        else:
            print(f"  Not found: {args.identifier}")

    elif args.command == "untag":
        if crm.remove_tag(args.identifier, args.tag_name):
            print(f"  Removed tag '{args.tag_name}' from {args.identifier}")
        else:
            print(f"  Not found: {args.identifier}")

    elif args.command == "tagged":
        results = crm.list_by_tag(args.tag_name)
        if not results:
            print(f"  No contacts tagged: {args.tag_name}")
        else:
            fmt_table(results, ["name", "company", "status", "tags"])

    # --- Pipeline Analytics (Wave 2) CLI handlers ---

    elif args.command == "win-loss":
        wl = crm.win_loss_analysis()
        print(f"  Win Rate: {wl['win_rate']:.1f}%  |  Avg Deal: ${wl['avg_deal_size']:,.0f}  |  Avg Days to Close: {wl['avg_days_to_close']:.0f}")
        print(f"  Top Source: {wl['top_source'] or 'N/A'}")
        if wl["wins"]:
            print(f"\n  WINS ({len(wl['wins'])}):")
            for w in wl["wins"]:
                print(f"    {w['name']} ({w['company'] or '-'}) — ${w['deal_size']:,.0f} — {w['days_to_close']}d")
        if wl["losses"]:
            print(f"\n  LOSSES ({len(wl['losses'])}):")
            for l in wl["losses"]:
                print(f"    {l['name']} ({l['company'] or '-'})")

    elif args.command == "cohorts":
        co = crm.cohort_analysis(period=args.period)
        if not co:
            print("  No cohort data")
        else:
            print(f"  {'Cohort':<12} {'Added':>6} {'Converted':>10} {'Conv %':>7}")
            print(f"  {'-'*12} {'-'*6} {'-'*10} {'-'*7}")
            for label, data in co.items():
                print(f"  {label:<12} {data['added']:>6} {data['converted']:>10} {data['conversion_rate']*100:>6.1f}%")

    elif args.command == "activity-report":
        act = crm.activity_summary(days=args.days)
        print(f"  Total Activities ({args.days}d): {act['total_activities']}  |  Daily Avg: {act['daily_avg']:.1f}")
        if act["by_type"]:
            print(f"\n  By Type:")
            for t, n in sorted(act["by_type"].items(), key=lambda x: x[1], reverse=True):
                print(f"    {t}: {n}")
        if act["by_contact"]:
            print(f"\n  Top Contacts:")
            for entry in act["by_contact"]:
                print(f"    {entry['name']}: {entry['count']}")

    elif args.command == "revenue":
        rev = crm.revenue_report()
        print(f"  MRR: ${rev['mrr']:,.0f}")
        print(f"  ARR: ${rev['arr']:,.0f}")
        print(f"  Pipeline Value: ${rev['pipeline_value']:,.0f}")
        print(f"  Avg Revenue/Customer: ${rev['avg_revenue_per_customer']:,.0f}")

    elif args.command == "diff":
        d = crm.diff(since=args.since)
        if not d:
            print("  No recent changes")
        else:
            for entry in d:
                ts = entry.get("timestamp", "")[:16]
                print(f"  [{ts}] {entry['type']}: {entry.get('detail', '')}")

    elif args.command == "snapshot":
        snap = crm.snapshot()
        print(f"  Snapshot at {snap['timestamp'][:19]}")
        print(f"  Total Contacts: {snap['total_contacts']}")
        print(f"  MRR: ${snap['mrr']:,.0f}  |  Pipeline: ${snap['pipeline_value']:,.0f}")
        print(f"\n  By Status:")
        for status, count in snap["contacts"].items():
            print(f"    {status}: {count}")
        if snap["top_deals"]:
            print(f"\n  Top Deals:")
            for d in snap["top_deals"]:
                print(f"    {d['name']} ({d['company'] or '-'}) — ${d['deal_size']:,.0f} [{d['status']}]")
        h = snap["health"]
        print(f"\n  Health: {h['healthy']} healthy, {h['at_risk']} at risk, {h['cold']} cold")
        sd = snap["score_distribution"]
        print(f"  Scores: {sd['high']} high, {sd['medium']} medium, {sd['low']} low")

    # --- Automation & Intelligence (Wave 4) CLI handlers ---

    elif args.command == "next-actions":
        actions = crm.next_actions(limit=args.limit)
        if not actions:
            print("  No recommended actions")
        else:
            for a in actions:
                prio = a["priority"].upper()
                val = f"${a['deal_value']:,.0f}" if a["deal_value"] else "-"
                print(f"  [{prio}] {a['contact']}: {a['action']} — {a['reason']} (deal: {val})")

    elif args.command == "suggest-status":
        result = crm.suggest_status(args.identifier)
        if not result:
            print(f"  Not found: {args.identifier}")
        else:
            print(f"  Current:    {result['current']}")
            print(f"  Suggested:  {result['suggested']}")
            print(f"  Reason:     {result['reason']}")
            print(f"  Confidence: {result['confidence']}")

    elif args.command == "auto-observe":
        facts = crm.auto_observe(args.identifier)
        if not facts:
            print(f"  No facts generated for: {args.identifier}")
        else:
            print(f"  Generated {len(facts)} facts:")
            for f in facts:
                print(f"    {f['entity']} → {f['key']} = {f['value']}")

    elif args.command == "enrich":
        profile = crm.enrich(args.identifier)
        if not profile:
            print(f"  Not found: {args.identifier}")
        else:
            print(f"  {profile['name']} ({profile.get('company') or '-'})")
            print(f"  Status: {profile['status']}  |  Score: {profile['score']}/100")
            if profile.get("deal_size"):
                print(f"  Deal: {profile['deal_size']}")
            if profile["facts"]:
                print(f"\n  Facts ({len(profile['facts'])}):")
                for k, v in profile["facts"].items():
                    print(f"    {k}: {v}")
            if profile["activities"]:
                print(f"\n  Activities ({len(profile['activities'])}):")
                for a in profile["activities"][:5]:
                    print(f"    [{a.get('date') or '-'}] {a['type']}: {a['summary']}")
            if profile["deals"]:
                print(f"\n  Deals ({len(profile['deals'])}):")
                for d in profile["deals"]:
                    print(f"    {d['name']} — {d['value'] or '-'} [{d['stage']}]")

    elif args.command == "search-graph":
        results = crm.search_graph(args.query)
        if not results:
            print(f"  No graph results for: {args.query}")
        else:
            for r in results:
                print(f"  {r['entity']} → {r['key']} = {r['value']}  (via {r['source']}, {r['observed_at'][:10]})")

    elif args.command == "ingest":
        src = args.source
        if src in ("all", "contacts"):
            c_added, c_facts = crm.ingest_macos_contacts()
            print(f"  Contacts: {c_added} added, {c_facts} facts")
        if src in ("all", "imessage"):
            m_found, m_facts = crm.ingest_macos_imessage(days=args.days)
            print(f"  iMessage: {m_found} handles, {m_facts} facts ({args.days}d)")
        if src in ("all", "calendar"):
            e_found, e_facts = crm.ingest_macos_calendar()
            print(f"  Calendar: {e_found} events, {e_facts} facts")
        if src in ("all", "mail"):
            ml_found, ml_facts = crm.ingest_macos_mail(days=args.days)
            print(f"  Mail: {ml_found} threads, {ml_facts} facts ({args.days}d)")
        if src == "all":
            stats = crm.graph_stats()
            print(f"\n  Graph: {stats['entities']} entities, {stats['facts']} total facts")

    elif args.command == "import-smart":
        result = crm.import_smart(args.csv_path)
        print(f"  Added: {result['contacts_added']}  |  Facts: {result['facts_added']}  |  Skipped: {result['skipped']}")
        print(f"  Mapping: {result['mapping_used']}")

    elif args.command == "import-salesforce":
        result = crm.import_salesforce(args.csv_path)
        print(f"  Added: {result['contacts_added']}  |  Facts: {result['facts_added']}  |  Skipped: {result['skipped']}")

    elif args.command == "import-hubspot":
        result = crm.import_hubspot(args.csv_path)
        print(f"  Added: {result['contacts_added']}  |  Facts: {result['facts_added']}  |  Skipped: {result['skipped']}")

    elif args.command == "velocity":
        vel = crm.velocity(args.identifier, window_days=args.window)
        if not vel:
            print(f"  Not found: {args.identifier}")
        else:
            print(f"  Velocity: {vel['velocity']}x  |  Trend: {vel['trend']}")
            print(f"  Current period: {vel['current_period']['activities']} activities ({vel['current_period']['period_days']}d)")
            print(f"  Previous period: {vel['previous_period']['activities']} activities")
            if vel["days_until_cold"]:
                print(f"  Days until cold: {vel['days_until_cold']}")
            if vel["response_time_avg_hours"]:
                print(f"  Avg response gap: {vel['response_time_avg_hours']:.0f}h")

    elif args.command == "relationship-health":
        report = crm.relationship_health_report()
        if not report:
            print("  No contacts with activity data")
        else:
            print(f"  {'Name':<20} {'Company':<15} {'Trend':<12} {'Velocity':>8} {'Deal':>10} {'Score':>6}")
            print(f"  {'-'*20} {'-'*15} {'-'*12} {'-'*8} {'-'*10} {'-'*6}")
            for r in report:
                deal = f"${r['deal_value']:,.0f}" if r["deal_value"] else "-"
                print(f"  {r['name']:<20} {(r['company'] or '-'):<15} {r['trend']:<12} {r['velocity']:>8.1f} {deal:>10} {r['score']:>6}")

    elif args.command == "view-save":
        count = crm.save_view(args.name, status=args.status, tags=args.tags,
                              company=args.company, min_score=getattr(args, 'min_score', None),
                              min_deal=getattr(args, 'min_deal', None),
                              max_stale_days=getattr(args, 'max_stale_days', None),
                              fact_key=getattr(args, 'fact_key', None),
                              fact_value=getattr(args, 'fact_value', None))
        print(f"  Saved view '{args.name}' ({count} matches)")

    elif args.command == "view-run":
        results = crm.run_view(args.name)
        if not results:
            print(f"  No matches for view '{args.name}'")
        else:
            fmt_table(results, ["name", "company", "status", "deal_size", "last_contacted"])

    elif args.command == "views":
        views = crm.list_views()
        if not views:
            print("  No saved views")
        else:
            for v in views:
                print(f"  {v['name']}: {v['count']} matches — {v['query']}")

    elif args.command == "view-delete":
        crm.delete_view(args.name)
        print(f"  Deleted view '{args.name}'")

    elif args.command == "view-watch":
        diff = crm.watch(args.name)
        print(f"  Total: {diff['total']}  |  Added: {len(diff['added'])}  |  Removed: {diff['removed_count']}")
        if diff["added"]:
            print("  New:")
            for c in diff["added"]:
                print(f"    {c['name']} ({c.get('company', '-')})")

    elif args.command == "prompt":
        prompt = crm.interaction_prompt(args.identifier, action_type=args.type)
        if not prompt:
            print(f"  Not found: {args.identifier}")
        else:
            print(prompt)

    elif args.command == "batch-prompts":
        prompts = crm.batch_prompts(view_name=args.view, action_type=args.type)
        if not prompts:
            print("  No prompts generated")
        else:
            for p in prompts:
                print(f"\n{'='*60}")
                print(f"{p['contact']} ({p.get('email', '-')})")
                print(f"{'='*60}")
                print(p["prompt"])

    elif args.command == "ingest-email":
        t, f = crm.ingest_macos_mail(days=args.days)
        print(f"  Threads: {t}  |  Facts: {f}")

    elif args.command == "ingest-mbox":
        m, f = crm.ingest_mbox(args.mbox_path, days=args.days)
        print(f"  Messages: {m}  |  Facts: {f}")

    elif args.command == "network":
        ns = crm.network_summary()
        print(f"\n  === Relationship Network Summary ===\n")
        print(f"  Contacts:        {ns['total_contacts']}")
        print(f"  Graph Entities:  {ns['total_graph_entities']}")
        print(f"  Total Facts:     {ns['total_facts']}")
        print(f"  Pipeline Value:  ${ns['pipeline_value']:,.0f}")
        print(f"  Resolution Rate: {ns['resolution_rate']:.1f}%")
        print(f"\n  --- Sources ---")
        for src, cnt in ns["sources"].items():
            print(f"    {src}: {cnt} facts")
        if ns["top_contacts"]:
            print(f"\n  --- Top Contacts (by iMessage volume) ---")
            for i, tc in enumerate(ns["top_contacts"], 1):
                print(f"    {i:>2}. {tc['name']} ({tc['messages']} messages)")
        if ns["companies"]:
            print(f"\n  --- Companies ({len(ns['companies'])}) ---")
            print(f"    {', '.join(ns['companies'])}")
        print()

    elif args.command == "intros":
        intros = crm.find_intros(args.target)
        if not intros:
            print(f"  No warm intro paths found for: {args.target}")
            print(f"  Tip: Run 'python crm.py ingest all' to populate your knowledge graph from iMessage and Contacts.")
        else:
            print(f"\n  === Warm Intros to '{args.target}' ({len(intros)} paths) ===\n")
            for i, intro in enumerate(intros, 1):
                rel = intro["relationship_to_you"]
                warmth = intro["warmth_score"]
                bar = "#" * (warmth // 5) + "." * (20 - warmth // 5)
                print(f"  {i}. {intro['connector']}", end="")
                if intro.get("connector_email"):
                    print(f" ({intro['connector_email']})", end="")
                print()
                print(f"     Connection: {intro['connection_to_target']}")
                print(f"     Warmth:     [{bar}] {warmth}/100  (iMessage: {rel['imessage_total']} msgs, {rel['intensity']})")
                print(f"     Action:     {intro['suggested_action']}")
                print()

    crm.close()


if __name__ == "__main__":
    main()