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
    python crm.py export backup.json
    python crm.py import-csv contacts.csv
    python crm.py markdown
    python crm.py stats
"""

import ast
import sqlite3
import argparse
import csv
import sys
import os
import json
import math
import random
import re
import quopri
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

DEFAULT_DB = os.environ.get("CRM_DB", "crm.db")
SCHEMA_VERSION = 1

# Simple email regex: local@domain.tld — rejects obviously bogus values
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")

FREE_MAIL_DOMAINS = {
    "aol.com",
    "gmail.com",
    "hotmail.com",
    "icloud.com",
    "live.com",
    "me.com",
    "msn.com",
    "outlook.com",
    "proton.me",
    "protonmail.com",
    "yahoo.com",
}

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
    archived INTEGER NOT NULL DEFAULT 0,
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
    observed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    event_day DATE NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS reminders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    due_date DATE NOT NULL,
    note TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME,
    FOREIGN KEY (contact_id) REFERENCES contacts(id)
);

CREATE TABLE IF NOT EXISTS custom_fields (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER NOT NULL,
    field_name TEXT NOT NULL,
    field_value TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (contact_id) REFERENCES contacts(id),
    UNIQUE(contact_id, field_name)
);

CREATE INDEX IF NOT EXISTS idx_activity_contact ON activity(contact_id, created_at);
CREATE INDEX IF NOT EXISTS idx_deals_contact ON deals(contact_id);
CREATE INDEX IF NOT EXISTS idx_reminders_contact ON reminders(contact_id, due_date);
CREATE INDEX IF NOT EXISTS idx_reminders_due ON reminders(due_date, completed);
CREATE INDEX IF NOT EXISTS idx_custom_fields_contact ON custom_fields(contact_id);

CREATE INDEX IF NOT EXISTS idx_facts_entity ON facts(entity);
CREATE INDEX IF NOT EXISTS idx_facts_key ON facts(entity, key);
CREATE INDEX IF NOT EXISTS idx_facts_source ON facts(source);
CREATE INDEX IF NOT EXISTS idx_facts_value ON facts(value);
"""


class CRM:
    CADENCE_DEFAULTS = {
        "prospect": 14,
        "contacted": 7,
        "met": 5,
        "proposal_drafted": 3,
        "verbal_yes": 2,
        "active_customer": 30,
    }
    CADENCE_BASE_DAYS = 7

    def __init__(self, db_path=DEFAULT_DB):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()
        self._tuned_cadence_cache = None
        self._tuned_cadence_activity_count = None

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @staticmethod
    def _validate_email(email):
        """Validate email format. Raises ValueError if invalid."""
        if email and not _EMAIL_RE.match(email):
            raise ValueError(f"Invalid email format: {email}")

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
        # Migrate: add archived column if missing (existing DBs)
        try:
            self.conn.execute("SELECT archived FROM contacts LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE contacts ADD COLUMN archived INTEGER NOT NULL DEFAULT 0")
            self.conn.commit()
        # Queue surface events need to retain the same fact values on different
        # days while preserving the graph's existing exact-fact deduplication.
        try:
            self.conn.execute("SELECT event_day FROM facts LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE facts ADD COLUMN event_day DATE NOT NULL DEFAULT ''"
            )
        with self.conn:
            self.conn.execute("DROP INDEX IF EXISTS idx_facts_dedup")
            self.conn.execute(
                """DELETE FROM facts WHERE rowid NOT IN (
                       SELECT MAX(rowid) FROM facts
                       GROUP BY entity, key, value, source, event_day
                   )"""
            )
            self.conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_facts_dedup
                   ON facts(entity, key, value, source, event_day)"""
            )
            self.conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_surface_daily
                   ON facts(entity, key, source, event_day)
                   WHERE source IN ('queue', 'queue_meta') AND event_day != ''"""
            )

    # --- Contacts ---

    def add_contact(self, name, email=None, company=None, title=None,
                    deal_size=None, status="prospect", source=None,
                    notes=None, tags=None, warn_duplicate=False):
        self._validate_email(email)
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
        # Re-fetch by id so renames (no email) still return the updated row
        row = self.conn.execute("SELECT * FROM contacts WHERE id = ?", (contact["id"],)).fetchone()
        updated = dict(row) if row else None
        if (updated and "status" in updates
                and updates["status"] != contact.get("status")):
            self.observe(
                self._canonical_contact_entity(updated), "status",
                updates["status"], source="crm_status",
            )
        return updated

    def get_contact(self, identifier):
        """Get contact by email or name (partial match)."""
        row = self.conn.execute("SELECT * FROM contacts WHERE email = ?", (identifier,)).fetchone()
        if row:
            return dict(row)
        escaped = self._escape_like(identifier)
        row = self.conn.execute(
            "SELECT * FROM contacts WHERE name LIKE ? ESCAPE '\\'", (f"%{escaped}%",)
        ).fetchone()
        return dict(row) if row else None

    def list_contacts(self, status=None, company=None, include_archived=False):
        query = "SELECT * FROM contacts WHERE 1=1"
        params = []
        if not include_archived:
            query += " AND archived = 0"
        if status:
            query += " AND status = ?"
            params.append(status)
        if company:
            query += " AND company LIKE ? ESCAPE '\\'"
            params.append(f"%{self._escape_like(company)}%")
        query += " ORDER BY last_contacted DESC NULLS LAST, created_at DESC"
        return [dict(r) for r in self.conn.execute(query, params).fetchall()]

    def search(self, term):
        query = """SELECT * FROM contacts WHERE
                   name LIKE ? ESCAPE '\\' OR company LIKE ? ESCAPE '\\'
                   OR email LIKE ? ESCAPE '\\' OR notes LIKE ? ESCAPE '\\'
                   OR tags LIKE ? ESCAPE '\\'
                   ORDER BY last_contacted DESC NULLS LAST"""
        t = f"%{self._escape_like(term)}%"
        return [dict(r) for r in self.conn.execute(query, (t, t, t, t, t)).fetchall()]

    def unified_search(self, term):
        """Search across contacts, facts, and activity. Returns dict with sections."""
        t = f"%{self._escape_like(term)}%"
        contacts = self.conn.execute(
            """SELECT * FROM contacts WHERE
               name LIKE ? ESCAPE '\\' OR company LIKE ? ESCAPE '\\'
               OR email LIKE ? ESCAPE '\\' OR notes LIKE ? ESCAPE '\\'
               OR tags LIKE ? ESCAPE '\\'
               ORDER BY last_contacted DESC NULLS LAST""",
            (t, t, t, t, t)
        ).fetchall()
        facts = self.conn.execute(
            """SELECT entity, key, value, source, observed_at FROM facts
               WHERE entity LIKE ? ESCAPE '\\' OR key LIKE ? ESCAPE '\\'
               OR value LIKE ? ESCAPE '\\' OR source LIKE ? ESCAPE '\\'
               ORDER BY observed_at DESC""",
            (t, t, t, t)
        ).fetchall()
        activities = self.conn.execute(
            """SELECT a.type, a.summary, a.created_at, c.name as contact_name
               FROM activity a JOIN contacts c ON a.contact_id = c.id
               WHERE a.summary LIKE ? ESCAPE '\\'
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

    def _contact_for_merge(self, identifier):
        """Resolve a merge identifier, accepting an internal numeric id too."""
        if isinstance(identifier, int) or (
                isinstance(identifier, str) and identifier.isdigit()):
            row = self.conn.execute(
                "SELECT * FROM contacts WHERE id = ?", (int(identifier),)
            ).fetchone()
            return dict(row) if row else None
        return self.get_contact(identifier)

    def _next_merge_id(self):
        """Return the next deterministic integer id for a merge audit fact."""
        latest = 0
        rows = self.conn.execute(
            "SELECT DISTINCT entity FROM facts WHERE entity LIKE 'merge:%'"
        ).fetchall()
        for row in rows:
            suffix = row["entity"].partition(":")[2]
            if suffix.isdigit():
                latest = max(latest, int(suffix))
        return latest + 1

    @staticmethod
    def _insert_snapshot_row(conn, table, row):
        """Insert a previously serialized SQLite row with its original id."""
        columns = list(row)
        placeholders = ", ".join("?" for _ in columns)
        conn.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            tuple(row[column] for column in columns),
        )

    def merge_contacts(self, keep_identifier, drop_identifier):
        """Merge two contacts transactionally and retain a complete undo record.

        Activity, deals, reminders, custom fields, and graph facts are moved to
        the kept contact. Blank fields on the kept contact are filled from the
        dropped contact, which is then deleted. The returned contact includes
        ``merge_id`` for use with :meth:`undo_merge`.
        """
        keep = self._contact_for_merge(keep_identifier)
        drop = self._contact_for_merge(drop_identifier)
        if not keep or not drop:
            return None
        if keep["id"] == drop["id"]:
            return keep  # same contact

        moved_ids = {}
        for table in ("activity", "deals", "reminders", "custom_fields"):
            moved_ids[table] = [
                row["id"] for row in self.conn.execute(
                    f"SELECT id FROM {table} WHERE contact_id = ? ORDER BY id",
                    (drop["id"],),
                ).fetchall()
            ]

        custom_field_rows = [dict(row) for row in self.conn.execute(
            """SELECT * FROM custom_fields
               WHERE contact_id IN (?, ?) ORDER BY id""",
            (keep["id"], drop["id"]),
        ).fetchall()]

        entity_keys = sorted(set(
            self._contact_entity_keys(keep) + self._contact_entity_keys(drop)
        ))
        placeholders = ", ".join("?" for _ in entity_keys)
        fact_rows = [dict(row) for row in self.conn.execute(
            f"SELECT rowid, * FROM facts WHERE entity IN ({placeholders}) ORDER BY rowid",
            entity_keys,
        ).fetchall()]

        merge_id = self._next_merge_id()
        moved_ids["facts"] = []
        deleted_fact_ids = []
        undo_payload = {
            "version": 1,
            "merge_id": merge_id,
            "keep_contact": keep,
            "drop_contact": drop,
            "moved_row_ids": moved_ids,
            "pre_merge": {
                "contacts": {"keep": keep, "drop": drop},
                "custom_fields": custom_field_rows,
                "facts": fact_rows,
            },
        }

        with self.conn:
            for table in ("activity", "deals", "reminders"):
                self.conn.execute(
                    f"UPDATE {table} SET contact_id = ? WHERE contact_id = ?",
                    (keep["id"], drop["id"]),
                )

            # Custom field names are unique per contact. Preserve the kept
            # value on conflicts, except when it is blank and the drop value
            # can safely fill it.
            drop_fields = [
                row for row in custom_field_rows
                if row["contact_id"] == drop["id"]
            ]
            for field in drop_fields:
                existing = self.conn.execute(
                    """SELECT * FROM custom_fields
                       WHERE contact_id = ? AND field_name = ?""",
                    (keep["id"], field["field_name"]),
                ).fetchone()
                if existing:
                    if not existing["field_value"] and field["field_value"]:
                        self.conn.execute(
                            """UPDATE custom_fields
                               SET field_value = ?, updated_at = CURRENT_TIMESTAMP
                               WHERE id = ?""",
                            (field["field_value"], existing["id"]),
                        )
                    self.conn.execute(
                        "DELETE FROM custom_fields WHERE id = ?", (field["id"],)
                    )
                else:
                    self.conn.execute(
                        "UPDATE custom_fields SET contact_id = ? WHERE id = ?",
                        (keep["id"], field["id"]),
                    )

            # Canonicalize every known name/email variant for both contacts to
            # the kept contact's normalized-name entity. Resolve exact fact
            # collisions deterministically by retaining the newest observation.
            primary_keep = self._canonical_contact_entity(keep)
            for fact in fact_rows:
                if fact["entity"] == primary_keep:
                    continue
                collision = self.conn.execute(
                    """SELECT rowid, observed_at FROM facts
                       WHERE entity = ? AND key = ? AND value IS ? AND source IS ?
                       ORDER BY rowid LIMIT 1""",
                    (primary_keep, fact["key"], fact["value"], fact["source"]),
                ).fetchone()
                if collision:
                    fact_time = fact["observed_at"] or ""
                    collision_time = collision["observed_at"] or ""
                    if (fact_time, fact["rowid"]) > (
                            collision_time, collision["rowid"]):
                        self.conn.execute(
                            "DELETE FROM facts WHERE rowid = ?", (collision["rowid"],)
                        )
                        deleted_fact_ids.append(collision["rowid"])
                        self.conn.execute(
                            "UPDATE facts SET entity = ? WHERE rowid = ?",
                            (primary_keep, fact["rowid"]),
                        )
                        moved_ids["facts"].append(fact["rowid"])
                    else:
                        self.conn.execute(
                            "DELETE FROM facts WHERE rowid = ?", (fact["rowid"],)
                        )
                        deleted_fact_ids.append(fact["rowid"])
                else:
                    self.conn.execute(
                        "UPDATE facts SET entity = ? WHERE rowid = ?",
                        (primary_keep, fact["rowid"]),
                    )
                    moved_ids["facts"].append(fact["rowid"])

            # Delete before copying a missing email so the UNIQUE constraint is
            # never transiently violated.
            self.conn.execute("DELETE FROM contacts WHERE id = ?", (drop["id"],))
            fillable = (
                "email", "company", "title", "deal_size", "source", "notes",
                "tags", "last_contacted",
            )
            updates = {
                field: drop[field]
                for field in fillable
                if not keep.get(field) and drop.get(field)
            }
            if updates:
                assignments = ", ".join(f"{field} = ?" for field in updates)
                self.conn.execute(
                    f"""UPDATE contacts SET {assignments},
                        updated_at = CURRENT_TIMESTAMP WHERE id = ?""",
                    tuple(updates.values()) + (keep["id"],),
                )

            undo_payload["moved_row_ids"]["facts"] = sorted(
                set(moved_ids["facts"])
            )
            undo_payload["deleted_fact_row_ids"] = sorted(set(deleted_fact_ids))
            self.conn.execute(
                """INSERT INTO facts (entity, key, value, source)
                   VALUES (?, 'undo', ?, 'doctor')""",
                (f"merge:{merge_id}", json.dumps(undo_payload, sort_keys=True)),
            )

        row = self.conn.execute(
            "SELECT * FROM contacts WHERE id = ?", (keep["id"],)
        ).fetchone()
        result = dict(row)
        result["merge_id"] = merge_id
        return result

    def undo_merge(self, merge_id):
        """Restore both contacts and row ownership from a merge audit fact."""
        raw_id = str(merge_id)
        if raw_id.startswith("merge:"):
            raw_id = raw_id.partition(":")[2]
        if not raw_id.isdigit():
            return None
        entity = f"merge:{int(raw_id)}"
        undo_row = self.conn.execute(
            """SELECT rowid, value FROM facts
               WHERE entity = ? AND key = 'undo' AND source = 'doctor'
               ORDER BY rowid DESC LIMIT 1""",
            (entity,),
        ).fetchone()
        if not undo_row:
            return None

        payload = json.loads(undo_row["value"])
        keep = payload["pre_merge"]["contacts"]["keep"]
        drop = payload["pre_merge"]["contacts"]["drop"]
        moved = payload["moved_row_ids"]

        for contact in (keep, drop):
            email = contact.get("email")
            if not email:
                continue
            email_owner = self.conn.execute(
                """SELECT id FROM contacts
                   WHERE email = ? AND id NOT IN (?, ?) LIMIT 1""",
                (email, keep["id"], drop["id"]),
            ).fetchone()
            if email_owner:
                return {
                    "error": (
                        f"Cannot undo merge {raw_id}: email {email!r} "
                        "is now taken"
                    )
                }

        with self.conn:
            current_keep = self.conn.execute(
                "SELECT id FROM contacts WHERE id = ?", (keep["id"],)
            ).fetchone()
            if not current_keep:
                self._insert_snapshot_row(self.conn, "contacts", keep)
            else:
                columns = [column for column in keep if column != "id"]
                assignments = ", ".join(f"{column} = ?" for column in columns)
                self.conn.execute(
                    f"UPDATE contacts SET {assignments} WHERE id = ?",
                    tuple(keep[column] for column in columns) + (keep["id"],),
                )

            existing_drop = self.conn.execute(
                "SELECT id FROM contacts WHERE id = ?", (drop["id"],)
            ).fetchone()
            if existing_drop:
                raise ValueError(
                    f"Cannot undo merge {raw_id}: contact id {drop['id']} is in use"
                )
            self._insert_snapshot_row(self.conn, "contacts", drop)

            for table in ("activity", "deals", "reminders"):
                ids = moved.get(table, [])
                if ids:
                    placeholders = ", ".join("?" for _ in ids)
                    self.conn.execute(
                        f"""UPDATE {table} SET contact_id = ?
                            WHERE id IN ({placeholders})""",
                        (drop["id"], *ids),
                    )

            custom_rows = payload["pre_merge"].get("custom_fields", [])
            custom_ids = [row["id"] for row in custom_rows]
            if custom_ids:
                placeholders = ", ".join("?" for _ in custom_ids)
                self.conn.execute(
                    f"DELETE FROM custom_fields WHERE id IN ({placeholders})",
                    custom_ids,
                )
                for row in custom_rows:
                    conflict = self.conn.execute(
                        """SELECT id FROM custom_fields
                           WHERE contact_id = ? AND field_name = ?""",
                        (row["contact_id"], row["field_name"]),
                    ).fetchone()
                    if conflict:
                        self.conn.execute(
                            "DELETE FROM custom_fields WHERE id = ?", (conflict["id"],)
                        )
                    self._insert_snapshot_row(self.conn, "custom_fields", row)

            fact_rows = payload["pre_merge"].get("facts", [])
            fact_ids = [row["rowid"] for row in fact_rows]
            if fact_ids:
                placeholders = ", ".join("?" for _ in fact_ids)
                self.conn.execute(
                    f"DELETE FROM facts WHERE rowid IN ({placeholders})", fact_ids
                )
                for row in fact_rows:
                    self.conn.execute(
                        """DELETE FROM facts
                           WHERE entity = ? AND key = ? AND value IS ?
                             AND source IS ? AND rowid != ?""",
                        (row["entity"], row["key"], row["value"],
                         row["source"], row["rowid"]),
                    )
                    self._insert_snapshot_row(self.conn, "facts", row)

            self.conn.execute(
                "UPDATE facts SET key = 'undo_applied' WHERE rowid = ?",
                (undo_row["rowid"],),
            )

        return {
            "merge_id": int(raw_id),
            "restored": True,
            "keep": dict(self.conn.execute(
                "SELECT * FROM contacts WHERE id = ?", (keep["id"],)
            ).fetchone()),
            "drop": dict(self.conn.execute(
                "SELECT * FROM contacts WHERE id = ?", (drop["id"],)
            ).fetchone()),
        }

    def batch_add_contacts(self, contacts_list):
        """Add many contacts in a single transaction. Much faster than individual add_contact calls.

        contacts_list: list of dicts with keys matching add_contact params
            (name, email, company, title, deal_size, status, source, notes, tags)
        Returns dict with added, skipped, errors counts.
        """
        added = 0
        skipped = 0
        errors = 0
        for c in contacts_list:
            try:
                name = c.get("name", "").strip()
                if not name:
                    errors += 1
                    continue
                self.conn.execute(
                    """INSERT INTO contacts (name, email, company, title, deal_size, status, source, notes, tags)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (name, c.get("email"), c.get("company"), c.get("title"),
                     c.get("deal_size"), c.get("status", "prospect"),
                     c.get("source"), c.get("notes"), c.get("tags"))
                )
                added += 1
            except sqlite3.IntegrityError:
                skipped += 1
            except Exception:
                errors += 1
        self.conn.commit()
        return {"added": added, "skipped": skipped, "errors": errors}

    def delete_activity(self, activity_id):
        """Delete an activity entry by id."""
        result = self.conn.execute("DELETE FROM activity WHERE id = ?", (activity_id,))
        self.conn.commit()
        return result.rowcount > 0

    def archive_contact(self, identifier):
        """Soft-delete: hide a contact from list_contacts without destroying data."""
        contact = self.get_contact(identifier)
        if not contact:
            return None
        self.conn.execute(
            "UPDATE contacts SET archived = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (contact["id"],)
        )
        self.conn.commit()
        return True

    def unarchive_contact(self, identifier):
        """Restore an archived contact."""
        # Must search including archived
        row = self.conn.execute("SELECT * FROM contacts WHERE email = ?", (identifier,)).fetchone()
        if not row:
            escaped = self._escape_like(identifier)
            row = self.conn.execute(
                "SELECT * FROM contacts WHERE name LIKE ? ESCAPE '\\'", (f"%{escaped}%",)
            ).fetchone()
        if not row:
            return None
        self.conn.execute(
            "UPDATE contacts SET archived = 0, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (row["id"],)
        )
        self.conn.commit()
        return True

    def delete_contacts(self, identifiers):
        """Bulk delete contacts in a single transaction. Returns count deleted."""
        deleted = 0
        for identifier in identifiers:
            contact = self.get_contact(identifier)
            if not contact:
                continue
            self.conn.execute("DELETE FROM activity WHERE contact_id = ?", (contact["id"],))
            self.conn.execute("DELETE FROM deals WHERE contact_id = ?", (contact["id"],))
            self.conn.execute("DELETE FROM contacts WHERE id = ?", (contact["id"],))
            # Cascade delete facts
            entity_variants = self._contact_entity_keys(contact)
            placeholders = ",".join("?" * len(entity_variants))
            self.conn.execute(
                f"DELETE FROM facts WHERE entity LIKE 'contact:%' AND entity IN ({placeholders})",
                entity_variants
            )
            deleted += 1
        self.conn.commit()
        return deleted

    def export_deals_csv(self, path=None):
        """Export contacts with their deals as a CSV. Each row = one deal."""
        deals = self.conn.execute(
            """SELECT c.name, c.email, c.company, c.status as contact_status,
                      d.name as deal_name, d.value as deal_value, d.stage as deal_stage,
                      d.notes as deal_notes, d.created_at as deal_created, d.closed_at
               FROM deals d JOIN contacts c ON d.contact_id = c.id
               ORDER BY d.updated_at DESC"""
        ).fetchall()
        if not deals:
            return ""
        rows = [dict(r) for r in deals]
        output = path or sys.stdout
        should_close = False
        if isinstance(output, str):
            output = open(output, "w", newline="")
            should_close = True
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        if should_close:
            output.close()
        return path or "stdout"

    # --- Reminders ---

    def set_reminder(self, identifier, due_date=None, note=None):
        """Schedule a follow-up reminder for a contact.

        due_date: YYYY-MM-DD string or date object. When omitted, the contact's
        tuned status cadence determines the due date.
        Returns the reminder id.
        """
        contact = self.get_contact(identifier)
        if not contact:
            return None
        if not due_date:
            due_date = date.today() + timedelta(
                days=self.tuned_cadence(contact.get("status"))
            )
        if isinstance(due_date, date):
            due_date = due_date.isoformat()
        self.conn.execute(
            "INSERT INTO reminders (contact_id, due_date, note) VALUES (?, ?, ?)",
            (contact["id"], due_date, note)
        )
        self.conn.commit()
        return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def due_reminders(self, include_future_days=0):
        """Get all reminders due today or overdue. Optionally include upcoming N days."""
        cutoff = (date.today() + timedelta(days=include_future_days)).isoformat()
        rows = self.conn.execute(
            """SELECT r.*, c.name, c.email, c.company
               FROM reminders r JOIN contacts c ON r.contact_id = c.id
               WHERE r.completed = 0 AND r.due_date <= ?
               ORDER BY r.due_date ASC""",
            (cutoff,)
        ).fetchall()
        return [dict(r) for r in rows]

    def complete_reminder(self, reminder_id):
        """Mark a reminder as completed."""
        result = self.conn.execute(
            "UPDATE reminders SET completed = 1, completed_at = CURRENT_TIMESTAMP WHERE id = ? AND completed = 0",
            (reminder_id,)
        )
        self.conn.commit()
        return result.rowcount > 0

    def reminders_for_contact(self, identifier, include_completed=False):
        """Get all reminders for a contact."""
        contact = self.get_contact(identifier)
        if not contact:
            return []
        query = "SELECT * FROM reminders WHERE contact_id = ?"
        if not include_completed:
            query += " AND completed = 0"
        query += " ORDER BY due_date ASC"
        return [dict(r) for r in self.conn.execute(query, (contact["id"],)).fetchall()]

    def snooze_reminder(self, reminder_id, days=3):
        """Push a reminder forward by N days."""
        reminder = self.conn.execute("SELECT * FROM reminders WHERE id = ?", (reminder_id,)).fetchone()
        if not reminder:
            return None
        try:
            current_due = date.fromisoformat(reminder["due_date"])
        except (ValueError, TypeError):
            current_due = date.today()
        new_due = (current_due + timedelta(days=days)).isoformat()
        self.conn.execute("UPDATE reminders SET due_date = ? WHERE id = ?", (new_due, reminder_id))
        self.conn.commit()
        return new_due

    # --- Custom Fields ---

    def set_field(self, identifier, field_name, field_value):
        """Set a custom field on a contact. Creates or updates."""
        contact = self.get_contact(identifier)
        if not contact:
            return None
        self.conn.execute(
            """INSERT INTO custom_fields (contact_id, field_name, field_value)
               VALUES (?, ?, ?)
               ON CONFLICT(contact_id, field_name)
               DO UPDATE SET field_value = ?, updated_at = CURRENT_TIMESTAMP""",
            (contact["id"], field_name, field_value, field_value)
        )
        self.conn.commit()
        return True

    def get_field(self, identifier, field_name):
        """Get a custom field value for a contact."""
        contact = self.get_contact(identifier)
        if not contact:
            return None
        row = self.conn.execute(
            "SELECT field_value FROM custom_fields WHERE contact_id = ? AND field_name = ?",
            (contact["id"], field_name)
        ).fetchone()
        return row["field_value"] if row else None

    def get_fields(self, identifier):
        """Get all custom fields for a contact as a dict."""
        contact = self.get_contact(identifier)
        if not contact:
            return {}
        rows = self.conn.execute(
            "SELECT field_name, field_value FROM custom_fields WHERE contact_id = ? ORDER BY field_name",
            (contact["id"],)
        ).fetchall()
        return {r["field_name"]: r["field_value"] for r in rows}

    def delete_field(self, identifier, field_name):
        """Delete a custom field from a contact."""
        contact = self.get_contact(identifier)
        if not contact:
            return False
        result = self.conn.execute(
            "DELETE FROM custom_fields WHERE contact_id = ? AND field_name = ?",
            (contact["id"], field_name)
        )
        self.conn.commit()
        return result.rowcount > 0

    def contacts_by_field(self, field_name, field_value=None):
        """Find contacts that have a specific custom field (and optionally a specific value)."""
        if field_value is not None:
            rows = self.conn.execute(
                """SELECT c.* FROM contacts c
                   JOIN custom_fields cf ON c.id = cf.contact_id
                   WHERE cf.field_name = ? AND cf.field_value = ? AND c.archived = 0""",
                (field_name, field_value)
            ).fetchall()
        else:
            rows = self.conn.execute(
                """SELECT c.* FROM contacts c
                   JOIN custom_fields cf ON c.id = cf.contact_id
                   WHERE cf.field_name = ? AND c.archived = 0""",
                (field_name,)
            ).fetchall()
        return [dict(r) for r in rows]

    # --- View Batch Operations ---

    def view_batch_update(self, view_name, **fields):
        """Update all contacts in a saved view. Returns count updated."""
        contacts = self.run_view(view_name)
        count = 0
        for c in contacts:
            identifier = c.get("email") or c["name"]
            result = self.update_contact(identifier, **fields)
            if result:
                count += 1
        return count

    def view_batch_tag(self, view_name, tag):
        """Tag all contacts in a saved view. Returns count tagged."""
        contacts = self.run_view(view_name)
        count = 0
        for c in contacts:
            identifier = c.get("email") or c["name"]
            result = self.add_tag(identifier, tag)
            if result:
                count += 1
        return count

    def export_vcard(self, path=None):
        """Export all contacts as vCard 3.0 (.vcf). Standard format importable everywhere."""
        contacts = self.list_contacts()
        lines = []
        for c in contacts:
            lines.append("BEGIN:VCARD")
            lines.append("VERSION:3.0")
            name = c["name"]
            parts = name.split(None, 1)
            first = parts[0] if parts else name
            last = parts[1] if len(parts) > 1 else ""
            lines.append(f"N:{last};{first};;;")
            lines.append(f"FN:{name}")
            if c.get("email"):
                lines.append(f"EMAIL;TYPE=WORK:{c['email']}")
            if c.get("company"):
                lines.append(f"ORG:{c['company']}")
            if c.get("title"):
                lines.append(f"TITLE:{c['title']}")
            if c.get("notes"):
                # vCard escapes: newlines, commas, semicolons, backslashes
                note = c["notes"].replace("\\", "\\\\").replace(",", "\\,").replace(";", "\\;").replace("\n", "\\n")
                lines.append(f"NOTE:{note}")
            lines.append("END:VCARD")
        vcf_text = "\n".join(lines) + "\n"
        if path:
            with open(path, "w") as f:
                f.write(vcf_text)
            return path
        return vcf_text

    def import_vcard(self, path):
        """Import FN, EMAIL, ORG, and TEL fields from one or more vCards."""
        if not os.path.exists(path):
            return 0

        with open(path, "rb") as f:
            physical_lines = f.read().splitlines()

        # RFC 6350 folds long content lines by starting the continuation with
        # a space or tab. Some older vCard writers also use quoted-printable
        # soft line breaks, so accept those here too.
        lines = []
        for raw_line in physical_lines:
            if lines and (raw_line.startswith((b" ", b"\t")) or lines[-1].endswith(b"=")):
                if lines[-1].endswith(b"="):
                    lines[-1] = lines[-1][:-1]
                lines[-1] += raw_line[1:] if raw_line.startswith((b" ", b"\t")) else raw_line
            else:
                lines.append(raw_line)

        def decode_value(raw_value, params):
            encoding = params.get("ENCODING", "").upper()
            if encoding in ("QUOTED-PRINTABLE", "Q"):
                raw_value = quopri.decodestring(raw_value)
            charset = params.get("CHARSET", "utf-8")
            try:
                value = raw_value.decode(charset)
            except (LookupError, UnicodeDecodeError):
                value = raw_value.decode("utf-8", errors="replace")
            return (value.replace("\\n", "\n").replace("\\N", "\n")
                         .replace("\\,", ",").replace("\\;", ";")
                         .replace("\\:", ":").replace("\\\\", "\\"))

        count = 0
        card = None
        for raw_line in lines:
            if raw_line.strip().upper() == b"BEGIN:VCARD":
                card = {"FN": [], "EMAIL": [], "ORG": [], "TEL": [], "TITLE": [], "NOTE": []}
                continue
            if raw_line.strip().upper() == b"END:VCARD":
                if card:
                    name = card["FN"][0].strip() if card["FN"] else None
                    email = card["EMAIL"][0].strip() if card["EMAIL"] else None
                    company = card["ORG"][0].split(";", 1)[0].strip() if card["ORG"] else None
                    title = card["TITLE"][0].strip() if card["TITLE"] else None
                    notes = card["NOTE"][0].strip() if card["NOTE"] else None
                    if name:
                        try:
                            self._validate_email(email)
                            self.add_contact(name=name, email=email or None, company=company or None,
                                             title=title or None, notes=notes or None,
                                             source="vcard_import")
                            phones = [phone.strip() for phone in card["TEL"] if phone.strip()]
                            if phones:
                                entity = f"contact:{name.lower()}"
                                self.observe_many(
                                    [(entity, "phone", phone, "vcard_import") for phone in phones],
                                    source="vcard_import",
                                )
                            count += 1
                        except (sqlite3.IntegrityError, ValueError):
                            pass  # skip duplicates and invalid emails
                card = None
                continue
            if card is None or b":" not in raw_line:
                continue

            raw_head, raw_value = raw_line.split(b":", 1)
            head_parts = raw_head.decode("ascii", errors="ignore").split(";")
            property_name = head_parts[0].rsplit(".", 1)[-1].upper()
            params = {}
            for param in head_parts[1:]:
                if "=" in param:
                    key, value = param.split("=", 1)
                    params[key.upper()] = value.strip('"')
            if property_name in card:
                card[property_name].append(decode_value(raw_value, params))
        return count

    def dashboard(self):
        """All-in-one CRM dashboard: metrics, pipeline, actions, health — single call.

        Returns a dict with everything an agent needs for a daily briefing.
        """
        stats = self.stats()
        rev = self.revenue_report()
        pipeline = self.pipeline()
        actions = self.next_actions(limit=5)
        hc = self.health_check()
        stale = self.stale_contacts()
        gs = self.graph_stats()

        return {
            "metrics": {
                "total_contacts": stats["total_contacts"],
                "mrr": rev["mrr"],
                "arr": rev["arr"],
                "pipeline_value": rev["pipeline_value"],
                "active_last_7d": stats["contacted_last_7d"],
                "stale_14d": stats["stale_14d"],
            },
            "pipeline": pipeline,
            "top_actions": actions,
            "health": {
                "healthy": len(hc["healthy"]),
                "at_risk": len(hc["at_risk"]),
                "cold": len(hc["cold"]),
                "actions": hc["actions"][:5],
            },
            "stale_contacts": [{"name": c["name"], "company": c.get("company"),
                                "deal_size": c.get("deal_size")} for c in stale[:5]],
            "graph": gs,
        }

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
        # Record activity in knowledge graph so graph queries can find it
        entity = f"contact:{contact['name'].lower()}"
        self.observe(entity, f"activity_{activity_type}", summary, source="crm_activity")
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
        # Record deal in knowledge graph so graph queries surface it
        entity = f"contact:{contact['name'].lower()}"
        facts = [(entity, "deal", name, "crm_deal")]
        if value:
            facts.append((entity, "deal_value", str(value), "crm_deal"))
        if stage:
            facts.append((entity, "deal_stage", stage, "crm_deal"))
        self.observe_many(facts, source="crm_deal")
        return True

    def get_deal(self, deal_id):
        """Get a deal by its id."""
        row = self.conn.execute(
            "SELECT d.*, c.name as contact_name, c.company FROM deals d JOIN contacts c ON d.contact_id = c.id WHERE d.id = ?",
            (deal_id,)
        ).fetchone()
        return dict(row) if row else None

    def update_deal(self, deal_id, **fields):
        """Update a deal by id. Allowed fields: name, value, stage, notes."""
        deal = self.get_deal(deal_id)
        if not deal:
            return None
        allowed = {"name", "value", "stage", "notes"}
        updates = {k: v for k, v in fields.items() if k in allowed and v is not None}
        if not updates:
            return deal
        # If stage is changing to closed_won, set closed_at
        if updates.get("stage") == "closed_won" and deal.get("stage") != "closed_won":
            updates["closed_at"] = datetime.now().isoformat()
        sets = ", ".join(f"{k} = ?" for k in updates)
        vals = list(updates.values())
        vals.append(deal_id)
        self.conn.execute(f"UPDATE deals SET {sets}, updated_at = CURRENT_TIMESTAMP WHERE id = ?", vals)
        self.conn.commit()
        # Update knowledge graph
        contact = self.conn.execute("SELECT name FROM contacts WHERE id = ?", (deal["contact_id"],)).fetchone()
        if contact:
            entity = f"contact:{contact['name'].lower()}"
            if "stage" in updates:
                self.observe(entity, "deal_stage", updates["stage"], source="crm_deal")
            if "value" in updates:
                self.observe(entity, "deal_value", str(updates["value"]), source="crm_deal")
        return self.get_deal(deal_id)

    def delete_deal(self, deal_id):
        """Delete a deal by id."""
        deal = self.get_deal(deal_id)
        if not deal:
            return False
        self.conn.execute("DELETE FROM deals WHERE id = ?", (deal_id,))
        self.conn.commit()
        return True

    def close_deal(self, deal_id, notes=None):
        """Close a deal as won. Sets stage to closed_won and records closed_at."""
        updates = {"stage": "closed_won"}
        if notes:
            updates["notes"] = notes
        return self.update_deal(deal_id, **updates)

    def deals_for_contact(self, identifier):
        """Get all deals for a specific contact."""
        contact = self.get_contact(identifier)
        if not contact:
            return []
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM deals WHERE contact_id = ? ORDER BY updated_at DESC",
            (contact["id"],)
        ).fetchall()]

    def deal_pipeline(self):
        """Pipeline view focused on deals (not contacts).

        Returns stages with deal counts, total value, and top deals per stage.
        """
        stages = ["prospect", "qualification", "proposal", "negotiation", "closed_won", "closed_lost"]
        all_deals = self.conn.execute(
            "SELECT d.*, c.name as contact_name, c.company FROM deals d JOIN contacts c ON d.contact_id = c.id"
        ).fetchall()

        by_stage = {s: [] for s in stages}
        for d in all_deals:
            s = d["stage"]
            if s not in by_stage:
                by_stage[s] = []
            by_stage[s].append(dict(d))

        pipeline = []
        for stage in stages:
            deals = by_stage[stage]
            total_value = sum(self._parse_deal_size(d.get("value")) for d in deals)
            pipeline.append({
                "stage": stage,
                "count": len(deals),
                "total_value": round(total_value, 2),
                "deals": [{"name": d["name"], "value": d.get("value"), "contact": d.get("contact_name")}
                          for d in sorted(deals, key=lambda x: self._parse_deal_size(x.get("value")), reverse=True)[:5]],
            })
        return pipeline

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
               FROM contacts WHERE archived = 0 GROUP BY status ORDER BY count DESC"""
        ).fetchall()
        return [dict(r) for r in rows]

    def stats(self):
        total = self.conn.execute("SELECT COUNT(*) FROM contacts WHERE archived = 0").fetchone()[0]
        by_status = self.conn.execute(
            "SELECT status, COUNT(*) as n FROM contacts WHERE archived = 0 GROUP BY status ORDER BY n DESC"
        ).fetchall()
        recent = self.conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE archived = 0 AND last_contacted >= date('now', '-7 days')"
        ).fetchone()[0]
        stale = len(self.stale_contacts())
        return {
            "total_contacts": total,
            "contacted_last_7d": recent,
            # Kept for API compatibility; the count now uses per-status tuned cadence.
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
        lines.append(f"**Total:** {stats['total_contacts']} · **Active last 7d:** {stats['contacted_last_7d']} · **Stale (tuned):** {stats['stale_14d']}")
        return "\n".join(lines)

    # --- Import/Export ---

    _FULL_TABLE_COLUMNS = {
        "contacts": (
            "id", "email", "name", "company", "title", "deal_size", "status",
            "source", "notes", "tags", "archived", "last_contacted",
            "created_at", "updated_at",
        ),
        "activity": ("id", "contact_id", "type", "summary", "created_at"),
        "deals": (
            "id", "contact_id", "name", "value", "stage", "notes",
            "created_at", "updated_at", "closed_at",
        ),
        "facts": (
            "rowid", "entity", "key", "value", "source", "observed_at",
            "event_day",
        ),
    }

    _CSV_FIELDS = _FULL_TABLE_COLUMNS["contacts"]
    _CSV_ALIASES = {
        "name": {"name", "full name", "contact name"},
        "email": {"email", "email address", "e mail", "e-mail", "e-mail address"},
        "company": {"company", "company name", "organization", "organisation", "org"},
        "title": {"title", "job title", "role", "position"},
        "deal_size": {"deal size", "deal value", "amount", "value"},
        "status": {"status", "stage", "lifecycle", "lifecycle stage"},
        "source": {"source", "lead source", "origin"},
        "phone": {"phone", "phone number", "mobile", "mobile phone", "telephone", "tel"},
        "notes": {"notes", "note", "description", "comments", "comment"},
        "tags": {"tags", "tag"},
        "archived": {"archived"},
        "last_contacted": {"last contacted", "last contact", "last_contacted"},
        "created_at": {"created at", "created_at"},
        "updated_at": {"updated at", "updated_at"},
        "id": {"id", "contact id", "contact_id"},
    }

    @staticmethod
    def _normalized_csv_header(header):
        return re.sub(r"[^a-z0-9]+", " ", str(header).strip().lower()).strip()

    def export_full(self):
        """Return a lossless, versioned export of every core CRM table."""
        exported = {"schema_version": SCHEMA_VERSION}
        primary_keys = {"contacts": "id", "activity": "id", "deals": "id", "facts": "rowid"}
        for table in ("contacts", "activity", "deals", "facts"):
            rows = self.conn.execute(
                f"SELECT * FROM {table} ORDER BY {primary_keys[table]}"
            ).fetchall()
            exported[table] = [dict(row) for row in rows]
        return exported

    def import_full(self, data, merge=False):
        """Restore an ``export_full`` payload, optionally merging contacts.

        A non-merge restore preserves primary keys and timestamps exactly, and
        therefore requires empty core tables. Merge mode maps imported child
        rows to contacts matched by email, falling back to an exact name match.
        """
        required = ("contacts", "activity", "deals", "facts")
        if not isinstance(data, dict):
            return {"error": "Full import data must be an object"}
        if data.get("schema_version") != SCHEMA_VERSION:
            return {"error": f"Unsupported schema_version: {data.get('schema_version')}"}
        if any(not isinstance(data.get(table), list) for table in required):
            return {"error": "Full import requires contacts, activity, deals, and facts lists"}
        if any(not isinstance(row, dict) for table in required for row in data[table]):
            return {"error": "Full import rows must be objects"}

        if not merge:
            counts = {
                table: self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in required
            }
            if any(counts.values()):
                return {
                    "error": "Full import requires an empty database; use merge=True to combine data",
                    "counts": counts,
                }

        imported = {"contacts": 0, "activity": 0, "deals": 0, "facts": 0}
        updated_contacts = 0
        skipped = {"activity": 0, "deals": 0, "facts": 0}

        try:
            with self.conn:
                if not merge:
                    for table in required:
                        columns = self._FULL_TABLE_COLUMNS[table]
                        placeholders = ", ".join("?" for _ in columns)
                        column_sql = ", ".join(columns)
                        self.conn.executemany(
                            f"INSERT INTO {table} ({column_sql}) VALUES ({placeholders})",
                            [tuple(
                                (row.get(column) or "")
                                if table == "facts" and column == "event_day"
                                else row.get(column)
                                for column in columns
                            ) for row in data[table]],
                        )
                        imported[table] = len(data[table])
                else:
                    contact_columns = self._FULL_TABLE_COLUMNS["contacts"]
                    mutable_contact_columns = contact_columns[1:]
                    contact_ids = {}
                    for row in data["contacts"]:
                        existing = None
                        if row.get("email"):
                            existing = self.conn.execute(
                                "SELECT id FROM contacts WHERE email = ? COLLATE NOCASE",
                                (row["email"],),
                            ).fetchone()
                        if existing is None and row.get("name"):
                            existing = self.conn.execute(
                                "SELECT id FROM contacts WHERE name = ?", (row["name"],)
                            ).fetchone()

                        if existing:
                            target_id = existing["id"]
                            incoming_updates = {
                                column: row.get(column)
                                for column in mutable_contact_columns
                                if row.get(column) is not None
                                and row.get(column) != ""
                            }
                            if incoming_updates:
                                assignments = ", ".join(
                                    f"{column} = ?" for column in incoming_updates
                                )
                                self.conn.execute(
                                    f"UPDATE contacts SET {assignments} WHERE id = ?",
                                    tuple(incoming_updates.values()) + (target_id,),
                                )
                            updated_contacts += 1
                        else:
                            placeholders = ", ".join("?" for _ in mutable_contact_columns)
                            self.conn.execute(
                                f"INSERT INTO contacts ({', '.join(mutable_contact_columns)}) VALUES ({placeholders})",
                                tuple(row.get(column) for column in mutable_contact_columns),
                            )
                            target_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                        contact_ids[row.get("id")] = target_id
                        imported["contacts"] += 1

                    for row in data["activity"]:
                        target_id = contact_ids.get(row.get("contact_id"))
                        if target_id is None:
                            raise ValueError(f"activity references unknown contact_id {row.get('contact_id')}")
                        exists = self.conn.execute(
                            """SELECT 1 FROM activity
                               WHERE contact_id = ? AND type IS ? AND summary IS ? AND created_at IS ?""",
                            (target_id, row.get("type"), row.get("summary"), row.get("created_at")),
                        ).fetchone()
                        if exists:
                            skipped["activity"] += 1
                            continue
                        self.conn.execute(
                            "INSERT INTO activity (contact_id, type, summary, created_at) VALUES (?, ?, ?, ?)",
                            (target_id, row.get("type"), row.get("summary"), row.get("created_at")),
                        )
                        imported["activity"] += 1

                    for row in data["deals"]:
                        target_id = contact_ids.get(row.get("contact_id"))
                        if target_id is None:
                            raise ValueError(f"deal references unknown contact_id {row.get('contact_id')}")
                        deal_values = (
                            target_id, row.get("name"), row.get("value"), row.get("stage"),
                            row.get("notes"), row.get("created_at"), row.get("updated_at"),
                            row.get("closed_at"),
                        )
                        exists = self.conn.execute(
                            """SELECT 1 FROM deals WHERE contact_id = ? AND name IS ? AND value IS ?
                               AND stage IS ? AND notes IS ? AND created_at IS ?
                               AND updated_at IS ? AND closed_at IS ?""",
                            deal_values,
                        ).fetchone()
                        if exists:
                            skipped["deals"] += 1
                            continue
                        self.conn.execute(
                            """INSERT INTO deals
                               (contact_id, name, value, stage, notes, created_at, updated_at, closed_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            deal_values,
                        )
                        imported["deals"] += 1

                    for row in data["facts"]:
                        fact_values = (
                            row.get("entity"), row.get("key"), row.get("value"),
                            row.get("source"), row.get("observed_at"),
                            row.get("event_day") or "",
                        )
                        exists = self.conn.execute(
                            """SELECT 1 FROM facts WHERE entity IS ? AND key IS ? AND value IS ?
                               AND source IS ? AND observed_at IS ? AND event_day IS ?""",
                            fact_values,
                        ).fetchone()
                        if exists:
                            skipped["facts"] += 1
                            continue
                        try:
                            self.conn.execute(
                                """INSERT INTO facts
                                   (entity, key, value, source, observed_at, event_day)
                                   VALUES (?, ?, ?, ?, ?, ?)""",
                                fact_values,
                            )
                            imported["facts"] += 1
                        except sqlite3.IntegrityError:
                            # Older schemas deduplicate identical fact content even
                            # when the observation timestamps differ.
                            skipped["facts"] += 1
        except (sqlite3.Error, ValueError, TypeError) as exc:
            return {"error": f"Full import failed: {exc}"}

        return {
            "imported": imported,
            "updated_contacts": updated_contacts,
            "skipped": skipped,
            "merge": bool(merge),
        }

    def export_csv(self, path=None, enrich=False):
        """Export contacts to CSV.

        If enrich=True, append activity_count, deal_count, and score columns
        so the spreadsheet gives a complete picture of each contact.
        """
        contacts = [dict(row) for row in self.conn.execute("SELECT * FROM contacts ORDER BY id")]
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
        if isinstance(output, (str, os.PathLike)):
            output = open(output, "w", newline="", encoding="utf-8")
            should_close = True
        fieldnames = list(self._CSV_FIELDS)
        if enrich:
            fieldnames.extend(("activity_count", "deal_count", "score"))
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(contacts)
        if should_close:
            output.close()
        return path or "stdout"

    def import_csv(self, path, mapping=None):
        """Import contacts from CSV with common-header auto-detection."""
        outcome = {"imported": 0, "skipped": 0, "errors": 0}
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            sample = f.read(8192)
            f.seek(0)
            if not sample.strip():
                return outcome
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                reader = csv.DictReader(f, dialect=dialect, strict=True)
            except csv.Error:
                reader = csv.DictReader(f, strict=True)

            headers = reader.fieldnames or []
            field_map = {}
            for header in headers:
                normalized = self._normalized_csv_header(header)
                for field, aliases in self._CSV_ALIASES.items():
                    if normalized in {self._normalized_csv_header(alias) for alias in aliases}:
                        field_map[normalized] = field
                        break

            if mapping:
                canonical_fields = {
                    self._normalized_csv_header(field): field for field in self._CSV_ALIASES
                }
                for left, right in mapping.items():
                    left_normalized = self._normalized_csv_header(left)
                    right_normalized = self._normalized_csv_header(right)
                    if left_normalized in canonical_fields and right_normalized not in canonical_fields:
                        source_header = right_normalized
                        target_field = canonical_fields[left_normalized]
                    else:
                        source_header = left_normalized
                        target_field = canonical_fields.get(right_normalized)
                    if target_field is None:
                        continue
                    field_map = {
                        source: target for source, target in field_map.items()
                        if target != target_field
                    }
                    field_map[source_header] = target_field

            while True:
                try:
                    row = next(reader)
                except StopIteration:
                    break
                except csv.Error:
                    outcome["errors"] += 1
                    break

                if None in row or any(value is None for value in row.values()):
                    outcome["errors"] += 1
                    continue
                normalized = {}
                for header, value in row.items():
                    field = field_map.get(self._normalized_csv_header(header))
                    if field:
                        normalized[field] = value.strip() if isinstance(value, str) else value

                name = normalized.get("name", "")
                if not name:
                    outcome["skipped"] += 1
                    continue

                try:
                    archived = None
                    if "archived" in normalized and normalized["archived"] != "":
                        archived = int(normalized["archived"])
                    contact_id = self.add_contact(
                        name=name,
                        email=normalized.get("email") or None,
                        company=normalized.get("company") or None,
                        title=normalized.get("title") or None,
                        deal_size=normalized.get("deal_size") or None,
                        status=normalized.get("status") or "prospect",
                        source=normalized.get("source") or None,
                        notes=normalized.get("notes") or None,
                        tags=normalized.get("tags") or None,
                    )
                    metadata = {
                        key: normalized[key] for key in
                        ("last_contacted", "created_at", "updated_at")
                        if normalized.get(key)
                    }
                    if archived is not None:
                        metadata["archived"] = archived
                    if metadata:
                        assignments = ", ".join(f"{key} = ?" for key in metadata)
                        self.conn.execute(
                            f"UPDATE contacts SET {assignments} WHERE id = ?",
                            tuple(metadata.values()) + (contact_id,),
                        )
                        self.conn.commit()
                    phone = normalized.get("phone")
                    if phone:
                        self.observe(f"contact:{name.lower()}", "phone", phone, source="csv_import")
                    outcome["imported"] += 1
                except sqlite3.IntegrityError:
                    outcome["skipped"] += 1
                except (TypeError, ValueError):
                    outcome["errors"] += 1
        return outcome

    # --- Context Graph ---

    def observe(self, entity, key, value, source="manual"):
        """Record a fact. Any entity, any key, any value, any source.

        Deduplicates: if the exact (entity, key, value, source) tuple already
        exists, refreshes observed_at instead of creating a duplicate row.
        """
        if not entity or not key:
            raise ValueError("entity and key must be non-empty")
        self.conn.execute(
            """INSERT INTO facts (entity, key, value, source, event_day)
               VALUES (?, ?, ?, ?, '')
               ON CONFLICT(entity, key, value, source, event_day)
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
            """INSERT INTO facts (entity, key, value, source, event_day)
               VALUES (?, ?, ?, ?, '')
               ON CONFLICT(entity, key, value, source, event_day)
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
        """Facts not observed in N days. What needs re-verification.

        Finds entity+key pairs whose *latest* observation (across all values
        and sources) is older than the cutoff.  Returns the value and source
        from the most recently observed row so callers see accurate data.

        The previous implementation selected non-aggregated columns (value,
        source, observed_at) directly in a GROUP BY query, which in SQLite
        returns values from an arbitrary row in the group rather than the
        row with MAX(observed_at).  Now we identify stale (entity, key)
        groups first, then look up the actual latest row for each group via
        a correlated subquery so the returned columns are correct.
        """
        # Step 1: find stale (entity, key) groups and their latest observed_at
        stale_groups = self.conn.execute(
            """SELECT entity, key, MAX(observed_at) as latest_oa
               FROM facts
               GROUP BY entity, key
               HAVING MAX(observed_at) < date('now', ?)""",
            (f"-{days} days",)
        ).fetchall()
        # Step 2: for each stale group, fetch the actual latest row
        results = []
        for g in stale_groups:
            row = self.conn.execute(
                """SELECT entity, key, value, source, observed_at
                   FROM facts
                   WHERE entity = ? AND key = ?
                   ORDER BY observed_at DESC, rowid DESC
                   LIMIT 1""",
                (g["entity"], g["key"])
            ).fetchone()
            if row:
                results.append(dict(row))
        results.sort(key=lambda r: r["observed_at"])
        return results

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

        Uses a correlated subquery to reliably select the row with the
        highest rowid per entity.  The previous implementation used
        ``HAVING rowid = MAX(rowid)`` inside a GROUP BY, which compares
        an *arbitrary* row's rowid against the aggregate MAX — an
        undefined comparison in SQL that can silently drop entire
        entities from the result when the arbitrary rowid doesn't happen
        to be the maximum.
        """
        rows = self.conn.execute(
            """SELECT value, COUNT(*) as cnt FROM (
                   SELECT entity, value
                   FROM facts f
                   WHERE key = ?
                   AND rowid = (
                       SELECT MAX(f2.rowid) FROM facts f2
                       WHERE f2.entity = f.entity AND f2.key = f.key
                   )
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

        Handles duplicate (key, value, source) tuples gracefully: when both
        entities share a fact with the same key+value+source, the row with the
        latest observed_at is kept and the other is deleted, avoiding a UNIQUE
        constraint violation on idx_facts_dedup.
        """
        # Delete target rows that would collide with existing source rows,
        # keeping whichever has the later observed_at.  For each overlapping
        # (key, value, source) pair, delete the older of the two rows.
        self.conn.execute(
            """DELETE FROM facts WHERE rowid IN (
                   SELECT CASE
                       WHEN t.observed_at >= s.observed_at THEN s.rowid
                       ELSE t.rowid
                   END
                   FROM facts t
                   JOIN facts s
                     ON s.entity = ?
                    AND t.entity = ?
                    AND t.key = s.key
                    AND t.value = s.value
                    AND t.source = s.source
               )""",
            (source_entity, target_entity)
        )
        # Now re-attribute remaining target facts (no conflicts left)
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
    STAGE_PROBABILITIES = {
        "prospect": 0.10,
        "contacted": 0.20,
        "qualification": 0.25,
        "met": 0.30,
        "proposal": 0.45,
        "proposal_drafted": 0.50,
        "negotiating": 0.65,
        "negotiation": 0.65,
        "verbal_yes": 0.75,
        "active_customer": 1.0,
    }
    FORECAST_SIGNAL_NAMES = (
        "stage",
        "activity_recency",
        "activity_frequency",
        "source",
        "historical_win_rate",
    )
    NEXT_ACTION_SIGNAL_NAMES = (
        "overdue_reminder",
        "due_reminder",
        "cadence_overdue",
        "low_relationship_health",
        "stale_open_deal",
        "one_sided_messaging",
        "high_probability_quiet",
    )
    ACTION_LEARNING_MIN_OUTCOMES = 10
    ACTION_MOVE_WINDOW_DAYS = 30
    ACTION_MULTIPLIER_MIN = 0.6
    ACTION_MULTIPLIER_MAX = 1.4
    DEFAULT_FORECAST_WEIGHTS = {
        "stage": 0.45,
        "activity_recency": 0.20,
        "activity_frequency": 0.15,
        "source": 0.10,
        "historical_win_rate": 0.10,
    }
    FORECAST_LEARNING_MIN_OUTCOMES = 20
    FORECAST_LEARNING_BLEND_OUTCOMES = 10
    FORECAST_LEARNING_ITERATIONS = 800
    FORECAST_LEARNING_RATE = 0.3
    FORECAST_LEARNING_SEED = 9

    @staticmethod
    def _escape_like(s):
        """Escape SQL LIKE metacharacters (% and _) so they match literally.

        Without this, user input containing % or _ causes false matches:
        e.g. get_contact('%') would match every contact.
        """
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

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
        try:
            num = float(m.group(1))
        except (ValueError, OverflowError):
            return 0
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
            cadence_days = self.tuned_cadence(c.get("status"))

            if sc < 30 or (days_stale is not None and days_stale > cadence_days * 2):
                cold.append(entry)
                if days_stale is not None:
                    actions.append(f"Re-engage {c['name']} (last contacted {days_stale} days ago)")
                else:
                    actions.append(f"Initiate contact with {c['name']} (never contacted)")
            elif sc <= 60 or (days_stale is not None and days_stale > cadence_days):
                at_risk.append(entry)
                if days_stale is not None:
                    actions.append(f"Follow up with {c['name']} (last contacted {days_stale} days ago)")
                else:
                    actions.append(f"Schedule first contact with {c['name']}")
            else:
                healthy.append(entry)

        total = len(healthy) + len(at_risk) + len(cold)
        summary = f"{total} contacts: {len(healthy)} healthy, {len(at_risk)} at risk, {len(cold)} cold"
        return {"healthy": healthy, "at_risk": at_risk, "cold": cold, "actions": actions, "summary": summary}

    def conversion_funnel(self):
        """Pipeline conversion funnel — count, avg days, and conversion rate per stage."""
        funnel = {}
        today_str = date.today().isoformat()

        # Count lost/churned contacts once — they entered the pipeline but
        # dropped out.  Without stage-transition history we attribute them to
        # the earliest stage ("prospect") so top-of-funnel conversion rates
        # aren't artificially inflated.
        lost_churned = self.conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE status IN ('lost', 'churned')"
        ).fetchone()[0]

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

            # For the first pipeline stage, include lost/churned contacts in
            # the denominator — they entered the funnel but never converted.
            if idx == 0:
                total_entered += lost_churned

            # Average days in this status (use created_at to updated_at for current)
            total_days = 0
            parsed_count = 0
            for r in rows:
                try:
                    created = datetime.fromisoformat(r["created_at"]).date()
                    updated = datetime.fromisoformat(r["updated_at"]).date()
                    total_days += max((updated - created).days, 0)
                    parsed_count += 1
                except (ValueError, TypeError):
                    pass

            avg_days = round(total_days / parsed_count, 1) if parsed_count else 0
            conversion_rate = round((passed_through / total_entered) * 100, 1) if total_entered > 0 else 0

            funnel[status] = {
                "count": current_count,
                "avg_days": avg_days,
                "conversion_rate": conversion_rate,
            }
        return funnel

    def _deal_probability_for_contact(self, contact, history, activity_rows=None,
                                      weight_model=None):
        """Score one open contact using precomputed CRM-wide patterns."""
        status = contact.get("status") or "prospect"
        stage_score = self.STAGE_PROBABILITIES.get(status, 0.10)

        if activity_rows is None:
            activity_rows = self.conn.execute(
                "SELECT created_at FROM activity WHERE contact_id = ? ORDER BY created_at DESC",
                (contact["id"],)
            ).fetchall()
        activity_dates = []
        for row in activity_rows:
            try:
                activity_dates.append(datetime.fromisoformat(row["created_at"]).date())
            except (ValueError, TypeError):
                pass

        latest_activity = activity_dates[0] if activity_dates else None
        if latest_activity is None and contact.get("last_contacted"):
            try:
                latest_activity = date.fromisoformat(str(contact["last_contacted"])[:10])
            except (ValueError, TypeError):
                pass

        cadence_days = float(self.tuned_cadence(status))

        days_since_activity = None
        recency_score = 0.0
        if latest_activity:
            days_since_activity = max((date.today() - latest_activity).days, 0)
            if days_since_activity <= cadence_days:
                recency_score = 1.0
            elif days_since_activity <= cadence_days * 2:
                recency_score = 0.65
            elif days_since_activity <= cadence_days * 4:
                recency_score = 0.30

        frequency_window = max(30, int(round(cadence_days * 4)))
        activity_cutoff = date.today() - timedelta(days=frequency_window)
        recent_activity_count = sum(1 for activity_date in activity_dates
                                    if activity_date >= activity_cutoff)
        expected_touches = max(frequency_window / cadence_days, 1.0)
        frequency_score = min(recent_activity_count / expected_touches, 1.0)

        wins = history.get("wins", [])
        losses = history.get("losses", [])
        total_closed = len(wins) + len(losses)
        historical_score = len(wins) / total_closed if total_closed else 0.5

        source = contact.get("source") or "unknown"
        source_wins = sum(1 for win in wins if (win.get("source") or "unknown") == source)
        source_losses = sum(1 for loss in losses if (loss.get("source") or "unknown") == source)
        source_sample = source_wins + source_losses
        source_score = ((source_wins + 1) / (source_sample + 2)
                        if source_sample else historical_score)

        blend_alpha = 0.0
        if weight_model:
            outcome_count = weight_model.get(
                "outcome_count",
                self.FORECAST_LEARNING_MIN_OUTCOMES
                + self.FORECAST_LEARNING_BLEND_OUTCOMES,
            )
            blend_alpha = min(max(
                (outcome_count - self.FORECAST_LEARNING_MIN_OUTCOMES)
                / self.FORECAST_LEARNING_BLEND_OUTCOMES,
                0.0,
            ), 1.0)
        weights_source = "learned" if blend_alpha > 0 else "default"
        weights = (weight_model["weights"] if blend_alpha > 0
                   else self.DEFAULT_FORECAST_WEIGHTS)
        raw_signal_values = [
            ("stage", status, stage_score),
            ("activity_recency", days_since_activity, recency_score),
            ("activity_frequency", recent_activity_count, frequency_score),
            ("source", source, source_score),
            ("historical_win_rate", round(historical_score, 4), historical_score),
        ]
        signal_values = [
            (name, value, weights[name], score)
            for name, value, score in raw_signal_values
        ]
        signals = [{
            "name": name,
            "value": value,
            "weight": weight,
            "effect": round(weight * score, 4),
        } for name, value, weight, score in signal_values]
        default_probability = min(max(sum(
            round(self.DEFAULT_FORECAST_WEIGHTS[name] * score, 4)
            for name, _, score in raw_signal_values
        ), 0.0), 1.0)
        if weight_model:
            linear_score = sum(
                weight_model["weights"][name] * score
                for name, _, score in raw_signal_values
            )
            logit = weight_model["intercept"] + linear_score
            if logit >= 0:
                learned_probability = 1.0 / (1.0 + math.exp(-min(logit, 60.0)))
            else:
                exp_logit = math.exp(max(logit, -60.0))
                learned_probability = exp_logit / (1.0 + exp_logit)
            probability = (
                default_probability * (1.0 - blend_alpha)
                + learned_probability * blend_alpha
            )
        else:
            probability = default_probability
        probability = round(probability, 4)

        return {
            "probability": probability,
            "blend_alpha": round(blend_alpha, 4),
            "signals": signals,
            "contact": contact,
            "status": status,
            "weights_source": weights_source,
            "_signals_snapshot": {
                name: {"value": value, "score": round(score, 6)}
                for name, value, _, score in signal_values
            },
        }

    @staticmethod
    def _public_probability(scored):
        """Remove recording-only fields from a probability response."""
        return {key: value for key, value in scored.items()
                if not key.startswith("_")}

    def _record_prediction(self, contact, scored):
        """Persist a materially changed forecast as one three-fact event."""
        identifier = contact.get("email") or contact["name"]
        entity = f"prediction:{identifier}"
        latest = self.conn.execute(
            """SELECT value FROM facts
               WHERE entity = ? AND key = 'probability' AND source = 'forecast'
               ORDER BY observed_at DESC, rowid DESC LIMIT 1""",
            (entity,),
        ).fetchone()
        if latest:
            try:
                if abs(float(latest["value"]) - scored["probability"]) <= 0.020000000001:
                    return False
            except (TypeError, ValueError):
                pass

        observed_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(
            sep=" ", timespec="microseconds"
        )
        rows = (
            (entity, "probability", str(scored["probability"]), "forecast", observed_at),
            (entity, "status", scored["status"], "forecast", observed_at),
            (entity, "signals_snapshot", json.dumps(
                scored["_signals_snapshot"], sort_keys=True, separators=(",", ":")
            ), "forecast", observed_at),
        )
        with self.conn:
            self.conn.executemany(
                """INSERT INTO facts (entity, key, value, source, observed_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(entity, key, value, source, event_day)
                   DO UPDATE SET observed_at = excluded.observed_at""",
                rows,
            )
        return True

    @classmethod
    def _signal_scores_from_snapshot(cls, raw_snapshot):
        """Parse both native and simple name-to-score prediction fixtures."""
        try:
            snapshot = json.loads(raw_snapshot)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

        if isinstance(snapshot, list):
            snapshot = {
                item.get("name"): item for item in snapshot
                if isinstance(item, dict) and item.get("name")
            }
        if not isinstance(snapshot, dict):
            return None

        scores = {}
        for name in cls.FORECAST_SIGNAL_NAMES:
            item = snapshot.get(name)
            if isinstance(item, dict):
                item = item.get("score")
            try:
                scores[name] = min(max(float(item), 0.0), 1.0)
            except (TypeError, ValueError):
                return None
        return scores

    @staticmethod
    def _stage_from_snapshot(raw_snapshot):
        try:
            snapshot = json.loads(raw_snapshot)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if isinstance(snapshot, dict):
            stage = snapshot.get("stage")
            if isinstance(stage, dict):
                return stage.get("value")
        if isinstance(snapshot, list):
            for signal in snapshot:
                if isinstance(signal, dict) and signal.get("name") == "stage":
                    return signal.get("value")
        return None

    def _prediction_outcomes(self):
        """Return recorded predictions whose contacts now have closed outcomes."""
        rows = self.conn.execute(
            """SELECT p.rowid AS prediction_rowid, p.entity,
                      p.value AS probability, p.observed_at, c.status AS outcome_status,
                      (SELECT s.value FROM facts s
                       WHERE s.entity = p.entity AND s.key = 'status'
                         AND s.source = 'forecast' AND s.observed_at = p.observed_at
                       ORDER BY s.rowid DESC LIMIT 1) AS predicted_status,
                      (SELECT s.value FROM facts s
                       WHERE s.entity = p.entity AND s.key = 'signals_snapshot'
                         AND s.source = 'forecast' AND s.observed_at = p.observed_at
                       ORDER BY s.rowid DESC LIMIT 1) AS signals_snapshot
               FROM facts p
               JOIN contacts c
                 ON p.entity = 'prediction:' || COALESCE(NULLIF(c.email, ''), c.name)
               WHERE p.key = 'probability' AND p.source = 'forecast'
                 AND c.status IN ('active_customer', 'lost', 'churned')
               ORDER BY p.observed_at ASC, p.rowid ASC"""
        ).fetchall()
        outcomes = []
        for row in rows:
            try:
                probability = float(row["probability"])
            except (TypeError, ValueError):
                continue
            if not 0.0 <= probability <= 1.0:
                continue
            snapshot = row["signals_snapshot"]
            outcomes.append({
                "entity": row["entity"],
                "probability": probability,
                "outcome": 1 if row["outcome_status"] == "active_customer" else 0,
                "stage": (row["predicted_status"]
                          or self._stage_from_snapshot(snapshot) or "unknown"),
                "scores": self._signal_scores_from_snapshot(snapshot),
                "observed_at": row["observed_at"],
                "rowid": row["prediction_rowid"],
            })
        return outcomes

    def learned_weights(self):
        """Fit deterministic logistic signal weights from closed predictions."""
        latest_by_contact = {}
        for prediction in self._prediction_outcomes():
            if prediction["scores"] is not None:
                latest_by_contact[prediction["entity"]] = prediction
        samples = list(latest_by_contact.values())
        if len(samples) < self.FORECAST_LEARNING_MIN_OUTCOMES:
            return None

        rng = random.Random(self.FORECAST_LEARNING_SEED)
        weights = {
            name: rng.uniform(-0.01, 0.01)
            for name in self.FORECAST_SIGNAL_NAMES
        }
        intercept = rng.uniform(-0.01, 0.01)
        sample_count = float(len(samples))

        for _ in range(self.FORECAST_LEARNING_ITERATIONS):
            weight_gradients = {name: 0.0 for name in self.FORECAST_SIGNAL_NAMES}
            intercept_gradient = 0.0
            for sample in samples:
                logit = intercept + sum(
                    weights[name] * sample["scores"][name]
                    for name in self.FORECAST_SIGNAL_NAMES
                )
                if logit >= 0:
                    predicted = 1.0 / (1.0 + math.exp(-min(logit, 60.0)))
                else:
                    exp_logit = math.exp(max(logit, -60.0))
                    predicted = exp_logit / (1.0 + exp_logit)
                error = predicted - sample["outcome"]
                intercept_gradient += error
                for name in self.FORECAST_SIGNAL_NAMES:
                    weight_gradients[name] += error * sample["scores"][name]

            intercept -= self.FORECAST_LEARNING_RATE * (
                intercept_gradient / sample_count
            )
            for name in self.FORECAST_SIGNAL_NAMES:
                weights[name] -= self.FORECAST_LEARNING_RATE * (
                    weight_gradients[name] / sample_count
                )

        return {
            "weights": {name: round(weights[name], 8)
                        for name in self.FORECAST_SIGNAL_NAMES},
            "intercept": round(intercept, 8),
            "outcome_count": len(samples),
            "iterations": self.FORECAST_LEARNING_ITERATIONS,
        }

    def calibration_report(self):
        """Score past forecasts against closed outcomes with Brier calibration."""
        latest_by_contact = {}
        for prediction in self._prediction_outcomes():
            latest_by_contact[prediction["entity"]] = prediction
        predictions = list(latest_by_contact.values())
        learned = self.learned_weights()
        stage_errors = {}
        reliability_groups = [list() for _ in range(10)]

        for prediction in predictions:
            squared_error = (prediction["probability"] - prediction["outcome"]) ** 2
            stage_errors.setdefault(prediction["stage"], []).append(squared_error)
            decile = min(int(prediction["probability"] * 10), 9)
            reliability_groups[decile].append(prediction)

        by_stage = {
            stage: {
                "count": len(errors),
                "brier_score": round(sum(errors) / len(errors), 6),
            }
            for stage, errors in sorted(stage_errors.items())
        }
        reliability = []
        for decile, group in enumerate(reliability_groups):
            lower = decile * 10
            upper = (decile + 1) * 10
            reliability.append({
                "predicted_decile": f"{lower}-{upper}%",
                "count": len(group),
                "mean_prediction": (round(sum(p["probability"] for p in group) /
                                             len(group), 4) if group else None),
                "actual_win_rate": (round(sum(p["outcome"] for p in group) /
                                           len(group), 4) if group else None),
            })

        squared_errors = [
            (prediction["probability"] - prediction["outcome"]) ** 2
            for prediction in predictions
        ]
        return {
            "scored_predictions": len(predictions),
            "brier_score": (round(sum(squared_errors) / len(squared_errors), 6)
                            if squared_errors else None),
            "by_stage": by_stage,
            "reliability": reliability,
            "weights_source": "learned" if learned else "default",
            "learned_weights": learned,
        }

    def deal_probability(self, identifier):
        """Estimate the likelihood that a contact's open deal will close."""
        contact = self.get_contact(identifier)
        if (not contact or contact.get("archived")
                or contact.get("status") in ("active_customer", "lost", "churned")):
            return None
        scored = self._deal_probability_for_contact(
            contact, self.win_loss_analysis(), weight_model=self.learned_weights()
        )
        self._record_prediction(contact, scored)
        return self._public_probability(scored)

    def forecast(self, limit=None):
        """Rank open-pipeline contacts by close probability.

        The aggregate keys from the original stage forecast remain available
        for callers that use ``weighted_pipeline`` and ``by_stage``.
        """
        contacts = self.list_contacts()
        history = self.win_loss_analysis()
        weight_model = self.learned_weights()
        activity_by_contact = {}
        for row in self.conn.execute(
            "SELECT contact_id, created_at FROM activity ORDER BY contact_id, created_at DESC"
        ).fetchall():
            activity_by_contact.setdefault(row["contact_id"], []).append(row)
        ranked = []

        for contact in contacts:
            if contact.get("status") in ("active_customer", "lost", "churned"):
                continue
            scored = self._deal_probability_for_contact(
                contact, history, activity_by_contact.get(contact["id"], []),
                weight_model=weight_model,
            )
            self._record_prediction(contact, scored)
            deal_value = self._parse_deal_size(contact.get("deal_size"))
            ranked.append({
                "name": contact["name"],
                "email": contact.get("email"),
                "company": contact.get("company"),
                "status": scored["status"],
                "deal_size": contact.get("deal_size"),
                "deal_value": deal_value,
                "probability": scored["probability"],
                "weighted_value": round(deal_value * scored["probability"], 2),
                "signals": scored["signals"],
                "weights_source": scored["weights_source"],
                "blend_alpha": scored["blend_alpha"],
                "contact": contact,
            })

        ranked.sort(key=lambda row: (-row["probability"], -row["deal_value"], row["name"].lower()))
        weighted_total = round(sum(row["weighted_value"] for row in ranked), 2)
        closed_revenue = round(sum(
            self._parse_deal_size(contact.get("deal_size"))
            for contact in contacts if contact.get("status") == "active_customer"
        ), 2)

        stages = list(self.STATUS_ORDER)
        stages.extend(sorted({row["status"] for row in ranked if row["status"] not in stages}))
        by_stage = []
        for status in stages:
            stage_contacts = [contact for contact in contacts if contact.get("status") == status]
            stage_rows = [row for row in ranked if row["status"] == status]
            total_value = sum(self._parse_deal_size(contact.get("deal_size"))
                              for contact in stage_contacts)
            if status == "active_customer":
                weighted = total_value
                probability = 1.0
            else:
                weighted = sum(row["weighted_value"] for row in stage_rows)
                probability = weighted / total_value if total_value else self.STAGE_PROBABILITIES.get(status, 0)
            by_stage.append({
                "stage": status,
                "count": len(stage_contacts),
                "total_value": round(total_value, 2),
                "probability": round(probability, 4),
                "weighted_value": round(weighted, 2),
            })

        visible = ranked
        if limit is not None:
            visible = ranked[:max(limit, 0)]

        return {
            "contacts": visible,
            "count": len(ranked),
            "weighted_pipeline": weighted_total,
            "closed_revenue": closed_revenue,
            "by_stage": by_stage,
            "weights_source": (ranked[0]["weights_source"]
                               if ranked else "default"),
            "blend_alpha": (ranked[0]["blend_alpha"] if ranked else 0.0),
        }

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

    def stale_contacts(self, days=None):
        """Contacts past their follow-up cadence, sorted by deal size.

        Cross-references the knowledge graph: if a contact has recent facts
        (e.g. iMessage activity, calendar events) observed within the window,
        they are NOT considered stale even if last_contacted is old. Passing
        ``days`` explicitly overrides tuned cadence for all contacts.
        """
        rows = self.conn.execute(
            """SELECT * FROM contacts
               WHERE archived = 0
               AND status NOT IN ('active_customer', 'churned', 'lost')
               ORDER BY last_contacted ASC NULLS FIRST""",
        ).fetchall()
        contacts = []
        for r in rows:
            c = dict(r)
            cadence_days = self.tuned_cadence(c.get("status")) if days is None else max(int(days), 0)
            cutoff = (date.today() - timedelta(days=cadence_days)).isoformat()
            last_contacted = c.get("last_contacted")
            if last_contacted and str(last_contacted)[:10] >= cutoff:
                continue
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
            "SELECT * FROM contacts WHERE tags LIKE ? ESCAPE '\\'", (f"%{self._escape_like(q)}%",)
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
        t = f"%{self._escape_like(q)}%"
        rows = self.conn.execute(
            """SELECT DISTINCT entity FROM facts
               WHERE entity LIKE ? ESCAPE '\\' OR key LIKE ? ESCAPE '\\'
               OR value LIKE ? ESCAPE '\\'
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
            query += " AND company LIKE ? ESCAPE '\\'"
            params.append(f"%{self._escape_like(company)}%")
        if tags:
            query += " AND tags LIKE ? ESCAPE '\\'"
            params.append(f"%{self._escape_like(tags)}%")
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
        lines.append(f"- **Stale (tuned):** {stats['stale_14d']}")

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
        """Add a tag to a contact's comma-separated tags field. No duplicates.
        Also records the tag as a fact in the knowledge graph."""
        contact = self.get_contact(identifier)
        if not contact:
            return None
        existing = contact.get("tags") or ""
        tags = [t.strip() for t in existing.split(",") if t.strip()]
        clean_tag = tag.strip()
        if clean_tag not in tags:
            tags.append(clean_tag)
        new_tags = ",".join(tags)
        self.conn.execute(
            "UPDATE contacts SET tags = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (new_tags, contact["id"])
        )
        self.conn.commit()
        # Record tag in knowledge graph so graph queries can discover it
        entity = f"contact:{contact['name'].lower()}"
        self.observe(entity, "tag", clean_tag, source="crm_tag")
        return True

    def remove_tag(self, identifier, tag):
        """Remove a tag from a contact's tags field.
        Also removes the corresponding fact from the knowledge graph."""
        contact = self.get_contact(identifier)
        if not contact:
            return None
        existing = contact.get("tags") or ""
        tags = [t.strip() for t in existing.split(",") if t.strip()]
        clean_tag = tag.strip()
        tags = [t for t in tags if t != clean_tag]
        new_tags = ",".join(tags) if tags else None
        self.conn.execute(
            "UPDATE contacts SET tags = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (new_tags, contact["id"])
        )
        # Remove the tag fact from the knowledge graph
        entity = f"contact:{contact['name'].lower()}"
        self.conn.execute(
            "DELETE FROM facts WHERE entity = ? AND key = 'tag' AND value = ? AND source = 'crm_tag'",
            (entity, clean_tag)
        )
        self.conn.commit()
        return True

    def list_by_tag(self, tag):
        """Return contacts that have this tag in their tags field."""
        rows = self.conn.execute(
            "SELECT * FROM contacts WHERE tags LIKE ? ESCAPE '\\'",
            (f"%{self._escape_like(tag)}%",)
        ).fetchall()
        # Exact tag match within comma-separated list
        results = []
        for r in rows:
            tags = [t.strip() for t in (r["tags"] or "").split(",")]
            if tag.strip() in tags:
                results.append(dict(r))
        return results

    def add_note(self, identifier, note):
        """Append a timestamped note to a contact's activity log. Notes are permanent."""
        return self.log_activity(identifier, "note", note)

    def get_notes(self, identifier):
        """Get all notes (type='note') for a contact, newest first."""
        contact = self.get_contact(identifier)
        if not contact:
            return []
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM activity WHERE contact_id = ? AND type = 'note' ORDER BY created_at DESC",
            (contact["id"],)
        ).fetchall()]

    def contacts_without_email(self):
        """Find contacts missing an email address. Useful for data cleanup."""
        rows = self.conn.execute(
            "SELECT * FROM contacts WHERE (email IS NULL OR email = '') AND archived = 0 ORDER BY name"
        ).fetchall()
        return [dict(r) for r in rows]

    def contacts_by_company(self, company):
        """List all contacts at a specific company (exact match, case-insensitive)."""
        rows = self.conn.execute(
            "SELECT * FROM contacts WHERE LOWER(company) = LOWER(?) AND archived = 0 ORDER BY name",
            (company,)
        ).fetchall()
        return [dict(r) for r in rows]

    def compare_contacts(self, id_a, id_b):
        """Side-by-side comparison of two contacts. Useful for dedup decisions."""
        a = self.get_contact(id_a)
        b = self.get_contact(id_b)
        if not a or not b:
            return None

        def _info(c):
            acts = self.get_activity(c.get("email") or c["name"], limit=50)
            sc = self.score_contact(c.get("email") or c["name"])
            deals = self.deals_for_contact(c.get("email") or c["name"])
            entity_keys = self._contact_entity_keys(c)
            all_facts = {}
            for ek in entity_keys:
                all_facts.update(self.facts_about(ek))
            return {
                "name": c["name"], "email": c.get("email"), "company": c.get("company"),
                "title": c.get("title"), "status": c.get("status"), "deal_size": c.get("deal_size"),
                "score": sc["score"] if sc else 0,
                "activity_count": len(acts),
                "deal_count": len(deals),
                "fact_count": len(all_facts),
                "last_contacted": c.get("last_contacted"),
            }

        info_a = _info(a)
        info_b = _info(b)

        # Compute overlap
        overlap = []
        if a.get("email") and b.get("email") and a["email"].lower() == b["email"].lower():
            overlap.append("same email")
        if a.get("company") and b.get("company") and a["company"].lower() == b["company"].lower():
            overlap.append("same company")
        a_name = (a.get("name") or "").lower()
        b_name = (b.get("name") or "").lower()
        if len(a_name) >= 4 and len(b_name) >= 4 and a_name[:4] == b_name[:4]:
            overlap.append("similar name")

        return {"contact_a": info_a, "contact_b": info_b, "overlap": overlap}

    def activity_streak(self, identifier):
        """Calculate current and longest activity streaks (consecutive days with activity)."""
        contact = self.get_contact(identifier)
        if not contact:
            return None

        acts = self.conn.execute(
            "SELECT DISTINCT date(created_at) as d FROM activity WHERE contact_id = ? ORDER BY d DESC",
            (contact["id"],)
        ).fetchall()
        if not acts:
            return {"current_streak": 0, "longest_streak": 0, "total_active_days": 0}

        dates = []
        for a in acts:
            try:
                dates.append(date.fromisoformat(a["d"]))
            except (ValueError, TypeError):
                pass

        if not dates:
            return {"current_streak": 0, "longest_streak": 0, "total_active_days": 0}

        dates.sort(reverse=True)
        today = date.today()

        # Current streak (from today backwards)
        current = 0
        for i, d in enumerate(dates):
            expected = today - timedelta(days=i)
            if d == expected:
                current += 1
            else:
                break

        # Longest streak
        longest = 1
        streak = 1
        for i in range(1, len(dates)):
            if (dates[i - 1] - dates[i]).days == 1:
                streak += 1
                longest = max(longest, streak)
            else:
                streak = 1

        return {"current_streak": current, "longest_streak": longest, "total_active_days": len(dates)}

    def recent_contacts(self, limit=10):
        """Contacts sorted by most recent interaction (activity or last_contacted), newest first."""
        rows = self.conn.execute(
            """SELECT c.*, MAX(a.created_at) as last_activity_at
               FROM contacts c
               LEFT JOIN activity a ON a.contact_id = c.id
               WHERE c.archived = 0
               GROUP BY c.id
               ORDER BY COALESCE(MAX(a.created_at), c.last_contacted, c.created_at) DESC, c.id DESC
               LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def top_companies(self, limit=10):
        """Companies ranked by total deal value across all contacts."""
        rows = self.conn.execute(
            """SELECT company, COUNT(*) as contact_count, GROUP_CONCAT(DISTINCT status) as statuses
               FROM contacts WHERE company IS NOT NULL AND company != '' AND archived = 0
               GROUP BY LOWER(company) ORDER BY contact_count DESC"""
        ).fetchall()
        results = []
        for r in rows:
            company = r["company"]
            contacts = self.contacts_by_company(company)
            total_value = sum(self._parse_deal_size(c.get("deal_size")) for c in contacts)
            results.append({
                "company": company,
                "contact_count": r["contact_count"],
                "total_deal_value": round(total_value, 2),
                "statuses": r["statuses"],
            })
        results.sort(key=lambda x: x["total_deal_value"], reverse=True)
        return results[:limit]

    def contact_age(self, identifier):
        """How long since a contact was added. Returns dict with days, created_at."""
        contact = self.get_contact(identifier)
        if not contact:
            return None
        try:
            created = datetime.fromisoformat(contact["created_at"])
            days = max(0, (datetime.now() - created).days)
        except (ValueError, TypeError):
            days = 0
        return {"days": days, "created_at": contact["created_at"]}

    def detect_churning(self, decay_days=14):
        """Find contacts whose engagement is actively decaying — need intervention now.

        Returns contacts with decaying velocity, sorted by deal value (biggest risk first).
        Unlike stale_contacts, this catches contacts who ARE being contacted but less
        frequently — the relationship is cooling, not dead.
        """
        contacts = self.list_contacts()
        churning = []
        for c in contacts:
            if c.get("status") in ("lost", "churned"):
                continue
            identifier = c.get("email") or c["name"]
            vel = self.velocity(identifier, window_days=decay_days)
            if not vel:
                continue
            if vel["trend"] == "decaying":
                deal_val = self._parse_deal_size(c.get("deal_size"))
                churning.append({
                    "name": c["name"],
                    "email": c.get("email"),
                    "company": c.get("company"),
                    "status": c.get("status"),
                    "deal_size": c.get("deal_size"),
                    "deal_value": deal_val,
                    "velocity": vel["velocity"],
                    "days_until_cold": vel["days_until_cold"],
                    "current_activities": vel["current_period"]["activities"],
                    "previous_activities": vel["previous_period"]["activities"],
                })
        churning.sort(key=lambda x: (-x["deal_value"], x.get("days_until_cold") or 999))
        return churning

    def untouched_contacts(self, days=30):
        """Contacts with ZERO activity (not just stale last_contacted). Truly forgotten."""
        contacts = self.list_contacts()
        untouched = []
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        for c in contacts:
            if c.get("status") in ("lost", "churned"):
                continue
            act_count = self.conn.execute(
                "SELECT COUNT(*) FROM activity WHERE contact_id = ?",
                (c["id"],)
            ).fetchone()[0]
            if act_count == 0:
                untouched.append(c)
        return untouched

    def bulk_tag(self, identifiers, tag):
        """Tag multiple contacts at once. Returns count tagged."""
        count = 0
        for identifier in identifiers:
            result = self.add_tag(identifier, tag)
            if result:
                count += 1
        return count

    def bulk_untag(self, identifiers, tag):
        """Remove a tag from multiple contacts at once. Returns count untagged."""
        count = 0
        for identifier in identifiers:
            result = self.remove_tag(identifier, tag)
            if result:
                count += 1
        return count

    def touch_plan(self, identifier, cadence_days=None):
        """Auto-generate a follow-up schedule based on contact status and deal value.

        Returns a dict with suggested next_touch date, frequency, and channel.
        """
        contact = self.get_contact(identifier)
        if not contact:
            return None

        status = contact.get("status", "prospect")
        deal_val = self._parse_deal_size(c.get("deal_size")) if (c := contact) else 0
        lc = contact.get("last_contacted")
        today = date.today()

        # Tuned status cadence owns timing; deal value only influences channel.
        freq = (max(int(cadence_days), 1) if cadence_days is not None
                else self.tuned_cadence(status))
        if status in ("verbal_yes", "proposal_drafted"):
            channel = "call"
        elif status == "active_customer":
            channel = "email"
        elif deal_val > 50000:
            channel = "call"
        else:
            channel = "email"

        # Calculate next touch
        if lc:
            try:
                last = date.fromisoformat(str(lc)[:10])
                next_touch = last + timedelta(days=freq)
                if next_touch < today:
                    next_touch = today  # overdue
            except (ValueError, TypeError):
                next_touch = today
        else:
            next_touch = today

        overdue = next_touch <= today
        days_until = max(0, (next_touch - today).days)

        return {
            "contact": contact["name"],
            "status": status,
            "frequency_days": freq,
            "channel": channel,
            "next_touch": next_touch.isoformat(),
            "overdue": overdue,
            "days_until_next": days_until,
        }

    def deal_velocity_report(self):
        """How fast deals move through stages. Average days per stage transition."""
        deals = self.conn.execute(
            "SELECT d.*, c.name as contact_name FROM deals d JOIN contacts c ON d.contact_id = c.id"
        ).fetchall()
        if not deals:
            return {"total_deals": 0, "avg_days_in_pipeline": 0, "by_stage": {}}

        stage_days = {}
        total_pipeline_days = 0
        count_with_dates = 0

        for d in deals:
            try:
                created = datetime.fromisoformat(d["created_at"])
                updated = datetime.fromisoformat(d["updated_at"])
                days = max(0, (updated - created).days)
                stage = d["stage"]
                if stage not in stage_days:
                    stage_days[stage] = []
                stage_days[stage].append(days)
                total_pipeline_days += days
                count_with_dates += 1
            except (ValueError, TypeError):
                pass

        avg_by_stage = {}
        for stage, days_list in stage_days.items():
            avg_by_stage[stage] = {
                "count": len(days_list),
                "avg_days": round(sum(days_list) / len(days_list), 1) if days_list else 0,
                "min_days": min(days_list) if days_list else 0,
                "max_days": max(days_list) if days_list else 0,
            }

        return {
            "total_deals": len(deals),
            "avg_days_in_pipeline": round(total_pipeline_days / count_with_dates, 1) if count_with_dates else 0,
            "by_stage": avg_by_stage,
        }

    @staticmethod
    def _normalize_identity(value):
        """Normalize human-entered names and companies for deterministic matching."""
        if not value:
            return ""
        return " ".join(re.sub(r"[\W_]+", " ", str(value).casefold()).split())

    @staticmethod
    def _email_domain(email):
        normalized = (email or "").strip().casefold()
        return normalized.rpartition("@")[2] if "@" in normalized else ""

    @staticmethod
    def _edit_distance(left, right):
        """Compute Levenshtein distance with a small, stdlib-only DP row."""
        if len(left) < len(right):
            left, right = right, left
        previous = list(range(len(right) + 1))
        for left_index, left_char in enumerate(left, start=1):
            current = [left_index]
            for right_index, right_char in enumerate(right, start=1):
                current.append(min(
                    current[-1] + 1,
                    previous[right_index] + 1,
                    previous[right_index - 1] + (left_char != right_char),
                ))
            previous = current
        return previous[-1]

    def suggest_merges(self):
        """Suggest deterministic duplicate pairs with confidence in ``[0, 1]``."""
        contacts = [dict(row) for row in self.conn.execute(
            "SELECT * FROM contacts WHERE archived = 0 ORDER BY id"
        ).fetchall()]
        activity_counts = {
            row["contact_id"]: row["count"]
            for row in self.conn.execute(
                """SELECT contact_id, COUNT(*) AS count FROM activity
                   GROUP BY contact_id"""
            ).fetchall()
        }

        def richness(contact):
            populated = sum(
                bool(contact.get(field))
                for field in ("email", "company", "title", "deal_size", "notes", "tags")
            )
            return populated + min(activity_counts.get(contact["id"], 0), 5)

        suggestions = []
        for index, contact_a in enumerate(contacts):
            for contact_b in contacts[index + 1:]:
                name_a = self._normalize_identity(contact_a.get("name"))
                name_b = self._normalize_identity(contact_b.get("name"))
                company_a = self._normalize_identity(contact_a.get("company"))
                company_b = self._normalize_identity(contact_b.get("company"))
                email_a = (contact_a.get("email") or "").strip().casefold()
                email_b = (contact_b.get("email") or "").strip().casefold()
                domain_a = self._email_domain(contact_a.get("email"))
                domain_b = self._email_domain(contact_b.get("email"))

                confidence = 0.0
                reasons = []
                if email_a and email_a == email_b:
                    confidence = 0.98
                    reasons.append("exact email match")
                if name_a and name_a == name_b:
                    if company_a and company_a == company_b:
                        emails_conflict = bool(
                            email_a and email_b and email_a != email_b
                        )
                        confidence = max(
                            confidence, 0.88 if emails_conflict else 0.90
                        )
                        reasons.append("same normalized name + same company")
                    else:
                        confidence = max(confidence, 0.62)
                        reasons.append("same normalized name")
                elif name_a and name_b and min(len(name_a), len(name_b)) >= 4:
                    distance = self._edit_distance(name_a, name_b)
                    relative_distance = distance / max(len(name_a), len(name_b))
                    if (distance <= 2 and relative_distance <= 0.20
                            and domain_a and domain_a == domain_b):
                        confidence = max(confidence, 0.72)
                        reasons.append("fuzzy name + shared email domain")

                if not reasons:
                    continue
                score_a = richness(contact_a)
                score_b = richness(contact_b)
                if (score_a, -contact_a["id"]) >= (score_b, -contact_b["id"]):
                    keep, drop = contact_a, contact_b
                    keep_score, drop_score = score_a, score_b
                else:
                    keep, drop = contact_b, contact_a
                    keep_score, drop_score = score_b, score_a
                keep_summary = {
                    "id": keep["id"], "name": keep["name"],
                    "email": keep.get("email"), "richness": keep_score,
                }
                drop_summary = {
                    "id": drop["id"], "name": drop["name"],
                    "email": drop.get("email"), "richness": drop_score,
                }
                suggestions.append({
                    "keep": keep_summary,
                    "merge": drop_summary,
                    "drop": dict(drop_summary),
                    "reasons": reasons,
                    "confidence": round(confidence, 2),
                })

        suggestions.sort(key=lambda item: (
            -item["confidence"], item["keep"]["id"], item["merge"]["id"]
        ))
        return suggestions

    def auto_merge(self, threshold=0.9, dry_run=True):
        """Apply every merge suggestion at or above ``threshold``.

        Dry-run is the default and returns the exact ordered suggestions without
        changing the database.
        """
        threshold = float(threshold)
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("threshold must be between 0 and 1")
        candidates = [
            suggestion for suggestion in self.suggest_merges()
            if suggestion["confidence"] >= threshold
        ]
        if dry_run:
            return {
                "dry_run": True,
                "threshold": threshold,
                "count": len(candidates),
                "merges": candidates,
            }

        applied = []
        skipped = []
        representatives = {}

        def representative(contact_id):
            path = []
            while contact_id in representatives:
                path.append(contact_id)
                contact_id = representatives[contact_id]
            for prior_id in path:
                representatives[prior_id] = contact_id
            return contact_id

        for suggestion in candidates:
            keep_id = representative(suggestion["keep"]["id"])
            drop_id = representative(suggestion["merge"]["id"])
            if keep_id == drop_id:
                continue
            keep_before = self._contact_for_merge(keep_id)
            drop_before = self._contact_for_merge(drop_id)
            result = self.merge_contacts(keep_id, drop_id)
            if result:
                representatives[drop_id] = keep_id
                applied.append({
                    "merge_id": result.get("merge_id"),
                    "keep": {
                        "id": keep_id,
                        "name": keep_before["name"],
                        "email": keep_before.get("email"),
                    },
                    "drop": {
                        "id": drop_id,
                        "name": drop_before["name"],
                        "email": drop_before.get("email"),
                    },
                    "confidence": suggestion["confidence"],
                    "reasons": suggestion["reasons"],
                })
            else:
                skipped.append(suggestion)
        return {
            "dry_run": False,
            "threshold": threshold,
            "count": len(applied),
            "merges": applied,
            "skipped": skipped,
        }

    def _doctor_scan(self):
        """Collect integrity issues without changing the database."""
        orphans = {}
        for table in ("activity", "deals", "reminders"):
            orphans[table] = [dict(row) for row in self.conn.execute(
                f"""SELECT child.* FROM {table} child
                    LEFT JOIN contacts contact ON contact.id = child.contact_id
                    WHERE contact.id IS NULL ORDER BY child.id"""
            ).fetchall()]

        unresolved_facts = []
        fact_entities = self.conn.execute(
            """SELECT DISTINCT entity FROM facts
               WHERE entity LIKE 'contact:%' ORDER BY entity"""
        ).fetchall()
        for row in fact_entities:
            if not self._resolve_entity_to_contact(row["entity"]):
                unresolved_facts.append(row["entity"])

        invalid_statuses = [dict(row) for row in self.conn.execute(
            "SELECT id, name, status FROM contacts ORDER BY id"
        ).fetchall() if row["status"] not in self.VALID_STATUSES]

        bad_emails = []
        for row in self.conn.execute(
                "SELECT id, name, email FROM contacts WHERE email IS NOT NULL ORDER BY id"
        ).fetchall():
            normalized = row["email"].strip().lower()
            if row["email"] != normalized:
                issue = dict(row)
                issue["normalized"] = normalized
                bad_emails.append(issue)

        report = {
            "duplicates": self.suggest_merges(),
            "orphans": orphans,
            "unresolved_facts": unresolved_facts,
            "invalid_statuses": invalid_statuses,
            "bad_emails": bad_emails,
        }
        report["issue_count"] = (
            len(report["duplicates"])
            + sum(len(rows) for rows in orphans.values())
            + len(unresolved_facts)
            + len(invalid_statuses)
            + len(bad_emails)
        )
        report["clean"] = report["issue_count"] == 0
        return report

    def doctor(self, fix=False):
        """Report CRM integrity issues and optionally repair the safe subset."""
        report = self._doctor_scan()
        report["fixed"] = {
            "orphan_contacts_created": [],
            "emails_normalized": [],
        }
        if not fix:
            return report

        missing_contact_ids = sorted({
            row["contact_id"]
            for rows in report["orphans"].values()
            for row in rows
        })
        fixed = report["fixed"]
        with self.conn:
            for contact_id in missing_contact_ids:
                affected = {
                    table: [row["id"] for row in rows
                            if row["contact_id"] == contact_id]
                    for table, rows in report["orphans"].items()
                }
                self.conn.execute(
                    """INSERT INTO contacts
                       (id, name, status, source, notes, archived)
                       VALUES (?, ?, 'prospect', 'doctor', ?, 1)""",
                    (contact_id, f"[Recovered contact {contact_id}]",
                     "Placeholder created to preserve orphaned CRM rows"),
                )
                self.conn.execute(
                    """INSERT INTO facts (entity, key, value, source)
                       VALUES (?, 'relinked_rows', ?, 'doctor')
                       ON CONFLICT(entity, key, value, source, event_day)
                       DO UPDATE SET observed_at = CURRENT_TIMESTAMP""",
                    (f"doctor:recovered-contact:{contact_id}",
                     json.dumps(affected, sort_keys=True)),
                )
                fixed["orphan_contacts_created"].append({
                    "contact_id": contact_id, "rows": affected,
                })

            for issue in report["bad_emails"]:
                normalized = issue["normalized"]
                if not _EMAIL_RE.match(normalized):
                    continue
                conflict = self.conn.execute(
                    """SELECT id FROM contacts
                       WHERE id != ? AND LOWER(TRIM(email)) = ? LIMIT 1""",
                    (issue["id"], normalized),
                ).fetchone()
                if conflict:
                    continue
                self.conn.execute(
                    "UPDATE contacts SET email = ? WHERE id = ?",
                    (normalized, issue["id"]),
                )
                fixed["emails_normalized"].append({
                    "contact_id": issue["id"],
                    "from": issue["email"],
                    "to": normalized,
                })

        remaining = self._doctor_scan()
        remaining["fixed"] = fixed
        return remaining

    def relationship_score(self, identifier):
        """Comprehensive relationship strength score (0-100) using ALL data sources.

        Unlike score_contact (which focuses on pipeline progression),
        this measures the actual relationship: communication frequency,
        reciprocity, meeting history, response patterns.
        """
        contact = self.get_contact(identifier)
        if not contact:
            return None

        score = 0
        factors = []

        # 1. iMessage engagement (up to 30 pts)
        entity_keys = self._contact_entity_keys(contact)
        imsg_total = 0
        imsg_sent = 0
        imsg_received = 0
        for ek in entity_keys:
            facts = self.facts_about(ek)
            if "imessage_total" in facts:
                try:
                    t = int(facts["imessage_total"]["value"])
                    if t > imsg_total:
                        imsg_total = t
                        imsg_sent = int(facts.get("imessage_sent", {}).get("value", 0))
                        imsg_received = int(facts.get("imessage_received", {}).get("value", 0))
                except (ValueError, TypeError):
                    pass
        if imsg_total > 200:
            pts = 30
        elif imsg_total > 100:
            pts = 25
        elif imsg_total > 50:
            pts = 20
        elif imsg_total > 20:
            pts = 15
        elif imsg_total > 5:
            pts = 8
        elif imsg_total > 0:
            pts = 3
        else:
            pts = 0
        score += pts
        if pts > 0:
            factors.append(f"iMessage: {imsg_total} messages (+{pts}pts)")

        # 2. Reciprocity bonus (up to 15 pts)
        if imsg_sent > 0 and imsg_received > 0:
            ratio = min(imsg_sent, imsg_received) / max(imsg_sent, imsg_received)
            recip_pts = int(ratio * 15)
            score += recip_pts
            factors.append(f"Reciprocity: {ratio:.1%} balanced (+{recip_pts}pts)")

        # 3. Activity depth (up to 20 pts)
        acts = self.get_activity(identifier, limit=50)
        act_types = set(a["type"] for a in acts)
        act_pts = min(20, len(acts) * 2 + len(act_types) * 3)
        score += act_pts
        factors.append(f"{len(acts)} activities, {len(act_types)} types (+{act_pts}pts)")

        # 4. Meeting history (up to 15 pts)
        meetings = [a for a in acts if a["type"] == "meeting"]
        meet_pts = min(15, len(meetings) * 5)
        score += meet_pts
        if meetings:
            factors.append(f"{len(meetings)} meetings (+{meet_pts}pts)")

        # 5. Recency (up to 10 pts)
        lc = contact.get("last_contacted")
        if lc:
            try:
                days_ago = (date.today() - date.fromisoformat(str(lc)[:10])).days
                if days_ago <= 3:
                    rec_pts = 10
                elif days_ago <= 7:
                    rec_pts = 8
                elif days_ago <= 14:
                    rec_pts = 5
                elif days_ago <= 30:
                    rec_pts = 2
                else:
                    rec_pts = 0
                score += rec_pts
                factors.append(f"Last contact {days_ago}d ago (+{rec_pts}pts)")
            except (ValueError, TypeError):
                pass

        # 6. Graph richness (up to 10 pts)
        total_facts = sum(len(self.facts_about(ek)) for ek in entity_keys)
        fact_pts = min(10, total_facts)
        score += fact_pts
        factors.append(f"{total_facts} graph facts (+{fact_pts}pts)")

        return {"score": min(score, 100), "factors": factors}

    def weekly_digest(self):
        """Generate a weekly digest: what happened this week, what to do next week.

        Returns a dict with new_contacts, activities_this_week, deals_progressed,
        stale_alerts, and recommended_actions for next week.
        """
        week_ago = (datetime.now() - timedelta(days=7)).isoformat()

        # New contacts this week
        new_contacts = self.conn.execute(
            "SELECT name, company, status FROM contacts WHERE created_at >= ? AND archived = 0 ORDER BY created_at DESC",
            (week_ago,)
        ).fetchall()

        # Activities this week
        activities = self.conn.execute(
            """SELECT a.type, a.summary, a.created_at, c.name
               FROM activity a JOIN contacts c ON a.contact_id = c.id
               WHERE a.created_at >= ? ORDER BY a.created_at DESC""",
            (week_ago,)
        ).fetchall()

        # Activity breakdown
        by_type = {}
        by_contact = {}
        for a in activities:
            t = a["type"]
            by_type[t] = by_type.get(t, 0) + 1
            name = a["name"]
            by_contact[name] = by_contact.get(name, 0) + 1

        # Deals that changed stage this week
        deals_progressed = self.conn.execute(
            "SELECT d.name, d.stage, d.value, c.name as contact_name FROM deals d JOIN contacts c ON d.contact_id = c.id WHERE d.updated_at >= ?",
            (week_ago,)
        ).fetchall()

        # Next week actions
        actions = self.next_actions(limit=5)

        return {
            "period": f"{(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')} to {datetime.now().strftime('%Y-%m-%d')}",
            "new_contacts": [dict(r) for r in new_contacts],
            "new_contact_count": len(new_contacts),
            "activities_this_week": len(activities),
            "activity_breakdown": by_type,
            "most_active_contacts": sorted(by_contact.items(), key=lambda x: x[1], reverse=True)[:5],
            "deals_progressed": [dict(r) for r in deals_progressed],
            "next_week_actions": actions,
        }

    def source_attribution(self):
        """Which lead sources produce the best results? Analyze conversion by source."""
        all_contacts = self.list_contacts(include_archived=True)
        by_source = {}
        for c in all_contacts:
            src = c.get("source") or "unknown"
            if src not in by_source:
                by_source[src] = {"total": 0, "converted": 0, "total_deal_value": 0, "contacts": []}
            by_source[src]["total"] += 1
            if c.get("status") in ("active_customer", "verbal_yes"):
                by_source[src]["converted"] += 1
            by_source[src]["total_deal_value"] += self._parse_deal_size(c.get("deal_size"))
            by_source[src]["contacts"].append(c["name"])

        results = []
        for src, data in by_source.items():
            conv_rate = round(data["converted"] / data["total"] * 100, 1) if data["total"] else 0
            results.append({
                "source": src,
                "total_contacts": data["total"],
                "converted": data["converted"],
                "conversion_rate": conv_rate,
                "total_deal_value": round(data["total_deal_value"], 2),
            })
        results.sort(key=lambda x: x["conversion_rate"], reverse=True)
        return results

    def lifecycle_stages(self, identifier):
        """Track a contact's journey through status changes via activity log.

        Returns a list of stage transitions with timestamps.
        """
        contact = self.get_contact(identifier)
        if not contact:
            return None

        # Build timeline from activity + current status
        stages = [{"stage": "created", "timestamp": contact["created_at"]}]

        # Look for status-related activity entries and facts
        entity_keys = self._contact_entity_keys(contact)
        for ek in entity_keys:
            history = self.history_of(ek, key="status")
            for h in history:
                stages.append({"stage": h["value"], "timestamp": h["observed_at"]})

        # Current status
        stages.append({"stage": contact.get("status", "prospect"), "timestamp": contact["updated_at"]})

        # Deduplicate and sort
        seen = set()
        unique = []
        for s in sorted(stages, key=lambda x: x["timestamp"]):
            key = f"{s['stage']}_{s['timestamp'][:10]}"
            if key not in seen:
                seen.add(key)
                unique.append(s)
        return unique

    def outreach_effectiveness(self):
        """Which activity types correlate with status advancement?

        Looks at contacts who advanced status and what activities preceded the advance.
        Returns activity types ranked by correlation with advancement.
        """
        # Get all contacts with some activity
        contacts = self.list_contacts(include_archived=True)
        advanced_activity_types = {}
        all_activity_types = {}

        for c in contacts:
            acts = self.get_activity(c.get("email") or c["name"], limit=50)
            for a in acts:
                t = a["type"]
                all_activity_types[t] = all_activity_types.get(t, 0) + 1

            # Check if contact advanced past prospect
            if c.get("status") in ("contacted", "met", "proposal_drafted", "verbal_yes", "active_customer"):
                for a in acts:
                    t = a["type"]
                    advanced_activity_types[t] = advanced_activity_types.get(t, 0) + 1

        results = []
        for t, total in all_activity_types.items():
            advanced = advanced_activity_types.get(t, 0)
            rate = round(advanced / total * 100, 1) if total > 0 else 0
            results.append({
                "activity_type": t,
                "total_uses": total,
                "on_advanced_contacts": advanced,
                "effectiveness_rate": rate,
            })
        results.sort(key=lambda x: x["effectiveness_rate"], reverse=True)
        return results

    def contact_changelog(self, identifier, limit=20):
        """Full audit trail for a contact: all changes, activities, facts — chronological."""
        contact = self.get_contact(identifier)
        if not contact:
            return None

        changelog = []

        # Creation event
        changelog.append({
            "event": "created",
            "detail": f"Contact created: {contact['name']}",
            "timestamp": contact["created_at"],
        })

        # All activities
        acts = self.get_activity(identifier, limit=limit * 2)
        for a in acts:
            changelog.append({
                "event": "activity",
                "detail": f"{a['type']}: {a['summary']}",
                "timestamp": a["created_at"],
            })

        # All facts
        entity_keys = self._contact_entity_keys(contact)
        for ek in entity_keys:
            history = self.history_of(ek)
            for h in history:
                changelog.append({
                    "event": "fact",
                    "detail": f"{h['key']} = {h['value']} (via {h['source']})",
                    "timestamp": h["observed_at"],
                })

        # Sort chronologically and limit
        changelog.sort(key=lambda x: x.get("timestamp", ""))
        return changelog[:limit]

    def pipeline_forecast_detail(self):
        """Detailed pipeline forecast with expected close dates and risk assessment.

        Combines deal velocity data with pipeline position to estimate when
        each deal might close and flag risks.
        """
        contacts = self.list_contacts()
        forecast = []

        for c in contacts:
            if c.get("status") in ("lost", "churned", "active_customer"):
                continue
            deal_val = self._parse_deal_size(c.get("deal_size"))
            if deal_val == 0:
                continue

            identifier = c.get("email") or c["name"]
            vel = self.velocity(identifier)
            status = c.get("status", "prospect")
            prob = self.STAGE_PROBABILITIES.get(status, 0.1)

            # Risk assessment
            risk = "low"
            if vel and vel["trend"] == "decaying":
                risk = "high"
            elif vel and vel["trend"] == "dead":
                risk = "critical"
            elif vel and vel["trend"] == "stable":
                risk = "medium"

            # Expected value
            expected = round(deal_val * prob, 2)

            forecast.append({
                "name": c["name"],
                "company": c.get("company"),
                "deal_size": c.get("deal_size"),
                "deal_value": deal_val,
                "stage": status,
                "probability": prob,
                "expected_value": expected,
                "risk": risk,
                "trend": vel["trend"] if vel else "unknown",
            })

        forecast.sort(key=lambda x: x["expected_value"], reverse=True)
        return forecast

    def search_activity(self, term, limit=50):
        """Search across all activity summaries. Returns matching activities with contact info."""
        t = f"%{self._escape_like(term)}%"
        rows = self.conn.execute(
            """SELECT a.id, a.type, a.summary, a.created_at, c.name, c.email, c.company
               FROM activity a JOIN contacts c ON a.contact_id = c.id
               WHERE a.summary LIKE ? ESCAPE '\\'
               ORDER BY a.created_at DESC LIMIT ?""",
            (t, limit)
        ).fetchall()
        return [dict(r) for r in rows]

    def contacts_count(self):
        """Quick count of contacts by status. Lighter than full stats()."""
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as n FROM contacts WHERE archived = 0 GROUP BY status"
        ).fetchall()
        result = {r["status"]: r["n"] for r in rows}
        result["total"] = sum(result.values())
        return result

    def deals_summary(self):
        """Quick summary of all deals: total count, value, by stage."""
        deals = self.conn.execute(
            "SELECT stage, COUNT(*) as n, GROUP_CONCAT(name, ', ') as names FROM deals GROUP BY stage ORDER BY n DESC"
        ).fetchall()
        total_value = 0
        stages = []
        for d in deals:
            stage_deals = self.conn.execute(
                "SELECT value FROM deals WHERE stage = ?", (d["stage"],)
            ).fetchall()
            val = sum(self._parse_deal_size(r["value"]) for r in stage_deals)
            total_value += val
            stages.append({
                "stage": d["stage"],
                "count": d["n"],
                "value": round(val, 2),
                "deals": d["names"],
            })
        return {"total_deals": sum(d["n"] for d in deals), "total_value": round(total_value, 2), "by_stage": stages}

    def period_comparison(self, days=30):
        """Compare this period vs previous period: new contacts, activities, deals, revenue.

        Returns side-by-side metrics for quick trend spotting.
        """
        now = datetime.now()
        current_start = (now - timedelta(days=days)).isoformat()
        prev_start = (now - timedelta(days=days * 2)).isoformat()

        # Current period
        current_contacts = self.conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE created_at >= ? AND archived = 0", (current_start,)
        ).fetchone()[0]
        current_activities = self.conn.execute(
            "SELECT COUNT(*) FROM activity WHERE created_at >= ?", (current_start,)
        ).fetchone()[0]
        current_deals = self.conn.execute(
            "SELECT COUNT(*) FROM deals WHERE created_at >= ?", (current_start,)
        ).fetchone()[0]

        # Previous period
        prev_contacts = self.conn.execute(
            "SELECT COUNT(*) FROM contacts WHERE created_at >= ? AND created_at < ? AND archived = 0",
            (prev_start, current_start)
        ).fetchone()[0]
        prev_activities = self.conn.execute(
            "SELECT COUNT(*) FROM activity WHERE created_at >= ? AND created_at < ?",
            (prev_start, current_start)
        ).fetchone()[0]
        prev_deals = self.conn.execute(
            "SELECT COUNT(*) FROM deals WHERE created_at >= ? AND created_at < ?",
            (prev_start, current_start)
        ).fetchone()[0]

        def _change(current, prev):
            if prev == 0:
                return "+∞" if current > 0 else "0%"
            pct = round((current - prev) / prev * 100, 1)
            return f"+{pct}%" if pct > 0 else f"{pct}%"

        return {
            "period_days": days,
            "current_period": {
                "new_contacts": current_contacts,
                "activities": current_activities,
                "new_deals": current_deals,
            },
            "previous_period": {
                "new_contacts": prev_contacts,
                "activities": prev_activities,
                "new_deals": prev_deals,
            },
            "changes": {
                "contacts": _change(current_contacts, prev_contacts),
                "activities": _change(current_activities, prev_activities),
                "deals": _change(current_deals, prev_deals),
            },
        }

    def stale_deals(self, days=30):
        """Deals that haven't been updated in N days — need attention."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        rows = self.conn.execute(
            """SELECT d.*, c.name as contact_name, c.email, c.company
               FROM deals d JOIN contacts c ON d.contact_id = c.id
               WHERE d.stage NOT IN ('closed_won', 'closed_lost')
               AND d.updated_at < ?
               ORDER BY d.updated_at ASC""",
            (cutoff,)
        ).fetchall()
        return [dict(r) for r in rows]

    def contacts_needing_email(self):
        """Contacts with deals but no email — high-value data cleanup targets."""
        rows = self.conn.execute(
            """SELECT DISTINCT c.* FROM contacts c
               JOIN deals d ON c.id = d.contact_id
               WHERE (c.email IS NULL OR c.email = '') AND c.archived = 0
               ORDER BY c.name"""
        ).fetchall()
        return [dict(r) for r in rows]

    def activity_heatmap(self, days=90):
        """Activity counts by day of week and type. Shows when you're most active."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        rows = self.conn.execute(
            """SELECT strftime('%w', created_at) as dow, type, COUNT(*) as n
               FROM activity WHERE created_at >= ?
               GROUP BY dow, type ORDER BY dow, type""",
            (cutoff,)
        ).fetchall()
        day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        heatmap = {d: {} for d in day_names}
        for r in rows:
            try:
                day = day_names[int(r["dow"])]
                heatmap[day][r["type"]] = r["n"]
            except (ValueError, IndexError):
                pass
        # Add totals
        for day in day_names:
            heatmap[day]["_total"] = sum(heatmap[day].values())
        return heatmap

    def quick_add(self, text):
        """Parse a natural-language-ish string into a contact.

        Supports formats like:
        - "Alice Smith alice@co.com Acme"
        - "alice@co.com"
        - "Bob Jones, CEO at TechCo, bob@tech.com"

        Returns the new contact id or None.
        """
        text = text.strip()
        if not text:
            return None

        # Extract email if present
        email_match = re.search(r'[\w.%+-]+@[\w.-]+\.\w{2,}', text)
        email = email_match.group(0) if email_match else None

        # Remove email from text for name parsing
        remaining = text.replace(email, '').strip() if email else text
        remaining = remaining.strip(',').strip()

        # Try to parse "Name, Title at Company" or "Name at Company"
        company = None
        title = None
        at_match = re.search(r',?\s*(?:(\w[\w\s]*?)\s+)?(?:at|@)\s+(.+?)(?:,|$)', remaining, re.IGNORECASE)
        if at_match:
            title = at_match.group(1)
            company = at_match.group(2).strip().rstrip(',')
            remaining = remaining[:at_match.start()].strip().rstrip(',')

        name = remaining.strip() if remaining.strip() else (email.split('@')[0].replace('.', ' ').title() if email else None)
        if not name:
            return None

        try:
            self._validate_email(email)
            return self.add_contact(name=name, email=email, company=company, title=title)
        except (sqlite3.IntegrityError, ValueError):
            return None

    def find_by_domain(self, domain):
        """Find all contacts with email at a specific domain. For company prospecting."""
        domain = domain.lower().strip()
        if not domain:
            return []
        rows = self.conn.execute(
            "SELECT * FROM contacts WHERE LOWER(email) LIKE ? AND archived = 0 ORDER BY name",
            (f"%@{domain}",)
        ).fetchall()
        return [dict(r) for r in rows]

    def interaction_frequency(self, identifier, days=90):
        """How often you interact with a contact: weekly average, busiest week, gaps."""
        contact = self.get_contact(identifier)
        if not contact:
            return None

        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        rows = self.conn.execute(
            "SELECT date(created_at) as d FROM activity WHERE contact_id = ? AND created_at >= ? ORDER BY d",
            (contact["id"], cutoff)
        ).fetchall()

        if not rows:
            return {"weekly_avg": 0, "total_interactions": 0, "active_weeks": 0, "longest_gap_days": days}

        dates = [date.fromisoformat(r["d"]) for r in rows]
        total = len(dates)

        # Group by week
        weeks = {}
        for d in dates:
            week_key = d.isocalendar()[:2]
            weeks[week_key] = weeks.get(week_key, 0) + 1

        active_weeks = len(weeks)
        total_weeks = max(1, days // 7)
        weekly_avg = round(total / total_weeks, 1)
        busiest_week = max(weeks.values()) if weeks else 0

        # Longest gap
        longest_gap = 0
        for i in range(1, len(dates)):
            gap = (dates[i] - dates[i - 1]).days
            longest_gap = max(longest_gap, gap)

        return {
            "total_interactions": total,
            "weekly_avg": weekly_avg,
            "active_weeks": active_weeks,
            "total_weeks": total_weeks,
            "busiest_week_count": busiest_week,
            "longest_gap_days": longest_gap,
        }

    def contact_360(self, identifier):
        """Complete 360-degree view of a contact — everything in one call.

        Combines enrich, timeline, score, relationship_score, velocity,
        reminders, custom fields, and deals into a single response.
        """
        contact = self.get_contact(identifier)
        if not contact:
            return None

        # Core profile
        profile = self.enrich(identifier)
        rel_score = self.relationship_score(identifier)
        vel = self.velocity(identifier)
        streak = self.activity_streak(identifier)
        reminders = self.reminders_for_contact(identifier)
        custom = self.get_fields(identifier)
        freq = self.interaction_frequency(identifier)

        return {
            "profile": profile,
            "relationship_score": rel_score["score"] if rel_score else 0,
            "velocity": vel,
            "streak": streak,
            "reminders": reminders,
            "custom_fields": custom,
            "interaction_frequency": freq,
        }

    def pipeline_health_score(self):
        """Single 0-100 score for overall pipeline health.

        Factors: pipeline coverage ratio (pipeline vs revenue target),
        deal progression rate, stale deal percentage, average velocity.
        """
        contacts = self.list_contacts()
        if not contacts:
            return {"score": 0, "factors": ["No contacts"]}

        score = 0
        factors = []
        total = len(contacts)

        # 1. Pipeline diversity (up to 25 pts) — how many stages have contacts
        statuses = set(c.get("status") for c in contacts)
        stage_count = len(statuses - {"lost", "churned"})
        div_pts = min(25, stage_count * 5)
        score += div_pts
        factors.append(f"{stage_count} active stages (+{div_pts}pts)")

        # 2. Activity rate (up to 25 pts) — % of contacts with recent activity
        stats = self.stats()
        if total > 0:
            active_pct = stats["contacted_last_7d"] / total
            act_pts = min(25, int(active_pct * 50))
            score += act_pts
            factors.append(f"{stats['contacted_last_7d']}/{total} active last 7d (+{act_pts}pts)")

        # 3. Low stale rate (up to 25 pts) — fewer stale contacts = healthier
        if total > 0:
            stale_pct = stats["stale_14d"] / total
            stale_pts = max(0, 25 - int(stale_pct * 50))
            score += stale_pts
            factors.append(f"{stats['stale_14d']} stale (+{stale_pts}pts)")

        # 4. Deal coverage (up to 25 pts) — contacts with deals
        contacts_with_deals = self.conn.execute(
            "SELECT COUNT(DISTINCT contact_id) FROM deals WHERE stage NOT IN ('closed_won', 'closed_lost')"
        ).fetchone()[0]
        if total > 0:
            deal_pct = contacts_with_deals / total
            deal_pts = min(25, int(deal_pct * 50))
            score += deal_pts
            factors.append(f"{contacts_with_deals} contacts with active deals (+{deal_pts}pts)")

        return {"score": min(score, 100), "factors": factors}

    # --- Self-Improvement Engine ---

    @staticmethod
    def _experiment_slug(name):
        """Convert an experiment name into a stable, URL-friendly slug."""
        slug = re.sub(r"[^a-z0-9]+", "-", str(name).strip().lower()).strip("-")
        if not slug:
            raise ValueError("experiment name must contain at least one letter or number")
        return slug

    def log_experiment(self, name, hypothesis, metric, baseline_value):
        """Start an experiment and persist its definition in the facts table."""
        if name is None or not str(name).strip():
            raise ValueError("experiment name must be non-empty")

        base_slug = self._experiment_slug(name)
        slug = base_slug
        suffix = 2
        while self.conn.execute(
            "SELECT 1 FROM facts WHERE entity = ? AND source = 'evolve' LIMIT 1",
            (f"experiment:{slug}",),
        ).fetchone():
            slug = f"{base_slug}-{suffix}"
            suffix += 1

        entity = f"experiment:{slug}"
        self.observe_many([
            (entity, "name", str(name).strip()),
            (entity, "hypothesis", hypothesis),
            (entity, "metric", metric),
            (entity, "baseline_value", baseline_value),
            (entity, "status", "running"),
            (entity, "started_at", date.today().isoformat()),
        ], source="evolve")
        return slug

    def conclude_experiment(self, slug_or_name, result_value, verdict=None):
        """Complete an experiment, deriving its verdict when values are numeric.

        Returns the completed experiment record, or ``None`` when no matching
        experiment exists. Non-numeric comparisons require an explicit verdict.
        """
        identifier = str(slug_or_name).strip()
        if not identifier:
            return None

        entity = None
        if identifier.startswith("experiment:"):
            candidate = identifier
            if self.conn.execute(
                "SELECT 1 FROM facts WHERE entity = ? AND source = 'evolve' LIMIT 1",
                (candidate,),
            ).fetchone():
                entity = candidate
        else:
            # A human-facing name may map to multiple runs; conclude the newest.
            row = self.conn.execute(
                """SELECT entity, MAX(rowid) AS latest_rowid FROM facts
                   WHERE entity LIKE 'experiment:%' AND source = 'evolve'
                     AND key = 'name' AND LOWER(value) = LOWER(?)
                   GROUP BY entity ORDER BY latest_rowid DESC LIMIT 1""",
                (identifier,),
            ).fetchone()
            if row:
                entity = row["entity"]
            else:
                candidates = [identifier]
                try:
                    candidates.append(self._experiment_slug(identifier))
                except ValueError:
                    pass
                for candidate_slug in candidates:
                    candidate = f"experiment:{candidate_slug}"
                    if self.conn.execute(
                        "SELECT 1 FROM facts WHERE entity = ? AND source = 'evolve' LIMIT 1",
                        (candidate,),
                    ).fetchone():
                        entity = candidate
                        break

        if entity is None:
            return None

        normalized_verdict = None
        if verdict is not None:
            normalized_verdict = str(verdict).strip().lower()
            if normalized_verdict not in {"win", "loss", "neutral"}:
                raise ValueError("verdict must be one of: win, loss, neutral")
        else:
            baseline = self.facts_about(entity).get("baseline_value", {}).get("value")
            try:
                baseline_number = float(str(baseline).strip())
                result_number = float(str(result_value).strip())
            except (TypeError, ValueError):
                raise ValueError(
                    "explicit verdict required when baseline_value and result_value are not numeric"
                ) from None
            if baseline_number != baseline_number or result_number != result_number:
                raise ValueError(
                    "explicit verdict required when baseline_value and result_value are not numeric"
                )
            if result_number > baseline_number:
                normalized_verdict = "win"
            elif result_number < baseline_number:
                normalized_verdict = "loss"
            else:
                normalized_verdict = "neutral"

        self.observe_many([
            (entity, "result_value", result_value),
            (entity, "verdict", normalized_verdict),
            (entity, "status", "completed"),
            (entity, "concluded_at", date.today().isoformat()),
        ], source="evolve")

        slug = entity.split(":", 1)[1]
        return next(
            (experiment for experiment in self.experiment_history()
             if experiment["slug"] == slug),
            None,
        )

    def experiment_history(self, status=None):
        """List experiments with their definitions, outcomes, and dates.

        All rows are loaded in one query and reduced deterministically to the
        latest value for each fact key.
        """
        rows = self.conn.execute(
            """SELECT rowid, entity, key, value, observed_at FROM facts
               WHERE entity LIKE 'experiment:%' AND source = 'evolve'
               ORDER BY observed_at DESC, rowid DESC"""
        ).fetchall()

        grouped = {}
        for row in rows:
            entity = row["entity"]
            group = grouped.setdefault(entity, {
                "facts": {},
                "first_seen": row["observed_at"],
                "last_seen": row["observed_at"],
                "latest_rowid": row["rowid"],
            })
            group["first_seen"] = min(group["first_seen"], row["observed_at"])
            group["last_seen"] = max(group["last_seen"], row["observed_at"])
            group["latest_rowid"] = max(group["latest_rowid"], row["rowid"])
            if row["key"] not in group["facts"]:
                group["facts"][row["key"]] = row["value"]

        experiments = []
        wanted_status = str(status).strip().lower() if status is not None else None
        for entity, group in grouped.items():
            facts = group["facts"]
            current_status = facts.get("status", "unknown")
            if wanted_status and str(current_status).lower() != wanted_status:
                continue

            baseline = facts.get("baseline_value", facts.get("baseline"))
            result_value = facts.get("result_value", facts.get("result"))
            experiment = {
                "id": entity,
                "slug": entity.split(":", 1)[1],
                "name": facts.get("name") or entity.split(":", 1)[1],
                "hypothesis": facts.get("hypothesis") or facts.get("proposal"),
                "metric": facts.get("metric") or facts.get("measure"),
                "baseline_value": baseline,
                "baseline": baseline,
                "result_value": result_value,
                "result": result_value,
                "verdict": facts.get("verdict"),
                "status": current_status,
                "started_at": facts.get("started_at") or group["first_seen"],
                "concluded_at": facts.get("concluded_at"),
                # Legacy evolve() fields remain available to existing callers.
                "bottleneck": facts.get("bottleneck"),
                "proposal": facts.get("proposal") or facts.get("hypothesis"),
                "measure": facts.get("measure") or facts.get("metric"),
                "conclusion": facts.get("conclusion"),
                "_latest_rowid": group["latest_rowid"],
            }
            experiments.append(experiment)

        experiments.sort(key=lambda experiment: (-experiment["_latest_rowid"], experiment["id"]))
        for experiment in experiments:
            del experiment["_latest_rowid"]
        return experiments

    def evolve(self):
        """Run one self-improvement cycle. Analyzes CRM data, identifies the
        biggest opportunity, and proposes a specific experiment.

        Returns dict with analysis, proposal, and tracking info.
        Stores the experiment in the facts table for persistence.
        """
        history = self.experiment_history()
        wins = [experiment for experiment in history if experiment["verdict"] == "win"]
        losses = [experiment for experiment in history if experiment["verdict"] == "loss"]
        running = [experiment for experiment in history
                   if experiment["status"] in {"running", "active", "proposed"}]
        decided = len(wins) + len(losses)
        experiment_track_record = {
            "wins": wins,
            "losses": losses,
            "running": running,
            "win_rate": round(len(wins) / decided * 100, 1) if decided else 0.0,
        }

        stats = self.stats()
        pipeline = self.pipeline()
        wl = self.win_loss_analysis()
        hc = self.health_check()
        eff = self.outreach_effectiveness()
        attr = self.source_attribution()
        stale = self.stale_contacts()
        cadence = {
            row["status"]: {
                "tuned_days": row["interval_days"],
                "default_days": row["default_days"],
                "source": row["source"],
            }
            for row in self._tuned_cadence_rows()
        }

        # Identify the biggest bottleneck
        bottleneck = None
        proposal = None

        # Check 1: Too many stale contacts
        stale_pct = stats["stale_14d"] / max(stats["total_contacts"], 1) * 100
        if stale_pct > 30:
            bottleneck = "stale_contacts"
            proposal = {
                "experiment": "re-engagement-blast",
                "change": f"{len(stale)} contacts stale (>{stale_pct:.0f}%). Re-engage top 10 by deal value with personalized check-ins.",
                "measure": "Reply rate > 20% within 7 days",
                "batch_size": min(10, len(stale)),
                "baseline_value": 0,
            }

        # Check 2: Low win rate
        if not bottleneck and wl["win_rate"] < 30 and (len(wl["wins"]) + len(wl["losses"])) >= 3:
            bottleneck = "low_win_rate"
            top_source = wl["top_source"] or "unknown"
            proposal = {
                "experiment": "source-focus",
                "change": f"Win rate is {wl['win_rate']:.0f}%. Focus outreach on '{top_source}' source which produces most wins.",
                "measure": "Win rate > 40% over next 10 deals",
                "batch_size": 10,
                "baseline_value": wl["win_rate"],
            }

        # Check 3: No activity diversity
        if not bottleneck and eff:
            types_used = len(eff)
            if types_used < 3:
                bottleneck = "low_activity_diversity"
                missing = [t for t in ("call", "meeting", "email") if t not in [e["activity_type"] for e in eff]]
                proposal = {
                    "experiment": "channel-mix",
                    "change": f"Only {types_used} activity types used. Add {', '.join(missing)} to the mix.",
                    "measure": "Contacts with 3+ activity types advance 50% faster",
                    "batch_size": 5,
                    "baseline_value": max(
                        (activity["effectiveness_rate"] for activity in eff), default=0
                    ),
                }

        # Check 4: Pipeline imbalance (too many prospects, too few advanced)
        if not bottleneck:
            status_counts = {p["status"]: p["count"] for p in pipeline}
            prospect_count = status_counts.get("prospect", 0)
            advanced = sum(status_counts.get(s, 0) for s in ("met", "proposal_drafted", "verbal_yes"))
            if prospect_count > 10 and advanced == 0:
                bottleneck = "pipeline_stuck"
                proposal = {
                    "experiment": "pipeline-push",
                    "change": f"{prospect_count} prospects but 0 advanced. Focus on converting top 5 prospects to 'contacted' this week.",
                    "measure": "5 prospects advance to contacted within 7 days",
                    "batch_size": 5,
                    "baseline_value": advanced,
                }

        # Default: pipeline is healthy
        if not bottleneck:
            bottleneck = "none"
            proposal = {
                "experiment": "maintain",
                "change": "Pipeline looks healthy. Focus on closing existing advanced deals.",
                "measure": "Close 1 deal this week",
                "batch_size": 0,
                "baseline_value": 0,
            }

        # Store the new running experiment after reading the prior track record.
        slug = self.log_experiment(
            f"{date.today().isoformat()} {proposal['experiment']}",
            proposal["change"],
            proposal["measure"],
            proposal["baseline_value"],
        )
        experiment_id = f"experiment:{slug}"
        self.observe_many([
            (experiment_id, "bottleneck", bottleneck),
            (experiment_id, "proposal", proposal["change"]),
            (experiment_id, "measure", proposal["measure"]),
            (experiment_id, "batch_size", proposal["batch_size"]),
        ], source="evolve")

        return {
            "bottleneck": bottleneck,
            "analysis": {
                "total_contacts": stats["total_contacts"],
                "stale_pct": round(stale_pct, 1),
                "win_rate": wl["win_rate"],
                "activity_types": len(eff),
                "health": {
                    "healthy": len(hc["healthy"]),
                    "at_risk": len(hc["at_risk"]),
                    "cold": len(hc["cold"]),
                },
                "experiment_track_record": experiment_track_record,
            },
            "proposal": proposal,
            "experiment_id": experiment_id,
            "cadence": cadence,
        }

    def experiments(self, status=None):
        """Backward-compatible alias for :meth:`experiment_history`."""
        return self.experiment_history(status=status)

    def win_patterns(self):
        """Analyze what closed deals have in common: sources, activity types,
        time to close, activity count. Returns patterns that correlate with winning."""
        wins = self.conn.execute(
            "SELECT * FROM contacts WHERE status = 'active_customer'"
        ).fetchall()
        if not wins:
            return {"patterns": [], "win_count": 0}

        sources = {}
        activity_types = {}
        avg_activities = 0
        total_activities = 0
        companies = {}

        for w in wins:
            w = dict(w)
            src = w.get("source") or "unknown"
            sources[src] = sources.get(src, 0) + 1
            comp = w.get("company") or "unknown"
            companies[comp] = companies.get(comp, 0) + 1
            acts = self.get_activity(w.get("email") or w["name"], limit=100)
            total_activities += len(acts)
            for a in acts:
                t = a["type"]
                activity_types[t] = activity_types.get(t, 0) + 1

        win_count = len(wins)
        avg_activities = round(total_activities / win_count, 1) if win_count else 0

        patterns = []
        # Top source
        if sources:
            top_src = max(sources, key=sources.get)
            patterns.append(f"Top source: {top_src} ({sources[top_src]}/{win_count} wins)")
        # Activity mix
        if activity_types:
            top_type = max(activity_types, key=activity_types.get)
            patterns.append(f"Most common activity: {top_type} ({activity_types[top_type]} total)")
        patterns.append(f"Avg activities before close: {avg_activities}")

        return {
            "win_count": win_count,
            "avg_activities_to_close": avg_activities,
            "sources": sources,
            "activity_types": activity_types,
            "patterns": patterns,
        }

    def loss_patterns(self):
        """Analyze what lost deals have in common."""
        losses = self.conn.execute(
            "SELECT * FROM contacts WHERE status IN ('lost', 'churned')"
        ).fetchall()
        if not losses:
            return {"patterns": [], "loss_count": 0}

        sources = {}
        avg_activities = 0
        total_activities = 0

        for l in losses:
            l = dict(l)
            src = l.get("source") or "unknown"
            sources[src] = sources.get(src, 0) + 1
            acts = self.get_activity(l.get("email") or l["name"], limit=100)
            total_activities += len(acts)

        loss_count = len(losses)
        avg_activities = round(total_activities / loss_count, 1) if loss_count else 0

        patterns = []
        if sources:
            top_src = max(sources, key=sources.get)
            patterns.append(f"Top loss source: {top_src} ({sources[top_src]}/{loss_count} losses)")
        patterns.append(f"Avg activities before loss: {avg_activities}")
        # Compare to wins
        wl = self.win_loss_analysis()
        if wl["avg_days_to_close"] > 0:
            patterns.append(f"Avg days to close (wins): {wl['avg_days_to_close']}")

        return {
            "loss_count": loss_count,
            "avg_activities_before_loss": avg_activities,
            "sources": sources,
            "patterns": patterns,
        }

    def dead_pipeline(self, stale_days=60):
        """Contacts and deals that should probably be marked as lost.

        Finds pipeline contacts with no activity in stale_days AND decaying/dead velocity.
        """
        contacts = self.list_contacts()
        dead = []
        cutoff = (date.today() - timedelta(days=stale_days)).isoformat()

        for c in contacts:
            if c.get("status") in ("active_customer", "lost", "churned"):
                continue
            lc = c.get("last_contacted")
            if lc and lc >= cutoff:
                continue
            # Check velocity
            identifier = c.get("email") or c["name"]
            vel = self.velocity(identifier)
            if vel and vel["trend"] in ("dead", "decaying"):
                dead.append({
                    "name": c["name"],
                    "email": c.get("email"),
                    "company": c.get("company"),
                    "status": c.get("status"),
                    "deal_size": c.get("deal_size"),
                    "last_contacted": lc,
                    "trend": vel["trend"],
                    "recommendation": "Mark as lost" if vel["trend"] == "dead" else "Last chance re-engage",
                })

        dead.sort(key=lambda x: self._parse_deal_size(x.get("deal_size")), reverse=True)
        return dead

    def optimal_cadence(self):
        """Data-driven follow-up timing from actual conversion data.

        Analyzes time gaps between activities for won vs lost deals
        to find what cadence correlates with winning.
        """
        won_gaps = []
        lost_gaps = []

        for status, gap_list in [("active_customer", won_gaps), ("lost", lost_gaps)]:
            contacts = self.conn.execute(
                "SELECT * FROM contacts WHERE status = ?", (status,)
            ).fetchall()
            for c in contacts:
                acts = self.conn.execute(
                    "SELECT created_at FROM activity WHERE contact_id = ? ORDER BY created_at",
                    (c["id"],)
                ).fetchall()
                for i in range(1, len(acts)):
                    try:
                        t1 = datetime.fromisoformat(acts[i-1]["created_at"])
                        t2 = datetime.fromisoformat(acts[i]["created_at"])
                        gap = (t2 - t1).days
                        if gap > 0:
                            gap_list.append(gap)
                    except (ValueError, TypeError):
                        pass

        result = {
            "won_deals": {
                "sample_size": len(won_gaps),
                "avg_gap_days": round(sum(won_gaps) / len(won_gaps), 1) if won_gaps else 0,
                "median_gap_days": sorted(won_gaps)[len(won_gaps)//2] if won_gaps else 0,
            },
            "lost_deals": {
                "sample_size": len(lost_gaps),
                "avg_gap_days": round(sum(lost_gaps) / len(lost_gaps), 1) if lost_gaps else 0,
                "median_gap_days": sorted(lost_gaps)[len(lost_gaps)//2] if lost_gaps else 0,
            },
            "recommendation": "",
        }

        if won_gaps:
            avg = result["won_deals"]["avg_gap_days"]
            result["recommendation"] = f"Follow up every {max(1, int(avg))} days — that's the winning cadence."
        else:
            result["recommendation"] = "Not enough data yet. Close more deals to calibrate."

        return result

    def _tuned_cadence_snapshot(self):
        """Return cached cadence values and their calibration metadata."""
        activity_count = self.conn.execute("SELECT COUNT(*) FROM activity").fetchone()[0]
        if (self._tuned_cadence_cache is not None
                and self._tuned_cadence_activity_count == activity_count):
            return self._tuned_cadence_cache

        winning = self.optimal_cadence().get("won_deals", {})
        sample_size = winning.get("sample_size") or 0
        winning_gap = winning.get("avg_gap_days") or winning.get("median_gap_days") or 0
        try:
            winning_gap = float(winning_gap)
        except (TypeError, ValueError):
            winning_gap = 0.0
        learned = bool(sample_size and winning_gap > 0)

        intervals = {}
        for status, default_days in self.CADENCE_DEFAULTS.items():
            if learned:
                scaled_days = winning_gap * default_days / self.CADENCE_BASE_DAYS
                intervals[status] = max(1, int(scaled_days + 0.5))
            else:
                intervals[status] = default_days

        self._tuned_cadence_cache = {
            "intervals": intervals,
            "source": "learned" if learned else "default",
            "winning_gap_days": winning_gap if learned else None,
        }
        self._tuned_cadence_activity_count = activity_count
        return self._tuned_cadence_cache

    def tuned_cadence(self, status=None):
        """Return tuned follow-up interval days for one status or every status.

        Winning activity gaps calibrate the contacted-stage base. Stage-shaped
        static defaults supply both the relative tightening/loosening and the
        fallback when no winning gaps have been observed yet.
        """
        cadence_snapshot = self._tuned_cadence_snapshot()
        intervals = cadence_snapshot["intervals"]
        if status is None:
            return dict(intervals)
        status_key = str(status).strip().lower().replace("-", "_").replace(" ", "_")
        return intervals.get(status_key, self.CADENCE_BASE_DAYS)

    def _tuned_cadence_rows(self):
        cadence_snapshot = self._tuned_cadence_snapshot()
        return [{
            "status": status,
            "interval_days": cadence_snapshot["intervals"][status],
            "default_days": default_days,
            "source": cadence_snapshot["source"],
        } for status, default_days in self.CADENCE_DEFAULTS.items()]

    def import_json(self, path):
        """Import contacts from a JSON file. Format: list of dicts or {contacts: [...]}.
        Returns count imported."""
        if not os.path.exists(path):
            return 0
        with open(path, "r") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = data.get("contacts", [])
        if not isinstance(data, list):
            return 0
        count = 0
        for c in data:
            if not isinstance(c, dict) or not c.get("name"):
                continue
            try:
                email = c.get("email")
                self._validate_email(email)
                self.add_contact(
                    name=c["name"], email=email, company=c.get("company"),
                    title=c.get("title"), deal_size=c.get("deal_size"),
                    status=c.get("status", "prospect"), source=c.get("source"),
                    notes=c.get("notes"), tags=c.get("tags"),
                )
                count += 1
            except (sqlite3.IntegrityError, ValueError):
                pass
        return count

    def company_summary(self, company):
        """Roll up all contacts, deals, and activity for a company."""
        contacts = self.contacts_by_company(company)
        if not contacts:
            return None
        total_deal_value = 0
        total_activities = 0
        all_deals = []
        statuses = {}
        for c in contacts:
            total_deal_value += self._parse_deal_size(c.get("deal_size"))
            act_count = self.conn.execute(
                "SELECT COUNT(*) FROM activity WHERE contact_id = ?", (c["id"],)
            ).fetchone()[0]
            total_activities += act_count
            deals = self.conn.execute(
                "SELECT name, value, stage FROM deals WHERE contact_id = ?", (c["id"],)
            ).fetchall()
            all_deals.extend([dict(d) for d in deals])
            s = c.get("status", "prospect")
            statuses[s] = statuses.get(s, 0) + 1

        return {
            "company": company,
            "contact_count": len(contacts),
            "contacts": [{"name": c["name"], "title": c.get("title"), "status": c.get("status")} for c in contacts],
            "total_deal_value": round(total_deal_value, 2),
            "total_activities": total_activities,
            "deals": all_deals,
            "statuses": statuses,
        }

    def all_tags(self):
        """Return all unique tags in use across contacts, sorted alphabetically."""
        rows = self.conn.execute(
            "SELECT tags FROM contacts WHERE tags IS NOT NULL AND tags != '' AND archived = 0"
        ).fetchall()
        tag_set = set()
        for r in rows:
            for t in (r["tags"] or "").split(","):
                t = t.strip()
                if t:
                    tag_set.add(t)
        return sorted(tag_set)

    def rename_tag(self, old_tag, new_tag):
        """Rename a tag across all contacts. Returns count of contacts updated."""
        contacts = self.list_by_tag(old_tag)
        count = 0
        for c in contacts:
            self.remove_tag(c.get("email") or c["name"], old_tag)
            self.add_tag(c.get("email") or c["name"], new_tag)
            count += 1
        return count

    def contact_summary(self, identifier):
        """One-line summary for quick scanning: 'Alice (Acme) — prospect, $5K/mo, score 72, 3 activities'"""
        contact = self.get_contact(identifier)
        if not contact:
            return None
        parts = [contact["name"]]
        if contact.get("company"):
            parts[0] = f"{contact['name']} ({contact['company']})"
        parts.append(contact.get("status", "prospect"))
        if contact.get("deal_size"):
            parts.append(contact["deal_size"])
        sc = self.score_contact(identifier)
        if sc:
            parts.append(f"score {sc['score']}")
        act_count = self.conn.execute(
            "SELECT COUNT(*) FROM activity WHERE contact_id = ?", (contact["id"],)
        ).fetchone()[0]
        if act_count:
            parts.append(f"{act_count} activities")
        return " — ".join(parts)

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
        wins_with_deal = [w for w in wins if w["deal_size"] > 0]
        avg_deal_size = round(sum(w["deal_size"] for w in wins_with_deal) / len(wins_with_deal), 2) if wins_with_deal else 0
        wins_with_days = [w for w in wins if w["days_to_close"] > 0]
        avg_days = round(sum(w["days_to_close"] for w in wins_with_days) / len(wins_with_days), 1) if wins_with_days else 0

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
            if r["status"] in ("active_customer", "churned"):
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

        # Updated contacts (modified since cutoff but created before it)
        updated_contacts = self.conn.execute(
            """SELECT name, company, status, deal_size, updated_at FROM contacts
               WHERE updated_at >= ? AND created_at < ?
               ORDER BY updated_at DESC""",
            (cutoff, cutoff)
        ).fetchall()
        for r in updated_contacts:
            detail_parts = [f"Updated contact: {r['name']}"]
            if r["status"]:
                detail_parts.append(f"status={r['status']}")
            if r["deal_size"]:
                detail_parts.append(f"deal={r['deal_size']}")
            changes.append({
                "type": "contact_update",
                "entity": r["name"],
                "name": r["name"],
                "company": r["company"],
                "status": r["status"],
                "detail": " ".join(detail_parts),
                "timestamp": r["updated_at"],
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

    @staticmethod
    def _queue_timestamp(value):
        """Parse SQLite/ISO timestamps into comparable naive UTC datetimes."""
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except (TypeError, ValueError):
            return None
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    @classmethod
    def _queue_status_rank(cls, status):
        """Return a comparable sales-stage rank; terminal losses never advance."""
        normalized = str(status or "").strip().lower().replace("-", "_").replace(" ", "_")
        ranks = {
            "prospect": 0,
            "contacted": 1,
            "qualification": 1,
            "qualified": 1,
            "met": 2,
            "proposal": 3,
            "proposal_drafted": 3,
            "negotiating": 4,
            "negotiation": 4,
            "verbal_yes": 5,
            "active_customer": 6,
        }
        return ranks.get(normalized)

    def _surface_outcome_events(self, window_days):
        """Join mature daily queue surface events to batched CRM outcomes."""
        surface_rows = self.conn.execute(
            """SELECT entity,
                      COALESCE(NULLIF(event_day, ''), observed_at) AS event_id,
                      MIN(CASE WHEN source = 'queue' THEN observed_at END) AS surfaced_at,
                      MAX(CASE WHEN source = 'queue' AND key = 'signal_name'
                               THEN value END) AS signal_name,
                      MAX(CASE WHEN source = 'queue' AND key = 'action'
                               THEN value END) AS action,
                      MAX(CASE WHEN source = 'queue' AND key = 'urgency'
                               THEN value END) AS urgency,
                      MAX(CASE WHEN source = 'queue_meta' AND key = 'contact_id'
                               THEN value END) AS contact_id,
                      MAX(CASE WHEN source = 'queue_meta' AND key = 'status'
                               THEN value END) AS surfaced_status
               FROM facts
               WHERE entity LIKE 'surfaced:%'
                 AND source IN ('queue', 'queue_meta')
               GROUP BY entity, COALESCE(NULLIF(event_day, ''), observed_at)
               HAVING signal_name IS NOT NULL AND action IS NOT NULL
                  AND urgency IS NOT NULL
               ORDER BY surfaced_at, entity"""
        ).fetchall()

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        maturity_cutoff = now - timedelta(days=self.ACTION_MOVE_WINDOW_DAYS)
        surfaces = []
        for row in surface_rows:
            surfaced_at = self._queue_timestamp(row["surfaced_at"])
            if surfaced_at is None or surfaced_at > maturity_cutoff:
                continue
            surfaces.append({**dict(row), "surfaced_at": surfaced_at})
        if not surfaces:
            return []

        contacts = [dict(row) for row in self.conn.execute(
            "SELECT * FROM contacts ORDER BY id"
        ).fetchall()]
        contacts_by_id = {contact["id"]: contact for contact in contacts}
        contacts_by_key = {
            str(contact.get("email") or contact["name"]).casefold(): contact
            for contact in contacts
        }
        for surface in surfaces:
            contact = None
            try:
                contact = contacts_by_id.get(int(surface.get("contact_id")))
            except (TypeError, ValueError):
                pass
            if contact is None:
                contact_key = surface["entity"].split(":", 1)[1].casefold()
                contact = contacts_by_key.get(contact_key)
            surface["contact"] = contact

        relevant_ids = {
            surface["contact"]["id"] for surface in surfaces
            if surface["contact"] is not None
        }
        activities_by_contact = {contact_id: [] for contact_id in relevant_ids}
        for row in self.conn.execute(
                "SELECT contact_id, created_at FROM activity ORDER BY created_at"):
            if row["contact_id"] in relevant_ids:
                activity_at = self._queue_timestamp(row["created_at"])
                if activity_at is not None:
                    activities_by_contact[row["contact_id"]].append(activity_at)

        entity_to_contact = {}
        for contact in contacts:
            if contact["id"] not in relevant_ids:
                continue
            for entity in self._contact_entity_keys(contact):
                entity_to_contact[entity] = contact["id"]
        statuses_by_contact = {contact_id: [] for contact_id in relevant_ids}
        for row in self.conn.execute(
                """SELECT entity, value, observed_at FROM facts
                   WHERE key = 'status' AND source != 'queue_meta'
                   ORDER BY observed_at, rowid"""):
            contact_id = entity_to_contact.get(row["entity"])
            if contact_id is None:
                continue
            status_at = self._queue_timestamp(row["observed_at"])
            if status_at is not None:
                statuses_by_contact[contact_id].append(
                    (status_at, row["value"])
                )

        outcomes = []
        for surface in surfaces:
            contact = surface["contact"]
            if contact is None:
                continue
            surfaced_at = surface["surfaced_at"]
            act_deadline = surfaced_at + timedelta(days=window_days)
            move_deadline = surfaced_at + timedelta(
                days=self.ACTION_MOVE_WINDOW_DAYS
            )
            activity_times = activities_by_contact.get(contact["id"], [])
            acted = any(
                surfaced_at < activity_at <= act_deadline
                for activity_at in activity_times
            )

            baseline_status = surface.get("surfaced_status")
            status_events = statuses_by_contact.get(contact["id"], [])
            if self._queue_status_rank(baseline_status) is None:
                earlier_statuses = [
                    status for status_at, status in status_events
                    if status_at <= surfaced_at
                ]
                if earlier_statuses:
                    baseline_status = earlier_statuses[-1]
                else:
                    updated_at = self._queue_timestamp(contact.get("updated_at"))
                    if updated_at is not None and updated_at <= surfaced_at:
                        baseline_status = contact.get("status")

            baseline_rank = self._queue_status_rank(baseline_status)
            advanced = False
            if baseline_rank is not None:
                advanced = any(
                    surfaced_at < status_at <= move_deadline
                    and self._queue_status_rank(status) is not None
                    and self._queue_status_rank(status) > baseline_rank
                    for status_at, status in status_events
                )
                if not advanced:
                    updated_at = self._queue_timestamp(contact.get("updated_at"))
                    current_rank = self._queue_status_rank(contact.get("status"))
                    advanced = (
                        updated_at is not None
                        and surfaced_at < updated_at <= move_deadline
                        and current_rank is not None
                        and current_rank > baseline_rank
                    )

            outcomes.append({
                "signal": surface["signal_name"],
                "acted": acted,
                # Attribute movement only when the surfaced action was taken.
                "moved": acted and advanced,
            })
        return outcomes

    def action_outcomes(self, window_days=7):
        """Summarize mature surfaced actions and their downstream outcomes.

        An action is ``acted`` when activity is logged after surfacing and
        within ``window_days``. It is ``moved`` when that acted-on contact also
        advances sales stage within 30 days. Only events old enough to observe
        the full movement window are scored, avoiding partial-outcome bias.
        """
        try:
            window_days = max(int(window_days), 0)
        except (TypeError, ValueError):
            window_days = 7
        grouped = {}
        for outcome in self._surface_outcome_events(window_days):
            stats = grouped.setdefault(outcome["signal"], {
                "signal": outcome["signal"],
                "surfaced": 0,
                "acted": 0,
                "moved": 0,
            })
            stats["surfaced"] += 1
            stats["acted"] += int(outcome["acted"])
            stats["moved"] += int(outcome["moved"])

        signal_order = {
            signal: index for index, signal in enumerate(self.NEXT_ACTION_SIGNAL_NAMES)
        }
        rows = []
        for signal, stats in grouped.items():
            surfaced = stats["surfaced"]
            stats["act_rate"] = round(stats["acted"] / surfaced, 4)
            stats["move_rate"] = round(stats["moved"] / surfaced, 4)
            rows.append(stats)
        rows.sort(key=lambda row: (
            signal_order.get(row["signal"], len(signal_order)), row["signal"]
        ))
        return rows

    def _urgency_weight_model(self):
        """Return multipliers, learned signal names, and their outcome stats."""
        outcome_rows = self.action_outcomes()
        by_signal = {row["signal"]: row for row in outcome_rows}
        all_signals = list(self.NEXT_ACTION_SIGNAL_NAMES)
        all_signals.extend(sorted(set(by_signal) - set(all_signals)))
        multipliers = {signal: 1.0 for signal in all_signals}
        learned_signals = {
            signal for signal, stats in by_signal.items()
            if stats["surfaced"] >= self.ACTION_LEARNING_MIN_OUTCOMES
        }
        if learned_signals:
            mean_move_rate = sum(
                by_signal[signal]["move_rate"] for signal in learned_signals
            ) / len(learned_signals)
            if mean_move_rate > 0:
                for signal in learned_signals:
                    relative_rate = by_signal[signal]["move_rate"] / mean_move_rate
                    multipliers[signal] = round(min(max(
                        relative_rate, self.ACTION_MULTIPLIER_MIN
                    ), self.ACTION_MULTIPLIER_MAX), 4)
        return multipliers, learned_signals, outcome_rows

    def tuned_urgency_weights(self):
        """Return deterministic per-signal urgency multipliers from outcomes."""
        return self._urgency_weight_model()[0]

    def queue_stats(self):
        """Return action outcomes alongside the multiplier currently in use."""
        multipliers, learned_signals, outcome_rows = self._urgency_weight_model()
        by_signal = {row["signal"]: row for row in outcome_rows}
        signal_names = list(self.NEXT_ACTION_SIGNAL_NAMES)
        signal_names.extend(sorted(set(by_signal) - set(signal_names)))
        rows = []
        for signal in signal_names:
            row = dict(by_signal.get(signal, {
                "signal": signal,
                "surfaced": 0,
                "acted": 0,
                "moved": 0,
                "act_rate": 0.0,
                "move_rate": 0.0,
            }))
            row["multiplier"] = multipliers[signal]
            row["weights_source"] = (
                "learned" if signal in learned_signals else "default"
            )
            rows.append(row)
        return rows

    def _record_surfaced_actions(self, actions):
        """Persist one grouped queue fact event per returned contact and day."""
        if not actions:
            return 0
        event_day = date.today().isoformat()
        observed_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(
            sep=" ", timespec="microseconds"
        )
        rows = []
        for action in actions:
            entity = f"surfaced:{action['_contact_key']}"
            rows.extend((
                (entity, "signal_name", action["_primary_signal"], "queue",
                 observed_at, event_day),
                (entity, "action", action["action"], "queue",
                 observed_at, event_day),
                (entity, "urgency", str(action["urgency_score"]), "queue",
                 observed_at, event_day),
                (entity, "contact_id", str(action["_contact_id"]), "queue_meta",
                 observed_at, event_day),
                (entity, "status", action["_status"], "queue_meta",
                 observed_at, event_day),
            ))
        with self.conn:
            self.conn.executemany(
                """INSERT INTO facts
                   (entity, key, value, source, observed_at, event_day)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT DO UPDATE SET
                       value = excluded.value,
                       observed_at = excluded.observed_at""",
                rows,
            )
        return len(actions)

    def next_actions(self, limit=10):
        """Build one ranked, explainable "do this today" queue.

        The queue composes existing reminder, cadence, relationship-health,
        stale-deal, and close-probability signals. Each contact appears once;
        the strongest signal chooses the action while all supporting signals
        remain available for agents and other callers.
        """
        try:
            limit = max(int(limit), 0)
        except (TypeError, ValueError):
            limit = 10
        if limit == 0:
            return []

        urgency_weights, learned_signals, _ = self._urgency_weight_model()
        today = date.today()
        contacts = [
            contact for contact in self.list_contacts()
            if contact.get("status") not in ("lost", "churned")
        ]
        contacts_by_id = {contact["id"]: contact for contact in contacts}
        contacts_by_email = {
            contact["email"].lower(): contact
            for contact in contacts if contact.get("email")
        }
        contacts_by_name = {contact["name"].lower(): contact for contact in contacts}
        queued = {}

        def add_signal(contact, action, reason, score, name, value):
            entry = queued.setdefault(contact["id"], {
                "contact": contact["name"],
                "action": action,
                "reason": reason,
                "deal_value": self._parse_deal_size(contact.get("deal_size")),
                "signals": [],
                "_primary_score": -1,
                "_primary_signal": None,
                "_contact_id": contact["id"],
                "_contact_key": contact.get("email") or contact["name"],
                "_status": contact.get("status") or "prospect",
            })
            base_score = round(float(score), 1)
            multiplier = urgency_weights.get(name, 1.0)
            signal_score = round(base_score * multiplier, 1)
            weights_source = "learned" if name in learned_signals else "default"
            entry["signals"].append({
                "name": name,
                "value": value,
                "urgency_score": signal_score,
                "base_urgency_score": base_score,
                "multiplier": multiplier,
                "weights_source": weights_source,
            })
            if signal_score > entry["_primary_score"]:
                entry["action"] = action
                entry["reason"] = reason
                entry["_primary_score"] = signal_score
                entry["_primary_signal"] = name
                entry["weights_source"] = weights_source

        # Explicit commitments always win. Even a reminder due today scores
        # above the maximum possible non-reminder action.
        for reminder in self.due_reminders():
            contact = contacts_by_id.get(reminder["contact_id"])
            if not contact:
                continue
            try:
                due_date = date.fromisoformat(str(reminder["due_date"])[:10])
                days_overdue = max((today - due_date).days, 0)
            except (ValueError, TypeError):
                days_overdue = 0
            if days_overdue:
                reason = f"Reminder overdue by {days_overdue} day{'s' if days_overdue != 1 else ''}"
                signal_name = "overdue_reminder"
            else:
                reason = "Reminder due today"
                signal_name = "due_reminder"
            add_signal(
                contact,
                reminder.get("note") or "Complete reminder",
                reason,
                120 + min(days_overdue, 30),
                signal_name,
                {"reminder_id": reminder["id"], "due_date": reminder["due_date"],
                 "days_overdue": days_overdue},
            )

        for contact in contacts:
            cadence_days = self.tuned_cadence(contact.get("status"))
            last_contacted = contact.get("last_contacted")
            days_since = None
            if last_contacted:
                try:
                    days_since = max(
                        (today - date.fromisoformat(str(last_contacted)[:10])).days, 0
                    )
                except (ValueError, TypeError):
                    pass
            if days_since is not None and days_since <= cadence_days:
                continue

            if days_since is None:
                reason = f"Never contacted; tuned cadence is every {cadence_days} days"
                score = 50
            else:
                overdue_days = days_since - cadence_days
                reason = (f"Last touch was {days_since} days ago; tuned cadence is "
                          f"every {cadence_days} days")
                score = min(45 + (overdue_days / cadence_days) * 15, 70)
            status = contact.get("status")
            if status == "active_customer":
                action = "Check in"
            elif status == "prospect" and days_since is None:
                action = "Initial outreach"
            else:
                action = "Follow up"
            add_signal(
                contact, action, reason, score, "cadence_overdue",
                {"days_since_contact": days_since, "cadence_days": cadence_days},
            )

        # relationship_health_report owns the health/velocity calculation; this
        # queue only translates low scores into a ranked action.
        for health in self.relationship_health_report():
            contact = None
            if health.get("email"):
                contact = contacts_by_email.get(health["email"].lower())
            if not contact:
                contact = contacts_by_name.get(health["name"].lower())
            if not contact or health.get("score", 0) >= 40:
                continue
            score = health.get("score", 0)
            reason = f"Relationship health is low at {score}/100"
            add_signal(
                contact, "Rebuild relationship", reason,
                35 + (40 - score) * 0.5,
                "low_relationship_health",
                {"score": score, "trend": health.get("trend")},
            )

        for deal in self.stale_deals(days=30):
            contact = contacts_by_id.get(deal["contact_id"])
            if not contact:
                continue
            try:
                updated = date.fromisoformat(str(deal["updated_at"])[:10])
                stale_days = max((today - updated).days, 30)
            except (ValueError, TypeError):
                stale_days = 30
            reason = f"Open deal '{deal['name']}' has been stale for {stale_days} days"
            add_signal(
                contact, "Review stale deal", reason,
                min(75 + stale_days / 3, 90),
                "stale_open_deal",
                {"deal_id": deal["id"], "deal": deal["name"], "stale_days": stale_days},
            )

        # iMessage reciprocity: they message you heavily, you barely reply.
        imsg_rows = self.conn.execute(
            """SELECT entity, key, value FROM facts
               WHERE source = 'imessage'
               AND key IN ('imessage_sent', 'imessage_received', 'imessage_total')"""
        ).fetchall()
        imsg_by_entity = {}
        for row in imsg_rows:
            imsg_by_entity.setdefault(row["entity"], {})[row["key"]] = row["value"]
        if imsg_by_entity:
            for contact in contacts:
                best = None
                for variant in self._contact_entity_keys(contact):
                    data = imsg_by_entity.get(variant)
                    if data and "imessage_total" in data:
                        total = int(data.get("imessage_total", 0))
                        if best is None or total > best["total"]:
                            best = {
                                "sent": int(data.get("imessage_sent", 0)),
                                "received": int(data.get("imessage_received", 0)),
                                "total": total,
                            }
                if (best and best["received"] > 5 and best["sent"] > 0
                        and best["received"] > best["sent"] * 5):
                    reason = (f"They've sent {best['received']} messages, you've sent "
                              f"{best['sent']} — relationship is one-sided")
                    add_signal(
                        contact, "Reply", reason,
                        min(85 + best["received"] / best["sent"], 95),
                        "one_sided_messaging",
                        {"sent": best["sent"], "received": best["received"]},
                    )

        for forecast_row in self.forecast()["contacts"]:
            probability = forecast_row["probability"]
            forecast_cadence_days = self.tuned_cadence(forecast_row["status"])
            recency = next(
                (signal.get("value") for signal in forecast_row["signals"]
                 if signal.get("name") == "activity_recency"),
                None,
            )
            if (probability < 0.40
                    or (recency is not None and recency <= forecast_cadence_days)):
                continue
            contact = contacts_by_id.get(forecast_row["contact"]["id"])
            if not contact:
                continue
            quiet_reason = ("no recorded activity" if recency is None
                            else f"quiet for {recency} days")
            reason = f"{probability:.0%} close probability, but {quiet_reason}"
            status = forecast_row["status"]
            if status == "verbal_yes":
                action = "Send contract"
            elif status == "proposal_drafted":
                action = "Follow up on proposal"
            else:
                action = "Re-engage"
            extra_staleness = (0 if recency is None
                               else max(recency - forecast_cadence_days, 0))
            add_signal(
                contact, action, reason,
                min(80 + probability * 15 + min(extra_staleness, 10) * 0.5, 105),
                "high_probability_quiet",
                {"probability": probability, "days_since_activity": recency},
            )

        actions = []
        for entry in queued.values():
            supporting_bonus = min(max(len(entry["signals"]) - 1, 0) * 2, 4)
            entry["urgency_score"] = round(entry.pop("_primary_score") + supporting_bonus, 1)
            if entry["urgency_score"] >= 80:
                entry["priority"] = "high"
            elif entry["urgency_score"] >= 55:
                entry["priority"] = "medium"
            else:
                entry["priority"] = "low"
            actions.append(entry)

        actions.sort(key=lambda item: (
            -item["urgency_score"], item["contact"].lower(), item["action"].lower()
        ))
        surfaced = actions[:limit]
        self._record_surfaced_actions(surfaced)
        for entry in surfaced:
            for private_key in (
                    "_primary_signal", "_contact_id", "_contact_key", "_status"):
                entry.pop(private_key, None)
        return surfaced

    def propose_ticks(self, benchmark_path="benchmark.py", crm_path="crm.py",
                      roadmap_path="roadmap.md"):
        """Propose deterministic roadmap ticks from code, data, and cached perf gaps."""
        benchmark_file = Path(benchmark_path)
        crm_file = Path(crm_path)
        roadmap_file = Path(roadmap_path)

        crm_source = crm_file.read_text(encoding="utf-8")
        benchmark_source = benchmark_file.read_text(encoding="utf-8")
        crm_tree = ast.parse(crm_source, filename=str(crm_file))
        crm_class = next(
            (
                node for node in crm_tree.body
                if isinstance(node, ast.ClassDef) and node.name == "CRM"
            ),
            None,
        )
        if crm_class is None:
            raise ValueError(f"No CRM class found in {crm_file}")

        ranked_proposals = []

        def add_proposal(rank, title, rationale, kind, evidence):
            ranked_proposals.append((rank, {
                "title": title,
                "rationale": rationale,
                "kind": kind,
                "evidence": evidence,
            }))

        public_methods = sorted({
            node.name for node in crm_class.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and not node.name.startswith("_")
        })
        for method_name in public_methods:
            reference_count = len(re.findall(
                rf"\b{re.escape(method_name)}\b", benchmark_source
            ))
            if reference_count == 0:
                add_proposal(
                    0,
                    f"Benchmark {method_name} capability",
                    f"Add focused benchmark cases for CRM.{method_name}().",
                    "untested capability",
                    (f"{crm_file}: public CRM method {method_name}; "
                     f"{benchmark_file}: {reference_count} name references"),
                )

        action_limit = max(
            self.conn.execute(
                "SELECT COUNT(*) FROM contacts WHERE archived = 0"
            ).fetchone()[0],
            10,
        )
        actions = self.next_actions(limit=action_limit)
        forecast_rows = self.forecast()["contacts"]
        known_action_signals = set(self.NEXT_ACTION_SIGNAL_NAMES)
        known_forecast_signals = set(self.FORECAST_SIGNAL_NAMES)
        fired_action_signals = {
            signal.get("name")
            for action in actions
            for signal in action.get("signals", [])
            if signal.get("name") in known_action_signals
        }
        fired_forecast_signals = {
            signal.get("name")
            for forecast_row in forecast_rows
            for signal in forecast_row.get("signals", [])
            if signal.get("name") in known_forecast_signals
        }

        signal_groups = (
            ("next_actions", self.NEXT_ACTION_SIGNAL_NAMES,
             fired_action_signals, len(actions)),
            ("forecast", self.FORECAST_SIGNAL_NAMES,
             fired_forecast_signals, len(forecast_rows)),
        )
        for source_name, known_signals, fired_signals, output_count in signal_groups:
            for signal_name in sorted(set(known_signals) - fired_signals):
                add_proposal(
                    1,
                    f"Activate {signal_name} signal",
                    (f"Inspect or seed representative CRM records for {signal_name}. "
                     "This gap is data-dependent and is not evidence of a code bug."),
                    "signal never fires on this data",
                    (f"Current connected DB: {source_name} returned {output_count} "
                     f"records and emitted 0 {signal_name} signals"),
                )

        cache_candidates = (
            Path(f"{benchmark_file}.results"),
            Path(f"{benchmark_file}.out"),
            benchmark_file.with_name(f"{benchmark_file.stem}.results"),
            benchmark_file.with_name(f"{benchmark_file.stem}.results.txt"),
            benchmark_file.with_name(f"{benchmark_file.stem}_results.txt"),
            benchmark_file.with_name("benchmark-results.txt"),
            benchmark_file.with_name("benchmark_results.txt"),
            benchmark_file.with_name("benchmark.out"),
        )
        cached_results = next((path for path in cache_candidates if path.is_file()), None)
        if cached_results:
            cached_source = cached_results.read_text(encoding="utf-8")
            perf_sections = re.split(
                r"(?=^PERFORMANCE TIMINGS\b)", cached_source, flags=re.MULTILINE
            )
            timing_pattern = re.compile(
                r"^\s*([A-Za-z0-9_.-]+)\s+([0-9]+(?:\.[0-9]+)?)\s+ms\s+"
                r"limit\s+([0-9]+(?:\.[0-9]+)?)\s+ms\s+(PASS|FAIL)\s*$",
                re.MULTILINE,
            )
            perf_runs = []
            for section in perf_sections:
                timings = {
                    match.group(1): {
                        "elapsed_ms": float(match.group(2)),
                        "threshold_ms": float(match.group(3)),
                        "status": match.group(4),
                    }
                    for match in timing_pattern.finditer(section)
                }
                if timings:
                    perf_runs.append(timings)

            if perf_runs:
                current_timings = perf_runs[-1]
                prior_timings = perf_runs[-2] if len(perf_runs) > 1 else {}
                for operation_name, timing in sorted(current_timings.items()):
                    elapsed_ms = timing["elapsed_ms"]
                    threshold_ms = timing["threshold_ms"]
                    utilization = elapsed_ms / threshold_ms if threshold_ms else float("inf")
                    prior = prior_timings.get(operation_name)
                    slowing = bool(
                        prior and prior["elapsed_ms"] > 0
                        and elapsed_ms > prior["elapsed_ms"]
                    )
                    if timing["status"] != "FAIL" and utilization < 0.8 and not slowing:
                        continue
                    trend_detail = ""
                    if prior:
                        trend_detail = f"; prior {prior['elapsed_ms']:.3f} ms"
                    add_proposal(
                        2,
                        f"Restore {operation_name} performance headroom",
                        (f"Profile {operation_name} before it reaches or exceeds "
                         "the benchmark limit."),
                        "performance trend",
                        (f"{cached_results}: current {elapsed_ms:.3f} ms / "
                         f"{threshold_ms:.3f} ms limit ({utilization:.0%})"
                         f"{trend_detail}"),
                    )

        roadmap_source = (
            roadmap_file.read_text(encoding="utf-8") if roadmap_file.is_file() else ""
        )
        tick_pattern = re.compile(
            r"^###\s+(?:\[[ xX]\]\s+)?([A-Za-z0-9-]+)\.\s+(.+?)\s*$",
            re.MULTILINE,
        )
        existing_ticks = tick_pattern.findall(roadmap_source)
        generic_title_words = {
            "activate", "add", "benchmark", "capability", "coverage", "exercise",
            "headroom", "improve", "performance", "restore", "signal", "test", "tests",
        }

        def title_words(title):
            words = set(re.findall(r"[a-z0-9]+", title.lower().replace("_", " ")))
            specific_words = words - generic_title_words
            return specific_words or words

        existing_title_words = [title_words(title) for _, title in existing_ticks]
        proposals = []
        for _, proposal in sorted(
            ranked_proposals,
            key=lambda item: (item[0], item[1]["title"].lower()),
        ):
            proposal_words = title_words(proposal["title"])
            if any(proposal_words <= tick_words for tick_words in existing_title_words):
                continue
            proposals.append(proposal)

        tick_number_values = {
            word: number for number, word in enumerate((
                "Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven",
                "Eight", "Nine", "Ten", "Eleven", "Twelve", "Thirteen", "Fourteen",
                "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen",
            ))
        }
        existing_numbers = []
        for ordinal, _ in existing_ticks:
            if ordinal.isdigit():
                existing_numbers.append(int(ordinal))
            elif ordinal.title() in tick_number_values:
                existing_numbers.append(tick_number_values[ordinal.title()])
        first_tick_number = max(existing_numbers, default=0) + 1

        def tick_number_word(number):
            small = (
                "Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven",
                "Eight", "Nine", "Ten", "Eleven", "Twelve", "Thirteen", "Fourteen",
                "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen",
            )
            tens = {20: "Twenty", 30: "Thirty", 40: "Forty", 50: "Fifty",
                    60: "Sixty", 70: "Seventy", 80: "Eighty", 90: "Ninety"}
            if number < len(small):
                return small[number]
            if number < 100:
                base = number - number % 10
                return tens[base] if number == base else f"{tens[base]}-{small[number % 10]}"
            return str(number)

        if proposals:
            markdown_lines = ["## Wave three — self-proposed", ""]
            for offset, proposal in enumerate(proposals):
                ordinal = tick_number_word(first_tick_number + offset)
                markdown_lines.extend((
                    f"### {ordinal}. {proposal['title']}",
                    proposal["rationale"],
                    f"Kind: {proposal['kind']}. Evidence: {proposal['evidence']}.",
                    "",
                ))
            markdown = "\n".join(markdown_lines).rstrip()
        else:
            markdown = "<!-- No new roadmap ticks proposed. -->"

        return {"proposals": proposals, "markdown": markdown}

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
        target_escaped = self._escape_like(target_lower)
        results = []
        seen_connectors = set()

        # --- Step 1: Find all entities related to target ---

        # Find contacts who work at the target company (via contacts table)
        company_matches = self.conn.execute(
            "SELECT * FROM contacts WHERE LOWER(company) LIKE ? ESCAPE '\\'",
            (f"%{target_escaped}%",)
        ).fetchall()

        # Also match by email domain: @acme.com matches target "acme"
        # This catches contacts whose company field is empty but email reveals affiliation
        domain_matches = self.conn.execute(
            "SELECT * FROM contacts WHERE email IS NOT NULL AND LOWER(email) LIKE ? ESCAPE '\\'",
            (f"%@{target_escaped}%",)
        ).fetchall()
        # Merge domain matches into company_matches (avoid duplicates by id)
        company_match_ids = {r["id"] for r in company_matches}
        company_matches = list(company_matches) + [r for r in domain_matches if r["id"] not in company_match_ids]

        # Find contacts whose name matches the target
        name_matches = self.conn.execute(
            "SELECT * FROM contacts WHERE LOWER(name) LIKE ? ESCAPE '\\'",
            (f"%{target_escaped}%",)
        ).fetchall()

        # Find contacts by email match
        email_matches = self.conn.execute(
            "SELECT * FROM contacts WHERE LOWER(email) LIKE ? ESCAPE '\\'",
            (f"%{target_escaped}%",)
        ).fetchall()

        # Also search the facts graph for company references
        graph_company_entities = self.conn.execute(
            """SELECT DISTINCT entity FROM facts
               WHERE key = 'company' AND LOWER(value) LIKE ? ESCAPE '\\'""",
            (f"%{target_escaped}%",)
        ).fetchall()

        # Search for any entity or value matching target in the graph
        graph_entity_matches = self.conn.execute(
            """SELECT DISTINCT entity FROM facts
               WHERE LOWER(entity) LIKE ? ESCAPE '\\' OR LOWER(value) LIKE ? ESCAPE '\\'""",
            (f"%{target_escaped}%", f"%{target_escaped}%")
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
                   WHERE LOWER(value) LIKE ? ESCAPE '\\'""",
                (f"%{self._escape_like(target_person)}%",)
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

    def intro_paths(self, target_identifier, max_hops=3):
        """Find ranked 2-3 hop paths from strong relationships to a target.

        The graph is undirected. Explicit contact references in facts form its
        strongest edges, while shared companies and email domains provide
        weaker inferred edges. The user's side of each path is represented by
        an anchor contact with message or activity history.
        """
        try:
            max_hops = min(3, int(max_hops))
        except (TypeError, ValueError):
            return []
        target_text = str(target_identifier or "").strip()
        if max_hops < 2 or not target_text:
            return []
        if target_text.casefold() in {"me", "myself", "self", "you"}:
            return []

        contacts = [dict(row) for row in self.conn.execute(
            "SELECT * FROM contacts WHERE archived = 0 ORDER BY id"
        ).fetchall()]
        if not contacts:
            return []

        contact_nodes = {c["id"]: f"crm:{c['id']}" for c in contacts}
        contacts_by_node = {contact_nodes[c["id"]]: c for c in contacts}
        aliases = {}

        def normalize_contact_ref(value):
            value = str(value or "").strip().casefold()
            if not value.startswith("contact:"):
                return None
            suffix = " ".join(value[len("contact:"):].replace("_", " ").split())
            return f"contact:{suffix}" if suffix else None

        def add_alias(alias, node):
            normalized = normalize_contact_ref(alias)
            if normalized:
                aliases.setdefault(normalized, set()).add(node)

        for contact in contacts:
            node = contact_nodes[contact["id"]]
            for entity_key in self._contact_entity_keys(contact):
                add_alias(entity_key, node)

        def resolve_contact_ref(value):
            normalized = normalize_contact_ref(value)
            if not normalized:
                return None
            matches = aliases.get(normalized, set())
            if len(matches) == 1:
                return next(iter(matches))
            return f"fact:{normalized}"

        def node_name(node):
            contact = contacts_by_node.get(node)
            if contact:
                return contact["name"]
            raw = node[len("fact:contact:"):] if node.startswith("fact:contact:") else node
            return raw if "@" in raw else raw.title()

        graph = {}
        edge_priority = {
            "introduced_by": 5,
            "knows": 4,
            "works_with": 3,
            "contact_reference": 2,
            "shared_company": 1,
            "shared_domain": 0,
        }

        def add_edge(left, right, weight, kind, detail=None):
            if not left or not right or left == right:
                return
            edge = {"weight": weight, "kind": kind, "detail": detail}
            existing = graph.setdefault(left, {}).get(right)
            if existing:
                existing_rank = (existing["weight"], edge_priority[existing["kind"]])
                new_rank = (weight, edge_priority[kind])
                if new_rank <= existing_rank:
                    return
            graph.setdefault(left, {})[right] = edge
            graph.setdefault(right, {})[left] = edge

        company_groups = {}
        domain_groups = {}
        for contact in contacts:
            node = contact_nodes[contact["id"]]
            company = " ".join((contact.get("company") or "").casefold().split())
            if company:
                company_groups.setdefault(company, {"label": contact["company"], "nodes": set()})["nodes"].add(node)
            email = (contact.get("email") or "").strip().casefold()
            if "@" in email:
                domain = email.rsplit("@", 1)[1]
                if domain:
                    domain_groups.setdefault(domain, set()).add(node)

        fact_rows = self.conn.execute(
            "SELECT entity, key, value FROM facts ORDER BY rowid"
        ).fetchall()
        direct_totals = {}
        direct_intensities = {}
        intensity_rank = {"low": 1, "medium": 2, "high": 3}

        for row in fact_rows:
            left = resolve_contact_ref(row["entity"])
            key = str(row["key"] or "").strip().casefold()
            value = str(row["value"] or "").strip()
            right = resolve_contact_ref(value)

            if left and right:
                if key in {"introduced_by", "introduced_to", "introduction"}:
                    add_edge(left, right, 0.95, "introduced_by")
                elif key in {"knows", "friend_of"}:
                    add_edge(left, right, 0.85, "knows")
                elif key in {"works_with", "worked_with", "colleague_of"}:
                    add_edge(left, right, 0.75, "works_with")
                else:
                    add_edge(left, right, 0.70, "contact_reference")

            if left in contacts_by_node and key in {"company", "works_at", "employer"} and not right:
                company = value.casefold()
                if company.startswith("company:"):
                    company = company[len("company:"):]
                company = " ".join(company.replace("_", " ").split())
                if company:
                    company_groups.setdefault(company, {"label": value.replace("company:", ""), "nodes": set()})["nodes"].add(left)

            if left in contacts_by_node and key == "email" and "@" in value:
                domain = value.casefold().rsplit("@", 1)[1]
                if domain:
                    domain_groups.setdefault(domain, set()).add(left)

            if left in contacts_by_node and key in {"imessage_total", "email_total"}:
                try:
                    direct_totals[left] = max(direct_totals.get(left, 0), int(value))
                except (TypeError, ValueError):
                    pass
            elif left in contacts_by_node and key == "message_intensity":
                intensity = value.casefold()
                current = direct_intensities.get(left, "")
                if intensity_rank.get(intensity, 0) > intensity_rank.get(current, 0):
                    direct_intensities[left] = intensity

        for company, group in sorted(company_groups.items()):
            nodes = sorted(group["nodes"], key=lambda node: (node_name(node).casefold(), node))
            for index, left in enumerate(nodes):
                for right in nodes[index + 1:]:
                    add_edge(left, right, 0.60, "shared_company", group["label"] or company)

        for domain, group in sorted(domain_groups.items()):
            if domain in FREE_MAIL_DOMAINS:
                continue
            nodes = sorted(group, key=lambda node: (node_name(node).casefold(), node))
            for index, left in enumerate(nodes):
                for right in nodes[index + 1:]:
                    add_edge(left, right, 0.40, "shared_domain", domain)

        activity_counts = {
            contact_nodes[row["contact_id"]]: row["activity_count"]
            for row in self.conn.execute(
                "SELECT contact_id, COUNT(*) AS activity_count FROM activity GROUP BY contact_id"
            ).fetchall()
            if row["contact_id"] in contact_nodes
        }
        anchors = []
        for node, contact in contacts_by_node.items():
            total = direct_totals.get(node, 0)
            intensity = direct_intensities.get(node, "low")
            message_strength = self._intro_warmth_score(total, intensity) / 100 if total else 0
            activity_count = activity_counts.get(node, 0)
            activity_strength = min(0.90, 0.35 + activity_count * 0.10) if activity_count else 0
            strength = max(message_strength, activity_strength)
            if strength >= 0.35:
                anchors.append((node, strength))
        anchors.sort(key=lambda item: (-item[1], node_name(item[0]).casefold(), item[0]))
        if not anchors:
            return []

        target_folded = target_text.casefold()
        exact_targets = {
            node for node, contact in contacts_by_node.items()
            if target_folded in {
                contact["name"].strip().casefold(),
                (contact.get("email") or "").strip().casefold(),
            }
        }
        target_nodes = set(exact_targets)
        if not target_nodes:
            target_domain = target_folded[1:] if target_folded.startswith("@") else target_folded
            for node, contact in contacts_by_node.items():
                name = contact["name"].casefold()
                email = (contact.get("email") or "").casefold()
                company = (contact.get("company") or "").casefold()
                domain = email.rsplit("@", 1)[1] if "@" in email else ""
                if (target_folded in name or target_folded in email or target_folded in company
                        or target_domain == domain or target_domain in domain):
                    target_nodes.add(node)

            normalized_target = normalize_contact_ref(
                target_text if target_folded.startswith("contact:") else f"contact:{target_text}"
            )
            for node in graph:
                if node in contacts_by_node:
                    continue
                graph_ref = node[len("fact:"):] if node.startswith("fact:") else node
                if (graph_ref == normalized_target
                        or target_folded in node_name(node).casefold()):
                    target_nodes.add(node)

        if not target_nodes:
            return []
        if exact_targets and exact_targets.issubset({node for node, _ in anchors}):
            return []

        def edge_explanation(left, right, edge):
            left_name = node_name(left)
            right_name = node_name(right)
            if edge["kind"] == "introduced_by":
                return f"{left_name} and {right_name} have an explicit introduction link"
            if edge["kind"] == "knows":
                return f"{left_name} knows {right_name}"
            if edge["kind"] == "works_with":
                return f"{left_name} works with {right_name}"
            if edge["kind"] == "shared_company":
                return f"{left_name} and {right_name} both work at {edge['detail']}"
            if edge["kind"] == "shared_domain":
                return f"{left_name} and {right_name} share @{edge['detail']}"
            return f"{left_name} is linked to {right_name} in the facts graph"

        paths = []
        seen_paths = set()
        for anchor, anchor_strength in anchors:
            if anchor in target_nodes:
                continue
            frontier = [(anchor, [anchor], [], anchor_strength)]
            for hops in range(1, max_hops + 1):
                next_by_node = {}
                for current, path, edges, strength in frontier:
                    neighbors = sorted(
                        graph.get(current, {}).items(),
                        key=lambda item: (-item[1]["weight"], node_name(item[0]).casefold(), item[0]),
                    )
                    for neighbor, edge in neighbors:
                        if neighbor in path:
                            continue
                        next_path = path + [neighbor]
                        next_edges = edges + [(current, neighbor, edge)]
                        next_strength = strength * edge["weight"] * 0.90
                        candidate = (neighbor, next_path, next_edges, next_strength)
                        existing = next_by_node.get(neighbor)
                        candidate_names = tuple(node_name(node).casefold() for node in next_path)
                        if existing:
                            existing_names = tuple(node_name(node).casefold() for node in existing[1])
                            if (next_strength < existing[3]
                                    or (next_strength == existing[3]
                                        and candidate_names >= existing_names)):
                                continue
                        next_by_node[neighbor] = candidate

                frontier = []
                for neighbor, path, edges, strength in sorted(
                        next_by_node.values(),
                        key=lambda item: tuple(node_name(node).casefold() for node in item[1])):
                    if neighbor in target_nodes:
                        if hops >= 2:
                            display_path = [node_name(node) for node in path]
                            path_key = tuple(name.casefold() for name in display_path)
                            if path_key not in seen_paths:
                                seen_paths.add(path_key)
                                explanation_parts = [
                                    f"Start with {display_path[0]}, a strong direct relationship"
                                ]
                                explanation_parts.extend(
                                    edge_explanation(left, right, edge)
                                    for left, right, edge in edges
                                )
                                paths.append({
                                    "path": display_path,
                                    "hops": hops,
                                    "strength": round(strength, 4),
                                    "explanation": "; ".join(explanation_parts) + ".",
                                })
                        continue
                    frontier.append((neighbor, path, edges, strength))

        paths.sort(key=lambda item: (
            item["hops"],
            -item["strength"],
            tuple(name.casefold() for name in item["path"]),
        ))
        return paths

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
        q = f"%{self._escape_like(query)}%"
        rows = self.conn.execute(
            """SELECT entity, key, value, source, observed_at FROM facts
               WHERE entity LIKE ? ESCAPE '\\' OR key LIKE ? ESCAPE '\\'
               OR value LIKE ? ESCAPE '\\' OR source LIKE ? ESCAPE '\\'
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

    @staticmethod
    def _record_value(record, key, default=None):
        """Read a field from a sqlite Row or mapping without failing the ingest."""
        try:
            return record[key]
        except (KeyError, IndexError, TypeError):
            return default

    def _read_macos_contact_records(self, db_paths):
        """Read AddressBook databases into source-independent contact records."""
        records = []
        for db_path in db_paths:
            try:
                ab = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
                ab.row_factory = sqlite3.Row
            except Exception as e:
                print(f"Warning: AddressBook open error ({db_path}): {e}", file=sys.stderr)
                continue

            try:
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
                    rowid = row["rowid"]
                    emails = ab.execute("""
                        SELECT ZADDRESSNORMALIZED as email
                        FROM ZABCDEMAILADDRESS
                        WHERE ZOWNER = ?
                    """, (rowid,)).fetchall()
                    phones = ab.execute("""
                        SELECT ZFULLNUMBER as phone
                        FROM ZABCDPHONENUMBER
                        WHERE ZOWNER = ?
                    """, (rowid,)).fetchall()
                    records.append({
                        "first": row["first"],
                        "last": row["last"],
                        "company": row["company"],
                        "title": row["title"],
                        "notes": row["notes"],
                        "emails": [email["email"] for email in emails],
                        "phones": [phone["phone"] for phone in phones],
                    })
            except Exception as e:
                print(f"Warning: AddressBook read error: {e}", file=sys.stderr)
            finally:
                ab.close()
        return records

    def _ingest_macos_contact_records(self, records):
        """Transform AddressBook-shaped records and store contacts and facts."""
        contacts_added = 0
        facts = []

        for record in records:
            try:
                first = (self._record_value(record, "first") or "").strip()
                last = (self._record_value(record, "last") or "").strip()
                name = f"{first} {last}".strip()
                if not name:
                    continue

                company = (self._record_value(record, "company") or "").strip() or None
                title = (self._record_value(record, "title") or "").strip() or None
                emails = self._record_value(record, "emails", []) or []
                phones = self._record_value(record, "phones", []) or []
                if not isinstance(emails, (list, tuple)) or not isinstance(phones, (list, tuple)):
                    continue

                email = emails[0] if emails else None
                entity = f"contact:{name.lower()}"

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
                for raw_phone in phones:
                    phone = str(raw_phone or "").strip()
                    if phone:
                        normalized = self._normalize_phone(phone)
                        if normalized:
                            facts.append((f"phone:{normalized}", "name", name, "macos_contacts"))
                            if company:
                                facts.append((f"phone:{normalized}", "company", company, "macos_contacts"))

                if company:
                    facts.append((entity, "company", company, "macos_contacts"))
                if title:
                    facts.append((entity, "title", title, "macos_contacts"))
                for address in emails:
                    if address:
                        facts.append((entity, "email", address, "macos_contacts"))
                for raw_phone in phones:
                    if raw_phone:
                        facts.append((entity, "phone", raw_phone, "macos_contacts"))
                raw_notes = self._record_value(record, "notes")
                if isinstance(raw_notes, str) and raw_notes.strip():
                    facts.append((entity, "notes", raw_notes.strip()[:500], "macos_contacts"))
            except (AttributeError, TypeError, ValueError):
                continue

        facts_added = self.observe_many(facts, source="macos_contacts") if facts else 0
        return (contacts_added, facts_added)

    def ingest_macos_contacts(self, db_paths=None, records=None):
        """Import contacts from macOS AddressBook into CRM + context graph.

        Reads ~/Library/Application Support/AddressBook/Sources/*/AddressBook-v22.abcddb
        Pass db_paths or pre-read records to ingest fixtures without reading live data.
        Returns (contacts_added, facts_added) tuple.
        """
        if records is None:
            if db_paths is None:
                import glob as globmod
                ab_pattern = os.path.expanduser(
                    "~/Library/Application Support/AddressBook/Sources/*/AddressBook-v22.abcddb"
                )
                db_paths = globmod.glob(ab_pattern)
            elif isinstance(db_paths, (str, os.PathLike)):
                db_paths = [db_paths]
            records = self._read_macos_contact_records(db_paths)
        return self._ingest_macos_contact_records(records)

    def _read_macos_imessage_rows(self, db_path, days):
        """Query the iMessage source database and return aggregate rows."""
        if not os.path.exists(db_path):
            return []
        try:
            mdb = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            mdb.row_factory = sqlite3.Row
        except Exception as e:
            print(f"Warning: iMessage DB open error: {e}", file=sys.stderr)
            return []

        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        apple_epoch_offset = 978307200
        cutoff_apple = (
            datetime.strptime(cutoff, "%Y-%m-%d").timestamp() - apple_epoch_offset
        ) * 1_000_000_000
        try:
            return mdb.execute("""
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
        except Exception as e:
            print(f"Warning: iMessage read error: {e}", file=sys.stderr)
            return []
        finally:
            mdb.close()

    def _ingest_macos_imessage_rows(self, rows):
        """Transform iMessage aggregate rows and store relationship facts."""
        facts = []
        contacts_found = set()

        for row in rows:
            try:
                handle = self._record_value(row, "handle")
                if not isinstance(handle, str) or not handle.strip():
                    continue
                handle = handle.strip()
                msg_count = int(self._record_value(row, "msg_count"))
                sent = int(self._record_value(row, "sent"))
                received = int(self._record_value(row, "received"))
                if min(msg_count, sent, received) < 0:
                    continue

                if "@" in handle:
                    entity = f"contact:{handle.lower()}"
                else:
                    normalized_phone = self._normalize_phone(handle)
                    entity = f"phone:{normalized_phone}" if normalized_phone else f"phone:{handle}"

                contacts_found.add(handle)
                facts.append((entity, "imessage_total", str(msg_count), "imessage"))
                facts.append((entity, "imessage_sent", str(sent), "imessage"))
                facts.append((entity, "imessage_received", str(received), "imessage"))
                facts.append((entity, "imessage_ratio", f"{sent}:{received}", "imessage"))

                if msg_count > 100:
                    intensity = "high"
                elif msg_count > 20:
                    intensity = "medium"
                else:
                    intensity = "low"
                facts.append((entity, "message_intensity", intensity, "imessage"))

                if "@" in handle:
                    existing_row = self.conn.execute(
                        "SELECT * FROM contacts WHERE LOWER(email) = LOWER(?) LIMIT 1",
                        (handle,)
                    ).fetchone()
                    existing = dict(existing_row) if existing_row else None
                    if existing:
                        crm_entity = f"contact:{existing['name'].lower()}"
                        facts.append((crm_entity, "imessage_handle", handle, "imessage"))
                        facts.append((crm_entity, "imessage_total", str(msg_count), "imessage"))
                        facts.append((crm_entity, "imessage_sent", str(sent), "imessage"))
                        facts.append((crm_entity, "imessage_received", str(received), "imessage"))
                        facts.append((crm_entity, "message_intensity", intensity, "imessage"))
                else:
                    name_fact = self.conn.execute(
                        "SELECT value FROM facts WHERE entity = ? AND key = 'name' AND source = 'macos_contacts' LIMIT 1",
                        (entity,)
                    ).fetchone()
                    if name_fact:
                        crm_entity = f"contact:{name_fact[0].lower()}"
                        facts.append((crm_entity, "imessage_handle", handle, "imessage"))
                        facts.append((crm_entity, "imessage_total", str(msg_count), "imessage"))
                        facts.append((crm_entity, "imessage_sent", str(sent), "imessage"))
                        facts.append((crm_entity, "imessage_received", str(received), "imessage"))
                        facts.append((crm_entity, "message_intensity", intensity, "imessage"))
            except (AttributeError, TypeError, ValueError):
                continue

        facts_added = self.observe_many(facts, source="imessage") if facts else 0
        return (len(contacts_found), facts_added)

    def ingest_macos_imessage(self, days=90, db_path=None, rows=None):
        """Import iMessage conversation partners and message counts into context graph.

        Reads ~/Library/Messages/chat.db (requires Full Disk Access).
        Pass db_path or pre-aggregated rows to ingest fixtures without reading live data.
        Returns (contacts_found, facts_added) tuple.
        """
        if rows is None:
            db_path = db_path or os.path.expanduser("~/Library/Messages/chat.db")
            rows = self._read_macos_imessage_rows(db_path, days)
        return self._ingest_macos_imessage_rows(rows)

    def _read_macos_calendar_records(self, db_path, start_apple, end_apple):
        """Query Calendar Cache and return event and attendee records."""
        if not os.path.exists(db_path):
            return ([], [])
        try:
            cdb = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            cdb.row_factory = sqlite3.Row
        except Exception as e:
            print(f"Warning: Calendar DB open error: {e}", file=sys.stderr)
            return ([], [])

        try:
            records = cdb.execute("""
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
        except Exception as e:
            print(f"Warning: Calendar read error: {e}", file=sys.stderr)
            cdb.close()
            return ([], [])

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
        except Exception:
            attendees = []  # Attendee table may not exist in all versions
        finally:
            cdb.close()
        return (records, attendees)

    def _ingest_macos_calendar_records(self, records, attendees=None):
        """Transform Calendar records and store meeting facts."""
        apple_epoch = 978307200
        facts = []
        events_found = 0

        for record in records:
            try:
                record_facts = []
                title = (self._record_value(record, "title") or "").strip()
                if not title:
                    continue
                start_date = self._record_value(record, "start_date")
                ts = start_date + apple_epoch if start_date else None
                date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else "unknown"
                entity = f"meeting:{title.lower()[:80]}"

                record_facts.append((entity, "title", title, "macos_calendar"))
                record_facts.append((entity, "date", date_str, "macos_calendar"))
                location = self._record_value(record, "location")
                if location:
                    record_facts.append((entity, "location", location.strip(), "macos_calendar"))
                notes = (self._record_value(record, "notes") or "").strip()
                if notes:
                    record_facts.append((entity, "notes", notes[:500], "macos_calendar"))

                for word in title.split():
                    if len(word) > 2 and word[0].isupper():
                        match = self.get_contact(word)
                        if match:
                            crm_entity = f"contact:{match['name'].lower()}"
                            record_facts.append((crm_entity, "meeting", title, "macos_calendar"))
                            record_facts.append((crm_entity, "last_meeting_date", date_str, "macos_calendar"))
                facts.extend(record_facts)
                events_found += 1
            except (AttributeError, TypeError, ValueError, OSError, OverflowError):
                continue

        for attendee in attendees or []:
            try:
                attendee_facts = []
                att_name = (self._record_value(attendee, "name") or "").strip()
                att_addr = (self._record_value(attendee, "address") or "").replace("mailto:", "").strip()
                event_title = (self._record_value(attendee, "event_title") or "").strip()
                if not event_title:
                    continue

                if att_addr and "@" in att_addr:
                    att_entity = f"contact:{att_addr.lower()}"
                    attendee_facts.append((att_entity, "meeting", event_title, "macos_calendar"))
                    event_date = self._record_value(attendee, "event_date")
                    ts = event_date + apple_epoch if event_date else None
                    if ts:
                        attendee_facts.append((att_entity, "last_meeting_date",
                                               datetime.fromtimestamp(ts).strftime("%Y-%m-%d"),
                                               "macos_calendar"))
                elif att_name:
                    att_entity = f"contact:{att_name.lower()}"
                    attendee_facts.append((att_entity, "meeting", event_title, "macos_calendar"))
                facts.extend(attendee_facts)
            except (AttributeError, TypeError, ValueError, OSError, OverflowError):
                continue

        facts_added = self.observe_many(facts, source="macos_calendar") if facts else 0
        return (events_found, facts_added)

    def ingest_macos_calendar(self, days_back=30, days_forward=30, db_path=None,
                              records=None, attendees=None):
        """Import calendar events into context graph as meeting facts.

        Reads ~/Library/Calendars/Calendar Cache (requires calendar access).
        Pass records and attendees to ingest fixtures without reading live data.
        Returns (events_found, facts_added) tuple.
        """
        now = datetime.now()
        start = (now - timedelta(days=days_back)).timestamp()
        end = (now + timedelta(days=days_forward)).timestamp()
        apple_epoch = 978307200
        start_apple = start - apple_epoch
        end_apple = end - apple_epoch
        if records is None:
            db_path = db_path or os.path.expanduser("~/Library/Calendars/Calendar Cache")
            records, attendees = self._read_macos_calendar_records(
                db_path, start_apple, end_apple
            )
        return self._ingest_macos_calendar_records(records, attendees)

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

        Auto-detects delimiter (comma, semicolon, tab) via csv.Sniffer so
        European-locale and TSV exports work out of the box.

        Returns dict with contacts_added, facts_added, skipped, mapping_used.
        """
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            sample = f.read(8192)
            f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            except csv.Error:
                dialect = None  # fall back to default comma
            reader = csv.DictReader(f, dialect=dialect) if dialect else csv.DictReader(f)
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
                    # Resolve entity key to the existing contact's name so
                    # extra-column facts attach to the right entity instead
                    # of creating orphaned facts under the CSV row's name.
                    existing = self.get_contact(normalized.get("email", ""))
                    if existing:
                        name = existing["name"]

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

    def _read_macos_mail_rows(self, db_path, days):
        """Query Mail's Envelope Index and return message/recipient rows."""
        if not os.path.exists(db_path):
            return []
        try:
            mdb = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            mdb.row_factory = sqlite3.Row
        except Exception as e:
            print(f"Warning: Mail DB open error: {e}", file=sys.stderr)
            return []

        cutoff = int((datetime.now() - timedelta(days=days)).timestamp())
        try:
            return mdb.execute("""
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
        except Exception as e:
            print(f"Warning: Mail read error: {e}", file=sys.stderr)
            return []
        finally:
            mdb.close()

    def _ingest_macos_mail_rows(self, rows):
        """Aggregate Mail rows by address and store email relationship facts."""
        facts = []
        threads_found = 0
        email_counts = {}

        for row in rows:
            try:
                from_addr = (self._record_value(row, "from_addr") or "").strip().lower()
                to_addr = (self._record_value(row, "to_addr") or "").strip().lower()
                if not from_addr and not to_addr:
                    continue

                valid_addresses = [
                    address for address in (from_addr, to_addr)
                    if address and "@" in address
                ]
                if not valid_addresses:
                    continue

                threads_found += 1
                for address in valid_addresses:
                    if address not in email_counts:
                        email_counts[address] = {"sent": 0, "received": 0, "last": ""}
                    email_counts[address]["last"] = str(
                        self._record_value(row, "date_sent") or ""
                    )

                if from_addr and "@" in from_addr:
                    email_counts[from_addr]["sent"] += 1
                if to_addr and "@" in to_addr:
                    email_counts[to_addr]["received"] += 1
            except (AttributeError, TypeError, ValueError):
                continue

        for address, counts in email_counts.items():
            entity = f"email:{address}"
            total = counts["sent"] + counts["received"]
            facts.append((entity, "email_total", str(total), "macos_mail"))
            facts.append((entity, "email_sent", str(counts["sent"]), "macos_mail"))
            facts.append((entity, "email_received", str(counts["received"]), "macos_mail"))

            existing_row = self.conn.execute(
                "SELECT * FROM contacts WHERE LOWER(email) = LOWER(?) LIMIT 1",
                (address,)
            ).fetchone()
            if existing_row:
                existing = dict(existing_row)
                crm_entity = f"contact:{existing['name'].lower()}"
                facts.append((crm_entity, "email_total", str(total), "macos_mail"))
                facts.append((crm_entity, "email_frequency",
                              "high" if total > 50 else "medium" if total > 10 else "low",
                              "macos_mail"))

        facts_added = self.observe_many(facts, source="macos_mail") if facts else 0
        return (threads_found, facts_added)

    def ingest_macos_mail(self, days=90, db_path=None, rows=None):
        """Import email threads from Mail.app into activity log + context graph.

        Reads ~/Library/Mail/V10/MailData/Envelope Index (SQLite).
        Pass db_path or pre-read rows to ingest fixtures without reading live data.
        Returns (threads_found, facts_added) tuple.
        """
        if rows is None:
            if db_path is None:
                db_path = os.path.expanduser("~/Library/Mail/V10/MailData/Envelope Index")
                if not os.path.exists(db_path):
                    db_path = os.path.expanduser("~/Library/Mail/V9/MailData/Envelope Index")
            rows = self._read_macos_mail_rows(db_path, days)
        return self._ingest_macos_mail_rows(rows)

    def ingest_mbox(self, mbox_path, days=90):
        """Import from any mbox file (Gmail Takeout, Thunderbird, etc).

        Uses stdlib mailbox module. Cross-platform.
        Returns (messages_imported, facts_added) tuple.
        """
        import mailbox
        import email.utils

        if not os.path.exists(mbox_path):
            return (0, 0)

        # Use timezone-aware cutoff so comparison with timezone-aware email
        # dates (the common case) doesn't raise TypeError.  For the rare
        # naive date (no tz offset in the header), we fall back to a naive
        # cutoff in the comparison below.
        cutoff_aware = datetime.now(timezone.utc) - timedelta(days=days)
        cutoff_naive = datetime.now() - timedelta(days=days)
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
                    # Compare using matching tz-awareness to avoid TypeError
                    cutoff = cutoff_aware if date_tuple.tzinfo else cutoff_naive
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

    def _canonical_contact_entity(self, contact):
        """Return the stable primary graph entity for a contact."""
        normalized_name = self._normalize_identity(contact.get("name"))
        return f"contact:{normalized_name}"

    def _contact_entity_keys(self, contact):
        """Return all entity key variants for a contact, used across graph lookups."""
        raw_name_lower = contact["name"].casefold()
        name_lower = " ".join(raw_name_lower.split())
        normalized_name = self._normalize_identity(contact["name"])
        parts = normalized_name.split()
        keys = {
            f"contact:{raw_name_lower}",
            f"contact:{raw_name_lower.replace(' ', '_')}",
            f"contact:{name_lower}",
            f"contact:{name_lower.replace(' ', '_')}",
            f"contact:{normalized_name}",
            f"contact:{normalized_name.replace(' ', '_')}",
            *(f"contact:{part}" for part in parts),
        }
        if contact.get("email"):
            keys.add(f"contact:{contact['email'].strip().casefold()}")
        return sorted(keys)

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

            # Derive display name from entity, resolving phone handles to
            # real names so the same person isn't listed twice (once as a
            # phone number and once as a contact name).
            if entity.startswith("contact:"):
                raw_name = entity[len("contact:"):]
                name = " ".join(w.capitalize() for w in raw_name.split())
            elif entity.startswith("phone:"):
                name_fact = self.conn.execute(
                    "SELECT value FROM facts WHERE entity = ? AND key = 'name' LIMIT 1",
                    (entity,)
                ).fetchone()
                if name_fact:
                    name = name_fact[0]
                else:
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

        # Gather all context — use all entity key variants so facts stored
        # under contact:alice_smith, contact:alice, phone:+1..., etc. are found
        entity_keys = self._contact_entity_keys(contact)
        facts = {}
        for ek in entity_keys:
            facts.update(self.facts_about(ek))
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

    p_archive = sub.add_parser("archive", help="Archive a contact (soft-delete)")
    p_archive.add_argument("identifier")

    p_unarchive = sub.add_parser("unarchive", help="Restore an archived contact")
    p_unarchive.add_argument("identifier")

    p_archived = sub.add_parser("archived", help="List archived contacts")

    p_export_deals = sub.add_parser("export-deals", help="Export deals as CSV")
    p_export_deals.add_argument("--output", "-o", help="Output file path")

    p_export_vcf = sub.add_parser("export-vcard", help="Export contacts as vCard (.vcf)")
    p_export_vcf.add_argument("--output", "-o", help="Output file path")

    p_import_vcf = sub.add_parser("import-vcard", help="Import contacts from vCard (.vcf) file")
    p_import_vcf.add_argument("file", help="Path to .vcf file")

    sub.add_parser("dashboard", help="All-in-one CRM dashboard")

    sub.add_parser("churning", help="Detect contacts with decaying engagement")

    p_untouched = sub.add_parser("untouched", help="Contacts with zero activity")
    p_untouched.add_argument("--days", type=int, default=30)

    p_touch = sub.add_parser("touch-plan", help="Auto-generate follow-up schedule")
    p_touch.add_argument("identifier")

    sub.add_parser("deal-velocity", help="Deal stage velocity report")

    p_comp = sub.add_parser("company", help="Company summary")
    p_comp.add_argument("company_name")

    p_compare = sub.add_parser("compare", help="Side-by-side contact comparison")
    p_compare.add_argument("contact_a")
    p_compare.add_argument("contact_b")

    p_recent = sub.add_parser("recent", help="Most recently active contacts")
    p_recent.add_argument("--limit", type=int, default=10)

    sub.add_parser("top-companies", help="Companies ranked by deal value")

    sub.add_parser("all-tags", help="List all tags in use")

    # self-improvement
    sub.add_parser("evolve", help="Run one self-improvement cycle")
    p_experiments = sub.add_parser("experiments", help="List past experiments")
    p_experiments.add_argument("--status", choices=["running", "completed"],
                               help="Filter by experiment status")

    p_experiment = sub.add_parser("experiment", help="Log or conclude an experiment")
    experiment_sub = p_experiment.add_subparsers(dest="experiment_command", required=True)
    p_experiment_log = experiment_sub.add_parser("log", help="Start a new experiment")
    p_experiment_log.add_argument("name")
    p_experiment_log.add_argument("hypothesis")
    p_experiment_log.add_argument("metric")
    p_experiment_log.add_argument("baseline_value")
    p_experiment_conclude = experiment_sub.add_parser("conclude", help="Complete an experiment")
    p_experiment_conclude.add_argument("slug_or_name")
    p_experiment_conclude.add_argument("result_value")
    p_experiment_conclude.add_argument("--verdict", choices=["win", "loss", "neutral"])
    sub.add_parser("win-patterns", help="What do closed deals have in common")
    sub.add_parser("loss-patterns", help="What do lost deals have in common")
    sub.add_parser("dead", help="Find contacts that should be marked as lost")
    sub.add_parser("cadence", help="Data-driven optimal follow-up timing")

    p_qa = sub.add_parser("quick-add", help="Parse natural text into a contact")
    p_qa.add_argument("text", nargs="+", help="Natural language contact description")

    p_domain = sub.add_parser("domain", help="Find contacts by email domain")
    p_domain.add_argument("domain", help="Email domain (e.g. acme.com)")

    p_c360 = sub.add_parser("360", help="Complete 360-degree contact view")
    p_c360.add_argument("identifier", help="Email or name")

    sub.add_parser("pipeline-score", help="Single 0-100 pipeline health score")

    p_period = sub.add_parser("period", help="Compare this period vs previous")
    p_period.add_argument("--days", type=int, default=30)

    # reminders
    p_remind = sub.add_parser("remind", help="Set a reminder for a contact")
    p_remind.add_argument("identifier")
    p_remind.add_argument("date", nargs="?",
                          help="Due date (YYYY-MM-DD, 'tomorrow', '+3d'); omit for tuned cadence")
    p_remind.add_argument("note", nargs="?", help="Reminder note")

    sub.add_parser("reminders", help="Show due and overdue reminders")

    p_remind_done = sub.add_parser("remind-done", help="Complete a reminder")
    p_remind_done.add_argument("reminder_id", type=int)

    p_snooze = sub.add_parser("snooze", help="Snooze a reminder")
    p_snooze.add_argument("reminder_id", type=int)
    p_snooze.add_argument("--days", type=int, default=3)

    # custom fields
    p_setfield = sub.add_parser("set-field", help="Set a custom field on a contact")
    p_setfield.add_argument("identifier")
    p_setfield.add_argument("field_name")
    p_setfield.add_argument("field_value")

    p_getfields = sub.add_parser("fields", help="Show custom fields for a contact")
    p_getfields.add_argument("identifier")

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

    # import/export
    p_export = sub.add_parser("export", help="Export a full CRM backup as JSON")
    p_export.add_argument("file", nargs="?", help="JSON output path (prints to stdout if omitted)")

    p_import = sub.add_parser("import", help="Import a full CRM JSON backup")
    p_import.add_argument("file", help="JSON backup path")
    p_import.add_argument("--merge", action="store_true", help="Merge into a non-empty database")

    p_import_csv = sub.add_parser("import-csv", help="Import contacts from CSV")
    p_import_csv.add_argument("file", help="CSV file path")

    p_export_csv = sub.add_parser("export-csv", help="Export contacts to CSV")
    p_export_csv.add_argument("file", help="CSV output path")

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

    p_forecast = sub.add_parser("forecast", help="Rank open deals by close probability")
    p_forecast.add_argument("--limit", "-n", type=int, help="Maximum contacts to show")

    sub.add_parser("calibration", help="Forecast Brier score, reliability, and learned weights")

    sub.add_parser("queue-stats", help="Queue outcomes and learned urgency multipliers")

    sub.add_parser("propose", help="Propose the CRM's next roadmap ticks")

    p_today = sub.add_parser("today", help="Rank the best CRM actions to do today")
    p_today.add_argument("--limit", "-n", type=int, default=10, help="Maximum actions to show")

    sub.add_parser("duplicates", help="Find potential duplicate contacts")

    p_stalec = sub.add_parser("stale-contacts", help="Contacts not contacted in N days")
    p_stalec.add_argument("--days", "-D", type=int,
                          help="Override per-status tuned cadence")

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

    # deal management
    p_deal_update = sub.add_parser("deal-update", help="Update a deal")
    p_deal_update.add_argument("deal_id", type=int, help="Deal ID")
    p_deal_update.add_argument("--name", help="Deal name")
    p_deal_update.add_argument("--value", help="Deal value")
    p_deal_update.add_argument("--stage", help="Deal stage")
    p_deal_update.add_argument("--notes", help="Deal notes")

    p_deal_close = sub.add_parser("deal-close", help="Close a deal as won")
    p_deal_close.add_argument("deal_id", type=int, help="Deal ID")
    p_deal_close.add_argument("--notes", help="Closing notes")

    p_deal_delete = sub.add_parser("deal-delete", help="Delete a deal")
    p_deal_delete.add_argument("deal_id", type=int, help="Deal ID")

    p_deal_view = sub.add_parser("deals", help="List deals for a contact")
    p_deal_view.add_argument("identifier", nargs="?", help="Email or name (omit for all)")
    p_deal_view.add_argument("--stage", help="Filter by stage")

    sub.add_parser("deal-pipeline", help="Deal-centric pipeline view")

    p_merge = sub.add_parser("merge", help="Merge two contacts into one")
    p_merge.add_argument("keep", help="Contact to keep (email or name)")
    p_merge.add_argument("drop", help="Contact to absorb (will be deleted)")

    p_unmerge = sub.add_parser("unmerge", help="Undo a contact merge by merge id")
    p_unmerge.add_argument("merge_id", help="Merge id, e.g. 3 or merge:3")

    p_automerge = sub.add_parser(
        "automerge", help="Merge high-confidence duplicate contacts"
    )
    p_automerge.add_argument(
        "--apply", action="store_true", help="Apply merges (default is dry-run)"
    )
    p_automerge.add_argument(
        "--threshold", type=float, default=0.9,
        help="Minimum confidence from 0 to 1 (default: 0.9)",
    )

    p_doctor = sub.add_parser("doctor", help="Check CRM data integrity")
    p_doctor.add_argument(
        "--fix", action="store_true", help="Repair safe integrity issues"
    )

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
    p_intros.add_argument("--deep", action="store_true", help="Also search ranked 2-3 hop relationship paths")

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

    elif args.command == "archive":
        if crm.archive_contact(args.identifier):
            print(f"Archived: {args.identifier}")
        else:
            print(f"Not found: {args.identifier}")

    elif args.command == "unarchive":
        if crm.unarchive_contact(args.identifier):
            print(f"Unarchived: {args.identifier}")
        else:
            print(f"Not found: {args.identifier}")

    elif args.command == "archived":
        contacts = crm.list_contacts(include_archived=True)
        archived = [c for c in contacts if c.get("archived")]
        if not archived:
            print("  No archived contacts")
        else:
            fmt_table(archived, ["name", "company", "status", "deal_size", "last_contacted"])

    elif args.command == "export-deals":
        crm.export_deals_csv(args.output)
        if args.output:
            print(f"Exported deals to: {args.output}")

    elif args.command == "export-vcard":
        result = crm.export_vcard(args.output)
        if args.output:
            print(f"Exported vCard to: {args.output}")
        else:
            print(result)

    elif args.command == "import-vcard":
        count = crm.import_vcard(args.file)
        print(f"Imported: {count} contacts")

    elif args.command == "dashboard":
        d = crm.dashboard()
        m = d["metrics"]
        print(f"\n  === CRM Dashboard ===\n")
        print(f"  Contacts: {m['total_contacts']}  |  MRR: ${m['mrr']:,.0f}  |  Pipeline: ${m['pipeline_value']:,.0f}")
        print(f"  Active (7d): {m['active_last_7d']}  |  Stale (tuned): {m['stale_14d']}")
        h = d["health"]
        print(f"  Health: {h['healthy']} healthy, {h['at_risk']} at risk, {h['cold']} cold")
        if d["top_actions"]:
            print(f"\n  Top Actions:")
            for a in d["top_actions"]:
                prio = a["priority"].upper()
                print(f"    [{prio}] {a['contact']}: {a['action']}")
        if d["stale_contacts"]:
            print(f"\n  Stale Contacts:")
            for s in d["stale_contacts"]:
                print(f"    {s['name']} ({s.get('company') or '-'}) — {s.get('deal_size') or 'no deal'}")
        g = d["graph"]
        if g["entities"] > 0:
            print(f"\n  Graph: {g['entities']} entities, {g['facts']} facts")
        print()

    elif args.command == "churning":
        churning = crm.detect_churning()
        if not churning:
            print("  No decaying relationships detected")
        else:
            for c in churning:
                deal = f"${c['deal_value']:,.0f}" if c["deal_value"] else "-"
                cold = f"cold in {c['days_until_cold']}d" if c.get("days_until_cold") else ""
                print(f"  {c['name']} ({c.get('company') or '-'}) — {c['velocity']:.1f}x velocity, deal {deal} {cold}")

    elif args.command == "untouched":
        untouched = crm.untouched_contacts(days=args.days)
        if not untouched:
            print(f"  No untouched contacts (>{args.days} days)")
        else:
            fmt_table(untouched, ["name", "company", "status", "deal_size"])

    elif args.command == "touch-plan":
        plan = crm.touch_plan(args.identifier)
        if not plan:
            print(f"  Not found: {args.identifier}")
        else:
            status = "OVERDUE" if plan["overdue"] else f"in {plan['days_until_next']}d"
            print(f"  {plan['contact']}: {plan['channel']} every {plan['frequency_days']}d — next: {plan['next_touch']} ({status})")

    elif args.command == "deal-velocity":
        report = crm.deal_velocity_report()
        print(f"  Deals: {report['total_deals']}  |  Avg pipeline time: {report['avg_days_in_pipeline']}d")
        for stage, data in report["by_stage"].items():
            print(f"    {stage}: {data['count']} deals, avg {data['avg_days']}d")

    elif args.command == "company":
        summary = crm.company_summary(args.company_name)
        if not summary:
            print(f"  No contacts at: {args.company_name}")
        else:
            print(f"  {summary['company']}: {summary['contact_count']} contacts, ${summary['total_deal_value']:,.0f} total deal value")
            for c in summary["contacts"]:
                print(f"    {c['name']} ({c.get('title') or '-'}) — {c.get('status')}")

    elif args.command == "compare":
        result = crm.compare_contacts(args.contact_a, args.contact_b)
        if not result:
            print("  One or both contacts not found")
        else:
            a, b = result["contact_a"], result["contact_b"]
            print(f"  {'':20} {'A':>20} {'B':>20}")
            for key in ("name", "company", "status", "score", "activity_count"):
                print(f"  {key:20} {str(a.get(key, '-')):>20} {str(b.get(key, '-')):>20}")
            if result["overlap"]:
                print(f"\n  Overlap: {', '.join(result['overlap'])}")

    elif args.command == "recent":
        recent = crm.recent_contacts(limit=args.limit)
        if not recent:
            print("  No contacts")
        else:
            fmt_table(recent, ["name", "company", "status", "last_contacted"])

    elif args.command == "top-companies":
        top = crm.top_companies()
        if not top:
            print("  No companies with contacts")
        else:
            for t in top:
                print(f"  {t['company']}: {t['contact_count']} contacts, ${t['total_deal_value']:,.0f}")

    elif args.command == "all-tags":
        tags = crm.all_tags()
        if not tags:
            print("  No tags in use")
        else:
            print(f"  Tags ({len(tags)}): {', '.join(tags)}")

    elif args.command == "evolve":
        result = crm.evolve()
        print(f"\n  === Evolve Cycle ===")
        print(f"  Bottleneck: {result['bottleneck']}")
        a = result["analysis"]
        print(f"  Contacts: {a['total_contacts']} | Stale: {a['stale_pct']}% | Win rate: {a['win_rate']}%")
        p = result["proposal"]
        print(f"\n  PROPOSAL: {p['experiment']}")
        print(f"  Change: {p['change']}")
        print(f"  Measure: {p['measure']}")
        print(f"  Batch size: {p['batch_size']}")
        cadence_summary = ", ".join(
            f"{status} {values['tuned_days']}d/{values['default_days']}d"
            for status, values in result["cadence"].items()
        )
        print(f"  Cadence (tuned/default): {cadence_summary}")
        print(f"  Experiment ID: {result['experiment_id']}\n")

    elif args.command == "experiments":
        exps = crm.experiment_history(status=args.status)
        if not exps:
            print("  No experiments recorded")
        else:
            for e in exps:
                label = e["verdict"] or e["status"] or "unknown"
                print(f"  [{label.upper()}] {e['name']} ({e['slug']})")
                print(f"    Hypothesis: {e['hypothesis'] or '-'}")
                print(f"    Metric: {e['metric'] or '-'}")
                outcome = e["result_value"] if e["result_value"] is not None else "running"
                print(f"    Baseline: {e['baseline_value']} | Result: {outcome}")
                dates = e["started_at"]
                if e["concluded_at"]:
                    dates += f" → {e['concluded_at']}"
                print(f"    Dates: {dates}")

    elif args.command == "experiment":
        if args.experiment_command == "log":
            slug = crm.log_experiment(
                args.name, args.hypothesis, args.metric, args.baseline_value
            )
            print(f"  Logged experiment: {slug}")
        elif args.experiment_command == "conclude":
            try:
                experiment = crm.conclude_experiment(
                    args.slug_or_name, args.result_value, verdict=args.verdict
                )
            except ValueError as exc:
                print(f"  Error: {exc}", file=sys.stderr)
                return
            if experiment is None:
                print(f"  Experiment not found: {args.slug_or_name}")
            else:
                print(f"  [{experiment['verdict'].upper()}] {experiment['name']}: "
                      f"{experiment['baseline_value']} → {experiment['result_value']}")

    elif args.command == "win-patterns":
        wp = crm.win_patterns()
        print(f"  Wins: {wp['win_count']} | Avg activities to close: {wp['avg_activities_to_close']}")
        for p in wp["patterns"]:
            print(f"    • {p}")

    elif args.command == "loss-patterns":
        lp = crm.loss_patterns()
        print(f"  Losses: {lp['loss_count']} | Avg activities before loss: {lp['avg_activities_before_loss']}")
        for p in lp["patterns"]:
            print(f"    • {p}")

    elif args.command == "dead":
        dead = crm.dead_pipeline()
        if not dead:
            print("  No dead pipeline contacts")
        else:
            for d in dead:
                print(f"  {d['name']} ({d.get('company') or '-'}) — {d['trend']}: {d['recommendation']}")

    elif args.command == "cadence":
        fmt_table(
            crm._tuned_cadence_rows(),
            ["status", "interval_days", "source", "default_days"],
        )

    elif args.command == "quick-add":
        text = " ".join(args.text)
        cid = crm.quick_add(text)
        if cid:
            c = crm.conn.execute("SELECT name, email, company FROM contacts WHERE id = ?", (cid,)).fetchone()
            print(f"  Added: {c['name']} ({c['email'] or 'no email'}) at {c['company'] or '-'}")
        else:
            print("  Could not parse contact from text")

    elif args.command == "domain":
        contacts = crm.find_by_domain(args.domain)
        if not contacts:
            print(f"  No contacts at @{args.domain}")
        else:
            fmt_table(contacts, ["name", "email", "company", "status"])

    elif args.command == "360":
        view = crm.contact_360(args.identifier)
        if not view:
            print(f"  Not found: {args.identifier}")
        else:
            p = view["profile"]
            print(f"\n  === {p['name']} ===")
            print(f"  Company: {p.get('company') or '-'} | Status: {p.get('status')} | Deal: {p.get('deal_size') or '-'}")
            print(f"  Score: {p.get('score', 0)}/100 | Relationship: {view['relationship_score']}/100")
            if view["velocity"]:
                print(f"  Velocity: {view['velocity']['trend']} ({view['velocity']['velocity']}x)")
            if view["streak"]:
                print(f"  Streak: {view['streak']['current_streak']}d current, {view['streak']['longest_streak']}d longest")
            if view["custom_fields"]:
                print(f"  Fields: {view['custom_fields']}")
            if view["reminders"]:
                print(f"  Reminders: {len(view['reminders'])} pending")
            print()

    elif args.command == "pipeline-score":
        phs = crm.pipeline_health_score()
        print(f"  Pipeline Health: {phs['score']}/100")
        for f in phs["factors"]:
            print(f"    • {f}")

    elif args.command == "period":
        pc = crm.period_comparison(days=args.days)
        c = pc["current_period"]
        p = pc["previous_period"]
        ch = pc["changes"]
        print(f"  {pc['period_days']}-day comparison")
        print(f"  {'':20} {'Current':>10} {'Previous':>10} {'Change':>10}")
        print(f"  {'Contacts':20} {c['new_contacts']:>10} {p['new_contacts']:>10} {ch['contacts']:>10}")
        print(f"  {'Activities':20} {c['activities']:>10} {p['activities']:>10} {ch['activities']:>10}")
        print(f"  {'Deals':20} {c['new_deals']:>10} {p['new_deals']:>10} {ch['deals']:>10}")

    elif args.command == "remind":
        note = args.note
        raw_date = args.date
        if note is None and raw_date is not None:
            note = raw_date
            raw_date = None
        if not note:
            p_remind.error("a reminder note is required")
        # Parse date shortcuts
        if raw_date is None:
            due = None
        elif raw_date == "tomorrow":
            due = (date.today() + timedelta(days=1)).isoformat()
        elif raw_date.startswith("+") and raw_date.endswith("d"):
            try:
                days = int(raw_date[1:-1])
                due = (date.today() + timedelta(days=days)).isoformat()
            except ValueError:
                due = raw_date
        else:
            due = raw_date
        rid = crm.set_reminder(args.identifier, due, note)
        if rid:
            saved = crm.conn.execute(
                "SELECT due_date FROM reminders WHERE id = ?", (rid,)
            ).fetchone()
            print(f"  Reminder set for {saved['due_date']}: {note} (id={rid})")
        else:
            print(f"  Not found: {args.identifier}")

    elif args.command == "reminders":
        due = crm.due_reminders(include_future_days=7)
        if not due:
            print("  No reminders due")
        else:
            for r in due:
                status = "OVERDUE" if r["due_date"] < date.today().isoformat() else r["due_date"]
                print(f"  [{status}] {r['name']}: {r['note']} (id={r['id']})")

    elif args.command == "remind-done":
        if crm.complete_reminder(args.reminder_id):
            print(f"  Completed reminder {args.reminder_id}")
        else:
            print(f"  Reminder not found or already completed: {args.reminder_id}")

    elif args.command == "snooze":
        new_date = crm.snooze_reminder(args.reminder_id, days=args.days)
        if new_date:
            print(f"  Snoozed to {new_date}")
        else:
            print(f"  Reminder not found: {args.reminder_id}")

    elif args.command == "set-field":
        if crm.set_field(args.identifier, args.field_name, args.field_value):
            print(f"  Set {args.field_name} = {args.field_value}")
        else:
            print(f"  Not found: {args.identifier}")

    elif args.command == "fields":
        fields = crm.get_fields(args.identifier)
        if not fields:
            print(f"  No custom fields for: {args.identifier}")
        else:
            for k, v in fields.items():
                print(f"  {k}: {v}")

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
        print(f"  Stale (tuned):   {s['stale_14d']}")
        print()
        for row in s["by_status"]:
            print(f"    {row['status']}: {row['n']}")

    elif args.command == "markdown":
        print(crm.markdown())

    elif args.command == "json":
        print(json.dumps(crm.to_json(), indent=2, default=str))

    elif args.command == "export":
        exported = crm.export_full()
        if args.file:
            with open(args.file, "w", encoding="utf-8") as f:
                json.dump(exported, f, indent=2, ensure_ascii=False)
                f.write("\n")
            print(f"Exported full backup to: {args.file}")
        else:
            print(json.dumps(exported, indent=2, ensure_ascii=False))

    elif args.command == "import":
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                imported_data = json.load(f)
            result = crm.import_full(imported_data, merge=args.merge)
        except (OSError, json.JSONDecodeError) as exc:
            result = {"error": f"Could not read full import: {exc}"}
        print(json.dumps(result, indent=2))

    elif args.command == "import-csv":
        result = crm.import_csv(args.file)
        print(json.dumps(result, indent=2))

    elif args.command == "export-csv":
        crm.export_csv(args.file)
        print(f"Exported contacts to: {args.file}")

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
        print(f"\n  {h['summary']}")
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
        fc = crm.forecast(limit=args.limit)
        rows = [{
            "name": row["name"],
            "company": row.get("company") or "-",
            "status": row["status"],
            "probability": f"{row['probability']:.1%}",
            "deal_value": f"${row['deal_value']:,.0f}",
            "weighted_value": f"${row['weighted_value']:,.0f}",
        } for row in fc["contacts"]]
        if not rows:
            print("  No open pipeline contacts")
        else:
            fmt_table(rows, ["name", "company", "status", "probability", "deal_value", "weighted_value"])
        print(f"\n  Weighted Pipeline: ${fc['weighted_pipeline']:,.0f}")

    elif args.command == "propose":
        print(crm.propose_ticks()["markdown"])

    elif args.command == "calibration":
        report = crm.calibration_report()
        print(f"  Scored predictions: {report['scored_predictions']}")
        if report["brier_score"] is None:
            print("  Brier score:        not available (no closed outcomes)")
        else:
            print(f"  Brier score:        {report['brier_score']:.4f}")
        print(f"  Weights source:     {report['weights_source']}")

        if report["by_stage"]:
            print("\n  By stage:")
            print(f"  {'Stage':<20} {'Count':>6} {'Brier':>8}")
            print(f"  {'-'*20} {'-'*6} {'-'*8}")
            for stage, stage_report in report["by_stage"].items():
                print(f"  {stage:<20} {stage_report['count']:>6} "
                      f"{stage_report['brier_score']:>8.4f}")

        print("\n  Reliability:")
        print(f"  {'Predicted':<12} {'Count':>6} {'Mean':>8} {'Actual':>8}")
        print(f"  {'-'*12} {'-'*6} {'-'*8} {'-'*8}")
        for bucket in report["reliability"]:
            mean = (f"{bucket['mean_prediction']:.1%}"
                    if bucket["mean_prediction"] is not None else "-")
            actual = (f"{bucket['actual_win_rate']:.1%}"
                      if bucket["actual_win_rate"] is not None else "-")
            print(f"  {bucket['predicted_decile']:<12} {bucket['count']:>6} "
                  f"{mean:>8} {actual:>8}")

    elif args.command == "queue-stats":
        rows = [{
            **row,
            "act_rate": f"{row['act_rate']:.1%}",
            "move_rate": f"{row['move_rate']:.1%}",
            "multiplier": f"{row['multiplier']:.2f}x",
        } for row in crm.queue_stats()]
        fmt_table(rows, [
            "signal", "surfaced", "acted", "moved", "act_rate",
            "move_rate", "multiplier", "weights_source",
        ])

    elif args.command == "today":
        actions = crm.next_actions(limit=args.limit)
        if not actions:
            print("  No actions for today")
        else:
            rows = [{
                "rank": rank,
                "name": item["contact"],
                "action": item["action"],
                "reason": item["reason"],
            } for rank, item in enumerate(actions, 1)]
            fmt_table(rows, ["rank", "name", "action", "reason"])

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
            window = f">{args.days} days" if args.days is not None else "past tuned cadence"
            print(f"  No stale contacts ({window})")
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

    elif args.command == "deal-update":
        result = crm.update_deal(args.deal_id, name=args.name, value=args.value,
                                  stage=args.stage, notes=args.notes)
        if result:
            print(f"  Updated deal {args.deal_id}: {result['name']} [{result['stage']}]")
        else:
            print(f"  Deal not found: {args.deal_id}")

    elif args.command == "deal-close":
        result = crm.close_deal(args.deal_id, notes=args.notes)
        if result:
            print(f"  Closed deal: {result['name']} ✓")
        else:
            print(f"  Deal not found: {args.deal_id}")

    elif args.command == "deal-delete":
        if crm.delete_deal(args.deal_id):
            print(f"  Deleted deal {args.deal_id}")
        else:
            print(f"  Deal not found: {args.deal_id}")

    elif args.command == "deals":
        if args.identifier:
            deals = crm.deals_for_contact(args.identifier)
        else:
            deals = crm.list_deals(stage=args.stage)
        if not deals:
            print("  No deals found")
        else:
            fmt_table(deals, ["id", "name", "value", "stage", "contact_name", "updated_at"])

    elif args.command == "deal-pipeline":
        pipeline = crm.deal_pipeline()
        print(f"\n  {'Stage':<18} {'Count':>6} {'Total Value':>12}")
        print(f"  {'-'*18} {'-'*6} {'-'*12}")
        for s in pipeline:
            if s["count"] > 0:
                print(f"  {s['stage']:<18} {s['count']:>6} ${s['total_value']:>10,.0f}")
                for d in s["deals"]:
                    print(f"    • {d['name']} ({d['contact']}) — {d.get('value') or '-'}")
        print()

    elif args.command == "merge":
        result = crm.merge_contacts(args.keep, args.drop)
        if result:
            print(
                f"  Merged into: {result['name']} "
                f"(merge id: {result['merge_id']})"
            )
        else:
            print("  One or both contacts not found")

    elif args.command == "unmerge":
        result = crm.undo_merge(args.merge_id)
        if result:
            print(json.dumps(result, default=str, indent=2))
        else:
            print(f"  Merge not found or already undone: {args.merge_id}")

    elif args.command == "automerge":
        result = crm.auto_merge(
            threshold=args.threshold, dry_run=not args.apply
        )
        print(json.dumps(result, default=str, indent=2))

    elif args.command == "doctor":
        print(json.dumps(crm.doctor(fix=args.fix), default=str, indent=2))

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
        multi_hop_paths = crm.intro_paths(args.target) if args.deep or len(intros) < 3 else []
        if not intros and not multi_hop_paths:
            print(f"  No warm intro paths found for: {args.target}")
            print(f"  Tip: Run 'python crm.py ingest all' to populate your knowledge graph from iMessage and Contacts.")
        if intros:
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
        if multi_hop_paths:
            print(f"\n  === Multi-hop Paths ({len(multi_hop_paths)} paths) ===\n")
            for i, intro_path in enumerate(multi_hop_paths, 1):
                route = " -> ".join(intro_path["path"])
                print(f"  {i}. {route}")
                print(f"     Hops:        {intro_path['hops']}")
                print(f"     Strength:    {intro_path['strength']:.2f}")
                print(f"     Explanation: {intro_path['explanation']}")
                print()

    crm.close()


if __name__ == "__main__":
    main()
