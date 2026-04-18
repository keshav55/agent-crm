#!/usr/bin/env python3
"""
Post-send logger. Run this after you send a cold email to record the send
in the CRM and transition the contact from `prospect` to `contacted`.

If the contact does not exist yet, it is created with the minimal fields
passed on the command line.

Usage:
    python logged.py jane@acme.com "built something, want your take"
    python logged.py jane@acme.com "built something" --name "Jane Doe" --company "Acme"
    python logged.py jane@acme.com "quick nudge" --followup
    python logged.py jane@acme.com "subject" --db pipeline.db
    CRM_DB=pipeline.db python logged.py jane@acme.com "subject"
"""
import argparse
import os
import sqlite3
from datetime import datetime, timezone

DEFAULT_DB = os.environ.get("CRM_DB") or "crm.db"


def log_send(email: str, subject: str, name: str = "", company: str = "",
             followup: bool = False, db_path: str = DEFAULT_DB) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    row = conn.execute("SELECT id, status FROM contacts WHERE lower(email)=lower(?)", (email,)).fetchone()
    now = datetime.now(timezone.utc).isoformat(sep=" ", timespec="seconds").replace("+00:00", "")
    today = datetime.now().strftime("%Y-%m-%d")

    if row is None:
        if not name:
            name = email.split("@")[0].replace(".", " ").title()
        conn.execute(
            """INSERT INTO contacts (name, email, company, status, last_contacted, created_at, updated_at)
               VALUES (?, ?, ?, 'contacted', ?, ?, ?)""",
            (name, email, company, today, now, now),
        )
        cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        new_contact = True
        prev_status = None
    else:
        cid = row["id"]
        new_contact = False
        prev_status = row["status"]

    act_type = "email_followup" if followup else "email_sent"
    summary = f"Sent: {subject}" if not followup else f"Follow-up: {subject}"
    conn.execute(
        "INSERT INTO activity (contact_id, type, summary, created_at) VALUES (?, ?, ?, ?)",
        (cid, act_type, summary, now),
    )

    new_status = prev_status
    if not followup and prev_status == "prospect":
        conn.execute(
            "UPDATE contacts SET status='contacted', last_contacted=?, updated_at=? WHERE id=?",
            (today, now, cid),
        )
        new_status = "contacted"
    else:
        conn.execute(
            "UPDATE contacts SET last_contacted=?, updated_at=? WHERE id=?",
            (today, now, cid),
        )

    conn.commit()
    conn.close()

    return {
        "contact_id": cid,
        "email": email,
        "new_contact": new_contact,
        "prev_status": prev_status,
        "new_status": new_status,
        "summary": summary,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("email")
    ap.add_argument("subject")
    ap.add_argument("--name", default="")
    ap.add_argument("--company", default="")
    ap.add_argument("--followup", action="store_true")
    ap.add_argument("--db", default=DEFAULT_DB)
    args = ap.parse_args()

    result = log_send(args.email, args.subject, args.name, args.company,
                      args.followup, args.db)

    action = "Created and logged" if result["new_contact"] else "Logged"
    status_msg = (
        f"status: {result['prev_status']} -> {result['new_status']}"
        if result["prev_status"] != result["new_status"]
        else f"status: {result['new_status']}"
    )
    print(f"{action} send to {result['email']}")
    print(f"  {status_msg}")
    print(f"  activity: {result['summary']}")


if __name__ == "__main__":
    main()
