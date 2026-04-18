#!/usr/bin/env python3
"""
Pipeline ROI dashboard. Reads crm.db directly, prints MRR/ARR/pipeline/activity
to stdout. Zero dependencies beyond the Python standard library.

Usage:
    python roi.py                  # plain-text dashboard
    python roi.py --json           # machine-readable
    python roi.py --db mydata.db   # override DB path
    CRM_DB=pipeline.db python roi.py
"""
import argparse
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEFAULT_DB = os.environ.get("CRM_DB") or "crm.db"


def parse_mrr(deal_size: str) -> float:
    """Parse a deal_size string like '$5K/mo' or '15000/mo' to monthly USD."""
    if not deal_size:
        return 0.0
    s = deal_size.lower().replace(",", "").replace("$", "")
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return 0.0
    n = float(m.group(1))
    if "k" in s:
        n *= 1000
    return n


def load(conn):
    contacts = list(conn.execute(
        "SELECT id, name, email, company, status, deal_size, last_contacted FROM contacts"
    ).fetchall())
    activity = list(conn.execute(
        "SELECT contact_id, type, summary, created_at FROM activity ORDER BY created_at DESC"
    ).fetchall())
    return contacts, activity


def compute(contacts, activity, active_statuses=("active_customer", "active_self_serve")):
    by_status = {}
    mrr = 0.0
    for c in contacts:
        status = c[4] or "unknown"
        by_status.setdefault(status, []).append(c)
        if status in active_statuses:
            mrr += parse_mrr(c[5])

    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    recent = [a for a in activity if (a[3] or "") >= cutoff]
    recent_contacts = {a[0] for a in recent}

    return {
        "mrr": mrr,
        "arr": mrr * 12,
        "total_contacts": len(contacts),
        "by_status": {k: len(v) for k, v in by_status.items()},
        "by_status_detail": by_status,
        "activity_7d": len(recent),
        "contacts_touched_7d": len(recent_contacts),
    }


def render(stats, contacts, target_arr=1_000_000):
    lines = []
    lines.append("=" * 60)
    lines.append(f"  CRM ROI  ·  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"  MRR:              ${stats['mrr']:>10,.0f}/mo")
    lines.append(f"  ARR:              ${stats['arr']:>10,.0f}/yr")
    if target_arr:
        gap = target_arr - stats['arr']
        lines.append(f"  Target ARR:       ${target_arr:>10,.0f}/yr")
        lines.append(f"  Gap:              ${gap:>10,.0f}/yr  ({gap/12:,.0f}/mo)")
    lines.append("")
    lines.append(f"  Total contacts:   {stats['total_contacts']}")
    lines.append(f"  Activity (7d):    {stats['activity_7d']} events across {stats['contacts_touched_7d']} contacts")
    lines.append("")
    lines.append("  PIPELINE BY STATUS")
    lines.append("  " + "-" * 56)
    for status, count in sorted(stats['by_status'].items(), key=lambda x: -x[1]):
        names = [c[1] for c in stats['by_status_detail'][status][:3]]
        tail = ", ".join(names)
        if count > 3:
            tail += f", +{count-3} more"
        lines.append(f"  {status:<22} {count:>3}  {tail}")
    lines.append("")

    actives = [c for c in contacts if c[4] in ("active_customer", "active_self_serve")]
    if actives:
        lines.append("  ACTIVE CUSTOMERS")
        lines.append("  " + "-" * 56)
        for c in sorted(actives, key=lambda x: -parse_mrr(x[5])):
            m = parse_mrr(c[5])
            company = c[3] or c[1]
            lines.append(f"  ${m:>8,.0f}/mo  {company:<24} {c[1]}")
        lines.append("")

    lines.append("=" * 60)
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--target-arr", type=int, default=1_000_000)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    contacts, activity = load(conn)
    stats = compute(contacts, activity)

    if args.json:
        out = {k: v for k, v in stats.items() if k != "by_status_detail"}
        print(json.dumps(out, indent=2))
    else:
        print(render(stats, contacts, args.target_arr))


if __name__ == "__main__":
    main()
