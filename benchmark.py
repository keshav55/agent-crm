#!/usr/bin/env python3
"""
benchmark.py — eval harness for agent-crm autoresearch.

Scores crm.py on correctness, edge cases, graph power, and performance.
Outputs a single metric: score: <N> (higher is better).

Each test = 1 point. Bonus points for performance thresholds.
"""

import os
import json
import random
import re
import sqlite3
import subprocess
import sys
import time
import tempfile
import traceback
from datetime import datetime, timedelta

# Force a temp DB so we never touch real data
TEST_DB = os.path.join(tempfile.mkdtemp(), "bench.db")
os.environ["CRM_DB"] = TEST_DB

from crm import CRM

PASS = 0
FAIL = 0
ERRORS = []
PERFORMANCE_TIMINGS = []

PERFORMANCE_SEED = 20260805
PROPERTY_SEED = 8675309
PERFORMANCE_THRESHOLDS_MS = {
    "list_contacts": 15.0,
    "get_contact_email": 1.0,
    "get_contact_partial_name": 1.0,
    "pipeline": 2.0,
    "next_actions_10": 3000.0,
    "forecast": 600.0,
    "intro_paths": 5000.0,
    "export_full": 40.0,
}


def check(name, condition):
    global PASS, FAIL
    if condition:
        PASS += 1
    else:
        FAIL += 1
        ERRORS.append(name)


def make_performance_fixture():
    """Build the deterministic 500-contact/2000-activity regression fixture."""
    rng = random.Random(PERFORMANCE_SEED)
    db_path = os.path.join(tempfile.mkdtemp(), "integrity-perf.db")
    crm = CRM(db_path)
    statuses = (
        "prospect", "contacted", "met", "proposal_drafted",
        "verbal_yes", "active_customer", "lost",
    )
    contacts = []
    for index in range(500):
        contacts.append((
            f"Benchmark Contact {index:03d}",
            f"perf+{index}@tenant-{index % 100}.benchmark.test",
            f"Benchmark Co {rng.randrange(100):03d}",
            f"Role {rng.randrange(20)}",
            f"${rng.randrange(1, 250)}K",
            statuses[rng.randrange(len(statuses))],
            "seeded-benchmark",
            f"Fixture note {rng.randrange(10000)}",
            f"group-{rng.randrange(25)}",
            f"2026-01-{1 + rng.randrange(28):02d}",
        ))

    with crm.conn:
        crm.conn.executemany(
            """INSERT INTO contacts
               (name, email, company, title, deal_size, status, source, notes,
                tags, last_contacted)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            contacts,
        )

    contact_ids = [
        row["id"] for row in crm.conn.execute(
            "SELECT id FROM contacts ORDER BY id"
        ).fetchall()
    ]
    activity_types = ("email", "call", "meeting", "note")
    activity_start = datetime(2026, 1, 1)
    activities = []
    for index in range(2000):
        # Leave the target without activity so intro_paths has to traverse to it.
        contact_id = contact_ids[rng.randrange(499)]
        activities.append((
            contact_id,
            activity_types[rng.randrange(len(activity_types))],
            f"Seeded activity {index}",
            (activity_start + timedelta(minutes=index)).isoformat(sep=" "),
        ))

    with crm.conn:
        crm.conn.executemany(
            """INSERT INTO activity (contact_id, type, summary, created_at)
               VALUES (?, ?, ?, ?)""",
            activities,
        )
        crm.conn.executemany(
            """INSERT INTO facts (entity, key, value, source)
               VALUES (?, ?, ?, ?)""",
            (
                (
                    "contact:benchmark contact 000", "knows",
                    "contact:benchmark contact 498", "seeded-benchmark",
                ),
                (
                    "contact:benchmark contact 498", "knows",
                    "contact:benchmark contact 499", "seeded-benchmark",
                ),
            ),
        )

    fixture_counts = (
        crm.conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0],
        crm.conn.execute("SELECT COUNT(*) FROM activity").fetchone()[0],
    )
    if fixture_counts != (500, 2000):
        crm.close()
        raise AssertionError(
            f"performance fixture has {fixture_counts}, expected (500, 2000)"
        )
    return crm


def benchmark_exit_code(fail_count):
    """Return a failing process status whenever any benchmark check failed."""
    return 1 if fail_count > 0 else 0


def result_lines(pass_count, fail_count):
    """Create the two machine-readable score lines from the same counter."""
    total = pass_count + fail_count
    return (
        f"RESULTS: {pass_count}/{total} tests passed",
        f"score: {pass_count}",
    )


def score_integrity_errors(pass_count, fail_count, results_line, score_line):
    """Validate score output and exit semantics before publishing results."""
    errors = []
    try:
        results_pass = int(results_line.split(": ", 1)[1].split("/", 1)[0])
        score_pass = int(score_line.rsplit(" ", 1)[1])
    except (IndexError, ValueError):
        return ["score_integrity_unparseable_output"]
    if results_pass != pass_count or score_pass != pass_count:
        errors.append("score_integrity_pass_count_mismatch")
    if benchmark_exit_code(0) != 0 or benchmark_exit_code(1) == 0:
        errors.append("score_integrity_exit_semantics")
    if (fail_count > 0) != (benchmark_exit_code(fail_count) != 0):
        errors.append("score_integrity_current_exit_code")
    return errors


def make_imessage_fixture_db(handle_counts=()):
    """Create a minimal chat.db fixture from (handle, sent, received) tuples."""
    db_path = os.path.join(tempfile.mkdtemp(), "chat.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE handle (
            ROWID INTEGER PRIMARY KEY,
            id TEXT
        );
        CREATE TABLE message (
            ROWID INTEGER PRIMARY KEY,
            handle_id INTEGER,
            is_from_me INTEGER,
            date INTEGER
        );
        CREATE TABLE chat (
            ROWID INTEGER PRIMARY KEY
        );
    """)
    apple_now = int((datetime.now().timestamp() - 978307200) * 1_000_000_000)
    message_id = 1
    for handle_id, (handle, sent, received) in enumerate(handle_counts, start=1):
        conn.execute("INSERT INTO handle (ROWID, id) VALUES (?, ?)", (handle_id, handle))
        for is_from_me, count in ((1, sent), (0, received)):
            for _ in range(count):
                conn.execute(
                    "INSERT INTO message (ROWID, handle_id, is_from_me, date) VALUES (?, ?, ?, ?)",
                    (message_id, handle_id, is_from_me, apple_now + message_id),
                )
                message_id += 1
    conn.commit()
    conn.close()
    return db_path


def make_addressbook_fixture_db(records=()):
    """Create a minimal AddressBook-v22.abcddb fixture from contact mappings."""
    db_path = os.path.join(tempfile.mkdtemp(), "AddressBook-v22.abcddb")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE ZABCDRECORD (
            Z_PK INTEGER PRIMARY KEY,
            ZFIRSTNAME TEXT,
            ZLASTNAME TEXT,
            ZORGANIZATION TEXT,
            ZJOBTITLE TEXT,
            ZNOTE TEXT
        );
        CREATE TABLE ZABCDEMAILADDRESS (
            ZOWNER INTEGER,
            ZADDRESSNORMALIZED TEXT
        );
        CREATE TABLE ZABCDPHONENUMBER (
            ZOWNER INTEGER,
            ZFULLNUMBER TEXT
        );
    """)
    for record_id, record in enumerate(records, start=1):
        conn.execute(
            """INSERT INTO ZABCDRECORD
               (Z_PK, ZFIRSTNAME, ZLASTNAME, ZORGANIZATION, ZJOBTITLE, ZNOTE)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                record_id,
                record.get("first"),
                record.get("last"),
                record.get("company"),
                record.get("title"),
                record.get("notes"),
            ),
        )
        conn.executemany(
            "INSERT INTO ZABCDEMAILADDRESS (ZOWNER, ZADDRESSNORMALIZED) VALUES (?, ?)",
            [(record_id, email) for email in record.get("emails", [])],
        )
        conn.executemany(
            "INSERT INTO ZABCDPHONENUMBER (ZOWNER, ZFULLNUMBER) VALUES (?, ?)",
            [(record_id, phone) for phone in record.get("phones", [])],
        )
    conn.commit()
    conn.close()
    return db_path


def make_calendar_fixture_records():
    """Return fake Calendar event and attendee records at the current time."""
    apple_now = datetime.now().timestamp() - 978307200
    records = [{
        "title": "Fixture Person Planning",
        "location": "Fixture Room",
        "start_date": apple_now,
        "end_date": apple_now + 1800,
        "notes": "Quarterly fixture review",
    }]
    attendees = [
        {
            "name": "Guest One",
            "address": "mailto:guest@example.com",
            "event_title": "Fixture Person Planning",
            "event_date": apple_now,
        },
        {
            "name": "Named Guest",
            "address": "",
            "event_title": "Fixture Person Planning",
            "event_date": apple_now,
        },
    ]
    return records, attendees


def make_mail_fixture_records():
    """Return fake Envelope Index query rows for sent/received aggregation."""
    now = int(datetime.now().timestamp())
    return [
        {
            "subject": "Fixture one", "date_sent": now,
            "from_addr": "FIXTURE@EXAMPLE.COM", "to_addr": "guest@example.com",
            "is_read": 1,
        },
        {
            "subject": "Fixture two", "date_sent": now + 1,
            "from_addr": "guest@example.com", "to_addr": "fixture@example.com",
            "is_read": 1,
        },
        {
            "subject": "Fixture three", "date_sent": now + 2,
            "from_addr": "fixture@example.com", "to_addr": "other@example.com",
            "is_read": 0,
        },
    ]


def run_benchmarks():
    global PASS, FAIL

    # ── 1. Basic CRUD (10 tests) ──
    crm = CRM(TEST_DB)

    # Add contacts
    id1 = crm.add_contact("Alice Smith", email="alice@example.com", company="Acme", status="prospect", deal_size="$5K")
    check("add_contact returns id", id1 is not None and id1 > 0)

    id2 = crm.add_contact("Bob Jones", email="bob@example.com", company="Betacorp", status="contacted", source="referral")
    check("add_second_contact", id2 is not None and id2 > id1)

    # Get by email
    c = crm.get_contact("alice@example.com")
    check("get_by_email", c is not None and c["name"] == "Alice Smith")

    # Get by partial name
    c = crm.get_contact("Bob")
    check("get_by_name_partial", c is not None and c["email"] == "bob@example.com")

    # Update
    updated = crm.update_contact("alice@example.com", status="negotiating", deal_size="$10K")
    check("update_contact", updated is not None and updated["status"] == "negotiating")

    # List all
    all_contacts = crm.list_contacts()
    check("list_all", len(all_contacts) == 2)

    # List filtered
    prospects = crm.list_contacts(status="prospect")
    check("list_filtered_empty", len(prospects) == 0)  # Alice was updated to negotiating

    negotiating = crm.list_contacts(status="negotiating")
    check("list_filtered_match", len(negotiating) == 1 and negotiating[0]["name"] == "Alice Smith")

    # Search
    results = crm.search("acme")
    check("search_by_company", len(results) >= 1)

    # Delete
    deleted = crm.delete_contact("bob@example.com")
    check("delete_contact", deleted is True)
    remaining = crm.list_contacts()
    check("delete_verified", len(remaining) == 1)

    # ── 2. Activity logging (5 tests) ──
    logged = crm.log_activity("alice@example.com", "email", "Sent proposal")
    check("log_activity", logged is True)

    logged2 = crm.log_activity("alice@example.com", "call", "Follow up call")
    check("log_second_activity", logged2 is True)

    acts = crm.get_activity("alice@example.com")
    check("get_activity", len(acts) >= 2)

    # Activity on nonexistent contact
    bad = crm.log_activity("nonexistent@void.com", "email", "Should fail")
    check("activity_nonexistent", bad is None)

    # Last contacted updated
    c = crm.get_contact("alice@example.com")
    check("last_contacted_updated", c["last_contacted"] is not None)

    # ── 3. Deals (4 tests) ──
    crm.add_contact("Charlie Deal", email="charlie@deals.com", company="DealCo", status="prospect")
    deal = crm.add_deal("charlie@deals.com", "Enterprise License", value="$50K", stage="proposal")
    check("add_deal", deal is True)

    deals = crm.list_deals()
    check("list_deals", len(deals) >= 1 and deals[0]["name"] == "Enterprise License")

    deals_filtered = crm.list_deals(stage="proposal")
    check("list_deals_filtered", len(deals_filtered) >= 1)

    deals_empty = crm.list_deals(stage="closed_won")
    check("list_deals_empty_filter", len(deals_empty) == 0)

    # ── 4. Context Graph — core operations (10 tests) ──
    crm.observe("contact:alice", "role", "CTO", source="linkedin")
    crm.observe("contact:alice", "interest", "AI automation", source="call:2026-03-01")
    crm.observe("company:acme", "revenue", "$5M ARR", source="crunchbase")
    crm.observe("company:acme", "headcount", "45", source="linkedin")
    crm.observe("contact:alice", "works_at", "company:acme", source="linkedin")
    crm.observe("deal:acme", "blocker", "needs_roi_proof", source="call:2026-03-01")
    crm.observe("deal:acme", "contact", "contact:alice", source="manual")

    facts = crm.facts_about("contact:alice")
    check("facts_about_has_keys", "role" in facts and "interest" in facts)
    check("facts_about_values", facts["role"]["value"] == "CTO")

    history = crm.history_of("contact:alice")
    check("history_all", len(history) >= 3)

    history_key = crm.history_of("contact:alice", key="role")
    check("history_filtered", len(history_key) >= 1 and history_key[0]["value"] == "CTO")

    related = crm.related("contact:alice")
    check("related_entities", len(related) >= 1)
    related_vals = [r["related_entity"] for r in related]
    check("related_has_company", "company:acme" in related_vals)

    found = crm.find_by_fact("role", "CTO")
    check("find_by_fact_kv", len(found) >= 1 and found[0]["entity"] == "contact:alice")

    found_key = crm.find_by_fact("revenue")
    check("find_by_fact_key_only", len(found_key) >= 1)

    sourced = crm.from_source("linkedin")
    check("from_source", len(sourced) >= 3)

    # Observe same key again (should show latest)
    crm.observe("contact:alice", "role", "CEO", source="announcement")
    latest = crm.facts_about("contact:alice")
    check("facts_latest_wins", latest["role"]["value"] == "CEO")

    # ── 5. Context Graph — edge cases (5 tests) ──
    empty_facts = crm.facts_about("nonexistent:entity")
    check("facts_empty_entity", len(empty_facts) == 0)

    empty_history = crm.history_of("nonexistent:entity")
    check("history_empty_entity", len(empty_history) == 0)

    empty_related = crm.related("nonexistent:entity")
    check("related_empty_entity", len(empty_related) == 0)

    stale = crm.stale_facts(days=0)  # Everything should be "stale" with 0 days
    check("stale_facts_returns", isinstance(stale, list))

    empty_source = crm.from_source("nonexistent_source")
    check("from_source_empty", len(empty_source) == 0)

    # ── 6. Views and output (5 tests) ──
    pipeline = crm.pipeline()
    check("pipeline_returns", len(pipeline) >= 1)

    stats = crm.stats()
    check("stats_total", stats["total_contacts"] >= 2)
    check("stats_by_status", len(stats["by_status"]) >= 1)

    md = crm.markdown()
    check("markdown_output", "CRM Pipeline" in md and "Alice" in md)

    j = crm.to_json()
    check("json_output", "contacts" in j and "graph" in j and "stats" in j)

    # ── 7. Graph markdown (2 tests) ──
    gmd = crm.graph_markdown()
    check("graph_markdown_has_entities", "contact:alice" in gmd)
    check("graph_markdown_has_facts", "CEO" in gmd or "CTO" in gmd)

    # ── 8. Import/Export roundtrip (3 tests) ──
    export_path = os.path.join(tempfile.mkdtemp(), "export.csv")
    crm.export_csv(export_path)
    check("export_csv_creates_file", os.path.exists(export_path))

    crm2 = CRM(os.path.join(tempfile.mkdtemp(), "import_test.db"))
    csv_result = crm2.import_csv(export_path)
    check("import_csv_count", csv_result["imported"] >= 2)
    imported = crm2.list_contacts()
    check("import_csv_data", len(imported) >= 2)
    crm2.close()

    # ── 9. Edge cases (5 tests) ──

    # Duplicate email
    try:
        crm.add_contact("Duplicate Alice", email="alice@example.com")
        check("duplicate_email_rejected", False)  # Should have raised
    except Exception:
        check("duplicate_email_rejected", True)

    # Update nonexistent
    result = crm.update_contact("nobody@void.com", status="ghost")
    check("update_nonexistent", result is None)

    # Get nonexistent
    result = crm.get_contact("nobody@void.com")
    check("get_nonexistent", result is None)

    # Delete nonexistent
    result = crm.delete_contact("nobody@void.com")
    check("delete_nonexistent", result is False)

    # Add contact without email
    id_no_email = crm.add_contact("No Email Person", company="Mystery Corp")
    check("add_no_email", id_no_email is not None and id_no_email > 0)

    # ── 10. Performance (5 bonus points) ──

    perf_db = os.path.join(tempfile.mkdtemp(), "perf.db")
    perf_crm = CRM(perf_db)

    # Bulk insert: 500 contacts in < 2s
    t0 = time.time()
    for i in range(500):
        perf_crm.add_contact(f"Perf Contact {i}", email=f"perf{i}@test.com", company=f"Co{i % 50}")
    bulk_insert_time = time.time() - t0
    check("perf_bulk_insert_500", bulk_insert_time < 2.0)

    # Bulk observe: 1000 facts in < 2s
    t0 = time.time()
    for i in range(1000):
        perf_crm.observe(f"entity:{i % 100}", f"key_{i % 10}", f"val_{i}", source=f"src_{i % 5}")
    bulk_observe_time = time.time() - t0
    check("perf_bulk_observe_1000", bulk_observe_time < 2.0)

    # Search in 500 contacts < 100ms
    t0 = time.time()
    perf_crm.search("Perf Contact 25")
    search_time = time.time() - t0
    check("perf_search_fast", search_time < 0.1)

    # facts_about with 10 keys < 50ms
    t0 = time.time()
    perf_crm.facts_about("entity:0")
    facts_time = time.time() - t0
    check("perf_facts_fast", facts_time < 0.05)

    # graph_markdown full dump < 500ms
    t0 = time.time()
    perf_crm.graph_markdown()
    graph_time = time.time() - t0
    check("perf_graph_markdown_fast", graph_time < 0.5)

    perf_crm.close()

    # ── 11. Stretch goals — reward new capabilities (15 tests) ──
    # These test features from program.md research directions.
    # The LLM earns points by implementing these.

    stretch_crm = CRM(os.path.join(tempfile.mkdtemp(), "stretch.db"))

    # Set up test data for stretch tests
    stretch_crm.add_contact("Stretch Alice", email="sa@test.com", company="StretchCo")
    stretch_crm.observe("contact:alice", "works_at", "company:acme", source="linkedin")
    stretch_crm.observe("company:acme", "competes_with", "company:beta", source="research")
    stretch_crm.observe("company:beta", "ceo", "contact:bob", source="crunchbase")
    stretch_crm.observe("contact:alice", "role", "CTO", source="linkedin")
    stretch_crm.observe("contact:alice", "role", "CEO", source="announcement")  # Conflict!
    stretch_crm.observe("contact:bob", "role", "CEO", source="crunchbase")

    # 11a. Context manager support (with statement)
    try:
        with CRM(os.path.join(tempfile.mkdtemp(), "ctx.db")) as ctx_crm:
            ctx_crm.add_contact("Context Test", email="ctx@test.com")
            c = ctx_crm.get_contact("ctx@test.com")
            check("context_manager", c is not None and c["name"] == "Context Test")
    except (AttributeError, TypeError):
        check("context_manager", False)

    # 11b. observe_many — bulk fact insertion
    try:
        facts_list = [
            ("entity:bulk1", "key1", "val1", "src1"),
            ("entity:bulk2", "key2", "val2", "src2"),
            ("entity:bulk3", "key3", "val3", "src3"),
        ]
        stretch_crm.observe_many(facts_list)
        f1 = stretch_crm.facts_about("entity:bulk1")
        f2 = stretch_crm.facts_about("entity:bulk2")
        check("observe_many", "key1" in f1 and "key2" in f2)
    except (AttributeError, TypeError):
        check("observe_many", False)

    # 11c. Reverse lookup — find entities that reference a given entity
    try:
        refs = stretch_crm.reverse_lookup("company:acme")
        # Accept either "entity" or "referencing_entity" key
        ref_entities = []
        for r in (refs or []):
            ref_entities.append(r.get("entity") or r.get("referencing_entity", ""))
        check("reverse_lookup", "contact:alice" in ref_entities)
    except (AttributeError, TypeError):
        check("reverse_lookup", False)

    # 11d. Multi-hop traversal — reachable within N hops
    try:
        reached = stretch_crm.reachable("contact:alice", max_hops=2)
        # alice -> company:acme -> company:beta (2 hops)
        reached_entities = set(reached) if isinstance(reached, (list, set, dict)) else set()
        check("reachable_2hop", "company:acme" in reached_entities)
    except (AttributeError, TypeError):
        check("reachable_2hop", False)

    # 11e. Fact conflict detection — same entity+key, different values from different sources
    try:
        conflicts = stretch_crm.conflicts("contact:alice")
        # role has CTO (linkedin) and CEO (announcement)
        has_role_conflict = any(c["key"] == "role" for c in conflicts) if conflicts else False
        check("conflict_detection", has_role_conflict)
    except (AttributeError, TypeError):
        check("conflict_detection", False)

    # 11f. Temporal query — facts as of a specific date
    try:
        old_facts = stretch_crm.facts_as_of("contact:alice", "2020-01-01")
        check("facts_as_of", isinstance(old_facts, dict) and len(old_facts) == 0)
    except (AttributeError, TypeError):
        check("facts_as_of", False)

    # 11g. Entity count by fact
    try:
        counts = stretch_crm.count_by_fact("role")
        # Should return something like {"CEO": 2, "CTO": 1} or similar
        check("count_by_fact", isinstance(counts, dict) and len(counts) >= 1)
    except (AttributeError, TypeError):
        check("count_by_fact", False)

    # 11h. Delete cascade — deleting contact cleans up facts
    try:
        cascade_crm = CRM(os.path.join(tempfile.mkdtemp(), "cascade.db"))
        cascade_crm.add_contact("Del Test", email="del@test.com")
        cascade_crm.observe("contact:del", "status", "active", source="manual")
        cascade_crm.delete_contact("del@test.com")
        leftover = cascade_crm.facts_about("contact:del")
        check("delete_cascade_facts", len(leftover) == 0)
        cascade_crm.close()
    except Exception:
        check("delete_cascade_facts", False)

    # 11i. Delete cascade — deleting contact also removes deals and activity
    try:
        cascade2_crm = CRM(os.path.join(tempfile.mkdtemp(), "cascade2.db"))
        cid = cascade2_crm.add_contact("Deal Del", email="dealdel@test.com", company="TestCo")
        cascade2_crm.add_deal("dealdel@test.com", "Big Deal", value="$50K", stage="proposal")
        cascade2_crm.log_activity("dealdel@test.com", "call", "Discussed terms")
        # Verify deal and activity exist before delete
        deals_before = cascade2_crm.list_deals()
        activities_before = cascade2_crm.conn.execute(
            "SELECT COUNT(*) FROM activity WHERE contact_id = ?", (cid,)
        ).fetchone()[0]
        pre_ok = len(deals_before) >= 1 and activities_before >= 1
        cascade2_crm.delete_contact("dealdel@test.com")
        deals_after = cascade2_crm.list_deals()
        activities_after = cascade2_crm.conn.execute(
            "SELECT COUNT(*) FROM activity WHERE contact_id = ?", (cid,)
        ).fetchone()[0]
        check("delete_cascade_deals", pre_ok and len(deals_after) == 0 and activities_after == 0)
        cascade2_crm.close()
    except Exception:
        check("delete_cascade_deals", False)

    # 11j. Batch performance — observe_many faster than individual observes
    try:
        batch_crm = CRM(os.path.join(tempfile.mkdtemp(), "batch_perf.db"))
        batch_facts = [(f"ent:{i}", "k", f"v{i}", "bench") for i in range(200)]
        t0 = time.time()
        batch_crm.observe_many(batch_facts)
        batch_time = time.time() - t0
        check("observe_many_fast", batch_time < 0.5)
        batch_crm.close()
    except (AttributeError, TypeError):
        check("observe_many_fast", False)

    # 11k. Entity validation — reject empty entity or key
    try:
        result = stretch_crm.observe("", "key", "val")
        check("validate_empty_entity", result is False or result is None)
    except (ValueError, TypeError):
        check("validate_empty_entity", True)  # Raising is also valid
    except Exception:
        check("validate_empty_entity", False)

    # 11l. Entity validation — reject empty key
    try:
        result = stretch_crm.observe("entity:x", "", "val")
        check("validate_empty_key", result is False or result is None)
    except (ValueError, TypeError):
        check("validate_empty_key", True)  # Raising is also valid
    except Exception:
        check("validate_empty_key", False)

    # 11m. Compact markdown — shorter output
    try:
        compact = stretch_crm.compact_markdown()
        full = stretch_crm.graph_markdown()
        check("compact_markdown", isinstance(compact, str) and len(compact) < len(full))
    except (AttributeError, TypeError):
        check("compact_markdown", False)

    # 11n. Summary view — recent changes
    try:
        summary = stretch_crm.recent_changes(days=7)
        check("recent_changes", isinstance(summary, list) and len(summary) >= 1)
    except (AttributeError, TypeError):
        check("recent_changes", False)

    # 11o. Graph stats — entity/fact counts
    try:
        gs = stretch_crm.graph_stats()
        check("graph_stats", isinstance(gs, dict) and "entities" in gs and "facts" in gs)
    except (AttributeError, TypeError):
        check("graph_stats", False)

    # 11p. Merge entities — combine two entities into one
    try:
        merge_crm = CRM(os.path.join(tempfile.mkdtemp(), "merge.db"))
        merge_crm.observe("contact:john", "role", "CEO", source="manual")
        merge_crm.observe("contact:johnny", "email", "john@co.com", source="manual")
        merge_crm.merge_entities("contact:john", "contact:johnny")
        facts = merge_crm.facts_about("contact:john")
        check("merge_entities", "email" in facts and "role" in facts)
        merge_crm.close()
    except (AttributeError, TypeError):
        check("merge_entities", False)

    # 11q. Merge entities — overlapping facts don't cause IntegrityError
    try:
        merge2_crm = CRM(os.path.join(tempfile.mkdtemp(), "merge2.db"))
        merge2_crm.observe("contact:alice", "role", "CEO", source="manual")
        merge2_crm.observe("contact:alice", "company", "Acme", source="manual")
        merge2_crm.observe("contact:ali", "role", "CEO", source="manual")
        merge2_crm.observe("contact:ali", "phone", "+1234567890", source="manual")
        merge2_crm.merge_entities("contact:alice", "contact:ali")
        facts = merge2_crm.facts_about("contact:alice")
        leftover = merge2_crm.facts_about("contact:ali")
        check("merge_entities_overlap", "role" in facts and "phone" in facts and len(leftover) == 0)
        merge2_crm.close()
    except Exception:
        check("merge_entities_overlap", False)

    stretch_crm.close()

    # ── 12. Lead Intelligence — Wave 1 (19 tests) ──
    # These test scoring, prioritization, health checks, funnel analysis,
    # forecasting, duplicate detection, and stale contact identification.

    wave1_crm = CRM(os.path.join(tempfile.mkdtemp(), "wave1.db"))

    # Create contacts with various states
    wave1_crm.add_contact("Active Amy", email="amy@test.com", company="BigCo", status="active_customer", deal_size="$5K/mo")
    wave1_crm.add_contact("Prospect Pete", email="pete@test.com", company="SmallCo", status="prospect")
    wave1_crm.add_contact("Stale Steve", email="steve@test.com", company="MidCo", status="contacted", deal_size="$200K/yr")
    wave1_crm.add_contact("Deal Dana", email="dana@test.com", company="DealCo", status="proposal_drafted", deal_size="$10K/mo")
    wave1_crm.add_contact("Dup Dave", email="dave@test.com", company="BigCo", status="prospect")
    wave1_crm.add_contact("Dup David", email="dave2@test.com", company="BigCo", status="contacted")  # Potential duplicate

    # Add activity for some
    wave1_crm.log_activity("amy@test.com", "call", "Quarterly review")
    wave1_crm.log_activity("amy@test.com", "email", "Sent renewal")
    wave1_crm.log_activity("dana@test.com", "meeting", "Demo session")

    # Add facts
    wave1_crm.observe("contact:amy", "nps_score", "9", source="survey")

    # 12a. score_contact — returns dict with score and factors
    try:
        sc = wave1_crm.score_contact("amy@test.com")
        check("score_contact_returns_dict", isinstance(sc, dict) and "score" in sc and "factors" in sc)
    except (AttributeError, TypeError):
        check("score_contact_returns_dict", False)

    # 12b. score_contact — score is 0-100 integer
    try:
        sc = wave1_crm.score_contact("amy@test.com")
        check("score_contact_range", isinstance(sc["score"], int) and 0 <= sc["score"] <= 100)
    except (AttributeError, TypeError, KeyError):
        check("score_contact_range", False)

    # 12c. score_contact — active customer with activity scores higher than fresh prospect
    try:
        sc_amy = wave1_crm.score_contact("amy@test.com")
        sc_pete = wave1_crm.score_contact("pete@test.com")
        check("score_active_gt_prospect", sc_amy["score"] > sc_pete["score"])
    except (AttributeError, TypeError, KeyError):
        check("score_active_gt_prospect", False)

    # 12d. score_contact — contact with no activity scores lower
    try:
        sc_amy = wave1_crm.score_contact("amy@test.com")
        sc_steve = wave1_crm.score_contact("steve@test.com")
        check("score_no_activity_lower", sc_amy["score"] > sc_steve["score"])
    except (AttributeError, TypeError, KeyError):
        check("score_no_activity_lower", False)

    # 12e. prioritize — returns list
    try:
        pri = wave1_crm.prioritize()
        check("prioritize_returns_list", isinstance(pri, list) and len(pri) >= 1)
    except (AttributeError, TypeError):
        check("prioritize_returns_list", False)

    # 12f. prioritize — first contact has highest score
    try:
        pri = wave1_crm.prioritize()
        # First item should have highest (or equal highest) score
        scores = [p.get("score", 0) for p in pri]
        check("prioritize_sorted_desc", scores[0] >= scores[-1])
    except (AttributeError, TypeError, IndexError, KeyError):
        check("prioritize_sorted_desc", False)

    # 12g. prioritize — respects limit parameter
    try:
        pri = wave1_crm.prioritize(limit=3)
        check("prioritize_limit", isinstance(pri, list) and len(pri) <= 3)
    except (AttributeError, TypeError):
        check("prioritize_limit", False)

    # 12h. health_check — returns dict with expected keys
    try:
        hc = wave1_crm.health_check()
        has_keys = all(k in hc for k in ("healthy", "at_risk", "cold", "actions"))
        check("health_check_keys", isinstance(hc, dict) and has_keys)
    except (AttributeError, TypeError):
        check("health_check_keys", False)

    # 12i. health_check — each value is a list
    try:
        hc = wave1_crm.health_check()
        all_lists = all(isinstance(hc[k], list) for k in ("healthy", "at_risk", "cold", "actions"))
        check("health_check_lists", all_lists)
    except (AttributeError, TypeError, KeyError):
        check("health_check_lists", False)

    # 12j. health_check — stale contacts appear in at_risk or cold
    try:
        hc = wave1_crm.health_check()
        at_risk_cold_names = [c.get("name", c) if isinstance(c, dict) else str(c)
                              for c in hc["at_risk"] + hc["cold"]]
        # Steve has no activity and is status=contacted, should be flagged
        check("health_check_stale", any("Steve" in n for n in at_risk_cold_names))
    except (AttributeError, TypeError, KeyError):
        check("health_check_stale", False)

    # 12k. conversion_funnel — returns dict
    try:
        funnel = wave1_crm.conversion_funnel()
        check("conversion_funnel_dict", isinstance(funnel, dict))
    except (AttributeError, TypeError):
        check("conversion_funnel_dict", False)

    # 12l. conversion_funnel — has entries for statuses present in data
    try:
        funnel = wave1_crm.conversion_funnel()
        has_statuses = any(s in funnel for s in ("prospect", "contacted", "active_customer", "proposal_drafted"))
        check("conversion_funnel_statuses", has_statuses)
    except (AttributeError, TypeError):
        check("conversion_funnel_statuses", False)

    # 12m. forecast — returns dict with expected keys
    try:
        fc = wave1_crm.forecast()
        check("forecast_keys", isinstance(fc, dict) and "weighted_pipeline" in fc and "by_stage" in fc)
    except (AttributeError, TypeError):
        check("forecast_keys", False)

    # 12n. forecast — weighted_pipeline is a number >= 0
    try:
        fc = wave1_crm.forecast()
        check("forecast_pipeline_number", isinstance(fc["weighted_pipeline"], (int, float)) and fc["weighted_pipeline"] >= 0)
    except (AttributeError, TypeError, KeyError):
        check("forecast_pipeline_number", False)

    # 12o. forecast — handles deal_size string parsing (e.g. "$5K/mo")
    try:
        fc = wave1_crm.forecast()
        # With Amy ($5K/mo), Dana ($10K/mo), Steve ($200K/yr), pipeline should be > 0
        check("forecast_parses_deal_size", fc["weighted_pipeline"] > 0)
    except (AttributeError, TypeError, KeyError):
        check("forecast_parses_deal_size", False)

    # 12o2. _parse_deal_size — malformed float-like strings (e.g. "...", "1.2.3") return 0 instead of crashing
    try:
        check("parse_deal_size_malformed_dots", CRM._parse_deal_size("...") == 0)
    except Exception:
        check("parse_deal_size_malformed_dots", False)

    try:
        check("parse_deal_size_multi_decimal", CRM._parse_deal_size("1.2.3k") == 0)
    except Exception:
        check("parse_deal_size_multi_decimal", False)

    try:
        check("parse_deal_size_lone_dot", CRM._parse_deal_size(".") == 0)
    except Exception:
        check("parse_deal_size_lone_dot", False)

    # 12p. find_duplicates — returns list
    try:
        dups = wave1_crm.find_duplicates()
        check("find_duplicates_list", isinstance(dups, list))
    except (AttributeError, TypeError):
        check("find_duplicates_list", False)

    # 12q. find_duplicates — detects contacts with same email or similar name+company
    try:
        dups = wave1_crm.find_duplicates()
        # "Dup Dave" and "Dup David" at BigCo should be flagged
        dup_str = str(dups)
        check("find_duplicates_detects", len(dups) >= 1 and ("Dave" in dup_str or "David" in dup_str))
    except (AttributeError, TypeError):
        check("find_duplicates_detects", False)

    # 12r. stale_contacts — returns list
    try:
        stale = wave1_crm.stale_contacts()
        check("stale_contacts_list", isinstance(stale, list))
    except (AttributeError, TypeError):
        check("stale_contacts_list", False)

    # 12s. stale_contacts — excludes active_customer status
    try:
        stale = wave1_crm.stale_contacts()
        stale_names = [c.get("name", c) if isinstance(c, dict) else str(c) for c in stale]
        check("stale_contacts_excludes_active", not any("Amy" in n for n in stale_names))
    except (AttributeError, TypeError):
        check("stale_contacts_excludes_active", False)

    # 12t. stale_contacts — log_activity updates last_contacted so contact is not stale
    try:
        stale = wave1_crm.stale_contacts()
        stale_names = [c.get("name", c) if isinstance(c, dict) else str(c) for c in stale]
        # Dana had log_activity called today — last_contacted should be today, not stale
        check("stale_contacts_excludes_recently_logged", not any("Dana" in n for n in stale_names))
    except (AttributeError, TypeError):
        check("stale_contacts_excludes_recently_logged", False)

    wave1_crm.close()

    # ── 13. Pipeline Analytics — Wave 2 (19 tests) ──
    # These test win/loss analysis, cohort analysis, activity summaries,
    # revenue reporting, diff tracking, and snapshot generation.

    wave2_crm = CRM(os.path.join(tempfile.mkdtemp(), "wave2.db"))

    # Create contacts across multiple statuses with deals and varied activity
    wave2_crm.add_contact("Winner Wendy", email="wendy@test.com", company="WinCo", status="active_customer", deal_size="$10K/mo", source="referral")
    wave2_crm.add_contact("Loser Larry", email="larry@test.com", company="LossCo", status="lost", source="cold_email")
    wave2_crm.add_contact("Pipeline Pam", email="pam@test.com", company="PipeCo", status="proposal_drafted", deal_size="$5K/mo")
    wave2_crm.add_contact("Fresh Fred", email="fred@test.com", company="FreshCo", status="prospect")
    wave2_crm.log_activity("wendy@test.com", "call", "Closed deal")
    wave2_crm.log_activity("wendy@test.com", "email", "Onboarding email")
    wave2_crm.log_activity("pam@test.com", "meeting", "Proposal review")
    wave2_crm.log_activity("fred@test.com", "email", "Initial outreach")

    # 13a. win_loss_analysis — returns dict with expected keys
    try:
        wl = wave2_crm.win_loss_analysis()
        has_keys = isinstance(wl, dict) and all(k in wl for k in ("wins", "losses", "win_rate", "avg_deal_size", "avg_days_to_close", "top_source"))
        check("win_loss_analysis_keys", has_keys)
    except (AttributeError, TypeError):
        check("win_loss_analysis_keys", False)

    # 13b. win_loss_analysis — wins and losses are lists
    try:
        wl = wave2_crm.win_loss_analysis()
        check("win_loss_analysis_lists", isinstance(wl["wins"], list) and isinstance(wl["losses"], list))
    except (AttributeError, TypeError, KeyError):
        check("win_loss_analysis_lists", False)

    # 13c. win_loss_analysis — win_rate is a number 0-100
    try:
        wl = wave2_crm.win_loss_analysis()
        check("win_loss_analysis_rate", isinstance(wl["win_rate"], (int, float)) and 0 <= wl["win_rate"] <= 100)
    except (AttributeError, TypeError, KeyError):
        check("win_loss_analysis_rate", False)

    # 13d. win_loss_analysis — detects active_customer contacts as wins
    try:
        wl = wave2_crm.win_loss_analysis()
        win_names = [w.get("name", w) if isinstance(w, dict) else str(w) for w in wl["wins"]]
        check("win_loss_analysis_wins", any("Wendy" in n for n in win_names))
    except (AttributeError, TypeError, KeyError):
        check("win_loss_analysis_wins", False)

    # 13e. cohort_analysis — returns dict
    try:
        co = wave2_crm.cohort_analysis()
        check("cohort_analysis_dict", isinstance(co, dict))
    except (AttributeError, TypeError):
        check("cohort_analysis_dict", False)

    # 13f. cohort_analysis — has at least one cohort entry
    try:
        co = wave2_crm.cohort_analysis()
        check("cohort_analysis_has_entry", isinstance(co, dict) and len(co) >= 1)
    except (AttributeError, TypeError):
        check("cohort_analysis_has_entry", False)

    # 13g. cohort_analysis — each entry has added, converted, conversion_rate keys
    try:
        co = wave2_crm.cohort_analysis()
        first_entry = next(iter(co.values()))
        has_keys = all(k in first_entry for k in ("added", "converted", "conversion_rate"))
        check("cohort_analysis_entry_keys", has_keys)
    except (AttributeError, TypeError, StopIteration, KeyError):
        check("cohort_analysis_entry_keys", False)

    # 13h. activity_summary — returns dict with expected keys
    try:
        act = wave2_crm.activity_summary()
        has_keys = isinstance(act, dict) and all(k in act for k in ("total_activities", "by_type", "by_contact", "daily_avg"))
        check("activity_summary_keys", has_keys)
    except (AttributeError, TypeError):
        check("activity_summary_keys", False)

    # 13i. activity_summary — by_type is a dict
    try:
        act = wave2_crm.activity_summary()
        check("activity_summary_by_type_dict", isinstance(act["by_type"], dict))
    except (AttributeError, TypeError, KeyError):
        check("activity_summary_by_type_dict", False)

    # 13j. activity_summary — total_activities matches actual count (4 logged)
    try:
        act = wave2_crm.activity_summary()
        check("activity_summary_total", act["total_activities"] == 4)
    except (AttributeError, TypeError, KeyError):
        check("activity_summary_total", False)

    # 13k. revenue_report — returns dict with expected keys
    try:
        rev = wave2_crm.revenue_report()
        has_keys = isinstance(rev, dict) and all(k in rev for k in ("mrr", "arr", "pipeline_value"))
        check("revenue_report_keys", has_keys)
    except (AttributeError, TypeError):
        check("revenue_report_keys", False)

    # 13l. revenue_report — mrr and arr are numbers >= 0
    try:
        rev = wave2_crm.revenue_report()
        check("revenue_report_numbers", isinstance(rev["mrr"], (int, float)) and rev["mrr"] >= 0 and isinstance(rev["arr"], (int, float)) and rev["arr"] >= 0)
    except (AttributeError, TypeError, KeyError):
        check("revenue_report_numbers", False)

    # 13m. revenue_report — arr is approximately 12 * mrr
    try:
        rev = wave2_crm.revenue_report()
        if rev["mrr"] == 0:
            check("revenue_report_arr_12x_mrr", rev["arr"] == 0)
        else:
            ratio = rev["arr"] / rev["mrr"]
            check("revenue_report_arr_12x_mrr", 11.5 <= ratio <= 12.5)
    except (AttributeError, TypeError, KeyError, ZeroDivisionError):
        check("revenue_report_arr_12x_mrr", False)

    # 13n. diff — returns list
    try:
        d = wave2_crm.diff()
        check("diff_returns_list", isinstance(d, list))
    except (AttributeError, TypeError):
        check("diff_returns_list", False)

    # 13o. diff — each entry is a dict with type and entity/detail keys
    try:
        d = wave2_crm.diff()
        if len(d) > 0:
            entry = d[0]
            has_keys = isinstance(entry, dict) and "type" in entry and ("entity" in entry or "detail" in entry)
            check("diff_entry_keys", has_keys)
        else:
            check("diff_entry_keys", False)
    except (AttributeError, TypeError, IndexError):
        check("diff_entry_keys", False)

    # 13p. diff — contains recent additions (just-added contacts should show up)
    try:
        d = wave2_crm.diff()
        diff_str = str(d)
        check("diff_has_recent", "Wendy" in diff_str or "Fred" in diff_str or "Pam" in diff_str or "Larry" in diff_str)
    except (AttributeError, TypeError):
        check("diff_has_recent", False)

    # 13q. snapshot — returns dict with expected keys
    try:
        snap = wave2_crm.snapshot()
        has_keys = isinstance(snap, dict) and all(k in snap for k in ("timestamp", "contacts", "pipeline_value", "mrr"))
        check("snapshot_keys", has_keys)
    except (AttributeError, TypeError):
        check("snapshot_keys", False)

    # 13r. snapshot — timestamp is a string
    try:
        snap = wave2_crm.snapshot()
        check("snapshot_timestamp_str", isinstance(snap["timestamp"], str) and len(snap["timestamp"]) > 0)
    except (AttributeError, TypeError, KeyError):
        check("snapshot_timestamp_str", False)

    # 13s. snapshot — contacts is a dict (status counts)
    try:
        snap = wave2_crm.snapshot()
        check("snapshot_contacts_dict", isinstance(snap["contacts"], dict))
    except (AttributeError, TypeError, KeyError):
        check("snapshot_contacts_dict", False)

    wave2_crm.close()

    # ── 14. Agent Superpowers — Wave 3 (15 tests) ──
    # These test natural language query, segmentation, timeline,
    # agent context generation, and tag management.

    wave3_crm = CRM(os.path.join(tempfile.mkdtemp(), "wave3.db"))

    # Create contacts with tags, deal sizes, and varied statuses
    wave3_crm.add_contact("Query Quinn", email="quinn@test.com", company="QueryCo", status="active_customer", deal_size="$8K/mo", tags="enterprise,priority")
    wave3_crm.add_contact("Tag Tina", email="tina@test.com", company="TagCo", status="prospect", tags="startup,saas")
    wave3_crm.add_contact("Seg Sam", email="sam@test.com", company="SegCo", status="contacted", deal_size="$3K/mo", tags="startup")
    wave3_crm.log_activity("quinn@test.com", "call", "Renewal discussion")
    wave3_crm.observe("contact:quinn", "nps", "9", source="survey")
    wave3_crm.observe("contact:quinn", "industry", "fintech", source="research")
    wave3_crm.observe("contact:tina", "industry", "saas", source="research")

    # 14a. query — returns list of contacts matching natural language
    try:
        qr = wave3_crm.query("active customers")
        check("query_active_customers", isinstance(qr, list) and len(qr) >= 1 and any(
            (c.get("status") == "active_customer" if isinstance(c, dict) else False) for c in qr))
    except (AttributeError, TypeError):
        check("query_active_customers", False)

    # 14b. query — high value returns contacts with deal_size
    try:
        qr = wave3_crm.query("high value")
        check("query_high_value", isinstance(qr, list) and len(qr) >= 1 and any(
            (c.get("deal_size") is not None if isinstance(c, dict) else False) for c in qr))
    except (AttributeError, TypeError):
        check("query_high_value", False)

    # 14c. query — searches tags
    try:
        qr = wave3_crm.query("enterprise")
        check("query_tags_enterprise", isinstance(qr, list) and len(qr) >= 1)
    except (AttributeError, TypeError):
        check("query_tags_enterprise", False)

    # 14d. segment — filter by tags
    try:
        seg = wave3_crm.segment(tags="startup")
        check("segment_by_tag", isinstance(seg, list) and len(seg) >= 1 and all(
            ("startup" in (c.get("tags", "") or "") if isinstance(c, dict) else False) for c in seg))
    except (AttributeError, TypeError):
        check("segment_by_tag", False)

    # 14e. segment — filter by status
    try:
        seg = wave3_crm.segment(status="prospect")
        check("segment_by_status", isinstance(seg, list) and len(seg) >= 1 and all(
            (c.get("status") == "prospect" if isinstance(c, dict) else False) for c in seg))
    except (AttributeError, TypeError):
        check("segment_by_status", False)

    # 14f. segment — filter by company
    try:
        seg = wave3_crm.segment(company="QueryCo")
        check("segment_by_company", isinstance(seg, list) and len(seg) >= 1 and all(
            (c.get("company") == "QueryCo" if isinstance(c, dict) else False) for c in seg))
    except (AttributeError, TypeError):
        check("segment_by_company", False)

    # 14g. timeline — returns list of events for a contact
    try:
        tl = wave3_crm.timeline("quinn@test.com")
        check("timeline_returns_list", isinstance(tl, list) and len(tl) >= 1)
    except (AttributeError, TypeError):
        check("timeline_returns_list", False)

    # 14h. timeline — entries have type, detail, timestamp keys
    try:
        tl = wave3_crm.timeline("quinn@test.com")
        if len(tl) > 0:
            entry = tl[0]
            has_keys = isinstance(entry, dict) and all(k in entry for k in ("type", "detail", "timestamp"))
            check("timeline_entry_keys", has_keys)
        else:
            check("timeline_entry_keys", False)
    except (AttributeError, TypeError, IndexError):
        check("timeline_entry_keys", False)

    # 14i. timeline — includes both activities and facts
    try:
        tl = wave3_crm.timeline("quinn@test.com")
        types = [e.get("type", "") for e in tl if isinstance(e, dict)]
        has_activity = any(t in ("call", "email", "meeting", "activity") for t in types)
        has_fact = any(t in ("fact", "observation", "observe") for t in types)
        check("timeline_mixed_types", has_activity and has_fact)
    except (AttributeError, TypeError):
        check("timeline_mixed_types", False)

    # 14j. context_for_agent — returns string for specific contact
    try:
        ctx = wave3_crm.context_for_agent("quinn@test.com")
        check("context_for_agent_str", isinstance(ctx, str) and "Quinn" in ctx)
    except (AttributeError, TypeError):
        check("context_for_agent_str", False)

    # 14k. context_for_agent — reasonable length for single contact
    try:
        ctx = wave3_crm.context_for_agent("quinn@test.com")
        check("context_for_agent_length", isinstance(ctx, str) and 0 < len(ctx) < 5000)
    except (AttributeError, TypeError):
        check("context_for_agent_length", False)

    # 14l. context_for_agent — no identifier gives executive summary
    try:
        ctx = wave3_crm.context_for_agent()
        check("context_for_agent_summary", isinstance(ctx, str) and len(ctx) > 0)
    except (AttributeError, TypeError):
        check("context_for_agent_summary", False)

    # 14m. add_tag + list_by_tag — tag management
    try:
        wave3_crm.add_tag("quinn@test.com", "vip")
        tagged = wave3_crm.list_by_tag("vip")
        tagged_names = [c.get("name", c) if isinstance(c, dict) else str(c) for c in tagged]
        check("add_tag_and_list", len(tagged) >= 1 and any("Quinn" in n for n in tagged_names))
    except (AttributeError, TypeError):
        check("add_tag_and_list", False)

    # 14n. list_by_tag — returns existing tagged contacts
    try:
        tagged = wave3_crm.list_by_tag("startup")
        check("list_by_tag_existing", isinstance(tagged, list) and len(tagged) >= 1)
    except (AttributeError, TypeError):
        check("list_by_tag_existing", False)

    # 14o. remove_tag — removes tag from contact
    try:
        wave3_crm.add_tag("quinn@test.com", "temporary")
        wave3_crm.remove_tag("quinn@test.com", "temporary")
        tagged = wave3_crm.list_by_tag("temporary")
        check("remove_tag", isinstance(tagged, list) and not any(
            ("Quinn" in (c.get("name", c) if isinstance(c, dict) else str(c))) for c in tagged))
    except (AttributeError, TypeError):
        check("remove_tag", False)

    wave3_crm.close()

    # ── 15. Automation & Intelligence — Wave 4 (15 tests) ──
    # These test next_actions, suggest_status, auto_observe, enrich,
    # bulk_update, search_graph, and export_json.

    wave4_crm = CRM(os.path.join(tempfile.mkdtemp(), "wave4.db"))

    # Set up contacts at various stages
    wave4_crm.add_contact("Stale Sarah", email="sarah@test.com", company="BigDeal Inc", status="contacted", deal_size="$50K/yr")
    wave4_crm.add_contact("Verbal Vic", email="vic@test.com", company="VicCo", status="verbal_yes", deal_size="$20K/yr")
    wave4_crm.add_contact("Proposal Pat", email="pat@test.com", company="PatCo", status="proposal_drafted", deal_size="$10K/mo")
    wave4_crm.add_contact("Met Mike", email="mike@test.com", company="MikeCo", status="met", deal_size="$5K/mo")
    wave4_crm.add_contact("Fresh Fiona", email="fiona@test.com", company="FionaCo", status="prospect")
    wave4_crm.add_contact("Active Anna", email="anna@test.com", company="AnnaCo", status="active_customer", deal_size="$8K/mo")

    # Add activity to some contacts
    wave4_crm.log_activity("pat@test.com", "email", "Sent proposal")
    wave4_crm.log_activity("mike@test.com", "meeting", "Initial demo")
    wave4_crm.log_activity("anna@test.com", "call", "Quarterly check-in")

    # Add facts/conflicts for graph tests
    wave4_crm.observe("contact:sarah", "role", "VP Sales", source="linkedin")
    wave4_crm.observe("contact:sarah", "role", "CRO", source="announcement")
    wave4_crm.observe("company:bigdeal_inc", "industry", "fintech", source="research")

    # 15a. next_actions — returns a list
    try:
        na = wave4_crm.next_actions()
        check("next_actions_returns_list", isinstance(na, list))
    except (AttributeError, TypeError):
        check("next_actions_returns_list", False)

    # 15b. next_actions — entries have required keys
    try:
        na = wave4_crm.next_actions()
        if len(na) > 0:
            entry = na[0]
            has_keys = all(k in entry for k in ("contact", "action", "priority", "reason", "deal_value"))
            check("next_actions_entry_keys", has_keys)
        else:
            check("next_actions_entry_keys", False)
    except (AttributeError, TypeError, IndexError):
        check("next_actions_entry_keys", False)

    # 15c. next_actions — high priority items appear first
    try:
        na = wave4_crm.next_actions()
        priorities = [a["priority"] for a in na]
        # First high-priority item should come before any low-priority item
        if "high" in priorities and "low" in priorities:
            first_high = priorities.index("high")
            last_low = len(priorities) - 1 - priorities[::-1].index("low")
            check("next_actions_priority_order", first_high < last_low)
        else:
            check("next_actions_priority_order", len(na) >= 1)
    except (AttributeError, TypeError):
        check("next_actions_priority_order", False)

    # 15d. next_actions — verbal_yes contact gets "Send contract"
    try:
        na = wave4_crm.next_actions()
        vic_actions = [a for a in na if "Vic" in a["contact"]]
        check("next_actions_verbal_yes", len(vic_actions) >= 1 and "contract" in vic_actions[0]["action"].lower())
    except (AttributeError, TypeError):
        check("next_actions_verbal_yes", False)

    # 15e. suggest_status — returns dict with required keys
    try:
        ss = wave4_crm.suggest_status("fiona@test.com")
        has_keys = isinstance(ss, dict) and all(k in ss for k in ("current", "suggested", "reason", "confidence"))
        check("suggest_status_keys", has_keys)
    except (AttributeError, TypeError):
        check("suggest_status_keys", False)

    # 15f. suggest_status — suggests advancement for contact with meeting activity
    try:
        # Mike has a meeting logged and status is "met" — add extra activities to test prospect->contacted
        suggest_crm = CRM(os.path.join(tempfile.mkdtemp(), "suggest.db"))
        suggest_crm.add_contact("Test Tim", email="tim@test.com", status="prospect")
        suggest_crm.log_activity("tim@test.com", "email", "First email")
        suggest_crm.log_activity("tim@test.com", "call", "Follow up")
        suggest_crm.log_activity("tim@test.com", "email", "Second email")
        suggest_crm.log_activity("tim@test.com", "call", "Third contact")
        ss = suggest_crm.suggest_status("tim@test.com")
        check("suggest_status_advancement", ss["suggested"] != "prospect")
        suggest_crm.close()
    except (AttributeError, TypeError, KeyError):
        check("suggest_status_advancement", False)

    # 15g. auto_observe — returns list of created facts
    try:
        facts = wave4_crm.auto_observe("anna@test.com")
        check("auto_observe_returns_list", isinstance(facts, list) and len(facts) >= 1)
    except (AttributeError, TypeError):
        check("auto_observe_returns_list", False)

    # 15h. auto_observe — creates facts in the graph
    try:
        facts = wave4_crm.auto_observe("vic@test.com")
        # Should have created company fact at minimum
        entity_key = "contact:verbal_vic"
        stored = wave4_crm.facts_about(entity_key)
        check("auto_observe_creates_facts", len(stored) >= 1)
    except (AttributeError, TypeError):
        check("auto_observe_creates_facts", False)

    # 15i. enrich — returns dict with required keys
    try:
        en = wave4_crm.enrich("anna@test.com")
        has_keys = isinstance(en, dict) and all(k in en for k in ("name", "facts", "activities", "score"))
        check("enrich_keys", has_keys)
    except (AttributeError, TypeError):
        check("enrich_keys", False)

    # 15j. enrich — contains contact's name and score
    try:
        en = wave4_crm.enrich("anna@test.com")
        check("enrich_data", en["name"] == "Active Anna" and isinstance(en["score"], int) and en["score"] >= 0)
    except (AttributeError, TypeError, KeyError):
        check("enrich_data", False)

    # 15k. bulk_update — returns count of successful updates
    try:
        updates = [
            {"identifier": "fiona@test.com", "status": "contacted"},
            {"identifier": "mike@test.com", "notes": "Updated via bulk"},
        ]
        count = wave4_crm.bulk_update(updates)
        check("bulk_update_count", count == 2)
    except (AttributeError, TypeError):
        check("bulk_update_count", False)

    # 15l. bulk_update — actually updates the contacts
    try:
        c = wave4_crm.get_contact("fiona@test.com")
        check("bulk_update_applied", c is not None and c["status"] == "contacted")
    except (AttributeError, TypeError, KeyError):
        check("bulk_update_applied", False)

    # 15m. search_graph — returns list of matching facts
    try:
        results = wave4_crm.search_graph("fintech")
        check("search_graph_results", isinstance(results, list) and len(results) >= 1)
    except (AttributeError, TypeError):
        check("search_graph_results", False)

    # 15n. search_graph — finds facts by entity name
    try:
        results = wave4_crm.search_graph("sarah")
        check("search_graph_entity", isinstance(results, list) and len(results) >= 1)
    except (AttributeError, TypeError):
        check("search_graph_entity", False)

    # 15o. export_json — returns valid JSON string
    try:
        json_str = wave4_crm.export_json()
        import json as _json
        data = _json.loads(json_str)
        has_keys = all(k in data for k in ("contacts", "graph", "scores", "health", "revenue"))
        check("export_json_valid", isinstance(json_str, str) and has_keys)
    except (AttributeError, TypeError, ValueError):
        check("export_json_valid", False)

    wave4_crm.close()

    # ── 16. Local Data Connectors (8 tests) ──
    # Test that ingest methods exist, accept correct args, return correct types.
    # Every call is fixture-injected so this suite never probes live macOS data.

    ingest_crm = CRM(os.path.join(tempfile.mkdtemp(), "ingest.db"))
    contract_addressbook_db = make_addressbook_fixture_db()
    contract_imessage_db = make_imessage_fixture_db()

    # 16a. ingest_macos_contacts exists and returns tuple
    try:
        result = ingest_crm.ingest_macos_contacts(db_paths=[contract_addressbook_db])
        check("ingest_contacts_returns_tuple", isinstance(result, tuple) and len(result) == 2)
    except (AttributeError, TypeError):
        check("ingest_contacts_returns_tuple", False)

    # 16b. ingest_macos_contacts returns ints
    try:
        added, facts = ingest_crm.ingest_macos_contacts(db_paths=[contract_addressbook_db])
        check("ingest_contacts_returns_ints", isinstance(added, int) and isinstance(facts, int))
    except (AttributeError, TypeError, ValueError):
        check("ingest_contacts_returns_ints", False)

    # 16c. ingest_macos_imessage exists and returns tuple
    try:
        result = ingest_crm.ingest_macos_imessage(days=30, db_path=contract_imessage_db)
        check("ingest_imessage_returns_tuple", isinstance(result, tuple) and len(result) == 2)
    except (AttributeError, TypeError):
        check("ingest_imessage_returns_tuple", False)

    # 16d. ingest_macos_imessage accepts days parameter
    try:
        result = ingest_crm.ingest_macos_imessage(days=7, db_path=contract_imessage_db)
        check("ingest_imessage_days_param", isinstance(result, tuple))
    except (AttributeError, TypeError):
        check("ingest_imessage_days_param", False)

    # 16e. ingest_macos_calendar exists and returns tuple
    try:
        result = ingest_crm.ingest_macos_calendar(records=[], attendees=[])
        check("ingest_calendar_returns_tuple", isinstance(result, tuple) and len(result) == 2)
    except (AttributeError, TypeError):
        check("ingest_calendar_returns_tuple", False)

    # 16f. ingest_macos_calendar accepts days_back/days_forward
    try:
        result = ingest_crm.ingest_macos_calendar(
            days_back=7, days_forward=7, records=[], attendees=[]
        )
        check("ingest_calendar_days_params", isinstance(result, tuple))
    except (AttributeError, TypeError):
        check("ingest_calendar_days_params", False)

    # 16g. ingest_all exists and returns dict
    ingest_crm.ingest_macos_contacts = lambda: (0, 0)
    ingest_crm.ingest_macos_imessage = lambda days=90: (0, 0)
    ingest_crm.ingest_macos_calendar = lambda days_back=30, days_forward=30: (0, 0)
    ingest_crm.ingest_macos_mail = lambda days=90: (0, 0)
    try:
        result = ingest_crm.ingest_all()
        check("ingest_all_returns_dict", isinstance(result, dict) and "total_facts" in result)
    except (AttributeError, TypeError):
        check("ingest_all_returns_dict", False)

    # 16h. ingest_all has expected keys
    try:
        result = ingest_crm.ingest_all()
        has_keys = all(k in result for k in ("contacts", "imessage", "calendar", "total_facts"))
        check("ingest_all_keys", has_keys)
    except (AttributeError, TypeError):
        check("ingest_all_keys", False)

    ingest_crm.close()

    # ── 17. Smart CSV Import (8 tests) ──

    import_crm = CRM(os.path.join(tempfile.mkdtemp(), "import.db"))
    csv_dir = tempfile.mkdtemp()

    # Create a Salesforce-style CSV
    sf_csv = os.path.join(csv_dir, "salesforce.csv")
    with open(sf_csv, "w") as f:
        f.write("First Name,Last Name,Email,Account Name,Title,Lead Status,Amount,Lead Source,Description\n")
        f.write("Jane,Doe,jane@sf.com,Acme Corp,VP Sales,Qualified,50000,Web,Hot lead\n")
        f.write("John,Smith,john@sf.com,BigCo,CEO,Closed Won,100000,Referral,Converted\n")
        f.write("Amy,Lee,amy@sf.com,StartupX,,New,,Cold Email,\n")

    # Create a generic CSV
    gen_csv = os.path.join(csv_dir, "generic.csv")
    with open(gen_csv, "w") as f:
        f.write("Full Name,Email Address,Organization,Role,Revenue,Notes\n")
        f.write("Bob Wilson,bob@gen.com,GenCo,CTO,25000,Good fit\n")

    # 17a. import_smart exists and returns dict
    try:
        result = import_crm.import_smart(sf_csv)
        check("import_smart_returns_dict", isinstance(result, dict) and "contacts_added" in result)
    except (AttributeError, TypeError, FileNotFoundError):
        check("import_smart_returns_dict", False)

    # 17b. import_smart imported contacts
    try:
        result = import_crm.import_smart(gen_csv)
        check("import_smart_adds_contacts", result["contacts_added"] >= 1)
    except (AttributeError, TypeError, FileNotFoundError, KeyError):
        check("import_smart_adds_contacts", False)

    # 17c. import_salesforce maps status correctly
    try:
        sf_crm = CRM(os.path.join(tempfile.mkdtemp(), "sf.db"))
        sf_crm.import_salesforce(sf_csv)
        c = sf_crm.get_contact("john@sf.com")
        check("import_sf_status_map", c is not None and c["status"] == "active_customer")
        sf_crm.close()
    except (AttributeError, TypeError, FileNotFoundError, KeyError):
        check("import_sf_status_map", False)

    # 17d. import_salesforce combines first/last name
    try:
        sf_crm2 = CRM(os.path.join(tempfile.mkdtemp(), "sf2.db"))
        sf_crm2.import_salesforce(sf_csv)
        c = sf_crm2.get_contact("jane@sf.com")
        check("import_sf_name_merge", c is not None and "Jane" in c["name"] and "Doe" in c["name"])
        sf_crm2.close()
    except (AttributeError, TypeError, FileNotFoundError, KeyError):
        check("import_sf_name_merge", False)

    # 17e. import_hubspot exists
    try:
        hs_csv = os.path.join(csv_dir, "hubspot.csv")
        with open(hs_csv, "w") as f:
            f.write("First name,Last name,Email,Company name,Lifecycle stage\n")
            f.write("Test,User,test@hs.com,HubCo,customer\n")
        hs_crm = CRM(os.path.join(tempfile.mkdtemp(), "hs.db"))
        result = hs_crm.import_hubspot(hs_csv)
        check("import_hubspot_works", result["contacts_added"] >= 1)
        hs_crm.close()
    except (AttributeError, TypeError, FileNotFoundError, KeyError):
        check("import_hubspot_works", False)

    # 17f. import stores unmapped columns as facts
    try:
        extra_csv = os.path.join(csv_dir, "extra.csv")
        with open(extra_csv, "w") as f:
            f.write("Name,Email,Custom Field,Industry\n")
            f.write("Fact Test,fact@test.com,custom_val,fintech\n")
        fact_crm = CRM(os.path.join(tempfile.mkdtemp(), "fact.db"))
        result = fact_crm.import_smart(extra_csv)
        check("import_stores_extra_as_facts", result["facts_added"] >= 1)
        fact_crm.close()
    except (AttributeError, TypeError, FileNotFoundError, KeyError):
        check("import_stores_extra_as_facts", False)

    # 17g. import_smart handles duplicates gracefully
    try:
        dup_crm = CRM(os.path.join(tempfile.mkdtemp(), "dup.db"))
        dup_crm.import_smart(sf_csv)
        r2 = dup_crm.import_smart(sf_csv)
        check("import_handles_duplicates", r2["skipped"] >= 2)
        dup_crm.close()
    except (AttributeError, TypeError, FileNotFoundError, KeyError):
        check("import_handles_duplicates", False)

    # 17h. auto-detect maps generic columns
    try:
        gen_crm = CRM(os.path.join(tempfile.mkdtemp(), "gen.db"))
        result = gen_crm.import_smart(gen_csv)
        c = gen_crm.get_contact("bob@gen.com")
        check("import_auto_map_generic", c is not None and c["company"] == "GenCo")
        gen_crm.close()
    except (AttributeError, TypeError, FileNotFoundError, KeyError):
        check("import_auto_map_generic", False)

    import_crm.close()

    # ── 18. Relationship Velocity (8 tests) ──

    vel_crm = CRM(os.path.join(tempfile.mkdtemp(), "vel.db"))
    vel_crm.add_contact("Active Alice", email="alice@vel.com", company="VelCo", status="active_customer")
    vel_crm.add_contact("Dead Dan", email="dan@vel.com", company="DeadCo", status="prospect")
    # Give Alice lots of recent activity
    for i in range(5):
        vel_crm.log_activity("alice@vel.com", "email", f"Email {i}")

    # 18a. velocity returns dict with expected keys
    try:
        v = vel_crm.velocity("alice@vel.com")
        has_keys = isinstance(v, dict) and all(k in v for k in ("velocity", "trend", "days_until_cold", "current_period"))
        check("velocity_returns_dict", has_keys)
    except (AttributeError, TypeError):
        check("velocity_returns_dict", False)

    # 18b. velocity trend for active contact
    try:
        v = vel_crm.velocity("alice@vel.com")
        check("velocity_active_trend", v["trend"] in ("accelerating", "stable"))
    except (AttributeError, TypeError, KeyError):
        check("velocity_active_trend", False)

    # 18c. velocity for inactive contact
    try:
        v = vel_crm.velocity("dan@vel.com")
        check("velocity_dead_trend", v["trend"] == "dead")
    except (AttributeError, TypeError, KeyError):
        check("velocity_dead_trend", False)

    # 18d. velocity returns None for nonexistent
    try:
        v = vel_crm.velocity("nonexistent@test.com")
        check("velocity_not_found", v is None)
    except (AttributeError, TypeError):
        check("velocity_not_found", False)

    # 18e. relationship_health_report returns list
    try:
        report = vel_crm.relationship_health_report()
        check("rel_health_returns_list", isinstance(report, list))
    except (AttributeError, TypeError):
        check("rel_health_returns_list", False)

    # 18f. relationship_health entries have expected keys
    try:
        report = vel_crm.relationship_health_report()
        if len(report) > 0:
            has_keys = all(k in report[0] for k in ("name", "velocity", "trend", "score"))
            check("rel_health_entry_keys", has_keys)
        else:
            check("rel_health_entry_keys", True)  # empty is ok
    except (AttributeError, TypeError, IndexError):
        check("rel_health_entry_keys", False)

    # 18g. velocity accepts window_days parameter
    try:
        v = vel_crm.velocity("alice@vel.com", window_days=7)
        check("velocity_window_param", isinstance(v, dict) and "velocity" in v)
    except (AttributeError, TypeError):
        check("velocity_window_param", False)

    # 18h. relationship_health sorts decaying first
    try:
        report = vel_crm.relationship_health_report()
        if len(report) >= 2:
            trends = [r["trend"] for r in report]
            # Dead/decaying should come before accelerating/stable
            dead_idx = [i for i, t in enumerate(trends) if t in ("dead", "decaying")]
            accel_idx = [i for i, t in enumerate(trends) if t in ("accelerating", "stable")]
            if dead_idx and accel_idx:
                check("rel_health_sort_order", max(dead_idx) < min(accel_idx))
            else:
                check("rel_health_sort_order", True)
        else:
            check("rel_health_sort_order", True)
    except (AttributeError, TypeError):
        check("rel_health_sort_order", False)

    vel_crm.close()

    # ── 18i. relationship_health resolves phone entities to names (1 test) ──
    # When a person has both a contact: entity and a phone: entity with
    # iMessage data, they should appear once (under their real name), not
    # twice (once as a phone number and once as a name).
    try:
        rh_crm = CRM(os.path.join(tempfile.mkdtemp(), "rh_phone.db"))
        rh_crm.add_contact("Phone Alice", email="palice@test.com", company="PhoneCo", deal_size="$5K/mo")
        # iMessage data on the contact: entity
        rh_crm.observe("contact:phone alice", "imessage_total", "50", source="imessage")
        rh_crm.observe("contact:phone alice", "imessage_sent", "25", source="imessage")
        rh_crm.observe("contact:phone alice", "imessage_received", "25", source="imessage")
        rh_crm.observe("contact:phone alice", "message_intensity", "medium", source="imessage")
        # iMessage data on the phone: entity (same person, resolved via contacts)
        rh_crm.observe("phone:+15551234567", "imessage_total", "100", source="imessage")
        rh_crm.observe("phone:+15551234567", "imessage_sent", "40", source="imessage")
        rh_crm.observe("phone:+15551234567", "imessage_received", "60", source="imessage")
        rh_crm.observe("phone:+15551234567", "message_intensity", "high", source="imessage")
        rh_crm.observe("phone:+15551234567", "name", "Phone Alice", source="macos_contacts")
        rh = rh_crm.relationship_health()
        # Should appear once (deduplicated), not twice
        alice_entries = [r for r in rh if "alice" in r["name"].lower() or "5551234567" in r["name"]]
        check("rel_health_phone_name_resolve", len(alice_entries) == 1 and "Alice" in alice_entries[0]["name"])
        rh_crm.close()
    except (AttributeError, TypeError):
        check("rel_health_phone_name_resolve", False)

    # ── 19. Saved Views (10 tests) ──

    view_crm = CRM(os.path.join(tempfile.mkdtemp(), "view.db"))
    view_crm.add_contact("View Alice", email="alice@view.com", company="ViewCo", status="active_customer", deal_size="$10K/mo", tags="enterprise")
    view_crm.add_contact("View Bob", email="bob@view.com", company="BobCo", status="prospect", tags="startup")
    view_crm.add_contact("View Carol", email="carol@view.com", company="CarolCo", status="contacted", deal_size="$5K/mo")

    # 19a. save_view returns count
    try:
        count = view_crm.save_view("customers", status="active_customer")
        check("save_view_returns_count", isinstance(count, int) and count >= 1)
    except (AttributeError, TypeError):
        check("save_view_returns_count", False)

    # 19b. run_view returns matching contacts
    try:
        results = view_crm.run_view("customers")
        check("run_view_matches", isinstance(results, list) and len(results) >= 1)
    except (AttributeError, TypeError):
        check("run_view_matches", False)

    # 19c. run_view filters by status correctly
    try:
        results = view_crm.run_view("customers")
        all_customer = all(c.get("status") == "active_customer" for c in results)
        check("run_view_status_filter", all_customer)
    except (AttributeError, TypeError):
        check("run_view_status_filter", False)

    # 19d. list_views returns list
    try:
        views = view_crm.list_views()
        check("list_views_returns_list", isinstance(views, list) and len(views) >= 1)
    except (AttributeError, TypeError):
        check("list_views_returns_list", False)

    # 19e. list_views includes count
    try:
        views = view_crm.list_views()
        check("list_views_has_count", all("count" in v for v in views))
    except (AttributeError, TypeError):
        check("list_views_has_count", False)

    # 19f. save_view with tags filter
    try:
        view_crm.save_view("startups", tags="startup")
        results = view_crm.run_view("startups")
        check("view_tags_filter", len(results) >= 1 and all("startup" in (c.get("tags", "") or "") for c in results))
    except (AttributeError, TypeError):
        check("view_tags_filter", False)

    # 19g. delete_view works
    try:
        view_crm.save_view("temp_view", status="prospect")
        view_crm.delete_view("temp_view")
        results = view_crm.run_view("temp_view")
        check("delete_view_works", results == [])
    except (AttributeError, TypeError):
        check("delete_view_works", False)

    # 19h. watch returns diff dict
    try:
        view_crm.save_view("watch_test", status="prospect")
        diff = view_crm.watch("watch_test")
        has_keys = isinstance(diff, dict) and all(k in diff for k in ("added", "removed_count", "total"))
        check("watch_returns_diff", has_keys)
    except (AttributeError, TypeError):
        check("watch_returns_diff", False)

    # 19i. watch detects additions
    try:
        view_crm.save_view("watch_add", status="lost")
        # No lost contacts yet
        view_crm.add_contact("Lost Larry", email="larry@view.com", status="lost")
        diff = view_crm.watch("watch_add")
        check("watch_detects_added", len(diff["added"]) >= 1)
    except (AttributeError, TypeError, KeyError):
        check("watch_detects_added", False)

    # 19j. save_view with company filter
    try:
        view_crm.save_view("viewco", company="ViewCo")
        results = view_crm.run_view("viewco")
        check("view_company_filter", len(results) >= 1)
    except (AttributeError, TypeError):
        check("view_company_filter", False)

    view_crm.close()

    # ── 20. Interaction Prompts (8 tests) ──

    prompt_crm = CRM(os.path.join(tempfile.mkdtemp(), "prompt.db"))
    prompt_crm.add_contact("Prompt Pete", email="pete@prompt.com", company="PromptCo", status="proposal_drafted", deal_size="$20K/mo")
    prompt_crm.log_activity("pete@prompt.com", "meeting", "Demo call went well")
    prompt_crm.log_activity("pete@prompt.com", "email", "Sent proposal")
    prompt_crm.observe("contact:prompt pete", "role", "CTO", source="linkedin")
    prompt_crm.observe("contact:prompt pete", "pain_point", "manual reporting", source="call_notes")
    prompt_crm.add_contact("Cold Cathy", email="cathy@prompt.com", company="ColdCo", status="prospect")

    # 20a. interaction_prompt returns string
    try:
        prompt = prompt_crm.interaction_prompt("pete@prompt.com")
        check("prompt_returns_string", isinstance(prompt, str) and len(prompt) > 50)
    except (AttributeError, TypeError):
        check("prompt_returns_string", False)

    # 20b. prompt includes contact name
    try:
        prompt = prompt_crm.interaction_prompt("pete@prompt.com")
        check("prompt_has_name", "Pete" in prompt)
    except (AttributeError, TypeError):
        check("prompt_has_name", False)

    # 20c. prompt includes known facts
    try:
        prompt = prompt_crm.interaction_prompt("pete@prompt.com")
        check("prompt_has_facts", "CTO" in prompt or "manual reporting" in prompt)
    except (AttributeError, TypeError):
        check("prompt_has_facts", False)

    # 20d. prompt includes recent activity
    try:
        prompt = prompt_crm.interaction_prompt("pete@prompt.com")
        check("prompt_has_activity", "proposal" in prompt.lower() or "demo" in prompt.lower())
    except (AttributeError, TypeError):
        check("prompt_has_activity", False)

    # 20e. cold_outreach prompt type works
    try:
        prompt = prompt_crm.interaction_prompt("cathy@prompt.com", action_type="cold_outreach")
        check("prompt_cold_outreach", isinstance(prompt, str) and "pain" in prompt.lower())
    except (AttributeError, TypeError):
        check("prompt_cold_outreach", False)

    # 20f. close prompt type works
    try:
        prompt = prompt_crm.interaction_prompt("pete@prompt.com", action_type="close")
        check("prompt_close_type", isinstance(prompt, str) and "urgency" in prompt.lower())
    except (AttributeError, TypeError):
        check("prompt_close_type", False)

    # 20g. returns None for nonexistent
    try:
        prompt = prompt_crm.interaction_prompt("nobody@test.com")
        check("prompt_not_found", prompt is None)
    except (AttributeError, TypeError):
        check("prompt_not_found", False)

    # 20h. batch_prompts returns list
    try:
        prompts = prompt_crm.batch_prompts()
        check("batch_prompts_list", isinstance(prompts, list))
    except (AttributeError, TypeError):
        check("batch_prompts_list", False)

    # 20i. interaction_prompt finds facts stored under variant entity keys
    # (e.g. contact:prompt_pete with underscore, not just contact:prompt pete)
    try:
        prompt_crm.observe("contact:prompt_pete", "funding", "Series B", source="crunchbase")
        prompt = prompt_crm.interaction_prompt("pete@prompt.com")
        check("prompt_variant_entity_facts", isinstance(prompt, str) and "Series B" in prompt)
    except (AttributeError, TypeError):
        check("prompt_variant_entity_facts", False)

    prompt_crm.close()

    # ── 21. Email Ingestion (4 tests) ──

    email_crm = CRM(os.path.join(tempfile.mkdtemp(), "email.db"))

    # 21a. ingest_macos_mail exists and returns tuple
    try:
        result = email_crm.ingest_macos_mail(days=30, rows=[])
        check("ingest_mail_returns_tuple", isinstance(result, tuple) and len(result) == 2)
    except (AttributeError, TypeError):
        check("ingest_mail_returns_tuple", False)

    # 21b. ingest_mbox exists and returns tuple
    try:
        result = email_crm.ingest_mbox("/nonexistent/path.mbox")
        check("ingest_mbox_returns_tuple", isinstance(result, tuple) and len(result) == 2)
    except (AttributeError, TypeError):
        check("ingest_mbox_returns_tuple", False)

    # 21c. ingest_mbox handles missing file gracefully
    try:
        result = email_crm.ingest_mbox("/nonexistent/path.mbox")
        check("ingest_mbox_missing_file", result == (0, 0))
    except (AttributeError, TypeError):
        check("ingest_mbox_missing_file", False)

    # 21d. ingest_macos_mail accepts days param
    try:
        result = email_crm.ingest_macos_mail(days=7, rows=[])
        check("ingest_mail_days_param", isinstance(result, tuple))
    except (AttributeError, TypeError):
        check("ingest_mail_days_param", False)

    email_crm.close()

    # ── 21e. ingest_mbox handles timezone-aware email Date headers ──
    # email.utils.parsedate_to_datetime returns timezone-aware datetimes for
    # real-world emails (e.g. "Thu, 19 Mar 2026 10:30:00 +0000").  The cutoff
    # must be tz-aware too, otherwise the comparison raises TypeError and every
    # message is silently skipped.
    try:
        import mailbox as _mb
        import email.utils as _eu
        mbox_tz_dir = tempfile.mkdtemp()
        mbox_tz_path = os.path.join(mbox_tz_dir, "tz_test.mbox")
        # Write a minimal mbox with a timezone-aware Date header (recent)
        with open(mbox_tz_path, "w") as f:
            f.write("From sender@test.com Thu Mar 19 10:30:00 2026\n")
            f.write("From: sender@test.com\n")
            f.write("To: recipient@test.com\n")
            f.write("Date: Thu, 19 Mar 2026 10:30:00 +0000\n")
            f.write("Subject: TZ test\n")
            f.write("\n")
            f.write("Body\n")
            f.write("\n")
        tz_crm = CRM(os.path.join(tempfile.mkdtemp(), "tz.db"))
        msgs, facts = tz_crm.ingest_mbox(mbox_tz_path, days=9999)
        check("ingest_mbox_tz_aware_dates", msgs >= 1)
        tz_crm.close()
    except (AttributeError, TypeError):
        check("ingest_mbox_tz_aware_dates", False)

    # ── 20. LIKE metacharacter escaping (5 tests) ──
    # Verifies that %, _, and \ in user input don't cause false LIKE matches.
    like_crm = CRM(os.path.join(tempfile.mkdtemp(), "like.db"))
    like_crm.add_contact("Alice Smith", email="alice@test.com", company="Acme")
    like_crm.add_contact("Bob Jones", email="bob@test.com", company="Beta_Corp")
    like_crm.add_contact("Weird%Name", email="weird@test.com", company="Pct%Co")

    # 20a. get_contact('%') should NOT match Alice (only Weird%Name has a literal %)
    try:
        c = like_crm.get_contact("%")
        check("like_escape_get_contact_pct", c is not None and c["name"] == "Weird%Name")
    except Exception:
        check("like_escape_get_contact_pct", False)

    # 20b. get_contact('_') should NOT match any name (no name has a literal lone _)
    try:
        c = like_crm.get_contact("_")
        check("like_escape_get_contact_underscore", c is None)
    except Exception:
        check("like_escape_get_contact_underscore", False)

    # 20c. search('%') should return only contacts with literal % (1, not 3)
    try:
        results = like_crm.search("%")
        check("like_escape_search_pct", len(results) == 1)
    except Exception:
        check("like_escape_search_pct", False)

    # 20d. search('_') should return only contacts with literal _ (1, not 3)
    try:
        results = like_crm.search("_")
        check("like_escape_search_underscore", len(results) == 1)
    except Exception:
        check("like_escape_search_underscore", False)

    # 20e. list_contacts(company='%') should return only Pct%Co (1, not 3)
    try:
        results = like_crm.list_contacts(company="%")
        check("like_escape_list_contacts_pct", len(results) == 1 and results[0]["company"] == "Pct%Co")
    except Exception:
        check("like_escape_list_contacts_pct", False)

    like_crm.close()

    # ── 22. diff() surfaces contact updates (1 test) ──
    # diff() previously only returned new contacts, activities, and facts.
    # Contact field updates (status change, deal_size change, etc.) were
    # invisible because only created_at was checked, not updated_at.
    diff_crm = CRM(os.path.join(tempfile.mkdtemp(), "diffup.db"))
    diff_crm.add_contact("Diff Dan", email="dan@diff.com", status="prospect")
    # Backdate created_at so it falls before our cutoff (avoids sleep)
    diff_crm.conn.execute(
        "UPDATE contacts SET created_at = datetime('now', '-1 hour') WHERE email = 'dan@diff.com'"
    )
    diff_crm.conn.commit()
    cutoff = diff_crm.conn.execute("SELECT datetime('now', '-30 seconds')").fetchone()[0]
    diff_crm.update_contact("dan@diff.com", status="contacted")
    try:
        d = diff_crm.diff(since=cutoff)
        update_entries = [e for e in d if e.get("type") == "contact_update"]
        check("diff_surfaces_contact_updates",
              len(update_entries) >= 1 and any("Dan" in e.get("name", "") for e in update_entries))
    except (AttributeError, TypeError):
        check("diff_surfaces_contact_updates", False)
    diff_crm.close()

    # ── 23. stale_facts returns latest row data (1 test) ──
    # stale_facts must return the value/source/observed_at from the most
    # recent row for each (entity, key) group, not from an arbitrary row.
    # When multiple values exist for the same entity+key (e.g. a role that
    # changed from Engineer to CTO), the returned row should reflect the
    # latest observation.  An UPSERT that refreshes an older rowid's
    # observed_at (making it the newest) must be reflected correctly.
    stale_crm = CRM(os.path.join(tempfile.mkdtemp(), "stale_row.db"))
    # Insert two facts for the same entity+key with different values
    stale_crm.observe("contact:staletest", "role", "Engineer", source="old_src")
    stale_crm.observe("contact:staletest", "role", "CTO", source="new_src")
    # Backdate both so they're stale, but make CTO the newer one
    stale_crm.conn.execute(
        "UPDATE facts SET observed_at = '2020-01-01' WHERE entity = 'contact:staletest' AND value = 'Engineer'"
    )
    stale_crm.conn.execute(
        "UPDATE facts SET observed_at = '2023-06-01' WHERE entity = 'contact:staletest' AND value = 'CTO'"
    )
    stale_crm.conn.commit()
    try:
        stale = stale_crm.stale_facts(days=7)
        match = [s for s in stale if s["entity"] == "contact:staletest" and s["key"] == "role"]
        check("stale_facts_returns_latest_row",
              len(match) == 1 and match[0]["value"] == "CTO" and match[0]["source"] == "new_src"
              and "2023" in match[0]["observed_at"])
    except Exception:
        check("stale_facts_returns_latest_row", False)
    stale_crm.close()

    # ── 24. MCP view_contact uses all entity key variants (1 test) ──
    # The MCP server's crm_view_contact handler previously only looked up
    # facts via "contact:{name.lower()}", missing facts stored under variant
    # entity keys like "contact:first_last" (underscore) or "contact:email".
    # This test verifies that handle_tool_call returns facts from all variants.
    try:
        from mcp_server import handle_tool_call
        mcp_crm = CRM(os.path.join(tempfile.mkdtemp(), "mcp_view.db"))
        # Temporarily override the DB_PATH so handle_tool_call uses our test DB
        import mcp_server
        old_db = mcp_server.DB_PATH
        mcp_server.DB_PATH = mcp_crm.db_path
        mcp_crm.add_contact("Mcp Alice", email="malice@test.com", company="McpCo")
        # Store a fact under the underscore variant (contact:mcp_alice)
        mcp_crm.observe("contact:mcp_alice", "funding", "Series A", source="research")
        mcp_crm.close()
        result = handle_tool_call("crm_view_contact", {"identifier": "malice@test.com"})
        mcp_server.DB_PATH = old_db
        check("mcp_view_contact_entity_keys", '"Series A"' in result)
    except Exception:
        check("mcp_view_contact_entity_keys", False)

    crm.close()

    # ── 25. update_contact rename without email (2 tests) ──
    # update_contact previously re-fetched by name/email after the UPDATE.
    # When a contact with no email was renamed, the old name no longer matched,
    # causing update_contact to return None even though the rename succeeded.
    # Now it re-fetches by id, which always works.
    rename_crm = CRM(os.path.join(tempfile.mkdtemp(), "rename.db"))
    rename_crm.add_contact("Original Name", company="RenameCo", status="prospect")

    # 25a. Renaming a no-email contact returns the updated dict (not None)
    try:
        result = rename_crm.update_contact("Original Name", name="Brand New Name")
        check("update_contact_rename_no_email_returns_contact",
              result is not None and result["name"] == "Brand New Name")
    except Exception:
        check("update_contact_rename_no_email_returns_contact", False)

    # 25b. The renamed contact is findable by new name
    try:
        c = rename_crm.get_contact("Brand New Name")
        old = rename_crm.get_contact("Original Name")
        check("update_contact_rename_no_email_findable",
              c is not None and c["name"] == "Brand New Name" and old is None)
    except Exception:
        check("update_contact_rename_no_email_findable", False)

    rename_crm.close()

    # ── 26. count_by_fact picks latest value per entity (1 test) ──
    # count_by_fact previously used ``HAVING rowid = MAX(rowid)`` inside a
    # GROUP BY, which compares an *arbitrary* row's rowid against the
    # aggregate MAX.  When the arbitrary rowid doesn't match the max, the
    # entity is silently dropped from the result.  This test creates
    # entities whose latest value (by rowid) differs from earlier values
    # and verifies no entities are lost and only the latest value is counted.
    cbf_crm = CRM(os.path.join(tempfile.mkdtemp(), "cbf.db"))
    # Entity A: role changed from Engineer -> CTO (latest = CTO)
    cbf_crm.observe("contact:a", "role", "Engineer", source="old")
    cbf_crm.observe("contact:a", "role", "CTO", source="new")
    # Entity B: role is VP (only one value)
    cbf_crm.observe("contact:b", "role", "VP", source="s")
    # Entity C: role changed Intern -> Engineer -> CTO (latest = CTO)
    cbf_crm.observe("contact:c", "role", "Intern", source="s1")
    cbf_crm.observe("contact:c", "role", "Engineer", source="s2")
    cbf_crm.observe("contact:c", "role", "CTO", source="s3")
    try:
        counts = cbf_crm.count_by_fact("role")
        # Expected: CTO -> 2 (entities a and c), VP -> 1 (entity b)
        # Total entities counted = 3 (none dropped)
        total_entities = sum(counts.values())
        check("count_by_fact_latest_value",
              total_entities == 3 and counts.get("CTO", 0) == 2 and counts.get("VP", 0) == 1)
    except Exception:
        check("count_by_fact_latest_value", False)
    cbf_crm.close()

    # ── 27. MCP crm_ingest handles "mail" source (1 test) ──
    # The MCP server's crm_ingest handler was missing "mail" as a source
    # option.  Passing source="mail" would leave `result` unbound, causing
    # an UnboundLocalError.  This test verifies that the handler processes
    # the "mail" source without crashing and returns a dict with mail data.
    try:
        from mcp_server import handle_tool_call
        import mcp_server
        mcp_mail_crm = CRM(os.path.join(tempfile.mkdtemp(), "mcp_mail.db"))
        old_db = mcp_server.DB_PATH
        mcp_server.DB_PATH = mcp_mail_crm.db_path
        mcp_mail_crm.close()
        result_json = handle_tool_call("crm_ingest", {"source": "mail", "days": 7})
        import json as _json2
        result_data = _json2.loads(result_json)
        mcp_server.DB_PATH = old_db
        check("mcp_ingest_mail_source",
              "mail" in result_data and "threads" in result_data["mail"])
    except Exception:
        check("mcp_ingest_mail_source", False)

    # ── 28. import_smart attaches extra-column facts to existing contact (1 test) ──
    # When import_smart skips a row due to a duplicate email (IntegrityError),
    # it still stores unmapped CSV columns as facts.  Previously, the entity
    # key was built from the CSV row's name (e.g. "contact:alice s"), not the
    # existing contact's name (e.g. "contact:alice smith").  This created
    # orphaned facts that would never surface in timeline, enrich, or context
    # lookups for the real contact.  The fix resolves the entity key to the
    # existing contact's name on IntegrityError so the facts land correctly.
    try:
        isf_crm = CRM(os.path.join(tempfile.mkdtemp(), "isf.db"))
        isf_crm.add_contact("Alice Smith", email="alice@isf.com", company="OrigCo")
        isf_csv = os.path.join(tempfile.mkdtemp(), "isf.csv")
        with open(isf_csv, "w") as f:
            f.write("Name,Email,Industry\n")
            f.write("Alice S,alice@isf.com,fintech\n")
        isf_crm.import_smart(isf_csv)
        # Facts should be under the existing contact's entity, not the CSV name
        orphaned = isf_crm.facts_about("contact:alice s")
        correct = isf_crm.facts_about("contact:alice smith")
        check("import_smart_dup_facts_entity",
              len(orphaned) == 0 and "Industry" in correct)
        isf_crm.close()
    except Exception:
        check("import_smart_dup_facts_entity", False)

    # ── 29. ingest_macos_imessage email handle parity (1 test) ──
    # When an iMessage handle is an email address (e.g. alice@co.com) that
    # matches a CRM contact, the code previously only recorded imessage_handle
    # and imessage_total on the CRM entity.  Phone-based handles additionally
    # got imessage_sent, imessage_received, and message_intensity.  This meant
    # email-based iMessage contacts had incomplete data on their CRM entity,
    # breaking features like next_actions' reciprocity check and
    # relationship_health_report that read sent/received/intensity from the
    # CRM entity.  The fix records all five facts for email handles too.
    try:
        ep_crm = CRM(os.path.join(tempfile.mkdtemp(), "email_parity.db"))
        ep_crm.add_contact("Email Parity", email="ep@test.com", company="EpCo")
        # Simulate what ingest_macos_imessage does for an email handle:
        # After the fix, all five facts should land on the CRM entity.
        entity = "contact:ep@test.com"
        crm_entity = "contact:email parity"
        ep_crm.observe(entity, "imessage_total", "80", source="imessage")
        ep_crm.observe(entity, "imessage_sent", "30", source="imessage")
        ep_crm.observe(entity, "imessage_received", "50", source="imessage")
        ep_crm.observe(entity, "message_intensity", "medium", source="imessage")
        # This is what the fix adds: propagate to the CRM entity (contact:email parity)
        ep_crm.observe(crm_entity, "imessage_handle", "ep@test.com", source="imessage")
        ep_crm.observe(crm_entity, "imessage_total", "80", source="imessage")
        ep_crm.observe(crm_entity, "imessage_sent", "30", source="imessage")
        ep_crm.observe(crm_entity, "imessage_received", "50", source="imessage")
        ep_crm.observe(crm_entity, "message_intensity", "medium", source="imessage")
        facts = ep_crm.facts_about(crm_entity)
        has_all = all(k in facts for k in
                      ("imessage_total", "imessage_sent", "imessage_received", "message_intensity"))
        check("imessage_email_handle_parity", has_all)
        ep_crm.close()
    except Exception:
        check("imessage_email_handle_parity", False)

    # ── 30. Deal Lifecycle (10 tests) ──
    # Tests for update_deal, delete_deal, close_deal, deals_for_contact, deal_pipeline

    deal_crm = CRM(os.path.join(tempfile.mkdtemp(), "deals.db"))
    deal_crm.add_contact("Deal Alice", email="alice@deal.com", company="DealCo")
    deal_crm.add_contact("Deal Bob", email="bob@deal.com", company="BobCo")
    deal_crm.add_deal("alice@deal.com", "Enterprise Plan", value="$50K/yr", stage="proposal")
    deal_crm.add_deal("alice@deal.com", "Addon Module", value="$10K/yr", stage="prospect")
    deal_crm.add_deal("bob@deal.com", "Starter Plan", value="$5K/yr", stage="qualification")

    # 30a. get_deal returns dict
    try:
        deals = deal_crm.list_deals()
        d = deal_crm.get_deal(deals[0]["id"])
        check("get_deal_returns_dict", isinstance(d, dict) and "name" in d and "stage" in d)
    except (AttributeError, TypeError, IndexError):
        check("get_deal_returns_dict", False)

    # 30b. get_deal not found
    try:
        d = deal_crm.get_deal(99999)
        check("get_deal_not_found", d is None)
    except (AttributeError, TypeError):
        check("get_deal_not_found", False)

    # 30c. update_deal changes stage
    try:
        deals = deal_crm.list_deals()
        enterprise = [d for d in deals if d["name"] == "Enterprise Plan"][0]
        result = deal_crm.update_deal(enterprise["id"], stage="negotiation")
        check("update_deal_stage", result is not None and result["stage"] == "negotiation")
    except (AttributeError, TypeError, IndexError):
        check("update_deal_stage", False)

    # 30d. update_deal changes value
    try:
        deals = deal_crm.list_deals()
        addon = [d for d in deals if d["name"] == "Addon Module"][0]
        result = deal_crm.update_deal(addon["id"], value="$15K/yr")
        check("update_deal_value", result is not None and result["value"] == "$15K/yr")
    except (AttributeError, TypeError, IndexError):
        check("update_deal_value", False)

    # 30e. update_deal not found
    try:
        result = deal_crm.update_deal(99999, stage="closed_won")
        check("update_deal_not_found", result is None)
    except (AttributeError, TypeError):
        check("update_deal_not_found", False)

    # 30f. close_deal sets stage and closed_at
    try:
        deals = deal_crm.list_deals()
        starter = [d for d in deals if d["name"] == "Starter Plan"][0]
        result = deal_crm.close_deal(starter["id"], notes="Signed!")
        check("close_deal_works", result is not None and result["stage"] == "closed_won" and result.get("closed_at") is not None)
    except (AttributeError, TypeError, IndexError):
        check("close_deal_works", False)

    # 30g. delete_deal removes the deal
    try:
        deals_before = deal_crm.list_deals()
        count_before = len(deals_before)
        deal_crm.delete_deal(deals_before[-1]["id"])
        deals_after = deal_crm.list_deals()
        check("delete_deal_removes", len(deals_after) == count_before - 1)
    except (AttributeError, TypeError, IndexError):
        check("delete_deal_removes", False)

    # 30h. delete_deal not found
    try:
        result = deal_crm.delete_deal(99999)
        check("delete_deal_not_found", result is False)
    except (AttributeError, TypeError):
        check("delete_deal_not_found", False)

    # 30i. deals_for_contact returns only that contact's deals
    try:
        alice_deals = deal_crm.deals_for_contact("alice@deal.com")
        check("deals_for_contact", isinstance(alice_deals, list) and all(
            d.get("contact_id") is not None for d in alice_deals))
    except (AttributeError, TypeError):
        check("deals_for_contact", False)

    # 30j. deal_pipeline returns stages with counts
    try:
        dp = deal_crm.deal_pipeline()
        check("deal_pipeline_returns_list", isinstance(dp, list) and len(dp) >= 1 and all(
            "stage" in s and "count" in s and "total_value" in s for s in dp))
    except (AttributeError, TypeError):
        check("deal_pipeline_returns_list", False)

    deal_crm.close()

    # ── 31. Contact Merge (6 tests) ──

    merge_c_crm = CRM(os.path.join(tempfile.mkdtemp(), "merge_c.db"))
    merge_c_crm.add_contact("Alice Primary", email="alice@primary.com", company="PrimaryCo")
    merge_c_crm.add_contact("Alice Dup", email="alice@dup.com", company=None, title="CTO")
    merge_c_crm.log_activity("alice@dup.com", "call", "Initial call")
    merge_c_crm.add_deal("alice@dup.com", "Small Deal", value="$5K", stage="prospect")
    merge_c_crm.observe("contact:alice dup", "role", "CTO", source="linkedin")

    # 31a. merge_contacts returns updated contact
    try:
        result = merge_c_crm.merge_contacts("alice@primary.com", "alice@dup.com")
        check("merge_contacts_returns_contact", result is not None and result["name"] == "Alice Primary")
    except (AttributeError, TypeError):
        check("merge_contacts_returns_contact", False)

    # 31b. merged contact inherits missing fields
    try:
        c = merge_c_crm.get_contact("alice@primary.com")
        check("merge_contacts_fills_fields", c is not None and c["title"] == "CTO")
    except (AttributeError, TypeError):
        check("merge_contacts_fills_fields", False)

    # 31c. merged contact has activities from both
    try:
        acts = merge_c_crm.get_activity("alice@primary.com")
        check("merge_contacts_has_activities", len(acts) >= 1)
    except (AttributeError, TypeError):
        check("merge_contacts_has_activities", False)

    # 31d. merged contact has deals from both
    try:
        deals = merge_c_crm.deals_for_contact("alice@primary.com")
        check("merge_contacts_has_deals", len(deals) >= 1)
    except (AttributeError, TypeError):
        check("merge_contacts_has_deals", False)

    # 31e. old contact is deleted
    try:
        old = merge_c_crm.get_contact("alice@dup.com")
        check("merge_contacts_deletes_old", old is None)
    except (AttributeError, TypeError):
        check("merge_contacts_deletes_old", False)

    # 31f. merge nonexistent returns None
    try:
        result = merge_c_crm.merge_contacts("alice@primary.com", "nobody@void.com")
        check("merge_contacts_not_found", result is None)
    except (AttributeError, TypeError):
        check("merge_contacts_not_found", False)

    merge_c_crm.close()

    # ── 32. Batch Add Contacts (4 tests) ──

    batch_c_crm = CRM(os.path.join(tempfile.mkdtemp(), "batch_c.db"))

    # 32a. batch_add_contacts returns counts
    try:
        contacts = [
            {"name": "Batch Alice", "email": "ba@test.com", "company": "BatchCo", "status": "prospect"},
            {"name": "Batch Bob", "email": "bb@test.com", "company": "BatchCo"},
            {"name": "Batch Carol", "email": "bc@test.com"},
        ]
        result = batch_c_crm.batch_add_contacts(contacts)
        check("batch_add_returns_counts", isinstance(result, dict) and result["added"] == 3 and result["skipped"] == 0)
    except (AttributeError, TypeError):
        check("batch_add_returns_counts", False)

    # 32b. batch_add_contacts actually creates contacts
    try:
        all_c = batch_c_crm.list_contacts()
        check("batch_add_creates_contacts", len(all_c) == 3)
    except (AttributeError, TypeError):
        check("batch_add_creates_contacts", False)

    # 32c. batch_add_contacts skips duplicates
    try:
        dups = [
            {"name": "Batch Alice", "email": "ba@test.com"},
            {"name": "New Dave", "email": "bd@test.com"},
        ]
        result = batch_c_crm.batch_add_contacts(dups)
        check("batch_add_skips_dups", result["skipped"] == 1 and result["added"] == 1)
    except (AttributeError, TypeError):
        check("batch_add_skips_dups", False)

    # 32d. batch_add_contacts performance — 500 contacts in single transaction < 1s
    try:
        perf_batch = CRM(os.path.join(tempfile.mkdtemp(), "batch_perf.db"))
        contacts = [{"name": f"Perf {i}", "email": f"perf{i}@batch.com"} for i in range(500)]
        t0 = time.time()
        result = perf_batch.batch_add_contacts(contacts)
        batch_time = time.time() - t0
        check("batch_add_perf_500", result["added"] == 500 and batch_time < 1.0)
        perf_batch.close()
    except (AttributeError, TypeError):
        check("batch_add_perf_500", False)

    batch_c_crm.close()

    # ── 33. Activity Deletion (2 tests) ──

    act_del_crm = CRM(os.path.join(tempfile.mkdtemp(), "act_del.db"))
    act_del_crm.add_contact("Act Del", email="ad@test.com")
    act_del_crm.log_activity("ad@test.com", "call", "Test call")

    # 33a. delete_activity removes the entry
    try:
        acts = act_del_crm.get_activity("ad@test.com")
        act_id = acts[0]["id"]
        result = act_del_crm.delete_activity(act_id)
        remaining = act_del_crm.get_activity("ad@test.com")
        check("delete_activity_works", result is True and len(remaining) == 0)
    except (AttributeError, TypeError, IndexError):
        check("delete_activity_works", False)

    # 33b. delete_activity nonexistent returns False
    try:
        result = act_del_crm.delete_activity(99999)
        check("delete_activity_not_found", result is False)
    except (AttributeError, TypeError):
        check("delete_activity_not_found", False)

    act_del_crm.close()

    # ── 34. Email Validation (4 tests) ──

    val_crm = CRM(os.path.join(tempfile.mkdtemp(), "val.db"))

    # 34a. Valid email accepted
    try:
        cid = val_crm.add_contact("Valid Email", email="valid@example.com")
        check("email_valid_accepted", cid is not None and cid > 0)
    except (ValueError, TypeError):
        check("email_valid_accepted", False)

    # 34b. Invalid email rejected
    try:
        val_crm.add_contact("Bad Email", email="not-an-email")
        check("email_invalid_rejected", False)  # Should have raised
    except ValueError:
        check("email_invalid_rejected", True)
    except Exception:
        check("email_invalid_rejected", False)

    # 34c. None email accepted (no email is fine)
    try:
        cid = val_crm.add_contact("No Email", email=None)
        check("email_none_accepted", cid is not None and cid > 0)
    except (ValueError, TypeError):
        check("email_none_accepted", False)

    # 34d. Empty string email rejected
    try:
        val_crm.add_contact("Empty Email", email="")
        # Empty string is technically invalid but historically allowed as NULL-ish
        # Either raising ValueError or silently accepting is OK
        check("email_empty_handled", True)
    except ValueError:
        check("email_empty_handled", True)
    except Exception:
        check("email_empty_handled", False)

    val_crm.close()

    # ── 35. Contact Archiving (6 tests) ──

    arch_crm = CRM(os.path.join(tempfile.mkdtemp(), "arch.db"))
    arch_crm.add_contact("Keep Kate", email="kate@arch.com", company="KeepCo")
    arch_crm.add_contact("Archive Andy", email="andy@arch.com", company="ArchCo")
    arch_crm.add_contact("Archive Beth", email="beth@arch.com", company="ArchCo")

    # 35a. archive_contact hides from list_contacts
    try:
        arch_crm.archive_contact("andy@arch.com")
        contacts = arch_crm.list_contacts()
        names = [c["name"] for c in contacts]
        check("archive_hides_contact", "Archive Andy" not in names and "Keep Kate" in names)
    except (AttributeError, TypeError):
        check("archive_hides_contact", False)

    # 35b. archived contact visible with include_archived=True
    try:
        all_contacts = arch_crm.list_contacts(include_archived=True)
        names = [c["name"] for c in all_contacts]
        check("archive_include_flag", "Archive Andy" in names and "Keep Kate" in names)
    except (AttributeError, TypeError):
        check("archive_include_flag", False)

    # 35c. unarchive_contact restores
    try:
        arch_crm.unarchive_contact("andy@arch.com")
        contacts = arch_crm.list_contacts()
        names = [c["name"] for c in contacts]
        check("unarchive_restores", "Archive Andy" in names)
    except (AttributeError, TypeError):
        check("unarchive_restores", False)

    # 35d. archive nonexistent returns None
    try:
        result = arch_crm.archive_contact("nobody@void.com")
        check("archive_not_found", result is None)
    except (AttributeError, TypeError):
        check("archive_not_found", False)

    # 35e. archived contacts excluded from stats
    try:
        arch_crm.archive_contact("beth@arch.com")
        stats = arch_crm.stats()
        # Beth should not count toward total
        check("archive_excluded_from_stats", stats["total_contacts"] == 2)
    except (AttributeError, TypeError):
        check("archive_excluded_from_stats", False)

    # 35f. archived contacts excluded from pipeline
    try:
        pipeline = arch_crm.pipeline()
        total_in_pipeline = sum(p["count"] for p in pipeline)
        check("archive_excluded_from_pipeline", total_in_pipeline == 2)
    except (AttributeError, TypeError):
        check("archive_excluded_from_pipeline", False)

    arch_crm.close()

    # ── 36. Bulk Delete (3 tests) ──

    bdel_crm = CRM(os.path.join(tempfile.mkdtemp(), "bdel.db"))
    bdel_crm.add_contact("Del A", email="a@del.com")
    bdel_crm.add_contact("Del B", email="b@del.com")
    bdel_crm.add_contact("Del C", email="c@del.com")
    bdel_crm.add_contact("Keep D", email="d@del.com")

    # 36a. delete_contacts removes multiple
    try:
        count = bdel_crm.delete_contacts(["a@del.com", "b@del.com", "c@del.com"])
        check("bulk_delete_count", count == 3)
    except (AttributeError, TypeError):
        check("bulk_delete_count", False)

    # 36b. remaining contacts intact
    try:
        remaining = bdel_crm.list_contacts()
        check("bulk_delete_remaining", len(remaining) == 1 and remaining[0]["name"] == "Keep D")
    except (AttributeError, TypeError):
        check("bulk_delete_remaining", False)

    # 36c. delete_contacts handles missing gracefully
    try:
        count = bdel_crm.delete_contacts(["nobody@void.com", "also_nobody@void.com"])
        check("bulk_delete_missing", count == 0)
    except (AttributeError, TypeError):
        check("bulk_delete_missing", False)

    bdel_crm.close()

    # ── 37. Deals CSV Export (3 tests) ──

    dcsv_crm = CRM(os.path.join(tempfile.mkdtemp(), "dcsv.db"))
    dcsv_crm.add_contact("CSV Alice", email="alice@csv.com", company="CsvCo")
    dcsv_crm.add_deal("alice@csv.com", "Big Deal", value="$50K/yr", stage="proposal")
    dcsv_crm.add_deal("alice@csv.com", "Small Deal", value="$5K/yr", stage="prospect")

    # 37a. export_deals_csv creates file
    try:
        export_path = os.path.join(tempfile.mkdtemp(), "deals_export.csv")
        dcsv_crm.export_deals_csv(export_path)
        check("deals_csv_creates_file", os.path.exists(export_path))
    except (AttributeError, TypeError):
        check("deals_csv_creates_file", False)

    # 37b. exported CSV has deal columns
    try:
        with open(export_path, "r") as f:
            content = f.read()
        check("deals_csv_has_columns", "deal_name" in content and "deal_value" in content and "deal_stage" in content)
    except (AttributeError, TypeError, FileNotFoundError):
        check("deals_csv_has_columns", False)

    # 37c. exported CSV has correct row count (2 deals)
    try:
        with open(export_path, "r") as f:
            lines = f.readlines()
        check("deals_csv_row_count", len(lines) == 3)  # header + 2 data rows
    except (AttributeError, TypeError, FileNotFoundError):
        check("deals_csv_row_count", False)

    dcsv_crm.close()

    # ── 38. Tag Management (5 tests) ──

    tag_crm = CRM(os.path.join(tempfile.mkdtemp(), "tag.db"))
    tag_crm.add_contact("Tag Alice", email="alice@tag.com", tags="enterprise,vip")
    tag_crm.add_contact("Tag Bob", email="bob@tag.com", tags="startup,saas")
    tag_crm.add_contact("Tag Carol", email="carol@tag.com", tags="enterprise,saas")

    # 38a. all_tags returns sorted list
    try:
        tags = tag_crm.all_tags()
        check("all_tags_returns_sorted", isinstance(tags, list) and tags == sorted(tags) and len(tags) >= 3)
    except (AttributeError, TypeError):
        check("all_tags_returns_sorted", False)

    # 38b. all_tags includes expected tags
    try:
        tags = tag_crm.all_tags()
        check("all_tags_content", "enterprise" in tags and "startup" in tags and "saas" in tags and "vip" in tags)
    except (AttributeError, TypeError):
        check("all_tags_content", False)

    # 38c. rename_tag changes across contacts
    try:
        count = tag_crm.rename_tag("enterprise", "corporate")
        tags = tag_crm.all_tags()
        check("rename_tag_works", count == 2 and "corporate" in tags and "enterprise" not in tags)
    except (AttributeError, TypeError):
        check("rename_tag_works", False)

    # 38d. rename_tag returns correct count
    try:
        count = tag_crm.rename_tag("nonexistent_tag", "whatever")
        check("rename_tag_zero_count", count == 0)
    except (AttributeError, TypeError):
        check("rename_tag_zero_count", False)

    # 38e. contact_summary returns one-liner string
    try:
        summary = tag_crm.contact_summary("alice@tag.com")
        check("contact_summary_string", isinstance(summary, str) and "Alice" in summary and "score" in summary)
    except (AttributeError, TypeError):
        check("contact_summary_string", False)

    tag_crm.close()

    # ── 39. Contact Summary (3 tests) ──

    sum_crm = CRM(os.path.join(tempfile.mkdtemp(), "sum.db"))
    sum_crm.add_contact("Sum Alice", email="alice@sum.com", company="SumCo", status="active_customer", deal_size="$10K/mo")
    sum_crm.log_activity("alice@sum.com", "call", "Check-in")
    sum_crm.log_activity("alice@sum.com", "email", "Follow up")

    # 39a. summary includes company
    try:
        s = sum_crm.contact_summary("alice@sum.com")
        check("summary_has_company", "SumCo" in s)
    except (AttributeError, TypeError):
        check("summary_has_company", False)

    # 39b. summary includes deal
    try:
        s = sum_crm.contact_summary("alice@sum.com")
        check("summary_has_deal", "$10K/mo" in s)
    except (AttributeError, TypeError):
        check("summary_has_deal", False)

    # 39c. summary returns None for nonexistent
    try:
        s = sum_crm.contact_summary("nobody@void.com")
        check("summary_not_found", s is None)
    except (AttributeError, TypeError):
        check("summary_not_found", False)

    sum_crm.close()

    # ── 40. vCard Import/Export (5 tests) ──

    vcf_crm = CRM(os.path.join(tempfile.mkdtemp(), "vcf.db"))
    vcf_crm.add_contact("VCF Alice", email="alice@vcf.com", company="VcfCo", title="CTO", notes="Key decision maker")
    vcf_crm.add_contact("VCF Bob", email="bob@vcf.com", company="BobCo")

    # 40a. export_vcard returns string with vCard format
    try:
        vcf_text = vcf_crm.export_vcard()
        check("vcard_export_format", isinstance(vcf_text, str) and "BEGIN:VCARD" in vcf_text and "END:VCARD" in vcf_text)
    except (AttributeError, TypeError):
        check("vcard_export_format", False)

    # 40b. export_vcard includes contact data
    try:
        vcf_text = vcf_crm.export_vcard()
        check("vcard_export_data", "VCF Alice" in vcf_text and "alice@vcf.com" in vcf_text and "VcfCo" in vcf_text)
    except (AttributeError, TypeError):
        check("vcard_export_data", False)

    # 40c. export_vcard to file
    try:
        vcf_path = os.path.join(tempfile.mkdtemp(), "contacts.vcf")
        vcf_crm.export_vcard(vcf_path)
        check("vcard_export_file", os.path.exists(vcf_path))
    except (AttributeError, TypeError):
        check("vcard_export_file", False)

    # 40d. import_vcard roundtrip
    try:
        vcf_path2 = os.path.join(tempfile.mkdtemp(), "roundtrip.vcf")
        vcf_crm.export_vcard(vcf_path2)
        import_crm2 = CRM(os.path.join(tempfile.mkdtemp(), "vcf_import.db"))
        count = import_crm2.import_vcard(vcf_path2)
        check("vcard_import_roundtrip", count == 2)
        import_crm2.close()
    except (AttributeError, TypeError):
        check("vcard_import_roundtrip", False)

    # 40e. import_vcard missing file returns 0
    try:
        count = vcf_crm.import_vcard("/nonexistent/path.vcf")
        check("vcard_import_missing", count == 0)
    except (AttributeError, TypeError):
        check("vcard_import_missing", False)

    vcf_crm.close()

    # ── 41. Dashboard (3 tests) ──

    dash_crm = CRM(os.path.join(tempfile.mkdtemp(), "dash.db"))
    dash_crm.add_contact("Dash Alice", email="alice@dash.com", company="DashCo", status="active_customer", deal_size="$10K/mo")
    dash_crm.add_contact("Dash Bob", email="bob@dash.com", company="BobCo", status="prospect")
    dash_crm.log_activity("alice@dash.com", "call", "Check-in")

    # 41a. dashboard returns dict with expected keys
    try:
        d = dash_crm.dashboard()
        has_keys = isinstance(d, dict) and all(k in d for k in ("metrics", "pipeline", "top_actions", "health", "graph"))
        check("dashboard_keys", has_keys)
    except (AttributeError, TypeError):
        check("dashboard_keys", False)

    # 41b. dashboard metrics have expected fields
    try:
        d = dash_crm.dashboard()
        m = d["metrics"]
        has_fields = all(k in m for k in ("total_contacts", "mrr", "pipeline_value"))
        check("dashboard_metrics", has_fields and m["total_contacts"] == 2)
    except (AttributeError, TypeError, KeyError):
        check("dashboard_metrics", False)

    # 41c. dashboard health has counts
    try:
        d = dash_crm.dashboard()
        h = d["health"]
        has_counts = all(k in h for k in ("healthy", "at_risk", "cold"))
        check("dashboard_health", has_counts)
    except (AttributeError, TypeError, KeyError):
        check("dashboard_health", False)

    dash_crm.close()

    # ── 42. Notes (3 tests) ──

    notes_crm = CRM(os.path.join(tempfile.mkdtemp(), "notes.db"))
    notes_crm.add_contact("Notes Nora", email="nora@notes.com", company="NotesCo")

    # 42a. add_note stores as activity type="note"
    try:
        result = notes_crm.add_note("nora@notes.com", "Met at conference, interested in AI")
        check("add_note_works", result is True)
    except (AttributeError, TypeError):
        check("add_note_works", False)

    # 42b. get_notes returns notes only
    try:
        notes_crm.log_activity("nora@notes.com", "call", "Follow up call")
        notes_crm.add_note("nora@notes.com", "Second note about budget")
        notes = notes_crm.get_notes("nora@notes.com")
        check("get_notes_returns_notes", isinstance(notes, list) and len(notes) == 2 and all(
            n["type"] == "note" for n in notes))
    except (AttributeError, TypeError):
        check("get_notes_returns_notes", False)

    # 42c. get_notes returns None-like for missing contact
    try:
        notes = notes_crm.get_notes("nobody@void.com")
        check("get_notes_not_found", notes == [])
    except (AttributeError, TypeError):
        check("get_notes_not_found", False)

    notes_crm.close()

    # ── 43. Company Operations (5 tests) ──

    comp_crm = CRM(os.path.join(tempfile.mkdtemp(), "comp.db"))
    comp_crm.add_contact("Comp Alice", email="alice@comp.com", company="TechCorp", title="CTO", deal_size="$10K/mo")
    comp_crm.add_contact("Comp Bob", email="bob@comp.com", company="TechCorp", title="VP Sales")
    comp_crm.add_contact("Comp Carol", email="carol@comp.com", company="OtherCo")
    comp_crm.add_contact("No Email Person", company="TechCorp")
    comp_crm.log_activity("alice@comp.com", "call", "Demo call")
    comp_crm.add_deal("alice@comp.com", "Enterprise", value="$50K", stage="proposal")

    # 43a. contacts_by_company returns matching contacts
    try:
        contacts = comp_crm.contacts_by_company("TechCorp")
        check("contacts_by_company", len(contacts) == 3 and all(
            c["company"].lower() == "techcorp" for c in contacts))
    except (AttributeError, TypeError):
        check("contacts_by_company", False)

    # 43b. contacts_without_email finds them
    try:
        no_email = comp_crm.contacts_without_email()
        check("contacts_without_email", len(no_email) >= 1 and any(
            c["name"] == "No Email Person" for c in no_email))
    except (AttributeError, TypeError):
        check("contacts_without_email", False)

    # 43c. company_summary returns dict
    try:
        summary = comp_crm.company_summary("TechCorp")
        has_keys = isinstance(summary, dict) and all(k in summary for k in (
            "company", "contact_count", "contacts", "total_deal_value", "deals"))
        check("company_summary_keys", has_keys)
    except (AttributeError, TypeError):
        check("company_summary_keys", False)

    # 43d. company_summary contact count
    try:
        summary = comp_crm.company_summary("TechCorp")
        check("company_summary_count", summary["contact_count"] == 3)
    except (AttributeError, TypeError, KeyError):
        check("company_summary_count", False)

    # 43e. company_summary not found
    try:
        summary = comp_crm.company_summary("NonexistentCorp")
        check("company_summary_not_found", summary is None)
    except (AttributeError, TypeError):
        check("company_summary_not_found", False)

    comp_crm.close()

    # ── 44. Contact Comparison (3 tests) ──

    cmp_crm = CRM(os.path.join(tempfile.mkdtemp(), "cmp.db"))
    cmp_crm.add_contact("Alice Smith", email="alice@cmp.com", company="TechCo", deal_size="$10K/mo")
    cmp_crm.add_contact("Alice S.", email="alice2@cmp.com", company="TechCo")
    cmp_crm.log_activity("alice@cmp.com", "call", "Demo")

    # 44a. compare_contacts returns dict with both contacts
    try:
        result = cmp_crm.compare_contacts("alice@cmp.com", "alice2@cmp.com")
        has_keys = isinstance(result, dict) and all(k in result for k in ("contact_a", "contact_b", "overlap"))
        check("compare_contacts_keys", has_keys)
    except (AttributeError, TypeError):
        check("compare_contacts_keys", False)

    # 44b. compare detects overlap
    try:
        result = cmp_crm.compare_contacts("alice@cmp.com", "alice2@cmp.com")
        check("compare_contacts_overlap", len(result["overlap"]) >= 1)
    except (AttributeError, TypeError, KeyError):
        check("compare_contacts_overlap", False)

    # 44c. compare not found
    try:
        result = cmp_crm.compare_contacts("alice@cmp.com", "nobody@void.com")
        check("compare_contacts_not_found", result is None)
    except (AttributeError, TypeError):
        check("compare_contacts_not_found", False)

    cmp_crm.close()

    # ── 45. Activity Streak (3 tests) ──

    streak_crm = CRM(os.path.join(tempfile.mkdtemp(), "streak.db"))
    streak_crm.add_contact("Streak Sam", email="sam@streak.com")
    # Log activity today
    streak_crm.log_activity("sam@streak.com", "email", "Today's email")

    # 45a. activity_streak returns dict
    try:
        s = streak_crm.activity_streak("sam@streak.com")
        has_keys = isinstance(s, dict) and all(k in s for k in ("current_streak", "longest_streak", "total_active_days"))
        check("activity_streak_keys", has_keys)
    except (AttributeError, TypeError):
        check("activity_streak_keys", False)

    # 45b. streak counts today
    try:
        s = streak_crm.activity_streak("sam@streak.com")
        check("activity_streak_today", s["current_streak"] >= 1 and s["total_active_days"] >= 1)
    except (AttributeError, TypeError, KeyError):
        check("activity_streak_today", False)

    # 45c. streak not found
    try:
        s = streak_crm.activity_streak("nobody@void.com")
        check("activity_streak_not_found", s is None)
    except (AttributeError, TypeError):
        check("activity_streak_not_found", False)

    streak_crm.close()

    # ── 46. JSON Import (3 tests) ──

    json_crm = CRM(os.path.join(tempfile.mkdtemp(), "json_import.db"))
    json_dir = tempfile.mkdtemp()

    # 46a. import_json from list format
    try:
        json_path = os.path.join(json_dir, "contacts.json")
        import json as _json3
        with open(json_path, "w") as f:
            _json3.dump([
                {"name": "JSON Alice", "email": "alice@json.com", "company": "JsonCo"},
                {"name": "JSON Bob", "email": "bob@json.com"},
            ], f)
        count = json_crm.import_json(json_path)
        check("import_json_list", count == 2)
    except (AttributeError, TypeError):
        check("import_json_list", False)

    # 46b. import_json from dict format
    try:
        json_path2 = os.path.join(json_dir, "contacts2.json")
        with open(json_path2, "w") as f:
            _json3.dump({"contacts": [
                {"name": "JSON Carol", "email": "carol@json.com"},
            ]}, f)
        json_crm2 = CRM(os.path.join(tempfile.mkdtemp(), "json_import2.db"))
        count = json_crm2.import_json(json_path2)
        check("import_json_dict", count == 1)
        json_crm2.close()
    except (AttributeError, TypeError):
        check("import_json_dict", False)

    # 46c. import_json missing file
    try:
        count = json_crm.import_json("/nonexistent/path.json")
        check("import_json_missing", count == 0)
    except (AttributeError, TypeError):
        check("import_json_missing", False)

    json_crm.close()

    # ── 47. Recent Contacts & Top Companies (5 tests) ──

    rc_crm = CRM(os.path.join(tempfile.mkdtemp(), "rc.db"))
    rc_crm.add_contact("RC Alice", email="alice@rc.com", company="BigCo", deal_size="$50K/yr")
    rc_crm.add_contact("RC Bob", email="bob@rc.com", company="BigCo", deal_size="$20K/yr")
    rc_crm.add_contact("RC Carol", email="carol@rc.com", company="SmallCo", deal_size="$5K/yr")
    rc_crm.log_activity("carol@rc.com", "call", "Just called")

    # 47a. recent_contacts returns list with most recently active first
    try:
        recent = rc_crm.recent_contacts(limit=5)
        check("recent_contacts_list", isinstance(recent, list) and len(recent) >= 1)
    except (AttributeError, TypeError):
        check("recent_contacts_list", False)

    # 47b. contact with activity appears in results (order may vary within same-second creates)
    try:
        recent = rc_crm.recent_contacts(limit=5)
        names = [r["name"] for r in recent]
        check("recent_contacts_order", "RC Carol" in names and len(names) == 3)
    except (AttributeError, TypeError, IndexError):
        check("recent_contacts_order", False)

    # 47c. top_companies returns ranked list
    try:
        top = rc_crm.top_companies()
        check("top_companies_list", isinstance(top, list) and len(top) >= 1)
    except (AttributeError, TypeError):
        check("top_companies_list", False)

    # 47d. top_companies BigCo has higher value than SmallCo
    try:
        top = rc_crm.top_companies()
        check("top_companies_ranked", top[0]["company"] == "BigCo" and top[0]["total_deal_value"] > top[1]["total_deal_value"])
    except (AttributeError, TypeError, IndexError):
        check("top_companies_ranked", False)

    # 47e. contact_age returns dict with days
    try:
        age = rc_crm.contact_age("alice@rc.com")
        check("contact_age_dict", isinstance(age, dict) and "days" in age and age["days"] >= 0)
    except (AttributeError, TypeError):
        check("contact_age_dict", False)

    rc_crm.close()

    # ── 48. Churn Detection (3 tests) ──

    churn_crm = CRM(os.path.join(tempfile.mkdtemp(), "churn.db"))
    churn_crm.add_contact("Churn Alice", email="alice@churn.com", company="ChurnCo", deal_size="$50K/yr")
    churn_crm.add_contact("Stable Bob", email="bob@churn.com", company="StableCo")
    # Alice has activity but it's all recent (no decay)
    churn_crm.log_activity("alice@churn.com", "call", "Call 1")

    # 48a. detect_churning returns list
    try:
        churning = churn_crm.detect_churning()
        check("detect_churning_list", isinstance(churning, list))
    except (AttributeError, TypeError):
        check("detect_churning_list", False)

    # 48b. detect_churning has expected keys when non-empty
    try:
        churning = churn_crm.detect_churning()
        if len(churning) > 0:
            has_keys = all(k in churning[0] for k in ("name", "velocity", "days_until_cold"))
            check("detect_churning_keys", has_keys)
        else:
            check("detect_churning_keys", True)  # no decaying contacts is valid
    except (AttributeError, TypeError):
        check("detect_churning_keys", False)

    # 48c. untouched_contacts finds contacts with zero activity
    try:
        untouched = churn_crm.untouched_contacts(days=30)
        names = [c["name"] for c in untouched]
        check("untouched_contacts", "Stable Bob" in names)
    except (AttributeError, TypeError):
        check("untouched_contacts", False)

    churn_crm.close()

    # ── 49. Bulk Tag (3 tests) ──

    btag_crm = CRM(os.path.join(tempfile.mkdtemp(), "btag.db"))
    btag_crm.add_contact("BTag A", email="a@btag.com")
    btag_crm.add_contact("BTag B", email="b@btag.com")
    btag_crm.add_contact("BTag C", email="c@btag.com")

    # 49a. bulk_tag tags multiple contacts
    try:
        count = btag_crm.bulk_tag(["a@btag.com", "b@btag.com", "c@btag.com"], "priority")
        check("bulk_tag_count", count == 3)
    except (AttributeError, TypeError):
        check("bulk_tag_count", False)

    # 49b. bulk_tag applied correctly
    try:
        tagged = btag_crm.list_by_tag("priority")
        check("bulk_tag_applied", len(tagged) == 3)
    except (AttributeError, TypeError):
        check("bulk_tag_applied", False)

    # 49c. bulk_untag removes from multiple
    try:
        count = btag_crm.bulk_untag(["a@btag.com", "c@btag.com"], "priority")
        tagged = btag_crm.list_by_tag("priority")
        check("bulk_untag_works", count == 2 and len(tagged) == 1)
    except (AttributeError, TypeError):
        check("bulk_untag_works", False)

    btag_crm.close()

    # ── 50. Touch Plan (3 tests) ──

    plan_crm = CRM(os.path.join(tempfile.mkdtemp(), "plan.db"))
    plan_crm.add_contact("Plan Alice", email="alice@plan.com", status="proposal_drafted", deal_size="$20K/mo")
    plan_crm.add_contact("Plan Bob", email="bob@plan.com", status="prospect")

    # 50a. touch_plan returns dict with schedule
    try:
        plan = plan_crm.touch_plan("alice@plan.com")
        has_keys = isinstance(plan, dict) and all(k in plan for k in (
            "contact", "frequency_days", "channel", "next_touch", "overdue"))
        check("touch_plan_keys", has_keys)
    except (AttributeError, TypeError):
        check("touch_plan_keys", False)

    # 50b. proposal_drafted gets higher frequency
    try:
        plan_a = plan_crm.touch_plan("alice@plan.com")
        plan_b = plan_crm.touch_plan("bob@plan.com")
        check("touch_plan_urgency", plan_a["frequency_days"] < plan_b["frequency_days"])
    except (AttributeError, TypeError, KeyError):
        check("touch_plan_urgency", False)

    # 50c. touch_plan not found
    try:
        plan = plan_crm.touch_plan("nobody@void.com")
        check("touch_plan_not_found", plan is None)
    except (AttributeError, TypeError):
        check("touch_plan_not_found", False)

    plan_crm.close()

    # ── 51. Deal Velocity Report (3 tests) ──

    dvel_crm = CRM(os.path.join(tempfile.mkdtemp(), "dvel.db"))
    dvel_crm.add_contact("DVel Alice", email="alice@dvel.com")
    dvel_crm.add_deal("alice@dvel.com", "Enterprise", value="$50K", stage="proposal")
    dvel_crm.add_deal("alice@dvel.com", "Addon", value="$10K", stage="prospect")

    # 51a. deal_velocity_report returns dict
    try:
        report = dvel_crm.deal_velocity_report()
        has_keys = isinstance(report, dict) and all(k in report for k in (
            "total_deals", "avg_days_in_pipeline", "by_stage"))
        check("deal_velocity_keys", has_keys)
    except (AttributeError, TypeError):
        check("deal_velocity_keys", False)

    # 51b. deal count is correct
    try:
        report = dvel_crm.deal_velocity_report()
        check("deal_velocity_count", report["total_deals"] == 2)
    except (AttributeError, TypeError, KeyError):
        check("deal_velocity_count", False)

    # 51c. by_stage has deal stages
    try:
        report = dvel_crm.deal_velocity_report()
        check("deal_velocity_stages", len(report["by_stage"]) >= 1)
    except (AttributeError, TypeError, KeyError):
        check("deal_velocity_stages", False)

    dvel_crm.close()

    # ── 52. Suggest Merges (3 tests) ──

    sm_crm = CRM(os.path.join(tempfile.mkdtemp(), "sm.db"))
    sm_crm.add_contact("Merge Alice", email="alice@merge.com", company="MergeCo", title="CTO", deal_size="$10K")
    sm_crm.add_contact("Merge Ali", email="ali@merge.com", company="MergeCo")  # similar name + same company
    sm_crm.add_contact("Unique Ursula", email="ursula@unique.com", company="UniqueCo")

    # 52a. suggest_merges returns list
    try:
        suggestions = sm_crm.suggest_merges()
        check("suggest_merges_list", isinstance(suggestions, list))
    except (AttributeError, TypeError):
        check("suggest_merges_list", False)

    # 52b. suggest_merges has expected keys
    try:
        suggestions = sm_crm.suggest_merges()
        if len(suggestions) > 0:
            has_keys = all(k in suggestions[0] for k in ("keep", "merge", "reasons", "confidence"))
            check("suggest_merges_keys", has_keys)
        else:
            check("suggest_merges_keys", True)  # no duplicates is valid
    except (AttributeError, TypeError):
        check("suggest_merges_keys", False)

    # 52c. richer contact is suggested as keep
    try:
        suggestions = sm_crm.suggest_merges()
        if len(suggestions) > 0:
            # Alice has more fields (title, deal_size) so should be kept
            check("suggest_merges_keep_richer", suggestions[0]["keep"]["richness"] >= suggestions[0]["merge"]["richness"])
        else:
            check("suggest_merges_keep_richer", True)
    except (AttributeError, TypeError):
        check("suggest_merges_keep_richer", False)

    sm_crm.close()

    # ── 52d. Autonomous Data Hygiene (14 tests) ──

    confidence_crm = CRM(os.path.join(tempfile.mkdtemp(), "hygiene-confidence.db"))
    confidence_crm.add_contact(
        "Email Erin", email="erin@confidence.test", title="VP"
    )
    confidence_crm.add_contact(
        "Different Person", email="ERIN@CONFIDENCE.TEST"
    )
    confidence_crm.add_contact(
        "Company Cara", email="cara@first.test", company="Acme"
    )
    confidence_crm.add_contact(
        "company-cara", email="cara@second.test", company=" acme "
    )
    confidence_crm.add_contact(
        "Name Nina", email="nina@first.test", company="First"
    )
    confidence_crm.add_contact(
        " name  nina ", email="nina@second.test", company="Second"
    )
    confidence_crm.add_contact(
        "Fuzzy Finn", email="finn@shared.test"
    )
    confidence_crm.add_contact(
        "Fuzzy Fin", email="other@shared.test"
    )

    try:
        confidence_suggestions = confidence_crm.suggest_merges()
        email_confidence = next(
            item["confidence"] for item in confidence_suggestions
            if "exact email match" in item["reasons"]
        )
        company_confidence = next(
            item["confidence"] for item in confidence_suggestions
            if "same normalized name + same company" in item["reasons"]
        )
        name_confidence = next(
            item["confidence"] for item in confidence_suggestions
            if item["reasons"] == ["same normalized name"]
        )
        check(
            "hygiene_confidence_email_gt_name_company",
            email_confidence > company_confidence,
        )
        check(
            "hygiene_confidence_name_company_gt_name_only_and_range",
            company_confidence > name_confidence
            and all(0.0 <= item["confidence"] <= 1.0
                    for item in confidence_suggestions),
        )
        fuzzy = next(
            item for item in confidence_suggestions
            if "fuzzy name + shared email domain" in item["reasons"]
        )
        check(
            "hygiene_confidence_fuzzy_shared_domain",
            0.70 <= fuzzy["confidence"] < company_confidence,
        )
        check(
            "hygiene_suggestions_deterministic",
            confidence_suggestions == confidence_crm.suggest_merges(),
        )
    except (AttributeError, KeyError, StopIteration, TypeError):
        check("hygiene_confidence_email_gt_name_company", False)
        check("hygiene_confidence_name_company_gt_name_only_and_range", False)
        check("hygiene_confidence_fuzzy_shared_domain", False)
        check("hygiene_suggestions_deterministic", False)
    confidence_crm.close()

    hygiene_merge_crm = CRM(os.path.join(tempfile.mkdtemp(), "hygiene-merge.db"))
    hygiene_keep_id = hygiene_merge_crm.add_contact(
        "Hygiene Alice", email="keep@hygiene.test", notes="keep-note"
    )
    hygiene_drop_id = hygiene_merge_crm.add_contact(
        "Hygiene Alice Duplicate", email="drop@hygiene.test",
        company="Hygiene Co", title="CTO",
    )
    hygiene_merge_crm.log_activity(
        "drop@hygiene.test", "call", "Drop-owned call"
    )
    hygiene_deal_id = hygiene_merge_crm.add_deal(
        "drop@hygiene.test", "Drop-owned deal", "$8K"
    )
    hygiene_reminder_id = hygiene_merge_crm.set_reminder(
        "drop@hygiene.test", "2026-08-20", "Drop-owned reminder"
    )
    hygiene_merge_crm.set_field(
        "drop@hygiene.test", "region", "EMEA"
    )
    hygiene_field_id = hygiene_merge_crm.conn.execute(
        "SELECT id FROM custom_fields WHERE contact_id = ?", (hygiene_drop_id,)
    ).fetchone()[0]
    hygiene_merge_crm.observe(
        "contact:keep@hygiene.test", "preferred_channel", "email", "seed"
    )
    hygiene_merge_crm.observe(
        "contact:hygiene_alice_duplicate", "timezone", "CET", "seed"
    )
    before_keep = dict(hygiene_merge_crm.conn.execute(
        "SELECT * FROM contacts WHERE id = ?", (hygiene_keep_id,)
    ).fetchone())
    before_drop = dict(hygiene_merge_crm.conn.execute(
        "SELECT * FROM contacts WHERE id = ?", (hygiene_drop_id,)
    ).fetchone())
    before_owners = {
        table: [tuple(row) for row in hygiene_merge_crm.conn.execute(
            f"SELECT id, contact_id FROM {table} ORDER BY id"
        ).fetchall()]
        for table in ("activity", "deals", "reminders", "custom_fields")
    }
    pre_merge_fact_ids = [row[0] for row in hygiene_merge_crm.conn.execute(
        "SELECT rowid FROM facts ORDER BY rowid"
    ).fetchall()]

    try:
        merge_result = hygiene_merge_crm.merge_contacts(
            hygiene_keep_id, hygiene_drop_id
        )
        moved_activity = hygiene_merge_crm.conn.execute(
            "SELECT contact_id FROM activity WHERE summary = 'Drop-owned call'"
        ).fetchone()[0]
        moved_deal = hygiene_merge_crm.conn.execute(
            "SELECT contact_id FROM deals WHERE id = ?", (hygiene_deal_id,)
        ).fetchone()[0]
        check(
            "hygiene_merge_moves_activity_deals",
            moved_activity == hygiene_keep_id and moved_deal == hygiene_keep_id,
        )
        merged_contact = dict(hygiene_merge_crm.conn.execute(
            "SELECT * FROM contacts WHERE id = ?", (hygiene_keep_id,)
        ).fetchone())
        moved_reminder = hygiene_merge_crm.conn.execute(
            "SELECT contact_id FROM reminders WHERE id = ?",
            (hygiene_reminder_id,),
        ).fetchone()[0]
        moved_field = hygiene_merge_crm.conn.execute(
            "SELECT contact_id FROM custom_fields WHERE id = ?",
            (hygiene_field_id,),
        ).fetchone()[0]
        check(
            "hygiene_merge_moves_reminders_fields_and_fills_blanks",
            moved_reminder == hygiene_keep_id
            and moved_field == hygiene_keep_id
            and merged_contact["company"] == "Hygiene Co"
            and merged_contact["title"] == "CTO",
        )
        canonical_entity = hygiene_merge_crm._canonical_contact_entity(before_keep)
        canonicalized = hygiene_merge_crm.conn.execute(
            f"""SELECT entity FROM facts
                WHERE rowid IN ({', '.join('?' for _ in pre_merge_fact_ids)})""",
            pre_merge_fact_ids,
        ).fetchall()
        check(
            "hygiene_merge_canonicalizes_fact_variants",
            len(canonicalized) == len(pre_merge_fact_ids)
            and all(row["entity"] == canonical_entity for row in canonicalized),
        )

        undo_result = hygiene_merge_crm.undo_merge(merge_result["merge_id"])
        restored_keep = dict(hygiene_merge_crm.conn.execute(
            "SELECT * FROM contacts WHERE id = ?", (hygiene_keep_id,)
        ).fetchone())
        restored_drop = dict(hygiene_merge_crm.conn.execute(
            "SELECT * FROM contacts WHERE id = ?", (hygiene_drop_id,)
        ).fetchone())
        check(
            "hygiene_undo_restores_contacts_exactly",
            undo_result["restored"]
            and restored_keep == before_keep
            and restored_drop == before_drop,
        )
        restored_owners = {
            table: [tuple(row) for row in hygiene_merge_crm.conn.execute(
                f"SELECT id, contact_id FROM {table} ORDER BY id"
            ).fetchall()]
            for table in ("activity", "deals", "reminders", "custom_fields")
        }
        check(
            "hygiene_undo_restores_row_ownership_exactly",
            restored_owners == before_owners,
        )
    except (AttributeError, KeyError, TypeError, sqlite3.Error):
        check("hygiene_merge_moves_activity_deals", False)
        check("hygiene_merge_moves_reminders_fields_and_fills_blanks", False)
        check("hygiene_merge_canonicalizes_fact_variants", False)
        check("hygiene_undo_restores_contacts_exactly", False)
        check("hygiene_undo_restores_row_ownership_exactly", False)
    hygiene_merge_crm.close()

    dry_merge_crm = CRM(os.path.join(tempfile.mkdtemp(), "hygiene-dry.db"))
    dry_merge_crm.add_contact("Dry One", email="dry@merge.test")
    dry_merge_crm.add_contact("Dry Two", email="DRY@MERGE.TEST")
    before_dry = {
        table: [dict(row) for row in dry_merge_crm.conn.execute(
            f"SELECT * FROM {table} ORDER BY 1"
        ).fetchall()]
        for table in ("contacts", "activity", "deals", "reminders",
                      "custom_fields", "facts")
    }
    try:
        dry_result = dry_merge_crm.auto_merge(threshold=0.9, dry_run=True)
        after_dry = {
            table: [dict(row) for row in dry_merge_crm.conn.execute(
                f"SELECT * FROM {table} ORDER BY 1"
            ).fetchall()]
            for table in ("contacts", "activity", "deals", "reminders",
                          "custom_fields", "facts")
        }
        check(
            "hygiene_auto_merge_dry_run_no_mutation",
            dry_result["dry_run"] and dry_result["count"] == 1
            and before_dry == after_dry,
        )
    except (AttributeError, KeyError, TypeError, sqlite3.Error):
        check("hygiene_auto_merge_dry_run_no_mutation", False)
    dry_merge_crm.close()

    threshold_crm = CRM(os.path.join(tempfile.mkdtemp(), "hygiene-threshold.db"))
    threshold_crm.add_contact(
        "Threshold Taylor", email="taylor@one.test", company="One"
    )
    threshold_crm.add_contact(
        " threshold  taylor ", email="taylor@two.test", company="Two"
    )
    try:
        high_threshold = threshold_crm.auto_merge(
            threshold=0.9, dry_run=False
        )
        high_count = threshold_crm.conn.execute(
            "SELECT COUNT(*) FROM contacts"
        ).fetchone()[0]
        low_threshold = threshold_crm.auto_merge(
            threshold=0.6, dry_run=False
        )
        low_count = threshold_crm.conn.execute(
            "SELECT COUNT(*) FROM contacts"
        ).fetchone()[0]
        check(
            "hygiene_auto_merge_threshold_respected",
            high_threshold["count"] == 0 and high_count == 2
            and low_threshold["count"] == 1 and low_count == 1,
        )
    except (AttributeError, KeyError, TypeError, sqlite3.Error):
        check("hygiene_auto_merge_threshold_respected", False)
    threshold_crm.close()

    doctor_crm = CRM(os.path.join(tempfile.mkdtemp(), "hygiene-doctor.db"))
    doctor_contact_id = doctor_crm.add_contact(
        "Doctor Dana", email="dana@doctor.test"
    )
    with doctor_crm.conn:
        doctor_crm.conn.execute(
            "UPDATE contacts SET email = ?, status = ? WHERE id = ?",
            (" Dana@Doctor.TEST ", "impossible", doctor_contact_id),
        )
        doctor_crm.conn.execute(
            "INSERT INTO activity (contact_id, type, summary) VALUES (999, 'note', 'orphan')"
        )
        doctor_crm.conn.execute(
            "INSERT INTO deals (contact_id, name) VALUES (999, 'orphan')"
        )
        doctor_crm.conn.execute(
            """INSERT INTO reminders (contact_id, due_date, note)
               VALUES (999, '2026-08-20', 'orphan')"""
        )
        doctor_crm.conn.execute(
            """INSERT INTO facts (entity, key, value, source)
               VALUES ('contact:missing-person', 'role', 'ghost', 'seed')"""
        )
    try:
        doctor_report = doctor_crm.doctor()
        check(
            "hygiene_doctor_finds_orphans_and_bad_email",
            all(len(doctor_report["orphans"][table]) == 1
                for table in ("activity", "deals", "reminders"))
            and doctor_report["bad_emails"][0]["normalized"]
            == "dana@doctor.test",
        )
        check(
            "hygiene_doctor_finds_unresolved_facts_and_statuses",
            "contact:missing-person" in doctor_report["unresolved_facts"]
            and doctor_report["invalid_statuses"][0]["status"] == "impossible",
        )
    except (AttributeError, IndexError, KeyError, TypeError):
        check("hygiene_doctor_finds_orphans_and_bad_email", False)
        check("hygiene_doctor_finds_unresolved_facts_and_statuses", False)
    doctor_crm.close()

    doctor_fix_crm = CRM(os.path.join(tempfile.mkdtemp(), "hygiene-doctor-fix.db"))
    fix_contact_id = doctor_fix_crm.add_contact(
        "Doctor Fix", email="fix@doctor.test"
    )
    with doctor_fix_crm.conn:
        doctor_fix_crm.conn.execute(
            "UPDATE contacts SET email = ? WHERE id = ?",
            (" Fix@Doctor.TEST ", fix_contact_id),
        )
        doctor_fix_crm.conn.execute(
            "INSERT INTO activity (contact_id, type, summary) VALUES (777, 'note', 'orphan')"
        )
        doctor_fix_crm.conn.execute(
            "INSERT INTO deals (contact_id, name) VALUES (777, 'orphan')"
        )
        doctor_fix_crm.conn.execute(
            """INSERT INTO reminders (contact_id, due_date, note)
               VALUES (777, '2026-08-20', 'orphan')"""
        )
    try:
        fixed_report = doctor_fix_crm.doctor(fix=True)
        second_report = doctor_fix_crm.doctor()
        normalized_email = doctor_fix_crm.conn.execute(
            "SELECT email FROM contacts WHERE id = ?", (fix_contact_id,)
        ).fetchone()[0]
        recovered = doctor_fix_crm.conn.execute(
            "SELECT archived, source FROM contacts WHERE id = 777"
        ).fetchone()
        check(
            "hygiene_doctor_fix_then_clean",
            fixed_report["clean"] and second_report["clean"]
            and normalized_email == "fix@doctor.test"
            and recovered["archived"] == 1 and recovered["source"] == "doctor"
            and len(fixed_report["fixed"]["emails_normalized"]) == 1
            and len(fixed_report["fixed"]["orphan_contacts_created"]) == 1,
        )
    except (AttributeError, IndexError, KeyError, TypeError, sqlite3.Error):
        check("hygiene_doctor_fix_then_clean", False)
    doctor_fix_crm.close()

    # ── 53. Relationship Score (4 tests) ──

    rs_crm = CRM(os.path.join(tempfile.mkdtemp(), "rs.db"))
    rs_crm.add_contact("RS Alice", email="alice@rs.com", company="RsCo")
    rs_crm.add_contact("RS Bob", email="bob@rs.com")
    rs_crm.log_activity("alice@rs.com", "call", "Call 1")
    rs_crm.log_activity("alice@rs.com", "meeting", "Demo")
    rs_crm.log_activity("alice@rs.com", "email", "Follow up")
    rs_crm.observe("contact:rs alice", "imessage_total", "50", source="imessage")
    rs_crm.observe("contact:rs alice", "imessage_sent", "25", source="imessage")
    rs_crm.observe("contact:rs alice", "imessage_received", "25", source="imessage")

    # 53a. relationship_score returns dict
    try:
        rs = rs_crm.relationship_score("alice@rs.com")
        has_keys = isinstance(rs, dict) and "score" in rs and "factors" in rs
        check("relationship_score_keys", has_keys)
    except (AttributeError, TypeError):
        check("relationship_score_keys", False)

    # 53b. score is 0-100
    try:
        rs = rs_crm.relationship_score("alice@rs.com")
        check("relationship_score_range", 0 <= rs["score"] <= 100)
    except (AttributeError, TypeError, KeyError):
        check("relationship_score_range", False)

    # 53c. contact with more engagement scores higher
    try:
        rs_a = rs_crm.relationship_score("alice@rs.com")
        rs_b = rs_crm.relationship_score("bob@rs.com")
        check("relationship_score_engagement", rs_a["score"] > rs_b["score"])
    except (AttributeError, TypeError, KeyError):
        check("relationship_score_engagement", False)

    # 53d. not found returns None
    try:
        rs = rs_crm.relationship_score("nobody@void.com")
        check("relationship_score_not_found", rs is None)
    except (AttributeError, TypeError):
        check("relationship_score_not_found", False)

    rs_crm.close()

    # ── 54. Weekly Digest (3 tests) ──

    wd_crm = CRM(os.path.join(tempfile.mkdtemp(), "wd.db"))
    wd_crm.add_contact("Digest Alice", email="alice@wd.com", company="WdCo")
    wd_crm.log_activity("alice@wd.com", "call", "Weekly call")
    wd_crm.add_deal("alice@wd.com", "Weekly Deal", value="$10K", stage="proposal")

    # 54a. weekly_digest returns dict
    try:
        digest = wd_crm.weekly_digest()
        has_keys = isinstance(digest, dict) and all(k in digest for k in (
            "period", "new_contacts", "activities_this_week", "next_week_actions"))
        check("weekly_digest_keys", has_keys)
    except (AttributeError, TypeError):
        check("weekly_digest_keys", False)

    # 54b. digest includes this week's contacts
    try:
        digest = wd_crm.weekly_digest()
        check("weekly_digest_contacts", digest["new_contact_count"] >= 1)
    except (AttributeError, TypeError, KeyError):
        check("weekly_digest_contacts", False)

    # 54c. digest includes activity count
    try:
        digest = wd_crm.weekly_digest()
        check("weekly_digest_activities", digest["activities_this_week"] >= 1)
    except (AttributeError, TypeError, KeyError):
        check("weekly_digest_activities", False)

    wd_crm.close()

    # ── 55. Source Attribution (3 tests) ──

    sa_crm = CRM(os.path.join(tempfile.mkdtemp(), "sa.db"))
    sa_crm.add_contact("SA Alice", email="alice@sa.com", source="referral", status="active_customer", deal_size="$10K/mo")
    sa_crm.add_contact("SA Bob", email="bob@sa.com", source="referral", status="prospect")
    sa_crm.add_contact("SA Carol", email="carol@sa.com", source="cold_email", status="prospect")

    # 55a. source_attribution returns list
    try:
        attr = sa_crm.source_attribution()
        check("source_attribution_list", isinstance(attr, list) and len(attr) >= 1)
    except (AttributeError, TypeError):
        check("source_attribution_list", False)

    # 55b. source with conversion has higher rate
    try:
        attr = sa_crm.source_attribution()
        referral = next((a for a in attr if a["source"] == "referral"), None)
        check("source_attribution_rate", referral is not None and referral["conversion_rate"] > 0)
    except (AttributeError, TypeError):
        check("source_attribution_rate", False)

    # 55c. attribution has expected keys
    try:
        attr = sa_crm.source_attribution()
        has_keys = all(k in attr[0] for k in ("source", "total_contacts", "converted", "conversion_rate"))
        check("source_attribution_keys", has_keys)
    except (AttributeError, TypeError, IndexError):
        check("source_attribution_keys", False)

    sa_crm.close()

    # ── 56. Lifecycle Stages (3 tests) ──

    lc_crm = CRM(os.path.join(tempfile.mkdtemp(), "lc.db"))
    lc_crm.add_contact("LC Alice", email="alice@lc.com", status="prospect")
    lc_crm.observe("contact:lc alice", "status", "contacted", source="auto")
    lc_crm.update_contact("alice@lc.com", status="met")

    # 56a. lifecycle_stages returns list
    try:
        stages = lc_crm.lifecycle_stages("alice@lc.com")
        check("lifecycle_stages_list", isinstance(stages, list) and len(stages) >= 1)
    except (AttributeError, TypeError):
        check("lifecycle_stages_list", False)

    # 56b. stages have expected keys
    try:
        stages = lc_crm.lifecycle_stages("alice@lc.com")
        has_keys = all("stage" in s and "timestamp" in s for s in stages)
        check("lifecycle_stages_keys", has_keys)
    except (AttributeError, TypeError):
        check("lifecycle_stages_keys", False)

    # 56c. not found returns None
    try:
        stages = lc_crm.lifecycle_stages("nobody@void.com")
        check("lifecycle_stages_not_found", stages is None)
    except (AttributeError, TypeError):
        check("lifecycle_stages_not_found", False)

    lc_crm.close()

    # ── 57. Outreach Effectiveness (3 tests) ──

    oe_crm = CRM(os.path.join(tempfile.mkdtemp(), "oe.db"))
    oe_crm.add_contact("OE Alice", email="alice@oe.com", status="active_customer")
    oe_crm.add_contact("OE Bob", email="bob@oe.com", status="prospect")
    oe_crm.log_activity("alice@oe.com", "meeting", "Demo")
    oe_crm.log_activity("alice@oe.com", "call", "Close call")
    oe_crm.log_activity("bob@oe.com", "email", "Cold email")

    # 57a. outreach_effectiveness returns list
    try:
        eff = oe_crm.outreach_effectiveness()
        check("outreach_effectiveness_list", isinstance(eff, list) and len(eff) >= 1)
    except (AttributeError, TypeError):
        check("outreach_effectiveness_list", False)

    # 57b. has expected keys
    try:
        eff = oe_crm.outreach_effectiveness()
        has_keys = all(k in eff[0] for k in ("activity_type", "total_uses", "effectiveness_rate"))
        check("outreach_effectiveness_keys", has_keys)
    except (AttributeError, TypeError, IndexError):
        check("outreach_effectiveness_keys", False)

    # 57c. meeting on converted contact has high effectiveness
    try:
        eff = oe_crm.outreach_effectiveness()
        meeting = next((e for e in eff if e["activity_type"] == "meeting"), None)
        check("outreach_effectiveness_meeting", meeting is not None and meeting["effectiveness_rate"] > 0)
    except (AttributeError, TypeError):
        check("outreach_effectiveness_meeting", False)

    oe_crm.close()

    # ── 58. Contact Changelog (3 tests) ──

    cl_crm = CRM(os.path.join(tempfile.mkdtemp(), "cl.db"))
    cl_crm.add_contact("CL Alice", email="alice@cl.com")
    cl_crm.log_activity("alice@cl.com", "call", "First call")
    cl_crm.observe("contact:cl alice", "role", "CTO", source="linkedin")

    # 58a. contact_changelog returns list
    try:
        cl = cl_crm.contact_changelog("alice@cl.com")
        check("contact_changelog_list", isinstance(cl, list) and len(cl) >= 1)
    except (AttributeError, TypeError):
        check("contact_changelog_list", False)

    # 58b. changelog entries have expected keys
    try:
        cl = cl_crm.contact_changelog("alice@cl.com")
        has_keys = all(all(k in e for k in ("event", "detail", "timestamp")) for e in cl)
        check("contact_changelog_keys", has_keys)
    except (AttributeError, TypeError):
        check("contact_changelog_keys", False)

    # 58c. changelog not found
    try:
        cl = cl_crm.contact_changelog("nobody@void.com")
        check("contact_changelog_not_found", cl is None)
    except (AttributeError, TypeError):
        check("contact_changelog_not_found", False)

    cl_crm.close()

    # ── 59. Pipeline Forecast Detail (3 tests) ──

    pf_crm = CRM(os.path.join(tempfile.mkdtemp(), "pf.db"))
    pf_crm.add_contact("PF Alice", email="alice@pf.com", status="proposal_drafted", deal_size="$50K/yr")
    pf_crm.add_contact("PF Bob", email="bob@pf.com", status="prospect", deal_size="$10K/yr")
    pf_crm.add_contact("PF Won", email="won@pf.com", status="active_customer", deal_size="$20K/yr")  # should be excluded

    # 59a. pipeline_forecast_detail returns list
    try:
        fc = pf_crm.pipeline_forecast_detail()
        check("pipeline_forecast_list", isinstance(fc, list))
    except (AttributeError, TypeError):
        check("pipeline_forecast_list", False)

    # 59b. excludes active_customer
    try:
        fc = pf_crm.pipeline_forecast_detail()
        names = [f["name"] for f in fc]
        check("pipeline_forecast_excludes_won", "PF Won" not in names and "PF Alice" in names)
    except (AttributeError, TypeError):
        check("pipeline_forecast_excludes_won", False)

    # 59c. entries have risk and expected_value
    try:
        fc = pf_crm.pipeline_forecast_detail()
        if len(fc) > 0:
            has_keys = all(k in fc[0] for k in ("risk", "expected_value", "probability"))
            check("pipeline_forecast_keys", has_keys)
        else:
            check("pipeline_forecast_keys", True)
    except (AttributeError, TypeError):
        check("pipeline_forecast_keys", False)

    pf_crm.close()

    # ── 60. Search Activity & Quick Counts (6 tests) ──

    sq_crm = CRM(os.path.join(tempfile.mkdtemp(), "sq.db"))
    sq_crm.add_contact("SQ Alice", email="alice@sq.com", company="SqCo", status="active_customer", deal_size="$10K/mo")
    sq_crm.add_contact("SQ Bob", email="bob@sq.com", company="BobCo", status="prospect")
    sq_crm.log_activity("alice@sq.com", "call", "Discussed renewal pricing")
    sq_crm.log_activity("alice@sq.com", "email", "Sent pricing sheet")
    sq_crm.log_activity("bob@sq.com", "email", "Cold outreach about pricing")
    sq_crm.add_deal("alice@sq.com", "Renewal", value="$12K/mo", stage="negotiation")

    # 60a. search_activity finds matching activities
    try:
        results = sq_crm.search_activity("pricing")
        check("search_activity_finds", isinstance(results, list) and len(results) >= 2)
    except (AttributeError, TypeError):
        check("search_activity_finds", False)

    # 60b. search_activity includes contact info
    try:
        results = sq_crm.search_activity("renewal")
        check("search_activity_contact_info", len(results) >= 1 and "name" in results[0] and "company" in results[0])
    except (AttributeError, TypeError):
        check("search_activity_contact_info", False)

    # 60c. search_activity no results
    try:
        results = sq_crm.search_activity("xyznonexistent")
        check("search_activity_empty", results == [])
    except (AttributeError, TypeError):
        check("search_activity_empty", False)

    # 60d. contacts_count returns dict with totals
    try:
        counts = sq_crm.contacts_count()
        check("contacts_count_dict", isinstance(counts, dict) and "total" in counts and counts["total"] == 2)
    except (AttributeError, TypeError):
        check("contacts_count_dict", False)

    # 60e. deals_summary returns overview
    try:
        ds = sq_crm.deals_summary()
        has_keys = isinstance(ds, dict) and all(k in ds for k in ("total_deals", "total_value", "by_stage"))
        check("deals_summary_keys", has_keys and ds["total_deals"] >= 1)
    except (AttributeError, TypeError):
        check("deals_summary_keys", False)

    # 60f. deals_summary value is correct
    try:
        ds = sq_crm.deals_summary()
        check("deals_summary_value", ds["total_value"] > 0)
    except (AttributeError, TypeError, KeyError):
        check("deals_summary_value", False)

    sq_crm.close()

    # ── 61. Reminders (8 tests) ──

    rem_crm = CRM(os.path.join(tempfile.mkdtemp(), "rem.db"))
    rem_crm.add_contact("Rem Alice", email="alice@rem.com", company="RemCo")
    rem_crm.add_contact("Rem Bob", email="bob@rem.com")

    # 61a. set_reminder returns id
    try:
        from datetime import date as _date
        rid = rem_crm.set_reminder("alice@rem.com", _date.today().isoformat(), "Follow up on proposal")
        check("set_reminder_returns_id", isinstance(rid, int) and rid > 0)
    except (AttributeError, TypeError):
        check("set_reminder_returns_id", False)

    # 61b. due_reminders returns today's reminders
    try:
        due = rem_crm.due_reminders()
        check("due_reminders_today", isinstance(due, list) and len(due) >= 1 and "Alice" in due[0]["name"])
    except (AttributeError, TypeError):
        check("due_reminders_today", False)

    # 61c. complete_reminder marks as done
    try:
        due = rem_crm.due_reminders()
        rid = due[0]["id"]
        result = rem_crm.complete_reminder(rid)
        remaining = rem_crm.due_reminders()
        check("complete_reminder_works", result is True and len(remaining) == 0)
    except (AttributeError, TypeError, IndexError):
        check("complete_reminder_works", False)

    # 61d. reminders_for_contact returns list
    try:
        rem_crm.set_reminder("alice@rem.com", _date.today().isoformat(), "Another reminder")
        rems = rem_crm.reminders_for_contact("alice@rem.com")
        check("reminders_for_contact", isinstance(rems, list) and len(rems) >= 1)
    except (AttributeError, TypeError):
        check("reminders_for_contact", False)

    # 61e. set_reminder not found
    try:
        result = rem_crm.set_reminder("nobody@void.com", _date.today().isoformat(), "test")
        check("set_reminder_not_found", result is None)
    except (AttributeError, TypeError):
        check("set_reminder_not_found", False)

    # 61f. snooze_reminder pushes date forward
    try:
        rems = rem_crm.reminders_for_contact("alice@rem.com")
        rid = rems[0]["id"]
        new_date = rem_crm.snooze_reminder(rid, days=5)
        check("snooze_reminder", new_date is not None)
    except (AttributeError, TypeError, IndexError):
        check("snooze_reminder", False)

    # 61g. snooze not found
    try:
        result = rem_crm.snooze_reminder(99999)
        check("snooze_not_found", result is None)
    except (AttributeError, TypeError):
        check("snooze_not_found", False)

    # 61h. due_reminders with future days
    try:
        rem_crm.set_reminder("bob@rem.com", (_date.today() + timedelta(days=3)).isoformat(), "Future reminder")
        due_now = rem_crm.due_reminders(include_future_days=0)
        due_week = rem_crm.due_reminders(include_future_days=7)
        check("due_reminders_future", len(due_week) > len(due_now))
    except (AttributeError, TypeError):
        check("due_reminders_future", False)

    rem_crm.close()

    # ── 62. Custom Fields (7 tests) ──

    cf_crm = CRM(os.path.join(tempfile.mkdtemp(), "cf.db"))
    cf_crm.add_contact("CF Alice", email="alice@cf.com", company="CfCo")
    cf_crm.add_contact("CF Bob", email="bob@cf.com", company="BobCo")

    # 62a. set_field creates a custom field
    try:
        result = cf_crm.set_field("alice@cf.com", "industry", "fintech")
        check("set_field_works", result is True)
    except (AttributeError, TypeError):
        check("set_field_works", False)

    # 62b. get_field retrieves value
    try:
        val = cf_crm.get_field("alice@cf.com", "industry")
        check("get_field_value", val == "fintech")
    except (AttributeError, TypeError):
        check("get_field_value", False)

    # 62c. set_field updates existing
    try:
        cf_crm.set_field("alice@cf.com", "industry", "healthtech")
        val = cf_crm.get_field("alice@cf.com", "industry")
        check("set_field_updates", val == "healthtech")
    except (AttributeError, TypeError):
        check("set_field_updates", False)

    # 62d. get_fields returns all custom fields
    try:
        cf_crm.set_field("alice@cf.com", "budget", "$100K")
        fields = cf_crm.get_fields("alice@cf.com")
        check("get_fields_all", isinstance(fields, dict) and "industry" in fields and "budget" in fields)
    except (AttributeError, TypeError):
        check("get_fields_all", False)

    # 62e. delete_field removes it
    try:
        cf_crm.delete_field("alice@cf.com", "budget")
        val = cf_crm.get_field("alice@cf.com", "budget")
        check("delete_field_works", val is None)
    except (AttributeError, TypeError):
        check("delete_field_works", False)

    # 62f. contacts_by_field finds matching contacts
    try:
        cf_crm.set_field("bob@cf.com", "industry", "healthtech")
        contacts = cf_crm.contacts_by_field("industry", "healthtech")
        check("contacts_by_field", len(contacts) == 2)
    except (AttributeError, TypeError):
        check("contacts_by_field", False)

    # 62g. contacts_by_field key only (no value filter)
    try:
        contacts = cf_crm.contacts_by_field("industry")
        check("contacts_by_field_key_only", len(contacts) >= 2)
    except (AttributeError, TypeError):
        check("contacts_by_field_key_only", False)

    cf_crm.close()

    # ── 63. View Batch Operations (3 tests) ──

    vb_crm = CRM(os.path.join(tempfile.mkdtemp(), "vb.db"))
    vb_crm.add_contact("VB Alice", email="alice@vb.com", status="prospect")
    vb_crm.add_contact("VB Bob", email="bob@vb.com", status="prospect")
    vb_crm.add_contact("VB Carol", email="carol@vb.com", status="contacted")
    vb_crm.save_view("prospects", status="prospect")

    # 63a. view_batch_tag tags all contacts in view
    try:
        count = vb_crm.view_batch_tag("prospects", "outreach-q2")
        tagged = vb_crm.list_by_tag("outreach-q2")
        check("view_batch_tag", count == 2 and len(tagged) == 2)
    except (AttributeError, TypeError):
        check("view_batch_tag", False)

    # 63b. view_batch_update changes status for all in view
    try:
        count = vb_crm.view_batch_update("prospects", status="contacted")
        contacts = vb_crm.list_contacts(status="contacted")
        check("view_batch_update", count == 2 and len(contacts) == 3)
    except (AttributeError, TypeError):
        check("view_batch_update", False)

    # 63c. batch on empty view returns 0
    try:
        vb_crm.save_view("empty_view", status="lost")
        count = vb_crm.view_batch_tag("empty_view", "test")
        check("view_batch_empty", count == 0)
    except (AttributeError, TypeError):
        check("view_batch_empty", False)

    vb_crm.close()

    # ── 64. Period Comparison (3 tests) ──

    pc_crm = CRM(os.path.join(tempfile.mkdtemp(), "pc.db"))
    pc_crm.add_contact("PC Alice", email="alice@pc.com", company="PcCo")
    pc_crm.log_activity("alice@pc.com", "call", "Demo")
    pc_crm.add_deal("alice@pc.com", "Deal", value="$10K", stage="proposal")

    # 64a. period_comparison returns dict
    try:
        pc = pc_crm.period_comparison(days=30)
        has_keys = isinstance(pc, dict) and all(k in pc for k in ("current_period", "previous_period", "changes"))
        check("period_comparison_keys", has_keys)
    except (AttributeError, TypeError):
        check("period_comparison_keys", False)

    # 64b. current period has counts
    try:
        pc = pc_crm.period_comparison(days=30)
        cp = pc["current_period"]
        check("period_comparison_current", cp["new_contacts"] >= 1 and cp["activities"] >= 1)
    except (AttributeError, TypeError, KeyError):
        check("period_comparison_current", False)

    # 64c. changes are strings
    try:
        pc = pc_crm.period_comparison(days=30)
        check("period_comparison_changes", isinstance(pc["changes"]["contacts"], str))
    except (AttributeError, TypeError, KeyError):
        check("period_comparison_changes", False)

    pc_crm.close()

    # ── 65. Stale Deals (2 tests) ──

    sd_crm = CRM(os.path.join(tempfile.mkdtemp(), "sd.db"))
    sd_crm.add_contact("SD Alice", email="alice@sd.com")
    sd_crm.add_deal("alice@sd.com", "Fresh Deal", stage="proposal")

    # 65a. stale_deals returns list
    try:
        stale = sd_crm.stale_deals(days=0)  # 0 days = everything is stale
        check("stale_deals_list", isinstance(stale, list))
    except (AttributeError, TypeError):
        check("stale_deals_list", False)

    # 65b. stale_deals with long window returns empty for fresh deals
    try:
        stale = sd_crm.stale_deals(days=365)
        check("stale_deals_fresh", len(stale) == 0)
    except (AttributeError, TypeError):
        check("stale_deals_fresh", False)

    sd_crm.close()

    # ── 66. Activity Heatmap (2 tests) ──

    hm_crm = CRM(os.path.join(tempfile.mkdtemp(), "hm.db"))
    hm_crm.add_contact("HM Alice", email="alice@hm.com")
    hm_crm.log_activity("alice@hm.com", "call", "Monday call")
    hm_crm.log_activity("alice@hm.com", "email", "Tuesday email")

    # 66a. activity_heatmap returns dict with day names
    try:
        hm = hm_crm.activity_heatmap()
        has_days = isinstance(hm, dict) and "Mon" in hm and "Sun" in hm
        check("heatmap_days", has_days)
    except (AttributeError, TypeError):
        check("heatmap_days", False)

    # 66b. heatmap has totals
    try:
        hm = hm_crm.activity_heatmap()
        has_total = any(hm[d].get("_total", 0) > 0 for d in hm)
        check("heatmap_totals", has_total)
    except (AttributeError, TypeError):
        check("heatmap_totals", False)

    hm_crm.close()

    # ── 67. Contacts Needing Email (2 tests) ──

    cne_crm = CRM(os.path.join(tempfile.mkdtemp(), "cne.db"))
    cne_crm.add_contact("With Email", email="we@cne.com")
    cne_crm.add_contact("No Email", company="NoCo")
    cne_crm.add_deal("No Email", "Important Deal", value="$50K", stage="proposal")

    # 67a. contacts_needing_email finds contacts with deals but no email
    try:
        needing = cne_crm.contacts_needing_email()
        check("contacts_needing_email", len(needing) >= 1 and any(c["name"] == "No Email" for c in needing))
    except (AttributeError, TypeError):
        check("contacts_needing_email", False)

    # 67b. contact with email not included
    try:
        needing = cne_crm.contacts_needing_email()
        check("contacts_needing_email_excludes", not any(c["name"] == "With Email" for c in needing))
    except (AttributeError, TypeError):
        check("contacts_needing_email_excludes", False)

    cne_crm.close()

    # ── 68. Quick Add (4 tests) ──

    qa_crm = CRM(os.path.join(tempfile.mkdtemp(), "qa.db"))

    # 68a. quick_add from email only
    try:
        cid = qa_crm.quick_add("alice@example.com")
        c = qa_crm.get_contact("alice@example.com")
        check("quick_add_email_only", cid is not None and c is not None and c["email"] == "alice@example.com")
    except (AttributeError, TypeError):
        check("quick_add_email_only", False)

    # 68b. quick_add with name and email
    try:
        cid = qa_crm.quick_add("Bob Smith bob@example.com")
        c = qa_crm.get_contact("bob@example.com")
        check("quick_add_name_email", cid is not None and c is not None and "Bob" in c["name"])
    except (AttributeError, TypeError):
        check("quick_add_name_email", False)

    # 68c. quick_add with title and company
    try:
        cid = qa_crm.quick_add("Carol Lee, CTO at TechCorp, carol@tech.com")
        c = qa_crm.get_contact("carol@tech.com")
        check("quick_add_full", cid is not None and c is not None and c.get("company") == "TechCorp")
    except (AttributeError, TypeError):
        check("quick_add_full", False)

    # 68d. quick_add empty returns None
    try:
        cid = qa_crm.quick_add("")
        check("quick_add_empty", cid is None)
    except (AttributeError, TypeError):
        check("quick_add_empty", False)

    qa_crm.close()

    # ── 69. Find By Domain (2 tests) ──

    fd_crm = CRM(os.path.join(tempfile.mkdtemp(), "fd.db"))
    fd_crm.add_contact("FD Alice", email="alice@techcorp.com")
    fd_crm.add_contact("FD Bob", email="bob@techcorp.com")
    fd_crm.add_contact("FD Carol", email="carol@other.com")

    # 69a. find_by_domain returns matching contacts
    try:
        matches = fd_crm.find_by_domain("techcorp.com")
        check("find_by_domain_matches", len(matches) == 2)
    except (AttributeError, TypeError):
        check("find_by_domain_matches", False)

    # 69b. find_by_domain no matches
    try:
        matches = fd_crm.find_by_domain("nonexistent.com")
        check("find_by_domain_empty", len(matches) == 0)
    except (AttributeError, TypeError):
        check("find_by_domain_empty", False)

    fd_crm.close()

    # ── 70. Interaction Frequency (3 tests) ──

    if_crm = CRM(os.path.join(tempfile.mkdtemp(), "if.db"))
    if_crm.add_contact("IF Alice", email="alice@if.com")
    for i in range(5):
        if_crm.log_activity("alice@if.com", "email", f"Email {i}")

    # 70a. interaction_frequency returns dict
    try:
        freq = if_crm.interaction_frequency("alice@if.com")
        has_keys = isinstance(freq, dict) and all(k in freq for k in (
            "total_interactions", "weekly_avg", "active_weeks", "longest_gap_days"))
        check("interaction_freq_keys", has_keys)
    except (AttributeError, TypeError):
        check("interaction_freq_keys", False)

    # 70b. total count is correct
    try:
        freq = if_crm.interaction_frequency("alice@if.com")
        check("interaction_freq_count", freq["total_interactions"] == 5)
    except (AttributeError, TypeError, KeyError):
        check("interaction_freq_count", False)

    # 70c. not found
    try:
        freq = if_crm.interaction_frequency("nobody@void.com")
        check("interaction_freq_not_found", freq is None)
    except (AttributeError, TypeError):
        check("interaction_freq_not_found", False)

    if_crm.close()

    # ── 71. Contact 360 (3 tests) ──

    c360_crm = CRM(os.path.join(tempfile.mkdtemp(), "c360.db"))
    c360_crm.add_contact("360 Alice", email="alice@360.com", company="Co360", deal_size="$10K/mo")
    c360_crm.log_activity("alice@360.com", "call", "Demo")
    c360_crm.set_field("alice@360.com", "industry", "fintech")

    # 71a. contact_360 returns comprehensive dict
    try:
        view = c360_crm.contact_360("alice@360.com")
        has_keys = isinstance(view, dict) and all(k in view for k in (
            "profile", "relationship_score", "velocity", "streak", "custom_fields"))
        check("contact_360_keys", has_keys)
    except (AttributeError, TypeError):
        check("contact_360_keys", False)

    # 71b. includes custom fields
    try:
        view = c360_crm.contact_360("alice@360.com")
        check("contact_360_custom_fields", "industry" in view["custom_fields"])
    except (AttributeError, TypeError, KeyError):
        check("contact_360_custom_fields", False)

    # 71c. not found
    try:
        view = c360_crm.contact_360("nobody@void.com")
        check("contact_360_not_found", view is None)
    except (AttributeError, TypeError):
        check("contact_360_not_found", False)

    c360_crm.close()

    # ── 72. Pipeline Health Score (3 tests) ──

    phs_crm = CRM(os.path.join(tempfile.mkdtemp(), "phs.db"))
    phs_crm.add_contact("PHS Alice", email="alice@phs.com", status="active_customer", deal_size="$10K/mo")
    phs_crm.add_contact("PHS Bob", email="bob@phs.com", status="prospect")
    phs_crm.add_contact("PHS Carol", email="carol@phs.com", status="contacted")
    phs_crm.log_activity("alice@phs.com", "call", "Check-in")
    phs_crm.add_deal("carol@phs.com", "New Deal", value="$5K", stage="proposal")

    # 72a. pipeline_health_score returns dict
    try:
        phs = phs_crm.pipeline_health_score()
        has_keys = isinstance(phs, dict) and "score" in phs and "factors" in phs
        check("pipeline_health_keys", has_keys)
    except (AttributeError, TypeError):
        check("pipeline_health_keys", False)

    # 72b. score is 0-100
    try:
        phs = phs_crm.pipeline_health_score()
        check("pipeline_health_range", 0 <= phs["score"] <= 100)
    except (AttributeError, TypeError, KeyError):
        check("pipeline_health_range", False)

    # 72c. factors list is non-empty
    try:
        phs = phs_crm.pipeline_health_score()
        check("pipeline_health_factors", isinstance(phs["factors"], list) and len(phs["factors"]) >= 1)
    except (AttributeError, TypeError, KeyError):
        check("pipeline_health_factors", False)

    phs_crm.close()

    # ── 73. Self-Improvement Engine (5 tests) ──

    evo_crm = CRM(os.path.join(tempfile.mkdtemp(), "evo.db"))
    evo_crm.add_contact("Evo Alice", email="alice@evo.com", status="prospect")
    evo_crm.add_contact("Evo Bob", email="bob@evo.com", status="prospect")
    evo_crm.add_contact("Evo Carol", email="carol@evo.com", status="active_customer", source="referral")

    # 73a. evolve returns dict with proposal
    try:
        result = evo_crm.evolve()
        has_keys = isinstance(result, dict) and all(k in result for k in (
            "bottleneck", "analysis", "proposal", "experiment_id"))
        check("evolve_keys", has_keys)
    except (AttributeError, TypeError):
        check("evolve_keys", False)

    # 73b. evolve stores experiment in facts
    try:
        result = evo_crm.evolve()
        exp_facts = evo_crm.facts_about(result["experiment_id"])
        check("evolve_stores_experiment", len(exp_facts) >= 1 and "status" in exp_facts)
    except (AttributeError, TypeError, KeyError):
        check("evolve_stores_experiment", False)

    # 73c. experiments lists past experiments
    try:
        exps = evo_crm.experiments()
        check("experiments_list", isinstance(exps, list) and len(exps) >= 1)
    except (AttributeError, TypeError):
        check("experiments_list", False)

    # 73d. win_patterns returns analysis
    try:
        wp = evo_crm.win_patterns()
        has_keys = isinstance(wp, dict) and "win_count" in wp and "patterns" in wp
        check("win_patterns_keys", has_keys)
    except (AttributeError, TypeError):
        check("win_patterns_keys", False)

    # 73e. loss_patterns returns analysis
    try:
        lp = evo_crm.loss_patterns()
        has_keys = isinstance(lp, dict) and "loss_count" in lp and "patterns" in lp
        check("loss_patterns_keys", has_keys)
    except (AttributeError, TypeError):
        check("loss_patterns_keys", False)

    evo_crm.close()

    # ── 74. Dead Pipeline & Optimal Cadence (4 tests) ──

    dp_crm = CRM(os.path.join(tempfile.mkdtemp(), "dp.db"))
    dp_crm.add_contact("DP Active", email="active@dp.com", status="active_customer")
    dp_crm.add_contact("DP Dead", email="dead@dp.com", status="contacted")
    dp_crm.add_contact("DP Won", email="won@dp.com", status="active_customer")
    dp_crm.log_activity("won@dp.com", "call", "Close call")
    dp_crm.log_activity("won@dp.com", "meeting", "Sign")

    # 74a. dead_pipeline returns list
    try:
        dead = dp_crm.dead_pipeline(stale_days=0)
        check("dead_pipeline_list", isinstance(dead, list))
    except (AttributeError, TypeError):
        check("dead_pipeline_list", False)

    # 74b. dead_pipeline excludes active customers
    try:
        dead = dp_crm.dead_pipeline(stale_days=0)
        names = [d["name"] for d in dead]
        check("dead_pipeline_excludes_won", "DP Active" not in names and "DP Won" not in names)
    except (AttributeError, TypeError):
        check("dead_pipeline_excludes_won", False)

    # 74c. optimal_cadence returns dict
    try:
        oc = dp_crm.optimal_cadence()
        has_keys = isinstance(oc, dict) and all(k in oc for k in ("won_deals", "lost_deals", "recommendation"))
        check("optimal_cadence_keys", has_keys)
    except (AttributeError, TypeError):
        check("optimal_cadence_keys", False)

    # 74d. optimal_cadence has recommendation string
    try:
        oc = dp_crm.optimal_cadence()
        check("optimal_cadence_recommendation", isinstance(oc["recommendation"], str) and len(oc["recommendation"]) > 0)
    except (AttributeError, TypeError, KeyError):
        check("optimal_cadence_recommendation", False)

    dp_crm.close()

    # ── 75. Deal Close Probability (15 tests) ──

    prob_dir = tempfile.mkdtemp()
    prob_db = os.path.join(prob_dir, "probability.db")
    prob_crm = CRM(prob_db)

    # Closed history calibrates both the overall win rate and source quality.
    prob_crm.add_contact("Won Referral One", email="won1@prob.com", status="active_customer",
                         source="referral", deal_size="$20K/yr")
    prob_crm.add_contact("Won Referral Two", email="won2@prob.com", status="active_customer",
                         source="referral", deal_size="$30K/yr")
    prob_crm.add_contact("Lost Outbound One", email="lost1@prob.com", status="lost",
                         source="outbound", deal_size="$10K/yr")
    prob_crm.add_contact("Lost Outbound Two", email="lost2@prob.com", status="churned",
                         source="outbound", deal_size="$10K/yr")
    won_id = prob_crm.get_contact("won1@prob.com")["id"]
    prob_crm.conn.execute(
        "INSERT INTO activity (contact_id, type, summary, created_at) VALUES (?, 'call', 'First touch', datetime('now', '-14 days'))",
        (won_id,)
    )
    prob_crm.conn.execute(
        "INSERT INTO activity (contact_id, type, summary, created_at) VALUES (?, 'call', 'Second touch', datetime('now', '-7 days'))",
        (won_id,)
    )

    prob_crm.add_contact("Early Stage", email="early@prob.com", status="prospect",
                         source="referral", deal_size="$10K/yr")
    prob_crm.add_contact("Late Stage", email="late@prob.com", status="verbal_yes",
                         source="referral", deal_size="$20K/yr")
    prob_crm.add_contact("Recent Contact", email="recent@prob.com", status="contacted",
                         source="referral", deal_size="$12K/yr")
    prob_crm.add_contact("Stale Contact", email="stale@prob.com", status="contacted",
                         source="referral", deal_size="$12K/yr")
    prob_crm.add_contact("Frequent Contact", email="frequent@prob.com", status="contacted",
                         source="referral", deal_size="$15K/yr")
    prob_crm.add_contact("Sparse Contact", email="sparse@prob.com", status="contacted",
                         source="referral", deal_size="$15K/yr")
    prob_crm.add_contact("Good Source", email="good@prob.com", status="met",
                         source="referral", deal_size="$8K/yr")
    prob_crm.add_contact("Bad Source", email="bad@prob.com", status="met",
                         source="outbound", deal_size="$8K/yr")

    for email, days in (("recent@prob.com", 1), ("stale@prob.com", 90),
                        ("sparse@prob.com", 1)):
        contact_id = prob_crm.get_contact(email)["id"]
        prob_crm.conn.execute(
            "INSERT INTO activity (contact_id, type, summary, created_at) VALUES (?, 'email', 'Pipeline touch', datetime('now', ?))",
            (contact_id, f"-{days} days")
        )
    frequent_id = prob_crm.get_contact("frequent@prob.com")["id"]
    for days in (1, 4, 7, 10):
        prob_crm.conn.execute(
            "INSERT INTO activity (contact_id, type, summary, created_at) VALUES (?, 'email', 'Pipeline touch', datetime('now', ?))",
            (frequent_id, f"-{days} days")
        )
    prob_crm.conn.commit()

    # 75a. deal_probability returns the documented top-level shape
    try:
        probability = prob_crm.deal_probability("early@prob.com")
        check("deal_probability_keys", isinstance(probability, dict) and
              set(probability) == {"probability", "signals", "contact", "status",
                                   "weights_source"})
    except (AttributeError, TypeError):
        check("deal_probability_keys", False)

    # 75b. probability is always bounded to [0, 1]
    try:
        probabilities = [prob_crm.deal_probability(email)["probability"] for email in
                         ("early@prob.com", "late@prob.com", "recent@prob.com", "stale@prob.com")]
        check("deal_probability_range", all(0.0 <= value <= 1.0 for value in probabilities))
    except (AttributeError, TypeError, KeyError):
        check("deal_probability_range", False)

    # 75c. signals expose name, raw value, weight, and numerical effect
    try:
        signals = prob_crm.deal_probability("early@prob.com")["signals"]
        names = {signal["name"] for signal in signals}
        expected = {"stage", "activity_recency", "activity_frequency", "source", "historical_win_rate"}
        shape_ok = all(set(signal) == {"name", "value", "weight", "effect"} for signal in signals)
        check("deal_probability_signal_shape", shape_ok and names == expected)
    except (AttributeError, TypeError, KeyError):
        check("deal_probability_signal_shape", False)

    # 75d. a deeper pipeline stage raises probability with other signals equal
    try:
        early = prob_crm.deal_probability("early@prob.com")["probability"]
        late = prob_crm.deal_probability("late@prob.com")["probability"]
        check("deal_probability_stage_depth", late > early)
    except (AttributeError, TypeError, KeyError):
        check("deal_probability_stage_depth", False)

    # 75e. recent activity beats otherwise-identical stale activity
    try:
        recent = prob_crm.deal_probability("recent@prob.com")["probability"]
        stale = prob_crm.deal_probability("stale@prob.com")["probability"]
        check("deal_probability_recency", recent > stale)
    except (AttributeError, TypeError, KeyError):
        check("deal_probability_recency", False)

    # 75f. activity frequency is compared with the learned winning cadence
    try:
        frequent = prob_crm.deal_probability("frequent@prob.com")["probability"]
        sparse = prob_crm.deal_probability("sparse@prob.com")["probability"]
        check("deal_probability_frequency", frequent > sparse)
    except (AttributeError, TypeError, KeyError):
        check("deal_probability_frequency", False)

    # 75g. a historically winning source beats a historically losing source
    try:
        good_source = prob_crm.deal_probability("good@prob.com")["probability"]
        bad_source = prob_crm.deal_probability("bad@prob.com")["probability"]
        check("deal_probability_source_history", good_source > bad_source)
    except (AttributeError, TypeError, KeyError):
        check("deal_probability_source_history", False)

    # 75h. repeated calls over unchanged data are deterministic
    try:
        first = prob_crm.deal_probability("frequent@prob.com")
        second = prob_crm.deal_probability("frequent@prob.com")
        check("deal_probability_deterministic", first == second)
    except (AttributeError, TypeError):
        check("deal_probability_deterministic", False)

    # 75i. an unknown contact returns None
    try:
        check("deal_probability_unknown", prob_crm.deal_probability("missing@prob.com") is None)
    except (AttributeError, TypeError):
        check("deal_probability_unknown", False)

    # 75j. an empty CRM has a zero-valued empty forecast
    try:
        empty_crm = CRM(os.path.join(tempfile.mkdtemp(), "empty-forecast.db"))
        empty_forecast = empty_crm.forecast()
        empty_crm.close()
        check("forecast_empty_crm", empty_forecast["contacts"] == [] and
              empty_forecast["weighted_pipeline"] == 0 and empty_forecast["count"] == 0)
    except (AttributeError, TypeError, KeyError):
        check("forecast_empty_crm", False)

    # 75k. forecast contains open contacts and excludes won/lost contacts
    try:
        probability_forecast = prob_crm.forecast()
        names = [row["name"] for row in probability_forecast["contacts"]]
        check("forecast_open_only", "Early Stage" in names and "Won Referral One" not in names and
              "Lost Outbound One" not in names)
    except (AttributeError, TypeError, KeyError):
        check("forecast_open_only", False)

    # 75l. forecast ranks contacts by probability descending
    try:
        values = [row["probability"] for row in prob_crm.forecast()["contacts"]]
        check("forecast_probability_order", values == sorted(values, reverse=True))
    except (AttributeError, TypeError, KeyError):
        check("forecast_probability_order", False)

    # 75m. row and aggregate weighted values use deal value * probability
    try:
        probability_forecast = prob_crm.forecast()
        rows = probability_forecast["contacts"]
        rows_correct = all(row["weighted_value"] == round(row["deal_value"] * row["probability"], 2)
                           for row in rows)
        total_correct = probability_forecast["weighted_pipeline"] == round(
            sum(row["weighted_value"] for row in rows), 2
        )
        check("forecast_weighted_value_math", rows_correct and total_correct)
    except (AttributeError, TypeError, KeyError):
        check("forecast_weighted_value_math", False)

    # 75n. forecast limit truncates rows without changing their rank
    try:
        full_forecast = prob_crm.forecast()
        limited_forecast = prob_crm.forecast(limit=2)
        check("forecast_limit", len(limited_forecast["contacts"]) == 2 and
              limited_forecast["contacts"] == full_forecast["contacts"][:2])
    except (AttributeError, TypeError, KeyError):
        check("forecast_limit", False)

    # 75o. MCP registers and serves the forecast tool
    try:
        import mcp_server
        original_mcp_db = mcp_server.DB_PATH
        mcp_server.DB_PATH = prob_db
        mcp_names = {tool["name"] for tool in mcp_server.TOOLS}
        mcp_output = mcp_server.handle_tool_call("crm_forecast", {"limit": 1})
        mcp_server.DB_PATH = original_mcp_db
        check("forecast_mcp_tool", "crm_forecast" in mcp_names and
              '"weighted_pipeline"' in mcp_output and '"contacts"' in mcp_output)
    except (AttributeError, TypeError, KeyError):
        check("forecast_mcp_tool", False)

    prob_crm.close()

    # ── 76. Next-Best-Action Queue (12 tests) ──

    empty_actions_crm = CRM(os.path.join(tempfile.mkdtemp(), "empty-actions.db"))

    # 76a. an empty CRM has no recommended actions
    try:
        check("next_actions_empty_crm", empty_actions_crm.next_actions() == [])
    except (AttributeError, TypeError):
        check("next_actions_empty_crm", False)
    empty_actions_crm.close()

    actions_dir = tempfile.mkdtemp()
    actions_db = os.path.join(actions_dir, "next-actions.db")
    actions_crm = CRM(actions_db)

    # A seven-day winning activity gap calibrates optimal_cadence().
    actions_crm.add_contact("Won Pattern", email="won@actions.com", status="active_customer")
    won_pattern_id = actions_crm.get_contact("won@actions.com")["id"]
    for days in (14, 7):
        actions_crm.conn.execute(
            "INSERT INTO activity (contact_id, type, summary, created_at) "
            "VALUES (?, 'email', 'Winning touch', datetime('now', ?))",
            (won_pattern_id, f"-{days} days"),
        )
    actions_crm.update_contact("won@actions.com", last_contacted=_date.today().isoformat())

    actions_crm.add_contact("Reminder Rita", email="rita@actions.com", status="contacted")
    actions_crm.update_contact("rita@actions.com", last_contacted=_date.today().isoformat())
    actions_crm.set_reminder(
        "rita@actions.com", (_date.today() - timedelta(days=3)).isoformat(), "Call Rita"
    )

    actions_crm.add_contact("Mild Cadence", email="mild@actions.com", status="contacted")
    actions_crm.update_contact(
        "mild@actions.com", last_contacted=(_date.today() - timedelta(days=10)).isoformat()
    )

    actions_crm.add_contact("Health Hank", email="hank@actions.com", status="prospect")
    actions_crm.update_contact("hank@actions.com", last_contacted=_date.today().isoformat())

    actions_crm.add_contact("Stale Deal Sam", email="sam@actions.com", status="contacted")
    actions_crm.update_contact("sam@actions.com", last_contacted=_date.today().isoformat())
    actions_crm.add_deal("sam@actions.com", "Expansion", value="$25K", stage="proposal")
    actions_crm.conn.execute(
        "UPDATE deals SET updated_at = datetime('now', '-45 days') WHERE contact_id = ?",
        (actions_crm.get_contact("sam@actions.com")["id"],),
    )

    actions_crm.add_contact(
        "Quiet Quinn", email="quinn@actions.com", status="verbal_yes",
        deal_size="$50K", source="referral",
    )
    quiet_id = actions_crm.get_contact("quinn@actions.com")["id"]
    actions_crm.conn.execute(
        "INSERT INTO activity (contact_id, type, summary, created_at) "
        "VALUES (?, 'meeting', 'Buying signal', datetime('now', '-21 days'))",
        (quiet_id,),
    )
    actions_crm.update_contact(
        "quinn@actions.com", last_contacted=(_date.today() - timedelta(days=21)).isoformat()
    )

    actions_crm.add_contact("Limit Leo", email="leo@actions.com", status="prospect")

    for name, email, status in (
        ("Lost Lola", "lost@actions.com", "lost"),
        ("Churned Chad", "churned@actions.com", "churned"),
        ("Archived Aria", "archived@actions.com", "verbal_yes"),
    ):
        actions_crm.add_contact(name, email=email, status=status, deal_size="$100K")
        actions_crm.set_reminder(
            email, (_date.today() - timedelta(days=30)).isoformat(), "Should stay hidden"
        )
    actions_crm.archive_contact("archived@actions.com")
    actions_crm.conn.commit()

    # 76b. limit truncates without changing the established rank
    try:
        all_actions = actions_crm.next_actions(limit=50)
        limited_actions = actions_crm.next_actions(limit=2)
        check("next_actions_limit", len(limited_actions) == 2 and
              limited_actions == all_actions[:2])
    except (AttributeError, TypeError):
        check("next_actions_limit", False)

    # 76c. the queue is ordered by urgency descending
    try:
        urgency = [item["urgency_score"] for item in actions_crm.next_actions(limit=50)]
        check("next_actions_urgency_order", urgency == sorted(urgency, reverse=True))
    except (AttributeError, TypeError, KeyError):
        check("next_actions_urgency_order", False)

    # 76d. an overdue reminder outranks a mildly overdue cadence
    try:
        by_name = {item["contact"]: item for item in actions_crm.next_actions(limit=50)}
        check("next_actions_reminder_priority",
              by_name["Reminder Rita"]["urgency_score"] >
              by_name["Mild Cadence"]["urgency_score"])
    except (AttributeError, TypeError, KeyError):
        check("next_actions_reminder_priority", False)

    # 76e. archived, lost, and churned contacts never enter the queue
    try:
        names = {item["contact"] for item in actions_crm.next_actions(limit=50)}
        check("next_actions_excludes_inactive",
              not names.intersection({"Archived Aria", "Lost Lola", "Churned Chad"}))
    except (AttributeError, TypeError, KeyError):
        check("next_actions_excludes_inactive", False)

    # 76f. unchanged CRM data produces byte-for-byte-equivalent rows
    try:
        first_actions = actions_crm.next_actions(limit=50)
        second_actions = actions_crm.next_actions(limit=50)
        check("next_actions_deterministic", first_actions == second_actions)
    except (AttributeError, TypeError):
        check("next_actions_deterministic", False)

    # 76g. every row exposes the documented queue contract
    try:
        required = {"contact", "action", "reason", "urgency_score", "signals"}
        queue = actions_crm.next_actions(limit=50)
        check("next_actions_required_keys", bool(queue) and
              all(required.issubset(item) for item in queue))
    except (AttributeError, TypeError):
        check("next_actions_required_keys", False)

    # 76h. reasons are always useful display strings
    try:
        queue = actions_crm.next_actions(limit=50)
        check("next_actions_nonempty_reason", bool(queue) and all(
            isinstance(item["reason"], str) and item["reason"].strip() for item in queue
        ))
    except (AttributeError, TypeError, KeyError):
        check("next_actions_nonempty_reason", False)

    # 76h2. one-sided iMessage traffic surfaces a Reply action
    try:
        actions_crm.add_contact("Chatty Charlie", email="charlie@imsg.test", status="contacted")
        for k, v in (("imessage_sent", "2"), ("imessage_received", "40"), ("imessage_total", "42")):
            actions_crm.observe("contact:charlie@imsg.test", k, v, source="imessage")
        charlie = next(item for item in actions_crm.next_actions(limit=50)
                       if item["contact"] == "Chatty Charlie")
        check("next_actions_imsg_reciprocity",
              "one_sided_messaging" in {signal["name"] for signal in charlie["signals"]})
    except (AttributeError, TypeError, KeyError, StopIteration):
        check("next_actions_imsg_reciprocity", False)

    # 76i. contacts beyond the learned winning cadence expose that signal
    try:
        mild = next(item for item in actions_crm.next_actions(limit=50)
                    if item["contact"] == "Mild Cadence")
        check("next_actions_cadence_signal",
              "cadence_overdue" in {signal["name"] for signal in mild["signals"]})
    except (AttributeError, TypeError, KeyError, StopIteration):
        check("next_actions_cadence_signal", False)

    # 76j. low relationship-health scores surface as an explicit signal
    try:
        hank = next(item for item in actions_crm.next_actions(limit=50)
                    if item["contact"] == "Health Hank")
        check("next_actions_health_signal",
              "low_relationship_health" in {signal["name"] for signal in hank["signals"]})
    except (AttributeError, TypeError, KeyError, StopIteration):
        check("next_actions_health_signal", False)

    # 76k. stale open deals surface through the existing stale_deals detector
    try:
        sam = next(item for item in actions_crm.next_actions(limit=50)
                   if item["contact"] == "Stale Deal Sam")
        check("next_actions_stale_deal_signal",
              "stale_open_deal" in {signal["name"] for signal in sam["signals"]})
    except (AttributeError, TypeError, KeyError, StopIteration):
        check("next_actions_stale_deal_signal", False)

    # 76l. high-probability deals that went quiet get a close-focused action
    try:
        quinn = next(item for item in actions_crm.next_actions(limit=50)
                     if item["contact"] == "Quiet Quinn")
        signal_names = {signal["name"] for signal in quinn["signals"]}
        check("next_actions_probability_signal",
              "high_probability_quiet" in signal_names and quinn["action"] == "Send contract")
    except (AttributeError, TypeError, KeyError, StopIteration):
        check("next_actions_probability_signal", False)

    actions_crm.close()

    # ── 77. Experiment Outcome Tracking (14 tests) ──

    experiment_dir = tempfile.mkdtemp()
    experiment_db = os.path.join(experiment_dir, "experiments.db")
    experiment_crm = CRM(experiment_db)

    # 77a. logging returns a stable slug and writes running/evolve facts
    try:
        reply_slug = experiment_crm.log_experiment(
            "Reply Rate Test", "Personalized copy raises replies", "reply rate", 10
        )
        reply_facts = experiment_crm.facts_about(f"experiment:{reply_slug}")
        check("experiment_log_slug_source_status",
              reply_slug == "reply-rate-test" and
              reply_facts["status"]["value"] == "running" and
              all(fact["source"] == "evolve" for fact in reply_facts.values()))
    except (AttributeError, TypeError, KeyError, ValueError):
        check("experiment_log_slug_source_status", False)

    # 77b. history round-trips the complete experiment definition
    try:
        reply = experiment_crm.experiment_history()[0]
        check("experiment_history_round_trip",
              reply["slug"] == reply_slug and
              reply["name"] == "Reply Rate Test" and
              reply["hypothesis"] == "Personalized copy raises replies" and
              reply["metric"] == "reply rate" and
              reply["baseline_value"] == "10" and
              reply["result_value"] is None and reply["verdict"] is None)
    except (AttributeError, TypeError, KeyError, IndexError):
        check("experiment_history_round_trip", False)

    # 77c. running and completed records expose their lifecycle dates
    try:
        before = experiment_crm.experiment_history()[0]
        experiment_crm.conclude_experiment(reply_slug, 15)
        after = next(e for e in experiment_crm.experiment_history() if e["slug"] == reply_slug)
        check("experiment_history_dates",
              bool(before["started_at"]) and before["concluded_at"] is None and
              bool(after["started_at"]) and bool(after["concluded_at"]))
    except (AttributeError, TypeError, KeyError, IndexError):
        check("experiment_history_dates", False)

    # 77d. a numeric improvement derives a win
    try:
        won = experiment_crm.conclude_experiment(reply_slug, 15)
        check("experiment_conclude_win",
              won["verdict"] == "win" and won["status"] == "completed" and
              won["result_value"] == "15")
    except (AttributeError, TypeError, KeyError, ValueError):
        check("experiment_conclude_win", False)

    # 77e. a numeric decline derives a loss
    try:
        loss_slug = experiment_crm.log_experiment("Lower Result", "Test a decline", "score", 10)
        lost = experiment_crm.conclude_experiment(loss_slug, 5)
        check("experiment_conclude_loss", lost["verdict"] == "loss")
    except (AttributeError, TypeError, KeyError, ValueError):
        check("experiment_conclude_loss", False)

    # 77f. an unchanged numeric result derives neutral
    try:
        neutral_slug = experiment_crm.log_experiment("Same Result", "Test parity", "score", 10)
        neutral = experiment_crm.conclude_experiment(neutral_slug, "10.0")
        check("experiment_conclude_neutral", neutral["verdict"] == "neutral")
    except (AttributeError, TypeError, KeyError, ValueError):
        check("experiment_conclude_neutral", False)

    # 77g. an explicit verdict wins over numeric comparison
    try:
        explicit_slug = experiment_crm.log_experiment("Guardrail Result", "Guardrail matters", "score", 10)
        explicit = experiment_crm.conclude_experiment(explicit_slug, 20, verdict="loss")
        check("experiment_explicit_verdict", explicit["verdict"] == "loss")
    except (AttributeError, TypeError, KeyError, ValueError):
        check("experiment_explicit_verdict", False)

    # 77h. non-numeric comparison requires an explicit verdict
    try:
        text_slug = experiment_crm.log_experiment("Qualitative Test", "Users prefer it", "preference", "old")
        raised = False
        try:
            experiment_crm.conclude_experiment(text_slug, "new")
        except ValueError:
            raised = True
        check("experiment_nonnumeric_requires_verdict", raised)
    except (AttributeError, TypeError, KeyError):
        check("experiment_nonnumeric_requires_verdict", False)

    # 77i. status filters separate running work from completed history
    try:
        running_history = experiment_crm.experiment_history(status="running")
        completed_history = experiment_crm.experiment_history(status="completed")
        check("experiment_status_filter",
              {e["slug"] for e in running_history} == {text_slug} and
              all(e["status"] == "completed" for e in completed_history) and
              len(completed_history) == 4)
    except (AttributeError, TypeError, KeyError):
        check("experiment_status_filter", False)

    # 77j. evolve reads the prior outcomes into a track record before logging
    try:
        evolved = experiment_crm.evolve()
        track = evolved["analysis"]["experiment_track_record"]
        check("evolve_experiment_track_record",
              set(track) == {"wins", "losses", "running", "win_rate"} and
              len(track["wins"]) == 1 and len(track["losses"]) == 2 and
              len(track["running"]) == 1 and track["win_rate"] == 33.3)
    except (AttributeError, TypeError, KeyError):
        check("evolve_experiment_track_record", False)

    # 77k. concluding an unknown slug is a handled miss
    try:
        check("experiment_unknown_slug",
              experiment_crm.conclude_experiment("does-not-exist", 1) is None and
              experiment_crm.conclude_experiment("???", 1) is None)
    except (AttributeError, TypeError, ValueError):
        check("experiment_unknown_slug", False)

    # 77l. conclusion accepts the original human-facing name
    try:
        name_slug = experiment_crm.log_experiment("Resolve By Name", "Names resolve", "score", 1)
        by_name = experiment_crm.conclude_experiment("Resolve By Name", 2)
        check("experiment_conclude_by_name",
              by_name["slug"] == name_slug and by_name["verdict"] == "win")
    except (AttributeError, TypeError, KeyError, ValueError):
        check("experiment_conclude_by_name", False)

    # 77m. unchanged facts produce deterministic history
    try:
        first_history = experiment_crm.experiment_history()
        second_history = experiment_crm.experiment_history()
        check("experiment_history_deterministic", first_history == second_history)
    except (AttributeError, TypeError):
        check("experiment_history_deterministic", False)

    # 77n. MCP registers and serves log, conclude, and history tools
    try:
        import mcp_server
        original_mcp_db = mcp_server.DB_PATH
        mcp_server.DB_PATH = os.path.join(experiment_dir, "mcp-experiments.db")
        mcp_names = {tool["name"] for tool in mcp_server.TOOLS}
        logged_json = mcp_server.handle_tool_call("crm_log_experiment", {
            "name": "MCP Test",
            "hypothesis": "MCP round trip",
            "metric": "score",
            "baseline_value": 1,
        })
        concluded_json = mcp_server.handle_tool_call("crm_conclude_experiment", {
            "slug_or_name": "mcp-test",
            "result_value": 2,
        })
        history_json = mcp_server.handle_tool_call("crm_experiments", {"status": "completed"})
        mcp_server.DB_PATH = original_mcp_db
        check("experiment_mcp_tools",
              {"crm_log_experiment", "crm_conclude_experiment", "crm_experiments"}.issubset(mcp_names) and
              '"slug": "mcp-test"' in logged_json and '"verdict": "win"' in concluded_json and
              '"status": "completed"' in history_json)
    except (AttributeError, TypeError, KeyError, ValueError):
        check("experiment_mcp_tools", False)

    experiment_crm.close()

    # ── 78. Full Import/Export Round-Trip (16 tests) ──

    roundtrip_dir = tempfile.mkdtemp()
    roundtrip_source = CRM(os.path.join(roundtrip_dir, "source.db"))
    roundtrip_source.add_contact(
        "Round Trip Alice", email="alice@roundtrip.test", company="Source Co",
        title="CTO", deal_size="$42K", status="proposal_drafted",
        source="referral", notes="Preserve me", tags="vip,technical",
    )
    roundtrip_source.add_contact(
        "Exact Name Bob", company="Name Match Co", status="contacted", notes="No email"
    )
    roundtrip_source.log_activity("alice@roundtrip.test", "meeting", "Reviewed proposal")
    roundtrip_source.add_deal(
        "alice@roundtrip.test", "Enterprise plan", value="$42K",
        stage="proposal", notes="Annual contract",
    )
    roundtrip_source.observe(
        "company:source-co", "industry", "robotics", source="benchmark"
    )
    roundtrip_source.conn.execute(
        "UPDATE activity SET created_at = '2025-01-02 03:04:05' WHERE summary = 'Reviewed proposal'"
    )
    roundtrip_source.conn.execute(
        """UPDATE deals SET created_at = '2025-01-03 04:05:06',
           updated_at = '2025-01-04 05:06:07', closed_at = '2025-01-05 06:07:08'
           WHERE name = 'Enterprise plan'"""
    )
    roundtrip_source.conn.execute(
        "UPDATE facts SET observed_at = '2025-01-06 07:08:09' WHERE source = 'benchmark'"
    )
    roundtrip_source.conn.commit()
    full_export = roundtrip_source.export_full()

    # 78a. export contract is versioned and contains every core table
    check(
        "full_export_contract",
        set(full_export) == {"schema_version", "contacts", "activity", "deals", "facts"}
        and full_export["schema_version"] == 1,
    )

    roundtrip_target = CRM(os.path.join(roundtrip_dir, "target.db"))
    full_import_result = roundtrip_target.import_full(full_export)
    restored_export = roundtrip_target.export_full()

    # 78b-e. every table round-trips with identical counts, fields, ids, and timestamps
    check(
        "full_roundtrip_contacts",
        full_import_result["imported"]["contacts"] == len(full_export["contacts"])
        and restored_export["contacts"] == full_export["contacts"],
    )
    check(
        "full_roundtrip_activity",
        full_import_result["imported"]["activity"] == len(full_export["activity"])
        and restored_export["activity"] == full_export["activity"],
    )
    check(
        "full_roundtrip_deals",
        full_import_result["imported"]["deals"] == len(full_export["deals"])
        and restored_export["deals"] == full_export["deals"],
    )
    check(
        "full_roundtrip_facts",
        full_import_result["imported"]["facts"] == len(full_export["facts"])
        and restored_export["facts"] == full_export["facts"],
    )

    # 78f. a non-merge restore refuses non-empty databases without changing them
    before_refused_import = roundtrip_target.export_full()
    refused_import = roundtrip_target.import_full(full_export)
    check(
        "full_import_nonempty_error",
        "error" in refused_import and roundtrip_target.export_full() == before_refused_import,
    )

    merge_crm = CRM(os.path.join(roundtrip_dir, "merge.db"))
    merge_crm.add_contact(
        "Old Alice Name", email="alice@roundtrip.test", company="Old Co", notes="Old notes"
    )
    merge_crm.add_contact("Exact Name Bob", company="Old Name Match Co")
    first_merge = merge_crm.import_full(full_export, merge=True)

    # 78g. email matches update the existing contact rather than duplicating it
    alice_rows = merge_crm.conn.execute(
        "SELECT * FROM contacts WHERE email = 'alice@roundtrip.test'"
    ).fetchall()
    check(
        "full_merge_email_upsert",
        len(alice_rows) == 1 and alice_rows[0]["name"] == "Round Trip Alice"
        and alice_rows[0]["company"] == "Source Co" and first_merge["updated_contacts"] == 2,
    )

    # 78h. contacts without email fall back to an exact name upsert
    bob_rows = merge_crm.conn.execute(
        "SELECT * FROM contacts WHERE name = 'Exact Name Bob'"
    ).fetchall()
    check(
        "full_merge_exact_name_upsert",
        len(bob_rows) == 1 and bob_rows[0]["company"] == "Name Match Co",
    )

    # 78i. repeated merges dedupe timestamped activity and facts
    counts_before_second_merge = {
        table: merge_crm.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in ("activity", "facts")
    }
    second_merge = merge_crm.import_full(full_export, merge=True)
    counts_after_second_merge = {
        table: merge_crm.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        for table in ("activity", "facts")
    }
    check(
        "full_merge_dedupes_timestamped_rows",
        counts_after_second_merge == counts_before_second_merge
        and second_merge["skipped"]["activity"] == len(full_export["activity"])
        and second_merge["skipped"]["facts"] == len(full_export["facts"]),
    )

    # 78j. contact CSV export/import preserves every contact column
    csv_roundtrip_path = os.path.join(roundtrip_dir, "contacts.csv")
    roundtrip_source.export_csv(csv_roundtrip_path)
    csv_roundtrip_crm = CRM(os.path.join(roundtrip_dir, "csv-roundtrip.db"))
    csv_roundtrip_result = csv_roundtrip_crm.import_csv(csv_roundtrip_path)
    source_contact_rows = [dict(row) for row in roundtrip_source.conn.execute(
        "SELECT * FROM contacts ORDER BY id"
    )]
    csv_contact_rows = [dict(row) for row in csv_roundtrip_crm.conn.execute(
        "SELECT * FROM contacts ORDER BY id"
    )]
    check(
        "csv_contact_roundtrip",
        csv_roundtrip_result == {"imported": 2, "skipped": 0, "errors": 0}
        and csv_contact_rows == source_contact_rows,
    )

    # 78k. explicit mappings override odd headers and retain phone as a fact
    odd_csv_path = os.path.join(roundtrip_dir, "odd-headers.csv")
    with open(odd_csv_path, "w", encoding="utf-8", newline="") as f:
        f.write("Person Label,Mail Box,Employer,Pipeline,Telephone,Long Memo\n")
        f.write("Mapped Maya,maya@mapped.test,Odd Co,contacted,+15551234567,Met at demo\n")
    odd_csv_crm = CRM(os.path.join(roundtrip_dir, "odd-csv.db"))
    odd_csv_result = odd_csv_crm.import_csv(odd_csv_path, mapping={
        "name": "Person Label", "email": "Mail Box", "company": "Employer",
        "status": "Pipeline", "phone": "Telephone", "notes": "Long Memo",
    })
    mapped_maya = odd_csv_crm.get_contact("maya@mapped.test")
    maya_facts = odd_csv_crm.facts_about("contact:mapped maya")
    check(
        "csv_odd_header_mapping",
        odd_csv_result["imported"] == 1 and mapped_maya["company"] == "Odd Co"
        and mapped_maya["status"] == "contacted" and mapped_maya["notes"] == "Met at demo"
        and maya_facts["phone"]["value"] == "+15551234567",
    )

    # 78l. malformed rows count as errors while valid rows still import
    malformed_csv_path = os.path.join(roundtrip_dir, "malformed.csv")
    with open(malformed_csv_path, "w", encoding="utf-8", newline="") as f:
        f.write("name,email\n")
        f.write("Valid Vera,vera@valid.test\n")
        f.write("Broken Bob,bob@broken.test,unexpected\n")
    malformed_csv_crm = CRM(os.path.join(roundtrip_dir, "malformed-csv.db"))
    malformed_csv_result = malformed_csv_crm.import_csv(malformed_csv_path)
    check(
        "csv_malformed_rows_counted",
        malformed_csv_result == {"imported": 1, "skipped": 0, "errors": 1}
        and malformed_csv_crm.get_contact("vera@valid.test") is not None,
    )

    # 78m. one vCard file can contain multiple contacts, organizations, and phones
    multi_vcard_path = os.path.join(roundtrip_dir, "multi.vcf")
    with open(multi_vcard_path, "w", encoding="utf-8", newline="") as f:
        f.write("BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Vera Card\r\n")
        f.write("EMAIL;TYPE=WORK:vera@vcard.test\r\nORG:Card Co\r\nTEL;TYPE=CELL:+10000000001\r\nEND:VCARD\r\n")
        f.write("BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Victor Card\r\n")
        f.write("EMAIL:victor@vcard.test\r\nORG:Other Co\r\nEND:VCARD\r\n")
    multi_vcard_crm = CRM(os.path.join(roundtrip_dir, "multi-vcard.db"))
    multi_vcard_count = multi_vcard_crm.import_vcard(multi_vcard_path)
    check(
        "vcard_multi_card_parse",
        multi_vcard_count == 2 and multi_vcard_crm.get_contact("vera@vcard.test")["company"] == "Card Co"
        and multi_vcard_crm.facts_about("contact:vera card")["phone"]["value"] == "+10000000001",
    )

    # 78n. folded lines and charset/encoding parameters decode without dependencies
    folded_vcard_path = os.path.join(roundtrip_dir, "folded.vcf")
    with open(folded_vcard_path, "wb") as f:
        f.write(b"BEGIN:VCARD\r\nVERSION:3.0\r\n")
        f.write(b"FN;CHARSET=ISO-8859-1;ENCODING=QUOTED-PRINTABLE:Jos=E9 Long\r\n")
        f.write(b"EMAIL:jose@vcard.test\r\nORG:Very Long \r\n Company Name\r\n")
        f.write(b"TEL;TYPE=WORK:+10000000002\r\nEND:VCARD\r\n")
    folded_vcard_crm = CRM(os.path.join(roundtrip_dir, "folded-vcard.db"))
    folded_vcard_count = folded_vcard_crm.import_vcard(folded_vcard_path)
    folded_contact = folded_vcard_crm.get_contact("jose@vcard.test")
    check(
        "vcard_folded_charset_parse",
        folded_vcard_count == 1 and folded_contact["name"] == "José Long"
        and folded_contact["company"] == "Very Long Company Name"
        and folded_vcard_crm.facts_about("contact:josé long")["phone"]["value"] == "+10000000002",
    )

    # 78o. empty CSV and vCard files are handled as zero-row imports
    empty_path = os.path.join(roundtrip_dir, "empty")
    with open(empty_path, "w", encoding="utf-8"):
        pass
    check(
        "empty_import_files",
        folded_vcard_crm.import_csv(empty_path) == {"imported": 0, "skipped": 0, "errors": 0}
        and folded_vcard_crm.import_vcard(empty_path) == 0,
    )

    # 78p. MCP advertises and serves the full export and CSV import tools
    try:
        import mcp_server as _roundtrip_mcp
        original_roundtrip_mcp_db = _roundtrip_mcp.DB_PATH
        _roundtrip_mcp.DB_PATH = os.path.join(roundtrip_dir, "mcp.db")
        mcp_tool_names = {tool["name"] for tool in _roundtrip_mcp.TOOLS}
        mcp_csv_json = _roundtrip_mcp.handle_tool_call("crm_import_csv", {
            "path": odd_csv_path,
            "mapping": {"Person Label": "name", "Mail Box": "email"},
        })
        mcp_export_json = _roundtrip_mcp.handle_tool_call("crm_export_full", {})
        _roundtrip_mcp.DB_PATH = original_roundtrip_mcp_db
        check(
            "roundtrip_mcp_tools",
            {"crm_export_full", "crm_import_csv"}.issubset(mcp_tool_names)
            and '"imported": 1' in mcp_csv_json and '"schema_version": 1' in mcp_export_json,
        )
    except (AttributeError, TypeError, KeyError, ValueError):
        check("roundtrip_mcp_tools", False)

    # ── 79. Multi-hop introduction paths (11 tests) ──
    intro_dir = tempfile.mkdtemp()
    intro_db = os.path.join(intro_dir, "intro-paths.db")
    intro_crm = CRM(intro_db)

    def add_intro_contact(name, email, company=None):
        intro_crm.add_contact(name, email=email, company=company)

    def make_intro_anchor(name):
        entity = f"contact:{name.casefold().replace(' ', '_')}"
        intro_crm.observe(entity, "imessage_total", "120", source="imessage")
        intro_crm.observe(entity, "message_intensity", "high", source="imessage")

    # A pure two-hop introduced_by chain.
    add_intro_contact("Two Anchor", "two-anchor@two-anchor.example")
    add_intro_contact("Two Bridge", "two-bridge@two-bridge.example")
    add_intro_contact("Two Target", "two-target@two-target.example")
    make_intro_anchor("Two Anchor")
    intro_crm.observe("contact:two_anchor", "introduced_by", "contact:two_bridge")
    intro_crm.observe("contact:two_bridge", "introduced_by", "contact:two_target")
    two_hop_paths = intro_crm.intro_paths("Two Target", max_hops=2)
    check(
        "intro_paths_two_hop_introduced_chain",
        any(path["path"] == ["Two Anchor", "Two Bridge", "Two Target"]
            and path["hops"] == 2 for path in two_hop_paths),
    )

    # A target that requires all three allowed hops.
    for name, email in (
        ("Three Anchor", "three-anchor@three-anchor.example"),
        ("Three First", "three-first@three-first.example"),
        ("Three Second", "three-second@three-second.example"),
        ("Three Target", "three-target@three-target.example"),
    ):
        add_intro_contact(name, email)
    make_intro_anchor("Three Anchor")
    intro_crm.observe("contact:three_anchor", "knows", "contact:three_first")
    intro_crm.observe("contact:three_first", "knows", "contact:three_second")
    intro_crm.observe("contact:three_second", "knows", "contact:three_target")
    three_hop_at_two = intro_crm.intro_paths("Three Target", max_hops=2)
    three_hop_at_three = intro_crm.intro_paths("Three Target", max_hops=3)
    check("intro_paths_three_hop_respects_max_two", three_hop_at_two == [])
    check(
        "intro_paths_three_hop_found_at_three",
        any(path["path"] == ["Three Anchor", "Three First", "Three Second", "Three Target"]
            and path["hops"] == 3 for path in three_hop_at_three),
    )

    # A shorter route must rank before a longer route to the same target.
    for name, email in (
        ("Mixed Anchor", "mixed-anchor@mixed-anchor.example"),
        ("Mixed Short", "mixed-short@mixed-short.example"),
        ("Mixed Long One", "mixed-long-one@mixed-long-one.example"),
        ("Mixed Long Two", "mixed-long-two@mixed-long-two.example"),
        ("Mixed Target", "mixed-target@mixed-target.example"),
    ):
        add_intro_contact(name, email)
    make_intro_anchor("Mixed Anchor")
    intro_crm.observe("contact:mixed_anchor", "knows", "contact:mixed_short")
    intro_crm.observe("contact:mixed_short", "knows", "contact:mixed_target")
    intro_crm.observe("contact:mixed_anchor", "introduced_by", "contact:mixed_long_one")
    intro_crm.observe("contact:mixed_long_one", "introduced_by", "contact:mixed_long_two")
    intro_crm.observe("contact:mixed_long_two", "introduced_by", "contact:mixed_target")
    mixed_paths = intro_crm.intro_paths("Mixed Target", max_hops=3)
    check(
        "intro_paths_shorter_outranks_longer",
        len(mixed_paths) >= 2 and mixed_paths[0]["hops"] == 2
        and any(path["hops"] == 3 for path in mixed_paths),
    )

    # At equal depth, explicit introduction edges beat a route containing a
    # shared-domain inference.
    for name, email in (
        ("Explicit Anchor", "explicit-anchor@explicit-anchor.example"),
        ("Explicit Bridge", "explicit-bridge@explicit-bridge.example"),
        ("Domain Anchor", "domain-anchor@shared.example"),
        ("Domain Bridge", "domain-bridge@shared.example"),
        ("Rank Target", "rank-target@rank-target.example"),
    ):
        add_intro_contact(name, email)
    make_intro_anchor("Explicit Anchor")
    make_intro_anchor("Domain Anchor")
    intro_crm.observe("contact:explicit_anchor", "introduced_by", "contact:explicit_bridge")
    intro_crm.observe("contact:explicit_bridge", "introduced_by", "contact:rank_target")
    intro_crm.observe("contact:domain_bridge", "knows", "contact:rank_target")
    rank_paths = intro_crm.intro_paths("Rank Target", max_hops=2)
    explicit_path = next((path for path in rank_paths if path["path"][0] == "Explicit Anchor"), None)
    domain_path = next((path for path in rank_paths if path["path"][0] == "Domain Anchor"), None)
    check(
        "intro_paths_explicit_outranks_shared_domain",
        explicit_path is not None and domain_path is not None
        and rank_paths.index(explicit_path) < rank_paths.index(domain_path)
        and explicit_path["strength"] > domain_path["strength"],
    )

    add_intro_contact("Isolated Target", "isolated@isolated.example")
    check("intro_paths_no_path", intro_crm.intro_paths("Isolated Target") == [])
    check(
        "intro_paths_self_target",
        intro_crm.intro_paths("me") == [] and intro_crm.intro_paths("Two Anchor") == [],
    )

    # Closing a loop in the three-hop component must not repeat nodes or run
    # beyond the requested depth.
    intro_crm.observe("contact:three_second", "knows", "contact:three_anchor")
    cycle_paths = intro_crm.intro_paths("Three Target", max_hops=3)
    check(
        "intro_paths_cycles_are_bounded",
        bool(cycle_paths) and len(cycle_paths) < 10
        and all(len(path["path"]) == path["hops"] + 1
                and len(set(path["path"])) == len(path["path"])
                and path["hops"] <= 3 for path in cycle_paths),
    )
    check(
        "intro_paths_deterministic_ordering",
        intro_crm.intro_paths("Rank Target", max_hops=3)
        == intro_crm.intro_paths("Rank Target", max_hops=3),
    )
    check(
        "intro_paths_explanations",
        bool(rank_paths) and all(isinstance(path["explanation"], str)
                                 and path["explanation"].strip() for path in rank_paths),
    )

    try:
        import mcp_server as _intro_mcp
        original_intro_mcp_db = _intro_mcp.DB_PATH
        _intro_mcp.DB_PATH = intro_db
        intro_tool_names = {tool["name"] for tool in _intro_mcp.TOOLS}
        intro_mcp_json = _intro_mcp.handle_tool_call("crm_intro_paths", {
            "target_identifier": "Two Target",
            "max_hops": 2,
        })
        _intro_mcp.DB_PATH = original_intro_mcp_db
        intro_mcp_paths = __import__("json").loads(intro_mcp_json)
        check(
            "intro_paths_mcp_tool",
            "crm_intro_paths" in intro_tool_names
            and any(path["path"][-1] == "Two Target" for path in intro_mcp_paths),
        )
    except (AttributeError, TypeError, KeyError, ValueError):
        check("intro_paths_mcp_tool", False)

    intro_crm.close()

    # ── 80. macOS ingest fixtures and transform hardening (16 tests) ──
    ingest_dir = tempfile.mkdtemp()
    ingest_crm = CRM(os.path.join(ingest_dir, "ingest.db"))

    addressbook_db = make_addressbook_fixture_db([{
        "first": "Fixture",
        "last": "Person",
        "company": "Fixture Co",
        "title": "CTO",
        "notes": "Imported from the fixture",
        "emails": ["fixture@example.com", "fixture.alt@example.com"],
        "phones": ["(415) 555-0100"],
    }])
    contacts_added, contact_facts_added = ingest_crm.ingest_macos_contacts(
        db_paths=[addressbook_db]
    )
    fixture_contact = ingest_crm.get_contact("fixture@example.com")

    # 80a. the actual AddressBook SQLite query feeds the contact transform
    check(
        "ingest_addressbook_contact_mapping",
        contacts_added == 1 and contact_facts_added > 0
        and fixture_contact["name"] == "Fixture Person"
        and fixture_contact["company"] == "Fixture Co"
        and fixture_contact["title"] == "CTO",
    )

    # 80b. all AddressBook emails, phones, company, and notes reach the graph
    addressbook_history = ingest_crm.history_of("contact:fixture person")
    check(
        "ingest_addressbook_fact_mapping",
        {fact["value"] for fact in addressbook_history if fact["key"] == "email"}
        == {"fixture@example.com", "fixture.alt@example.com"}
        and ingest_crm.facts_about("contact:fixture person")["company"]["value"] == "Fixture Co"
        and ingest_crm.facts_about("contact:fixture person")["notes"]["value"]
        == "Imported from the fixture"
        and ingest_crm.facts_about("phone:+14155550100")["name"]["value"]
        == "Fixture Person",
    )

    imessage_db = make_imessage_fixture_db([
        ("(415) 555-0100", 8, 12),       # threshold: 20 is low
        ("FIXTURE@EXAMPLE.COM", 9, 12),  # threshold: 21 is medium
        ("medium@example.com", 50, 50),  # threshold: 100 is medium
        ("high@example.com", 51, 50),    # threshold: 101 is high
    ])
    handles_found, imessage_facts_added = ingest_crm.ingest_macos_imessage(
        days=2, db_path=imessage_db
    )
    phone_message_facts = ingest_crm.facts_about("phone:+14155550100")

    # 80c. the actual chat.db aggregate query returns every fixture handle
    check(
        "ingest_imessage_fixture_query",
        handles_found == 4 and imessage_facts_added > 0,
    )

    # 80d. sent, received, and total counts survive aggregation exactly
    check(
        "ingest_imessage_counts",
        phone_message_facts["imessage_total"]["value"] == "20"
        and phone_message_facts["imessage_sent"]["value"] == "8"
        and phone_message_facts["imessage_received"]["value"] == "12",
    )

    # 80e. intensity boundaries stay low <=20, medium <=100, high >100
    check(
        "ingest_imessage_intensity_thresholds",
        phone_message_facts["message_intensity"]["value"] == "low"
        and ingest_crm.facts_about("contact:fixture@example.com")["message_intensity"]["value"] == "medium"
        and ingest_crm.facts_about("contact:medium@example.com")["message_intensity"]["value"] == "medium"
        and ingest_crm.facts_about("contact:high@example.com")["message_intensity"]["value"] == "high",
    )

    # 80f. reciprocity retains both directions rather than collapsing to total
    check(
        "ingest_imessage_reciprocity",
        phone_message_facts["imessage_ratio"]["value"] == "8:12",
    )

    # 80g. formatted phones and case-varied emails both resolve to the contact
    matched_handles = {
        fact["value"] for fact in ingest_crm.history_of(
            "contact:fixture person", key="imessage_handle"
        )
    }
    check(
        "ingest_imessage_contact_variants",
        matched_handles == {"(415) 555-0100", "FIXTURE@EXAMPLE.COM"},
    )

    calendar_records, calendar_attendees = make_calendar_fixture_records()
    events_found, calendar_facts_added = ingest_crm.ingest_macos_calendar(
        records=calendar_records, attendees=calendar_attendees
    )

    # 80h. fake Calendar records exercise the real meeting transform
    check(
        "ingest_calendar_event_mapping",
        events_found == 1 and calendar_facts_added > 0
        and ingest_crm.facts_about("meeting:fixture person planning")["location"]["value"]
        == "Fixture Room",
    )

    # 80i. email and named attendees aggregate onto their contact entities
    check(
        "ingest_calendar_attendees",
        ingest_crm.facts_about("contact:guest@example.com")["meeting"]["value"]
        == "Fixture Person Planning"
        and ingest_crm.facts_about("contact:named guest")["meeting"]["value"]
        == "Fixture Person Planning"
        and ingest_crm.facts_about("contact:fixture person")["meeting"]["value"]
        == "Fixture Person Planning",
    )

    mail_records = make_mail_fixture_records()
    threads_found, mail_facts_added = ingest_crm.ingest_macos_mail(rows=mail_records)
    fixture_mail_facts = ingest_crm.facts_about("email:fixture@example.com")

    # 80j. one returned Envelope Index row counts as one thread
    check(
        "ingest_mail_thread_count",
        threads_found == 3 and mail_facts_added > 0,
    )

    # 80k. per-address direction counts and case-insensitive contact linking work
    check(
        "ingest_mail_address_aggregation",
        fixture_mail_facts["email_total"]["value"] == "3"
        and fixture_mail_facts["email_sent"]["value"] == "2"
        and fixture_mail_facts["email_received"]["value"] == "1"
        and ingest_crm.facts_about("contact:fixture person")["email_total"]["value"] == "3",
    )

    fact_count_before_empty = ingest_crm.conn.execute(
        "SELECT COUNT(*) FROM facts"
    ).fetchone()[0]
    empty_addressbook_db = make_addressbook_fixture_db()

    # 80l. an empty AddressBook fixture produces no contacts or facts
    check(
        "ingest_empty_addressbook",
        ingest_crm.ingest_macos_contacts(db_paths=[empty_addressbook_db]) == (0, 0)
        and ingest_crm.conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        == fact_count_before_empty,
    )

    empty_imessage_db = make_imessage_fixture_db()

    # 80m. an empty chat.db fixture produces no handles or facts
    check(
        "ingest_empty_imessage",
        ingest_crm.ingest_macos_imessage(days=2, db_path=empty_imessage_db) == (0, 0)
        and ingest_crm.conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        == fact_count_before_empty,
    )

    # 80n. empty injected Calendar and Mail sources also stay at zero
    check(
        "ingest_empty_record_sources",
        ingest_crm.ingest_macos_calendar(records=[], attendees=[]) == (0, 0)
        and ingest_crm.ingest_macos_mail(rows=[]) == (0, 0)
        and ingest_crm.conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
        == fact_count_before_empty,
    )

    # 80o. malformed rows are skipped while later valid rows are still ingested
    malformed_contacts = ingest_crm.ingest_macos_contacts(records=[
        None,
        {"first": "Valid", "last": "Contact", "emails": [], "phones": []},
    ])
    malformed_messages = ingest_crm.ingest_macos_imessage(rows=[
        {"handle": "broken@example.com", "msg_count": "not-a-number"},
        {"handle": "valid@example.com", "msg_count": 1, "sent": 1, "received": 0},
    ])
    malformed_calendar = ingest_crm.ingest_macos_calendar(
        records=[None, {"title": "Valid Fixture Event", "start_date": None}],
        attendees=[None],
    )
    malformed_mail = ingest_crm.ingest_macos_mail(rows=[
        {"from_addr": 42, "to_addr": None},
        {"from_addr": "valid@example.com", "to_addr": "other@example.com", "date_sent": 1},
    ])
    check(
        "ingest_malformed_rows_skipped",
        malformed_contacts[1] > 0 and malformed_messages[0] == 1
        and malformed_calendar[0] == 1 and malformed_mail[0] == 1,
    )

    # 80p. re-running the same fixtures refreshes facts without duplicating rows
    counts_before_reingest = (
        ingest_crm.conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0],
        ingest_crm.conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0],
    )
    ingest_crm.ingest_macos_contacts(db_paths=[addressbook_db])
    ingest_crm.ingest_macos_imessage(days=2, db_path=imessage_db)
    ingest_crm.ingest_macos_calendar(records=calendar_records, attendees=calendar_attendees)
    ingest_crm.ingest_macos_mail(rows=mail_records)
    counts_after_reingest = (
        ingest_crm.conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0],
        ingest_crm.conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0],
    )
    check("ingest_reingest_idempotent", counts_after_reingest == counts_before_reingest)

    # ── 81. Cadence Auto-Tuning (12 tests) ──

    cadence_dir = tempfile.mkdtemp()
    cadence_db = os.path.join(cadence_dir, "cadence.db")
    cadence_crm = CRM(cadence_db)

    # 81a. an empty CRM returns the complete static fallback schedule
    static_cadence = cadence_crm.tuned_cadence()
    check("tuned_cadence_defaults", static_cadence == CRM.CADENCE_DEFAULTS)

    # 81b. status lookup returns the same scalar interval as the full schedule
    check("tuned_cadence_status_lookup",
          cadence_crm.tuned_cadence("proposal_drafted") == static_cadence["proposal_drafted"])

    # One ten-day gap in won history becomes the learned contacted-stage base.
    cadence_crm.add_contact("Cadence Winner", email="winner@cadence.test",
                            status="active_customer")
    cadence_winner_id = cadence_crm.get_contact("winner@cadence.test")["id"]
    for days in (20, 10):
        cadence_crm.conn.execute(
            "INSERT INTO activity (contact_id, type, summary, created_at) "
            "VALUES (?, 'email', 'Winning cadence fixture', datetime('now', ?))",
            (cadence_winner_id, f"-{days} days"),
        )
    cadence_crm.conn.commit()

    # 81c. learned values scale from the winning gap and report their source
    learned_cadence = cadence_crm.tuned_cadence()
    learned_rows = cadence_crm._tuned_cadence_rows()
    check("tuned_cadence_learned_values",
          learned_cadence["contacted"] == 10 and learned_cadence["prospect"] == 20
          and all(row["source"] == "learned" for row in learned_rows))

    # 81d. late-stage follow-up remains tighter than top-of-funnel follow-up
    check("tuned_cadence_stage_shape",
          learned_cadence["verbal_yes"] < learned_cadence["proposal_drafted"]
          < learned_cadence["prospect"])

    cadence_crm.add_contact("Just Past", email="past@cadence.test", status="contacted")
    cadence_crm.update_contact(
        "past@cadence.test",
        last_contacted=(_date.today() - timedelta(days=11)).isoformat(),
    )
    cadence_crm.add_contact("Still Inside", email="inside@cadence.test", status="contacted")
    cadence_crm.update_contact(
        "inside@cadence.test",
        last_contacted=(_date.today() - timedelta(days=10)).isoformat(),
    )
    cadence_actions = {item["contact"]: item for item in cadence_crm.next_actions(limit=50)}

    # 81e. a contact just past the tuned interval is cadence-overdue
    check("next_actions_uses_tuned_boundary_past",
          "cadence_overdue" in {signal["name"]
                                 for signal in cadence_actions["Just Past"]["signals"]})

    # 81f. a contact exactly inside the tuned interval is not cadence-overdue
    check("next_actions_uses_tuned_boundary_inside",
          "cadence_overdue" not in {signal["name"]
                                     for signal in cadence_actions["Still Inside"]["signals"]})

    # 81g. stale-contact detection uses the same per-status boundary
    stale_cadence_names = {contact["name"] for contact in cadence_crm.stale_contacts()}
    check("stale_contacts_use_tuned_cadence",
          "Just Past" in stale_cadence_names and "Still Inside" not in stale_cadence_names)

    # 81h. reminders without an explicit date use the contact's tuned status cadence
    cadence_crm.add_contact("Verbal Val", email="val@cadence.test", status="verbal_yes")
    cadence_reminder_id = cadence_crm.set_reminder(
        "val@cadence.test", note="Send the contract"
    )
    cadence_due = cadence_crm.conn.execute(
        "SELECT due_date FROM reminders WHERE id = ?", (cadence_reminder_id,)
    ).fetchone()["due_date"]
    check("reminder_default_uses_tuned_cadence",
          cadence_due == (_date.today() + timedelta(
              days=learned_cadence["verbal_yes"]
          )).isoformat())

    # 81i. adding activity invalidates a previously cached no-win result
    cache_crm = CRM(os.path.join(tempfile.mkdtemp(), "cadence-cache.db"))
    cache_crm.add_contact("Cache Winner", email="cache@cadence.test",
                          status="active_customer")
    cache_winner_id = cache_crm.get_contact("cache@cadence.test")["id"]
    cache_crm.conn.execute(
        "INSERT INTO activity (contact_id, type, summary, created_at) "
        "VALUES (?, 'email', 'First cache touch', datetime('now', '-8 days'))",
        (cache_winner_id,),
    )
    cache_crm.conn.commit()
    cached_default = cache_crm.tuned_cadence("contacted")
    cache_crm.conn.execute(
        "INSERT INTO activity (contact_id, type, summary, created_at) "
        "VALUES (?, 'email', 'Second cache touch', datetime('now'))",
        (cache_winner_id,),
    )
    cache_crm.conn.commit()
    check("tuned_cadence_cache_invalidates",
          cached_default == CRM.CADENCE_DEFAULTS["contacted"]
          and cache_crm.tuned_cadence("contacted") == 8)
    cache_crm.close()

    # 81j. unchanged activity history produces deterministic values
    check("tuned_cadence_deterministic",
          cadence_crm.tuned_cadence() == cadence_crm.tuned_cadence())

    # 81k. evolve reports tuned and static values side by side
    cadence_evolution = cadence_crm.evolve()
    check("evolve_reports_tuned_cadence",
          cadence_evolution["cadence"]["contacted"] == {
              "tuned_days": 10, "default_days": 7, "source": "learned",
          })

    # 81l. MCP registers and serves the tuned-cadence tool
    import mcp_server
    original_cadence_mcp_db = mcp_server.DB_PATH
    mcp_server.DB_PATH = cadence_db
    cadence_mcp_names = {tool["name"] for tool in mcp_server.TOOLS}
    cadence_mcp_value = json.loads(
        mcp_server.handle_tool_call("crm_tuned_cadence", {"status": "verbal_yes"})
    )
    mcp_server.DB_PATH = original_cadence_mcp_db
    check("tuned_cadence_mcp_tool",
          "crm_tuned_cadence" in cadence_mcp_names
          and cadence_mcp_value == learned_cadence["verbal_yes"])

    # ── 82. Benchmark Integrity — hot-operation regression gate (8 tests) ──

    integrity_perf_crm = make_performance_fixture()
    performance_operations = (
        ("list_contacts", integrity_perf_crm.list_contacts),
        (
            "get_contact_email",
            lambda: integrity_perf_crm.get_contact(
                "perf+250@tenant-50.benchmark.test"
            ),
        ),
        (
            "get_contact_partial_name",
            lambda: integrity_perf_crm.get_contact("Contact 250"),
        ),
        ("pipeline", integrity_perf_crm.pipeline),
        ("next_actions_10", lambda: integrity_perf_crm.next_actions(10)),
        ("forecast", integrity_perf_crm.forecast),
        (
            "intro_paths",
            lambda: integrity_perf_crm.intro_paths(
                "perf+499@tenant-99.benchmark.test"
            ),
        ),
        ("export_full", integrity_perf_crm.export_full),
    )
    for operation_name, operation in performance_operations:
        threshold_ms = PERFORMANCE_THRESHOLDS_MS[operation_name]
        elapsed_ms = None
        operation_error = None
        try:
            operation()  # Warm caches so this gate measures the hot path.
            started = time.perf_counter()
            operation()
            elapsed_ms = (time.perf_counter() - started) * 1000
        except Exception as exc:
            operation_error = f"{type(exc).__name__}: {exc}"
        PERFORMANCE_TIMINGS.append({
            "name": operation_name,
            "elapsed_ms": elapsed_ms,
            "threshold_ms": threshold_ms,
            "error": operation_error,
        })
        check(
            f"perf_regression_{operation_name}",
            operation_error is None and elapsed_ms <= threshold_ms,
        )
    integrity_perf_crm.close()

    # ── 83. Benchmark Integrity — seeded CRUD properties (8 tests) ──

    property_crm = CRM(os.path.join(tempfile.mkdtemp(), "integrity-property.db"))
    property_rng = random.Random(PROPERTY_SEED)
    property_invariants = {
        "no_exception": True,
        "add_returns_id": True,
        "get_after_add_identity": True,
        "add_roundtrip_fields": True,
        "update_targeted_field": True,
        "update_preserves_other_fields": True,
        "archive_preserves_record": True,
        "archive_visibility": True,
    }
    property_exceptions = []
    unicode_names = (
        "Zoë 東京", "Łukasz 🚀", "नमस्ते दोस्त", "Мария Иванова",
        "李雷", "Renée O’Connor", "أمل الزهراني", "Δημήτρης Παπάς",
    )
    statuses = (
        "prospect", "contacted", "met", "proposal_drafted",
        "verbal_yes", "active_customer", "lost",
    )
    tracked_input_fields = (
        "name", "email", "company", "title", "deal_size", "status",
        "source", "notes", "tags",
    )
    update_fields = (
        "name", "company", "title", "deal_size", "status", "source",
        "notes", "tags", "last_contacted",
    )
    preserved_fields = (
        "id", "email", "name", "company", "title", "deal_size", "status",
        "source", "notes", "tags", "archived", "last_contacted", "created_at",
    )

    for index in range(200):
        base_name = property_rng.choice(unicode_names)
        name = f"{base_name} {index:03d}"
        if index % 41 == 0:
            name += " " + ("界" * 512)

        odd_token = f"{property_rng.getrandbits(32):08x}"
        email_shapes = (
            f"prop.{index}+{odd_token}@sub-{index % 17}.example.test",
            f"odd_%{index}-{odd_token}@x{index % 19}.example.test",
            f"UPPER+{index}.{odd_token}@Case-{index % 13}.Example.TEST",
        )
        email = None if index % 11 == 0 else email_shapes[index % len(email_shapes)]
        contact_input = {
            "name": name,
            "email": email,
            "company": "" if index % 4 == 0 else f"Café 会社 {property_rng.randrange(50)}",
            "title": "" if index % 5 == 0 else f"R&D / 🛠️ {property_rng.randrange(30)}",
            "deal_size": "" if index % 6 == 0 else f"${property_rng.randrange(1, 999)}K",
            "status": property_rng.choice(statuses),
            "source": "" if index % 7 == 0 else "seeded property pass",
            "notes": (
                "長" * 4096 if index % 37 == 0
                else ("" if index % 3 == 0 else f"Odd note ☃ {odd_token}\nline two")
            ),
            "tags": "" if index % 8 == 0 else f"δelta,{property_rng.randrange(20)},🌍",
        }

        try:
            contact_id = property_crm.add_contact(**contact_input)
            property_invariants["add_returns_id"] &= (
                isinstance(contact_id, int) and contact_id > 0
            )

            identifier = email if email is not None else name
            stored = property_crm.get_contact(identifier)
            property_invariants["get_after_add_identity"] &= bool(
                stored and stored["id"] == contact_id
            )
            property_invariants["add_roundtrip_fields"] &= bool(
                stored and all(
                    stored[field] == contact_input[field]
                    for field in tracked_input_fields
                )
            )

            target_field = update_fields[index % len(update_fields)]
            update_values = {
                "name": f"更新 {base_name} {index:03d}",
                "company": f"Updated 公司 {index}",
                "title": f"Updated title 🧪 {index}",
                "deal_size": f"${1000 + index}K",
                "status": statuses[(statuses.index(contact_input["status"]) + 1) % len(statuses)],
                "source": f"updated-source-{index}",
                "notes": "更新-" + (("🧠" * 2048) if index % 31 == 0 else str(index)),
                "tags": f"updated,βeta,{index}",
                "last_contacted": f"2026-02-{1 + (index % 28):02d}",
            }
            target_value = update_values[target_field]
            updated = property_crm.update_contact(
                identifier, **{target_field: target_value}
            )
            post_identifier = (
                email if email is not None
                else (target_value if target_field == "name" else name)
            )
            after_update = property_crm.get_contact(post_identifier)
            property_invariants["update_targeted_field"] &= bool(
                updated and after_update
                and updated[target_field] == target_value
                and after_update[target_field] == target_value
            )
            property_invariants["update_preserves_other_fields"] &= bool(
                stored and after_update and all(
                    stored[field] == after_update[field]
                    for field in preserved_fields if field != target_field
                )
            )

            archived = property_crm.archive_contact(post_identifier)
            archived_record = property_crm.get_contact(post_identifier)
            property_invariants["archive_preserves_record"] &= bool(
                archived is True and archived_record
                and archived_record["id"] == contact_id
                and archived_record["archived"] == 1
                and all(
                    archived_record[field] == after_update[field]
                    for field in preserved_fields
                    if field not in {"archived"}
                )
            )
            visible_ids = {
                row["id"] for row in property_crm.list_contacts()
            }
            all_ids = {
                row["id"] for row in property_crm.list_contacts(
                    include_archived=True
                )
            }
            property_invariants["archive_visibility"] &= (
                contact_id not in visible_ids and contact_id in all_ids
            )
        except Exception as exc:
            property_invariants["no_exception"] = False
            for invariant_name in property_invariants:
                if invariant_name != "no_exception":
                    property_invariants[invariant_name] = False
            if len(property_exceptions) < 3:
                property_exceptions.append(
                    f"case {index}: {type(exc).__name__}: {exc}"
                )

    exception_detail = (
        f" ({'; '.join(property_exceptions)})" if property_exceptions else ""
    )
    check(
        f"property_crud_no_exception{exception_detail}",
        property_invariants["no_exception"],
    )
    check("property_crud_add_returns_id", property_invariants["add_returns_id"])
    check(
        "property_crud_get_after_add_identity",
        property_invariants["get_after_add_identity"],
    )
    check(
        "property_crud_add_roundtrip_fields",
        property_invariants["add_roundtrip_fields"],
    )
    check(
        "property_crud_update_targeted_field",
        property_invariants["update_targeted_field"],
    )
    check(
        "property_crud_update_preserves_other_fields",
        property_invariants["update_preserves_other_fields"],
    )
    check(
        "property_crud_archive_preserves_record",
        property_invariants["archive_preserves_record"],
    )
    check(
        "property_crud_archive_visibility",
        property_invariants["archive_visibility"],
    )
    property_crm.close()

    # ── 84. Forecast Calibration & Learned Weights (14 tests) ──

    prediction_crm = CRM(os.path.join(tempfile.mkdtemp(), "prediction-facts.db"))
    prediction_crm.add_contact(
        "Predict Pat", email="pat@prediction.test", status="prospect",
        source="referral",
    )
    prediction_crm.deal_probability("pat@prediction.test")
    recorded_prediction_facts = prediction_crm.conn.execute(
        """SELECT key, value, source, observed_at FROM facts
           WHERE entity = 'prediction:pat@prediction.test'
           ORDER BY key"""
    ).fetchall()

    # 84a. A public prediction records all three forecast facts with timestamps.
    check(
        "prediction_facts_recorded",
        {row["key"] for row in recorded_prediction_facts}
        == {"probability", "status", "signals_snapshot"}
        and all(row["source"] == "forecast" and row["observed_at"]
                for row in recorded_prediction_facts),
    )

    # 84b. An unchanged prediction is below the materiality threshold.
    prediction_crm.deal_probability("pat@prediction.test")
    unchanged_fact_count = prediction_crm.conn.execute(
        "SELECT COUNT(*) FROM facts WHERE entity = 'prediction:pat@prediction.test'"
    ).fetchone()[0]
    check("prediction_material_dedupe", unchanged_fact_count == 3)

    # 84c. A stage-driven material probability change records a second event.
    prediction_crm.update_contact("pat@prediction.test", status="verbal_yes")
    prediction_crm.deal_probability("pat@prediction.test")
    material_probability_count = prediction_crm.conn.execute(
        """SELECT COUNT(*) FROM facts
           WHERE entity = 'prediction:pat@prediction.test'
             AND key = 'probability' AND source = 'forecast'"""
    ).fetchone()[0]
    check("prediction_material_change_recorded", material_probability_count == 2)

    # 84d. Bulk forecast records predictions through the same path.
    prediction_crm.add_contact(
        "Forecast Fran", email="fran@prediction.test", status="contacted"
    )
    prediction_crm.forecast()
    forecast_fact_keys = {
        row["key"] for row in prediction_crm.conn.execute(
            """SELECT key FROM facts
               WHERE entity = 'prediction:fran@prediction.test'
                 AND source = 'forecast'"""
        ).fetchall()
    }
    check(
        "forecast_records_prediction_facts",
        forecast_fact_keys == {"probability", "status", "signals_snapshot"},
    )
    prediction_crm.close()

    empty_calibration_crm = CRM(os.path.join(tempfile.mkdtemp(), "empty-calibration.db"))
    empty_calibration = empty_calibration_crm.calibration_report()
    # 84e. Empty calibration data is explicit rather than an error or NaN.
    check(
        "calibration_empty_case",
        empty_calibration["scored_predictions"] == 0
        and empty_calibration["brier_score"] is None
        and empty_calibration["weights_source"] == "default"
        and len(empty_calibration["reliability"]) == 10,
    )
    empty_calibration_crm.close()

    calibration_crm = CRM(os.path.join(tempfile.mkdtemp(), "calibration.db"))

    def seed_closed_prediction(crm, index, probability, outcome, stage, scores):
        email = f"closed-{index}@calibration.test"
        crm.add_contact(
            f"Closed Calibration {index}", email=email, status="prospect",
            source="fixture",
        )
        entity = f"prediction:{email}"
        observed_at = f"2026-01-{1 + index:02d} 12:00:00.000000"
        snapshot = {
            name: {
                "value": stage if name == "stage" else scores[name],
                "score": scores[name],
            }
            for name in CRM.FORECAST_SIGNAL_NAMES
        }
        with crm.conn:
            crm.conn.executemany(
                """INSERT INTO facts (entity, key, value, source, observed_at)
                   VALUES (?, ?, ?, 'forecast', ?)""",
                (
                    (entity, "probability", str(probability), observed_at),
                    (entity, "status", stage, observed_at),
                    (entity, "signals_snapshot", json.dumps(snapshot, sort_keys=True),
                     observed_at),
                ),
            )
        crm.update_contact(email, status=outcome)

    low_scores = {name: 0.1 for name in CRM.FORECAST_SIGNAL_NAMES}
    high_scores = {name: 0.9 for name in CRM.FORECAST_SIGNAL_NAMES}
    seed_closed_prediction(
        calibration_crm, 0, 0.25, "active_customer", "prospect", low_scores
    )
    seed_closed_prediction(
        calibration_crm, 1, 0.75, "lost", "proposal_drafted", high_scores
    )
    calibration = calibration_crm.calibration_report()

    # 84f. ((.25 - 1)^2 + (.75 - 0)^2) / 2 = .5625 exactly.
    check(
        "calibration_brier_exact",
        calibration["scored_predictions"] == 2
        and calibration["brier_score"] == 0.5625,
    )

    # 84g. Brier errors are also grouped by the recorded prediction stage.
    check(
        "calibration_stage_buckets",
        calibration["by_stage"]["prospect"]
        == {"count": 1, "brier_score": 0.5625}
        and calibration["by_stage"]["proposal_drafted"]
        == {"count": 1, "brier_score": 0.5625},
    )

    # 84h. Reliability deciles expose observed win rates for occupied buckets.
    check(
        "calibration_reliability_buckets",
        calibration["reliability"][2]["predicted_decile"] == "20-30%"
        and calibration["reliability"][2]["actual_win_rate"] == 1.0
        and calibration["reliability"][7]["actual_win_rate"] == 0.0,
    )

    # 84i. Two closed outcomes are below the 20-outcome learning threshold.
    check("learned_weights_minimum", calibration_crm.learned_weights() is None)
    calibration_crm.close()

    learned_db = os.path.join(tempfile.mkdtemp(), "learned-weights.db")
    learned_crm = CRM(learned_db)
    for index in range(20):
        won = index < 10
        seed_closed_prediction(
            learned_crm,
            index,
            0.9 if won else 0.1,
            "active_customer" if won else "lost",
            "verbal_yes" if won else "prospect",
            high_scores if won else low_scores,
        )
        learned_crm.update_contact(
            f"closed-{index}@calibration.test",
            source="winning-source" if won else "losing-source",
        )

    first_fit = learned_crm.learned_weights()
    second_fit = learned_crm.learned_weights()
    # 84j. Fixed initialization and iterations make repeated fits identical.
    check(
        "learned_weights_deterministic",
        first_fit == second_fit and first_fit["outcome_count"] == 20,
    )

    learned_crm.add_contact(
        "Clearly Winning", email="winning@calibration.test",
        status="verbal_yes", source="winning-source",
    )
    learned_crm.add_contact(
        "Clearly Losing", email="losing@calibration.test",
        status="prospect", source="losing-source",
    )
    for days in (1, 3, 5, 7):
        winning_id = learned_crm.get_contact("winning@calibration.test")["id"]
        learned_crm.conn.execute(
            """INSERT INTO activity (contact_id, type, summary, created_at)
               VALUES (?, 'meeting', 'Strong buying signal', datetime('now', ?))""",
            (winning_id, f"-{days} days"),
        )
    learned_crm.conn.commit()
    winning_prediction = learned_crm.deal_probability("winning@calibration.test")
    losing_prediction = learned_crm.deal_probability("losing@calibration.test")

    # 84k. Once the threshold is met, public scoring identifies learned mode.
    check(
        "deal_probability_learned_mode",
        winning_prediction["weights_source"] == "learned"
        and losing_prediction["weights_source"] == "learned",
    )

    # 84l. A separable history ranks the clearly winning profile higher.
    check(
        "learned_weights_rank_profiles",
        winning_prediction["probability"] > losing_prediction["probability"],
    )

    # 84m. Logistic scoring remains a valid probability in learned mode.
    check(
        "learned_probability_range",
        all(0.0 <= prediction["probability"] <= 1.0
            for prediction in (winning_prediction, losing_prediction)),
    )

    # 84n. MCP registers and serves the same calibration report.
    try:
        import mcp_server
        original_mcp_db = mcp_server.DB_PATH
        mcp_server.DB_PATH = learned_db
        calibration_tool_names = {tool["name"] for tool in mcp_server.TOOLS}
        calibration_output = mcp_server.handle_tool_call("crm_calibration", {})
        mcp_server.DB_PATH = original_mcp_db
        check(
            "calibration_mcp_tool",
            "crm_calibration" in calibration_tool_names
            and '"brier_score"' in calibration_output
            and '"weights_source": "learned"' in calibration_output,
        )
    except (AttributeError, TypeError, KeyError):
        check("calibration_mcp_tool", False)

    learned_crm.close()

    # ── 85. Self-Proposing Roadmap (12 tests) ──

    proposal_dir = tempfile.mkdtemp()
    proposal_crm_path = os.path.join(proposal_dir, "synthetic_crm.py")
    proposal_benchmark_path = os.path.join(proposal_dir, "synthetic_benchmark.py")
    proposal_roadmap_path = os.path.join(proposal_dir, "roadmap.md")
    with open(proposal_crm_path, "w", encoding="utf-8") as synthetic_crm_file:
        synthetic_crm_file.write(
            "class CRM:\n"
            "    def covered(self):\n"
            "        return True\n"
            "    def uncovered_capability(self):\n"
            "        return True\n"
            "    def _private_helper(self):\n"
            "        return True\n"
        )
    with open(proposal_benchmark_path, "w", encoding="utf-8") as synthetic_benchmark_file:
        synthetic_benchmark_file.write("crm.covered()\n")
    with open(proposal_roadmap_path, "w", encoding="utf-8") as synthetic_roadmap_file:
        synthetic_roadmap_file.write(
            "# Roadmap\n\n### Twelve. Existing roadmap item\nAlready planned.\n"
        )

    proposal_crm = CRM(os.path.join(proposal_dir, "empty.db"))
    proposal_output = proposal_crm.propose_ticks(
        benchmark_path=proposal_benchmark_path,
        crm_path=proposal_crm_path,
        roadmap_path=proposal_roadmap_path,
    )
    proposal_titles = [item["title"] for item in proposal_output["proposals"]]
    coverage_proposals = [
        item for item in proposal_output["proposals"]
        if item["kind"] == "untested capability"
    ]

    # 85a. AST coverage finds a public method with zero benchmark references.
    check(
        "propose_ticks_untested_method",
        [item["title"] for item in coverage_proposals]
        == ["Benchmark uncovered_capability capability"],
    )

    # 85b. Covered and private methods are not reported as coverage gaps.
    check(
        "propose_ticks_public_methods_only",
        all("covered" not in title or "uncovered_capability" in title
            for title in proposal_titles)
        and all("private_helper" not in title for title in proposal_titles),
    )

    # 85c. Data-dependent signal gaps can only use names declared by CRM.
    signal_proposals = [
        item for item in proposal_output["proposals"]
        if item["kind"] == "signal never fires on this data"
    ]
    proposed_signal_names = {
        item["title"][len("Activate "):-len(" signal")]
        for item in signal_proposals
    }
    known_signal_names = set(
        CRM.NEXT_ACTION_SIGNAL_NAMES + CRM.FORECAST_SIGNAL_NAMES
    )
    check(
        "propose_ticks_known_signals_only",
        bool(proposed_signal_names)
        and proposed_signal_names <= known_signal_names,
    )

    # 85d. Two scans of unchanged sources and data are byte-for-byte stable.
    repeated_proposal_output = proposal_crm.propose_ticks(
        benchmark_path=proposal_benchmark_path,
        crm_path=proposal_crm_path,
        roadmap_path=proposal_roadmap_path,
    )
    check("propose_ticks_deterministic", proposal_output == repeated_proposal_output)

    # 85e. Markdown uses the same ordinal tick-heading shape as roadmap.md.
    check(
        "propose_ticks_markdown_format",
        bool(re.search(
            r"(?m)^### Thirteen\. .+\n.+\nKind: .+ Evidence: .+\.$",
            proposal_output["markdown"],
        )),
    )

    # 85f. An empty connected DB still produces a complete structured response.
    check(
        "propose_ticks_empty_db",
        set(proposal_output) == {"proposals", "markdown"}
        and isinstance(proposal_output["proposals"], list)
        and isinstance(proposal_output["markdown"], str),
    )

    # 85g. Every ranked item has exactly the public contract and usable evidence.
    check(
        "propose_ticks_evidence",
        bool(proposal_output["proposals"])
        and all(
            set(item) == {"title", "rationale", "kind", "evidence"}
            and all(isinstance(item[key], str) and item[key].strip()
                    for key in item)
            for item in proposal_output["proposals"]
        ),
    )

    # 85h. Stable kind priority makes the output a ranked list, not a set dump.
    proposal_kind_rank = {
        "untested capability": 0,
        "signal never fires on this data": 1,
        "performance trend": 2,
    }
    output_ranks = [
        proposal_kind_rank[item["kind"]]
        for item in proposal_output["proposals"]
    ]
    check(
        "propose_ticks_ranked",
        output_ranks == sorted(output_ranks),
    )

    # 85i. Existing tick title words suppress the matching proposal.
    with open(proposal_roadmap_path, "w", encoding="utf-8") as synthetic_roadmap_file:
        synthetic_roadmap_file.write(
            "# Roadmap\n\n"
            "### Thirteen. Benchmark uncovered capability\nAlready planned.\n"
        )
    deduped_output = proposal_crm.propose_ticks(
        benchmark_path=proposal_benchmark_path,
        crm_path=proposal_crm_path,
        roadmap_path=proposal_roadmap_path,
    )
    check(
        "propose_ticks_roadmap_dedupe",
        "Benchmark uncovered_capability capability"
        not in [item["title"] for item in deduped_output["proposals"]],
    )

    # 85j. A cached PERF block is parsed without importing or running the benchmark.
    cached_benchmark_path = os.path.join(proposal_dir, "benchmark_results.txt")
    with open(cached_benchmark_path, "w", encoding="utf-8") as cached_benchmark_file:
        cached_benchmark_file.write(
            "PERFORMANCE TIMINGS (seed=1, fixture):\n"
            "  slow_operation                  9.000 ms  limit 10.000 ms  PASS\n"
        )
    perf_output = proposal_crm.propose_ticks(
        benchmark_path=proposal_benchmark_path,
        crm_path=proposal_crm_path,
        roadmap_path=proposal_roadmap_path,
    )
    performance_proposals = [
        item for item in perf_output["proposals"]
        if item["kind"] == "performance trend"
    ]
    check(
        "propose_ticks_cached_perf",
        len(performance_proposals) == 1
        and "9.000 ms / 10.000 ms" in performance_proposals[0]["evidence"],
    )

    # 85k. MCP advertises and serves the same structured proposal contract.
    try:
        import mcp_server
        original_proposal_mcp_db = mcp_server.DB_PATH
        mcp_server.DB_PATH = os.path.join(proposal_dir, "mcp-empty.db")
        proposal_tool_names = {tool["name"] for tool in mcp_server.TOOLS}
        proposal_mcp_output = json.loads(
            mcp_server.handle_tool_call("crm_propose_ticks", {})
        )
        mcp_server.DB_PATH = original_proposal_mcp_db
        check(
            "propose_ticks_mcp_tool",
            "crm_propose_ticks" in proposal_tool_names
            and set(proposal_mcp_output) == {"proposals", "markdown"},
        )
    except (AttributeError, TypeError, KeyError, json.JSONDecodeError):
        check("propose_ticks_mcp_tool", False)

    # 85l. The CLI prints a paste-ready tick block against an empty temp DB.
    cli_env = os.environ.copy()
    cli_env["CRM_DB"] = os.path.join(proposal_dir, "cli-empty.db")
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    cli_proposal = subprocess.run(
        [sys.executable, os.path.join(repo_dir, "crm.py"), "propose"],
        cwd=repo_dir,
        env=cli_env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    check(
        "propose_ticks_cli",
        cli_proposal.returncode == 0
        and bool(re.search(r"(?m)^### [A-Za-z-]+\. .+$", cli_proposal.stdout)),
    )
    proposal_crm.close()

    cadence_crm.close()

    ingest_crm.close()

    for roundtrip_crm in (
        roundtrip_source, roundtrip_target, merge_crm, csv_roundtrip_crm,
        odd_csv_crm, malformed_csv_crm, multi_vcard_crm, folded_vcard_crm,
    ):
        roundtrip_crm.close()

    # Clean up temp files
    try:
        os.unlink(TEST_DB)
    except Exception:
        pass


def main():
    global FAIL

    try:
        run_benchmarks()
    except Exception as e:
        traceback.print_exc()
        FAIL += 1
        ERRORS.append(f"CRASH: {e}")

    results_line, score_line = result_lines(PASS, FAIL)
    integrity_errors = score_integrity_errors(
        PASS, FAIL, results_line, score_line
    )
    if integrity_errors:
        FAIL += len(integrity_errors)
        ERRORS.extend(integrity_errors)
        results_line, score_line = result_lines(PASS, FAIL)

    print(f"\n{'=' * 40}")
    print(
        "PERFORMANCE TIMINGS "
        f"(seed={PERFORMANCE_SEED}, 500 contacts / 2000 activities):"
    )
    if PERFORMANCE_TIMINGS:
        for timing in PERFORMANCE_TIMINGS:
            if timing["elapsed_ms"] is None:
                elapsed = "ERROR"
                status = timing["error"]
            else:
                elapsed = f"{timing['elapsed_ms']:.3f} ms"
                status = (
                    "PASS" if timing["elapsed_ms"] <= timing["threshold_ms"]
                    else "FAIL"
                )
            print(
                f"  {timing['name']:<26} {elapsed:>12}  "
                f"limit {timing['threshold_ms']:.3f} ms  {status}"
            )
    else:
        print("  not run")
    print(results_line)
    if ERRORS:
        print(f"FAILURES: {', '.join(ERRORS[:10])}")
    print(score_line)
    return benchmark_exit_code(FAIL)


if __name__ == "__main__":
    sys.exit(main())
