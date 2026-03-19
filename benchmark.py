#!/usr/bin/env python3
"""
benchmark.py — eval harness for agent-crm autoresearch.

Scores crm.py on correctness, edge cases, graph power, and performance.
Outputs a single metric: score: <N> (higher is better).

Each test = 1 point. Bonus points for performance thresholds.
"""

import os
import sys
import time
import tempfile
import traceback

# Force a temp DB so we never touch real data
TEST_DB = os.path.join(tempfile.mkdtemp(), "bench.db")
os.environ["CRM_DB"] = TEST_DB

from crm import CRM

PASS = 0
FAIL = 0
ERRORS = []


def check(name, condition):
    global PASS, FAIL
    if condition:
        PASS += 1
    else:
        FAIL += 1
        ERRORS.append(name)


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
    count = crm2.import_csv(export_path)
    check("import_csv_count", count >= 2)
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
    # These run on any machine (gracefully return (0,0) when macOS DBs not found).

    ingest_crm = CRM(os.path.join(tempfile.mkdtemp(), "ingest.db"))

    # 16a. ingest_macos_contacts exists and returns tuple
    try:
        result = ingest_crm.ingest_macos_contacts()
        check("ingest_contacts_returns_tuple", isinstance(result, tuple) and len(result) == 2)
    except (AttributeError, TypeError):
        check("ingest_contacts_returns_tuple", False)

    # 16b. ingest_macos_contacts returns ints
    try:
        added, facts = ingest_crm.ingest_macos_contacts()
        check("ingest_contacts_returns_ints", isinstance(added, int) and isinstance(facts, int))
    except (AttributeError, TypeError, ValueError):
        check("ingest_contacts_returns_ints", False)

    # 16c. ingest_macos_imessage exists and returns tuple
    try:
        result = ingest_crm.ingest_macos_imessage(days=30)
        check("ingest_imessage_returns_tuple", isinstance(result, tuple) and len(result) == 2)
    except (AttributeError, TypeError):
        check("ingest_imessage_returns_tuple", False)

    # 16d. ingest_macos_imessage accepts days parameter
    try:
        result = ingest_crm.ingest_macos_imessage(days=7)
        check("ingest_imessage_days_param", isinstance(result, tuple))
    except (AttributeError, TypeError):
        check("ingest_imessage_days_param", False)

    # 16e. ingest_macos_calendar exists and returns tuple
    try:
        result = ingest_crm.ingest_macos_calendar()
        check("ingest_calendar_returns_tuple", isinstance(result, tuple) and len(result) == 2)
    except (AttributeError, TypeError):
        check("ingest_calendar_returns_tuple", False)

    # 16f. ingest_macos_calendar accepts days_back/days_forward
    try:
        result = ingest_crm.ingest_macos_calendar(days_back=7, days_forward=7)
        check("ingest_calendar_days_params", isinstance(result, tuple))
    except (AttributeError, TypeError):
        check("ingest_calendar_days_params", False)

    # 16g. ingest_all exists and returns dict
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
        result = email_crm.ingest_macos_mail(days=30)
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
        result = email_crm.ingest_macos_mail(days=7)
        check("ingest_mail_days_param", isinstance(result, tuple))
    except (AttributeError, TypeError):
        check("ingest_mail_days_param", False)

    email_crm.close()

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

    crm.close()

    # Clean up temp files
    try:
        os.unlink(TEST_DB)
    except Exception:
        pass


if __name__ == "__main__":
    try:
        run_benchmarks()
    except Exception as e:
        traceback.print_exc()
        FAIL += 1
        ERRORS.append(f"CRASH: {e}")

    total = PASS + FAIL
    print(f"\n{'=' * 40}")
    print(f"RESULTS: {PASS}/{total} tests passed")
    if ERRORS:
        print(f"FAILURES: {', '.join(ERRORS[:10])}")
    print(f"score: {PASS}")
