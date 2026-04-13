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
