#!/usr/bin/env python3
"""
mcp_server.py — MCP (Model Context Protocol) server for agent-crm.

Exposes the CRM as a set of tools any MCP-compatible client can use.
Zero dependencies beyond stdlib + crm.py.

Usage:
    # stdio mode (for Claude Code, Cursor, etc.)
    python mcp_server.py

    # Custom DB path
    CRM_DB=my.db python mcp_server.py

Add to Claude Code's MCP config:
    {
      "mcpServers": {
        "crm": {
          "command": "python3",
          "args": ["/path/to/mcp_server.py"]
        }
      }
    }
"""

import json
import sys
import os
from crm import CRM

DB_PATH = os.environ.get("CRM_DB", "crm.db")

# MCP tool definitions
TOOLS = [
    {
        "name": "crm_add_contact",
        "description": "Add a new contact to the CRM",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Full name"},
                "email": {"type": "string", "description": "Email address"},
                "company": {"type": "string", "description": "Company name"},
                "title": {"type": "string", "description": "Job title"},
                "deal_size": {"type": "string", "description": "Deal size (e.g. '$5K/mo')"},
                "status": {"type": "string", "description": "Status: prospect, contacted, met, proposal_drafted, verbal_yes, active_customer, churned, lost"},
                "source": {"type": "string", "description": "Lead source"},
                "notes": {"type": "string", "description": "Notes"},
                "tags": {"type": "string", "description": "Comma-separated tags"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "crm_list_contacts",
        "description": "List all contacts, optionally filtered by status or company",
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "description": "Filter by status"},
                "company": {"type": "string", "description": "Filter by company (partial match)"},
            },
        },
    },
    {
        "name": "crm_view_contact",
        "description": "Get full details for a contact by email or name",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Email or name"},
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "crm_update_contact",
        "description": "Update a contact's fields",
        "inputSchema": {
            "type": "object",
            "properties": {
                "email": {"type": "string", "description": "Contact email to update"},
                "name": {"type": "string"},
                "company": {"type": "string"},
                "title": {"type": "string"},
                "deal_size": {"type": "string"},
                "status": {"type": "string"},
                "notes": {"type": "string"},
                "tags": {"type": "string"},
            },
            "required": ["email"],
        },
    },
    {
        "name": "crm_log_activity",
        "description": "Log an activity (email, call, meeting, note) for a contact",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
                "type": {"type": "string", "description": "Activity type: email, call, meeting, note, demo, proposal"},
                "summary": {"type": "string", "description": "What happened"},
            },
            "required": ["identifier", "type", "summary"],
        },
    },
    {
        "name": "crm_search",
        "description": "Search contacts by name, company, email, notes, or tags",
        "inputSchema": {
            "type": "object",
            "properties": {
                "term": {"type": "string", "description": "Search term"},
            },
            "required": ["term"],
        },
    },
    {
        "name": "crm_observe",
        "description": "Record a fact in the context graph (any entity, any key, any value)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entity": {"type": "string", "description": "Entity (e.g. 'contact:jane', 'company:acme')"},
                "key": {"type": "string", "description": "Fact key (e.g. 'role', 'industry')"},
                "value": {"type": "string", "description": "Fact value"},
                "source": {"type": "string", "description": "Where this fact came from", "default": "agent"},
            },
            "required": ["entity", "key", "value"],
        },
    },
    {
        "name": "crm_facts_about",
        "description": "Get all known facts about an entity from the context graph",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entity": {"type": "string", "description": "Entity to look up"},
            },
            "required": ["entity"],
        },
    },
    {
        "name": "crm_pipeline",
        "description": "Get pipeline summary — contacts grouped by status with deal values",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_stats",
        "description": "Get CRM statistics — total contacts, by status, recent activity",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_score_contact",
        "description": "Get engagement score (0-100) for a contact",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "crm_enrich",
        "description": "Get enriched profile: contact + facts + activities + score + deals",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "crm_next_actions",
        "description": "Get recommended next actions across all contacts, prioritized",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max actions to return", "default": 10},
            },
        },
    },
    {
        "name": "crm_context_for_agent",
        "description": "Get context string an AI agent needs — either for a specific contact or an executive summary",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email/name, or omit for executive summary"},
            },
        },
    },
    {
        "name": "crm_search_graph",
        "description": "Search across the entire context graph (entities, keys, values, sources)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search term"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "crm_query",
        "description": "Natural language query: 'active customers', 'high value', 'enterprise', etc.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Natural language query"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "crm_ingest",
        "description": "Auto-populate CRM from local data sources (macOS Contacts, iMessage, Calendar, Mail)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source": {"type": "string", "enum": ["all", "contacts", "imessage", "calendar", "mail"], "default": "all"},
                "days": {"type": "integer", "description": "Days of iMessage/mail history", "default": 90},
            },
        },
    },
    {
        "name": "crm_find_intros",
        "description": "Find warm intro paths to a target company, person, or email using the relationship graph",
        "inputSchema": {
            "type": "object",
            "properties": {
                "target": {"type": "string", "description": "Company name, person name, or email to find intros to"},
            },
            "required": ["target"],
        },
    },
    {
        "name": "crm_relationship_health",
        "description": "Analyze relationship health across all contacts — surfaces decaying relationships and opportunities",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_network_summary",
        "description": "High-level summary of your relationship network: contacts, companies, message volumes, pipeline value",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_add_deal",
        "description": "Add a deal to a contact. Track sales opportunities.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
                "name": {"type": "string", "description": "Deal name"},
                "value": {"type": "string", "description": "Deal value (e.g. '$50K/yr')"},
                "stage": {"type": "string", "description": "Stage: prospect, qualification, proposal, negotiation, closed_won, closed_lost"},
                "notes": {"type": "string", "description": "Notes"},
            },
            "required": ["identifier", "name"],
        },
    },
    {
        "name": "crm_update_deal",
        "description": "Update a deal's fields (name, value, stage, notes)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "deal_id": {"type": "integer", "description": "Deal ID"},
                "name": {"type": "string"},
                "value": {"type": "string"},
                "stage": {"type": "string"},
                "notes": {"type": "string"},
            },
            "required": ["deal_id"],
        },
    },
    {
        "name": "crm_close_deal",
        "description": "Close a deal as won",
        "inputSchema": {
            "type": "object",
            "properties": {
                "deal_id": {"type": "integer", "description": "Deal ID"},
                "notes": {"type": "string", "description": "Closing notes"},
            },
            "required": ["deal_id"],
        },
    },
    {
        "name": "crm_list_deals",
        "description": "List all deals, optionally filtered by stage or for a specific contact",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name (optional)"},
                "stage": {"type": "string", "description": "Filter by stage"},
            },
        },
    },
    {
        "name": "crm_deal_pipeline",
        "description": "Deal-centric pipeline view with stages, counts, and values",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_merge_contacts",
        "description": "Merge two contacts into one. Combines activities, deals, and facts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "keep": {"type": "string", "description": "Contact to keep (email or name)"},
                "merge": {"type": "string", "description": "Contact to absorb and delete"},
            },
            "required": ["keep", "merge"],
        },
    },
    {
        "name": "crm_unified_search",
        "description": "Search across contacts, facts, and activity simultaneously. More comprehensive than basic search.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "term": {"type": "string", "description": "Search term"},
            },
            "required": ["term"],
        },
    },
    {
        "name": "crm_archive_contact",
        "description": "Soft-delete a contact. Hides from lists/stats without destroying data.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Email or name"},
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "crm_unarchive_contact",
        "description": "Restore an archived contact",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Email or name"},
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "crm_dashboard",
        "description": "All-in-one CRM dashboard: metrics, pipeline, actions, health, graph stats. One call for daily briefing.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_delete_contacts",
        "description": "Bulk delete multiple contacts by email/name in a single transaction",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifiers": {"type": "array", "items": {"type": "string"}, "description": "List of emails or names"},
            },
            "required": ["identifiers"],
        },
    },
    {
        "name": "crm_touch_plan",
        "description": "Auto-generate a follow-up schedule for a contact based on status and deal value",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "crm_detect_churning",
        "description": "Find contacts with decaying engagement that need intervention, sorted by deal value",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_deal_velocity_report",
        "description": "How fast deals move through stages — avg days per stage, pipeline speed metrics",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_company_summary",
        "description": "Roll up all contacts, deals, and activity for a company",
        "inputSchema": {
            "type": "object",
            "properties": {
                "company": {"type": "string", "description": "Company name"},
            },
            "required": ["company"],
        },
    },
    {
        "name": "crm_recent_contacts",
        "description": "Contacts sorted by most recent interaction, newest first",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max contacts to return (default 10)"},
            },
        },
    },
    {
        "name": "crm_contact_summary",
        "description": "One-line summary of a contact for quick scanning",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "crm_set_reminder",
        "description": "Schedule a follow-up reminder for a contact",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
                "due_date": {"type": "string", "description": "Due date (YYYY-MM-DD)"},
                "note": {"type": "string", "description": "Reminder note"},
            },
            "required": ["identifier", "due_date", "note"],
        },
    },
    {
        "name": "crm_due_reminders",
        "description": "Get all reminders due today or overdue",
        "inputSchema": {
            "type": "object",
            "properties": {
                "include_future_days": {"type": "integer", "description": "Also show reminders due within N days"},
            },
        },
    },
    {
        "name": "crm_set_field",
        "description": "Set a custom field on a contact (creates or updates)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
                "field_name": {"type": "string", "description": "Field name"},
                "field_value": {"type": "string", "description": "Field value"},
            },
            "required": ["identifier", "field_name", "field_value"],
        },
    },
    {
        "name": "crm_get_fields",
        "description": "Get all custom fields for a contact",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "crm_weekly_digest",
        "description": "Weekly digest: what happened this week, what to do next week",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_source_attribution",
        "description": "Which lead sources produce the best results — conversion rates by source",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_suggest_merges",
        "description": "Find duplicate contacts and suggest which to merge, with confidence scores",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_relationship_score",
        "description": "Comprehensive relationship strength score (0-100) using all data sources",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "crm_evolve",
        "description": "Run one self-improvement cycle: analyze CRM, identify bottleneck, propose experiment",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_experiments",
        "description": "List all past CRM experiments with status and results",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_win_patterns",
        "description": "What do closed deals have in common — sources, activity types, touches to close",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_optimal_cadence",
        "description": "Data-driven follow-up timing from actual conversion data",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_dead_pipeline",
        "description": "Find contacts that should be marked as lost — stale + dead velocity",
        "inputSchema": {
            "type": "object",
            "properties": {
                "stale_days": {"type": "integer", "description": "Days without activity to consider dead (default 60)"},
            },
        },
    },
    {
        "name": "crm_contact_360",
        "description": "Complete 360-degree view — profile, scores, velocity, reminders, fields, everything",
        "inputSchema": {
            "type": "object",
            "properties": {
                "identifier": {"type": "string", "description": "Contact email or name"},
            },
            "required": ["identifier"],
        },
    },
    {
        "name": "crm_pipeline_health_score",
        "description": "Single 0-100 score for overall pipeline health with breakdown",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "crm_period_comparison",
        "description": "Compare this period vs previous: new contacts, activities, deals. Trend spotting.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Period length in days (default 30)"},
            },
        },
    },
    {
        "name": "crm_quick_add",
        "description": "Parse natural text into a contact: 'Alice Smith, CTO at Acme, alice@co.com'",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Natural language contact description"},
            },
            "required": ["text"],
        },
    },
    {
        "name": "crm_compare_contacts",
        "description": "Side-by-side comparison of two contacts for dedup decisions",
        "inputSchema": {
            "type": "object",
            "properties": {
                "contact_a": {"type": "string", "description": "First contact (email or name)"},
                "contact_b": {"type": "string", "description": "Second contact (email or name)"},
            },
            "required": ["contact_a", "contact_b"],
        },
    },
]


def handle_tool_call(name, arguments):
    """Execute a tool call and return the result."""
    crm = CRM(DB_PATH)
    try:
        if name == "crm_add_contact":
            result = crm.add_contact(**arguments, warn_duplicate=True)
            if isinstance(result, dict):
                dup = result["duplicate_of"]
                return (f"Warning: contact '{dup['name']}' already exists "
                        f"(id={dup['id']}, email={dup.get('email')}). "
                        f"Use crm_update_contact to update, or re-add with a different name.")
            return f"Added contact (id={result})"

        elif name == "crm_list_contacts":
            contacts = crm.list_contacts(**arguments)
            return json.dumps(contacts, default=str, indent=2)

        elif name == "crm_view_contact":
            c = crm.get_contact(arguments["identifier"])
            if not c:
                return f"Not found: {arguments['identifier']}"
            # Include activities and facts across all entity key variants
            acts = crm.get_activity(arguments["identifier"], limit=10)
            all_facts = {}
            for ek in crm._contact_entity_keys(c):
                all_facts.update(crm.facts_about(ek))
            return json.dumps({"contact": c, "activities": acts, "facts": {k: v["value"] for k, v in all_facts.items()}}, default=str, indent=2)

        elif name == "crm_update_contact":
            email = arguments.pop("email")
            c = crm.update_contact(email, **arguments)
            return json.dumps(c, default=str, indent=2) if c else f"Not found: {email}"

        elif name == "crm_log_activity":
            crm.log_activity(arguments["identifier"], arguments["type"], arguments["summary"])
            return f"Logged {arguments['type']} for {arguments['identifier']}"

        elif name == "crm_search":
            results = crm.search(arguments["term"])
            return json.dumps(results, default=str, indent=2)

        elif name == "crm_observe":
            crm.observe(arguments["entity"], arguments["key"], arguments["value"],
                       source=arguments.get("source", "agent"))
            return f"Recorded: {arguments['entity']} → {arguments['key']} = {arguments['value']}"

        elif name == "crm_facts_about":
            facts = crm.facts_about(arguments["entity"])
            return json.dumps({k: v["value"] for k, v in facts.items()}, indent=2)

        elif name == "crm_pipeline":
            return crm.markdown()

        elif name == "crm_stats":
            return json.dumps(crm.stats(), default=str, indent=2)

        elif name == "crm_score_contact":
            score = crm.score_contact(arguments["identifier"])
            return json.dumps(score, default=str, indent=2) if score else f"Not found: {arguments['identifier']}"

        elif name == "crm_enrich":
            profile = crm.enrich(arguments["identifier"])
            return json.dumps(profile, default=str, indent=2) if profile else f"Not found: {arguments['identifier']}"

        elif name == "crm_next_actions":
            actions = crm.next_actions(limit=arguments.get("limit", 10))
            return json.dumps(actions, default=str, indent=2)

        elif name == "crm_context_for_agent":
            ctx = crm.context_for_agent(arguments.get("identifier"))
            return ctx

        elif name == "crm_search_graph":
            results = crm.search_graph(arguments["query"])
            return json.dumps(results, default=str, indent=2)

        elif name == "crm_query":
            results = crm.query(arguments["query"])
            return json.dumps(results, default=str, indent=2)

        elif name == "crm_ingest":
            src = arguments.get("source", "all")
            days = arguments.get("days", 90)
            if src == "all":
                result = crm.ingest_all(imessage_days=days)
            elif src == "contacts":
                a, f = crm.ingest_macos_contacts()
                result = {"contacts": {"added": a, "facts": f}}
            elif src == "imessage":
                h, f = crm.ingest_macos_imessage(days=days)
                result = {"imessage": {"handles": h, "facts": f}}
            elif src == "calendar":
                e, f = crm.ingest_macos_calendar()
                result = {"calendar": {"events": e, "facts": f}}
            elif src == "mail":
                t, f = crm.ingest_macos_mail(days=days)
                result = {"mail": {"threads": t, "facts": f}}
            else:
                result = {"error": f"Unknown source: {src}"}
            return json.dumps(result, indent=2)

        elif name == "crm_find_intros":
            results = crm.find_intros(arguments["target"])
            return json.dumps(results, default=str, indent=2)

        elif name == "crm_relationship_health":
            results = crm.relationship_health()
            return json.dumps(results, default=str, indent=2)

        elif name == "crm_network_summary":
            result = crm.network_summary()
            return json.dumps(result, default=str, indent=2)

        elif name == "crm_add_deal":
            result = crm.add_deal(arguments["identifier"], arguments["name"],
                                   value=arguments.get("value"), stage=arguments.get("stage", "prospect"),
                                   notes=arguments.get("notes"))
            return f"Added deal '{arguments['name']}'" if result else f"Contact not found: {arguments['identifier']}"

        elif name == "crm_update_deal":
            deal_id = arguments.pop("deal_id")
            result = crm.update_deal(deal_id, **{k: v for k, v in arguments.items() if v is not None})
            return json.dumps(result, default=str, indent=2) if result else f"Deal not found: {deal_id}"

        elif name == "crm_close_deal":
            result = crm.close_deal(arguments["deal_id"], notes=arguments.get("notes"))
            return json.dumps(result, default=str, indent=2) if result else f"Deal not found: {arguments['deal_id']}"

        elif name == "crm_list_deals":
            if arguments.get("identifier"):
                deals = crm.deals_for_contact(arguments["identifier"])
            else:
                deals = crm.list_deals(stage=arguments.get("stage"))
            return json.dumps(deals, default=str, indent=2)

        elif name == "crm_deal_pipeline":
            return json.dumps(crm.deal_pipeline(), default=str, indent=2)

        elif name == "crm_merge_contacts":
            result = crm.merge_contacts(arguments["keep"], arguments["merge"])
            if result:
                return json.dumps(result, default=str, indent=2)
            return "One or both contacts not found"

        elif name == "crm_unified_search":
            results = crm.unified_search(arguments["term"])
            return json.dumps(results, default=str, indent=2)

        elif name == "crm_archive_contact":
            result = crm.archive_contact(arguments["identifier"])
            return f"Archived {arguments['identifier']}" if result else f"Not found: {arguments['identifier']}"

        elif name == "crm_unarchive_contact":
            result = crm.unarchive_contact(arguments["identifier"])
            return f"Unarchived {arguments['identifier']}" if result else f"Not found: {arguments['identifier']}"

        elif name == "crm_dashboard":
            return json.dumps(crm.dashboard(), default=str, indent=2)

        elif name == "crm_delete_contacts":
            count = crm.delete_contacts(arguments["identifiers"])
            return f"Deleted {count} contacts"

        elif name == "crm_touch_plan":
            result = crm.touch_plan(arguments["identifier"])
            return json.dumps(result, default=str, indent=2) if result else f"Not found: {arguments['identifier']}"

        elif name == "crm_detect_churning":
            return json.dumps(crm.detect_churning(), default=str, indent=2)

        elif name == "crm_deal_velocity_report":
            return json.dumps(crm.deal_velocity_report(), default=str, indent=2)

        elif name == "crm_company_summary":
            result = crm.company_summary(arguments["company"])
            return json.dumps(result, default=str, indent=2) if result else f"No contacts at: {arguments['company']}"

        elif name == "crm_recent_contacts":
            return json.dumps(crm.recent_contacts(limit=arguments.get("limit", 10)), default=str, indent=2)

        elif name == "crm_contact_summary":
            result = crm.contact_summary(arguments["identifier"])
            return result if result else f"Not found: {arguments['identifier']}"

        elif name == "crm_set_reminder":
            rid = crm.set_reminder(arguments["identifier"], arguments["due_date"], arguments["note"])
            return f"Reminder set (id={rid})" if rid else f"Not found: {arguments['identifier']}"

        elif name == "crm_due_reminders":
            return json.dumps(crm.due_reminders(include_future_days=arguments.get("include_future_days", 0)), default=str, indent=2)

        elif name == "crm_set_field":
            result = crm.set_field(arguments["identifier"], arguments["field_name"], arguments["field_value"])
            return f"Set {arguments['field_name']}={arguments['field_value']}" if result else f"Not found: {arguments['identifier']}"

        elif name == "crm_get_fields":
            return json.dumps(crm.get_fields(arguments["identifier"]), indent=2)

        elif name == "crm_weekly_digest":
            return json.dumps(crm.weekly_digest(), default=str, indent=2)

        elif name == "crm_source_attribution":
            return json.dumps(crm.source_attribution(), default=str, indent=2)

        elif name == "crm_suggest_merges":
            return json.dumps(crm.suggest_merges(), default=str, indent=2)

        elif name == "crm_relationship_score":
            result = crm.relationship_score(arguments["identifier"])
            return json.dumps(result, default=str, indent=2) if result else f"Not found: {arguments['identifier']}"

        elif name == "crm_evolve":
            return json.dumps(crm.evolve(), default=str, indent=2)

        elif name == "crm_experiments":
            return json.dumps(crm.experiments(), default=str, indent=2)

        elif name == "crm_win_patterns":
            return json.dumps(crm.win_patterns(), default=str, indent=2)

        elif name == "crm_optimal_cadence":
            return json.dumps(crm.optimal_cadence(), default=str, indent=2)

        elif name == "crm_dead_pipeline":
            return json.dumps(crm.dead_pipeline(stale_days=arguments.get("stale_days", 60)), default=str, indent=2)

        elif name == "crm_contact_360":
            result = crm.contact_360(arguments["identifier"])
            return json.dumps(result, default=str, indent=2) if result else f"Not found: {arguments['identifier']}"

        elif name == "crm_pipeline_health_score":
            return json.dumps(crm.pipeline_health_score(), default=str, indent=2)

        elif name == "crm_period_comparison":
            return json.dumps(crm.period_comparison(days=arguments.get("days", 30)), default=str, indent=2)

        elif name == "crm_quick_add":
            cid = crm.quick_add(arguments["text"])
            return f"Added contact (id={cid})" if cid else "Could not parse contact from text"

        elif name == "crm_compare_contacts":
            result = crm.compare_contacts(arguments["contact_a"], arguments["contact_b"])
            return json.dumps(result, default=str, indent=2) if result else "One or both contacts not found"

        else:
            return f"Unknown tool: {name}"
    finally:
        crm.close()


def send(msg):
    """Send a JSON-RPC message to stdout."""
    raw = json.dumps(msg)
    sys.stdout.write(f"Content-Length: {len(raw)}\r\n\r\n{raw}")
    sys.stdout.flush()


def read_message():
    """Read a JSON-RPC message from stdin."""
    # Read headers
    headers = {}
    while True:
        line = sys.stdin.readline()
        if not line or line.strip() == "":
            break
        if ":" in line:
            key, val = line.split(":", 1)
            headers[key.strip()] = val.strip()

    length = int(headers.get("Content-Length", 0))
    if length == 0:
        return None

    body = sys.stdin.read(length)
    return json.loads(body)


def main():
    """MCP stdio server main loop."""
    while True:
        msg = read_message()
        if msg is None:
            break

        method = msg.get("method", "")
        msg_id = msg.get("id")

        if method == "initialize":
            send({
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "agent-crm", "version": "1.0.0"},
                },
            })

        elif method == "notifications/initialized":
            pass  # No response needed

        elif method == "tools/list":
            send({
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"tools": TOOLS},
            })

        elif method == "tools/call":
            params = msg.get("params", {})
            tool_name = params.get("name", "")
            arguments = params.get("arguments", {})

            try:
                result_text = handle_tool_call(tool_name, arguments)
                send({
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "content": [{"type": "text", "text": result_text}],
                        "isError": False,
                    },
                })
            except Exception as e:
                send({
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "content": [{"type": "text", "text": f"Error: {e}"}],
                        "isError": True,
                    },
                })

        elif method == "ping":
            send({"jsonrpc": "2.0", "id": msg_id, "result": {}})

        elif msg_id is not None:
            send({
                "jsonrpc": "2.0",
                "id": msg_id,
                "error": {"code": -32601, "message": f"Method not found: {method}"},
            })


if __name__ == "__main__":
    main()
