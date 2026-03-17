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
        "description": "Auto-populate CRM from local data sources (macOS Contacts, iMessage, Calendar)",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source": {"type": "string", "enum": ["all", "contacts", "imessage", "calendar"], "default": "all"},
                "days": {"type": "integer", "description": "Days of iMessage history", "default": 90},
            },
        },
    },
]


def handle_tool_call(name, arguments):
    """Execute a tool call and return the result."""
    crm = CRM(DB_PATH)
    try:
        if name == "crm_add_contact":
            cid = crm.add_contact(**arguments)
            return f"Added contact (id={cid})"

        elif name == "crm_list_contacts":
            contacts = crm.list_contacts(**arguments)
            return json.dumps(contacts, default=str, indent=2)

        elif name == "crm_view_contact":
            c = crm.get_contact(arguments["identifier"])
            if not c:
                return f"Not found: {arguments['identifier']}"
            # Include activities and facts
            acts = crm.get_activity(arguments["identifier"], limit=10)
            facts = crm.facts_about(f"contact:{c['name'].lower()}")
            return json.dumps({"contact": c, "activities": acts, "facts": {k: v["value"] for k, v in facts.items()}}, default=str, indent=2)

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
            return json.dumps(result, indent=2)

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
