# agent-crm

Local-first CRM. One Python file, SQLite, zero dependencies.

Your Mac already knows your relationships. This CRM reads them.

```bash
git clone https://github.com/keshav55/agent-crm.git
cd agent-crm
python crm.py ls
```

No pip install. No account. No API key.

## 30-second demo

```bash
# Pull 90 days of your Mac's relationship data
python crm.py ingest all

# See who you actually talk to
python crm.py network

# Find warm intros to any company
python crm.py intros "Acme"

# Flag relationships that need attention
python crm.py health
```

## What it does

```bash
# Manage contacts
python crm.py add "Alice Smith" -e alice@acme.com -c Acme -s prospect
python crm.py view alice@acme.com
python crm.py update alice@acme.com -s contacted
python crm.py log alice@acme.com call "Discussed pricing"
python crm.py search Acme

# Pipeline
python crm.py pipeline
python crm.py ls

# Relationship intelligence
python crm.py network          # dashboard: contacts, entities, top relationships, pipeline value
python crm.py health           # flag one-sided convos, fading ties, people ghosting you
python crm.py intros "Acme"    # warm paths through your iMessage network to any target

# Agent-ready output
python crm.py markdown         # full pipeline dump for agent context
python crm.py json             # programmatic dump
```

## Local data connectors

Reads macOS databases directly (requires Full Disk Access in System Settings > Privacy):

| Command | Source | What it pulls |
|---------|--------|--------------|
| `ingest contacts` | macOS Contacts | Names, emails, phones, companies. Phone numbers map to iMessage handles. |
| `ingest imessage` | Messages/chat.db | Message counts per contact, sent/received ratio, intensity (high/medium/low) |
| `ingest calendar` | macOS Calendar | Meeting history, attendees, cross-linked to contacts |
| `ingest mail` | Apple Mail | Email thread counts, sent vs received |
| `ingest all` | Everything above | One command, all sources |

Phone numbers from iMessage auto-resolve to real names from your Contacts app.

## Knowledge graph

Not rows in a table. The `facts` table stores any fact about any entity from any source:

```python
from crm import CRM
crm = CRM("crm.db")

# Record facts from anywhere
crm.observe("contact:alice", "role", "CEO", source="linkedin")
crm.observe("company:acme", "funding", "$10M Series A", source="crunchbase")

# Query them
crm.facts_about("contact:alice")
# {'role': 'CEO', 'imessage_total': '142', 'message_intensity': 'high'}

# Search across everything
crm.graph_search("company", "acme")
```

Facts auto-deduplicate. Same fact from the same source updates its timestamp instead of creating duplicates.

## CRM integrations

Sync with your existing CRM. Skills in `skills/` provide bidirectional sync:

### HubSpot

```bash
# Symlink the skill into Claude Code
ln -s $(pwd)/skills/hubspot ~/.claude/skills/agent-crm-hubspot

# Then in Claude Code, ask it to sync
# "sync my HubSpot contacts into the CRM"
```

The HubSpot skill maps contacts, deals, and activities between HubSpot and the local CRM. Bidirectional: changes sync both ways. See `skills/hubspot/SKILL.md` for field mappings and API reference.

### Salesforce

```bash
ln -s $(pwd)/skills/salesforce ~/.claude/skills/agent-crm-salesforce
```

Maps Salesforce contacts, opportunities, accounts, and tasks. Stage mapping (Prospecting -> prospect, Negotiation -> proposal_drafted, Closed Won -> active_customer, etc.) is built in. See `skills/salesforce/SKILL.md` for details.

### Other CRMs

The pattern is the same for any CRM with an API: pull contacts and deals into the local graph, observe facts, push changes back. The `skills/` folder is where new integrations go.

## MCP server

21 tools for any AI agent. Add to your MCP client config:

```json
{
  "mcpServers": {
    "crm": {
      "command": "python3",
      "args": ["/absolute/path/to/mcp_server.py"]
    }
  }
}
```

Works with Claude Code, Cursor, Codex, or any MCP client.

**Available tools:**

| Tool | What it does |
|------|-------------|
| `crm_add_contact` | Add a contact |
| `crm_list_contacts` | List/filter contacts |
| `crm_view_contact` | Full contact details + facts + activity |
| `crm_update_contact` | Update fields |
| `crm_log_activity` | Log calls, emails, meetings |
| `crm_search` | Search contacts |
| `crm_unified_search` | Search across contacts, facts, and activity |
| `crm_observe` | Record a fact in the knowledge graph |
| `crm_facts_about` | Get all facts about an entity |
| `crm_search_graph` | Search the knowledge graph |
| `crm_pipeline` | Pipeline summary |
| `crm_stats` | CRM statistics |
| `crm_score_contact` | Engagement score (0-100) |
| `crm_enrich` | Full enriched profile |
| `crm_next_actions` | Recommended next actions |
| `crm_context_for_agent` | Context string for AI agents |
| `crm_query` | Natural language query |
| `crm_ingest` | Pull local macOS data |
| `crm_find_intros` | Warm intro paths |
| `crm_relationship_health` | Relationship health analysis |
| `crm_network_summary` | Network dashboard |

## As a library

```python
from crm import CRM
crm = CRM("crm.db")

crm.add_contact("Alice", email="alice@acme.com", company="Acme")
crm.log_activity("alice@acme.com", "call", "Discussed pricing")
crm.score_contact("alice@acme.com")
crm.find_intros("Acme")
crm.relationship_health()
crm.network_summary()
crm.unified_search("Acme")
```

All methods return dicts. `get_contact()` accepts email or partial name match.

## Self-improvement

The `skills/evolve/` skill runs an improvement loop on your CRM data:

```bash
ln -s $(pwd)/skills/evolve ~/.claude/skills/agent-crm-evolve

# Then in Claude Code:
# "run /evolve"
```

It reads your pipeline, analyzes what's working, proposes one experiment, tracks results. Each cycle makes the CRM a little smarter. Inspired by Karpathy's autoresearch.

## Benchmark

```bash
python benchmark.py
# 184/184 tests passed
```

## Your data stays local

SQLite on your machine. Nothing phones home. No cloud. No account. The `crm.db` file is gitignored. Your data never touches a remote server.
