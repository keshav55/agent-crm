# agent-crm

Agent first CRM that self improves. One Python file, SQLite, zero dependencies.

This repo is the reusable open-source CRM engine. Product/company-specific CRM instances should live in their own repos or databases.

It can process data from your Mac and other external CRM APIs.

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

## Agent API

One import. Every method returns dicts. Any agent framework can use it.

```python
from crm import CRM
crm = CRM("crm.db")

# Contacts
crm.add_contact("Alice", email="alice@acme.com", company="Acme")
crm.get_contact("alice@acme.com")       # by email or partial name
crm.update_contact("Alice", status="contacted")
crm.list_contacts(status="prospect")
crm.delete_contact("alice@acme.com")

# Activity
crm.log_activity("alice@acme.com", "call", "Discussed pricing")
crm.get_activity("alice@acme.com")

# Deals
crm.add_deal("alice@acme.com", "Enterprise License", value="$50K")

# Knowledge graph
crm.observe("contact:alice", "role", "CEO", source="linkedin")
crm.facts_about("contact:alice")
crm.graph_search("company", "acme")

# Intelligence
crm.score_contact("alice@acme.com")     # 0-100 engagement score
crm.find_intros("Acme")                 # warm paths through your network
crm.relationship_health()               # flag fading/one-sided relationships
crm.network_summary()                   # full network dashboard
crm.unified_search("Acme")              # search contacts + facts + activity
crm.context_for_agent("alice@acme.com") # context string for any AI agent
crm.interaction_prompt("alice@acme.com") # outreach prompt with warm intros

# Pipeline
crm.pipeline()                          # grouped by status
crm.stats()                             # counts and metrics
crm.next_actions()                      # prioritized recommendations

# Local data
crm.ingest_all()                        # pull Contacts, iMessage, Calendar, Mail
```

No server. No protocol. `from crm import CRM` and go.

## MCP server (optional)

If your tool speaks MCP, there's a server with 21 tools:

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
