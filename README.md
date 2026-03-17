# crm

Local-first CRM. One Python file, SQLite backend, zero dependencies.

Your Mac already knows your relationships. This CRM reads them.

## Install

```bash
git clone https://github.com/keshav55/crm.git
cd crm
python crm.py ls
```

No pip install. No account. No API key.

## What it does

```bash
# Add contacts
python crm.py add "Alice Smith" -e alice@acme.com -c Acme -s prospect

# Pull your Mac's data into the CRM
python crm.py ingest all          # Contacts + iMessage + Calendar + Mail

# See your network
python crm.py network             # Full relationship dashboard
python crm.py health              # Flag one-sided convos, fading ties
python crm.py intros "Acme"       # Find warm paths to any target

# Pipeline
python crm.py pipeline
python crm.py ls
python crm.py view alice@acme.com

# Agent-ready output
python crm.py markdown            # Dump for agent context
python crm.py json                # Dump for programmatic use
```

## Local data connectors

The CRM reads local macOS databases (requires Full Disk Access):

| Command | Source | What it pulls |
|---------|--------|--------------|
| `ingest contacts` | macOS AddressBook | Names, emails, phones, companies |
| `ingest imessage` | Messages/chat.db | Message counts, intensity, who you actually talk to |
| `ingest calendar` | macOS Calendar | Meeting history, attendees |
| `ingest mail` | Apple Mail | Email thread counts |

Phone numbers from iMessage auto-resolve to real names from your Contacts. The CRM builds a knowledge graph from all sources.

## Knowledge graph

Not just rows in a table. The `facts` table stores any fact about any entity from any source:

```python
from crm import CRM
crm = CRM("crm.db")

crm.observe("contact:alice", "role", "CEO", source="linkedin")
crm.observe("company:acme", "funding", "$10M Series A", source="crunchbase")

crm.facts_about("contact:alice")
# {'role': 'CEO', 'imessage_total': '142', 'message_intensity': 'high'}

crm.graph_search("company", "acme")
# Find everything connected to Acme
```

## MCP server

Expose the CRM as 17 tools for any AI agent:

```bash
python mcp_server.py
```

Works with Claude Code, Cursor, Windsurf, or any MCP client.

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
```

All methods return dicts. `get_contact()` accepts email or partial name match.

## Benchmark

```bash
python benchmark.py
# 184/184 tests passed
```

## Your data stays local

SQLite database on your machine. Nothing phones home. No cloud. No account.

The `crm.db` file is gitignored. Your data never touches a remote server.
