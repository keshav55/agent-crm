# crm

## Core
| File | What |
|------|------|
| `crm.py` | CRM library + CLI. Contacts, activity, deals, knowledge graph |
| `benchmark.py` | 320 tests. Correctness, edge cases, performance |
| `mcp_server.py` | 35 tools for any AI agent via MCP |

## Schema
| Table | Purpose |
|-------|---------|
| `contacts` | People: name, email, company, status, deal_size |
| `activity` | Timestamped interaction log per contact |
| `deals` | Opportunities tracked separately from contacts |
| `facts` | Knowledge graph: entity/key/value/source/observed_at |

## Facts table (the graph)
Five columns. Any entity, any fact, any source.
```
entity        | key            | value              | source          | observed_at
contact:alice | status         | negotiating        | manual          | 2026-03-12
contact:alice | introduced_by  | contact:bob        | manual          | 2026-03-05
company:acme  | competes_with  | company:betacorp   | research        | 2026-03-10
```

## Local data connectors
| Command | Source | What it pulls |
|---------|--------|--------------|
| `ingest contacts` | macOS AddressBook | Names, emails, phones, companies |
| `ingest imessage` | Messages/chat.db | Message counts, intensity, reciprocity |
| `ingest calendar` | macOS Calendar | Meeting history, attendees |
| `ingest mail` | Apple Mail | Email thread counts, sent/received |
