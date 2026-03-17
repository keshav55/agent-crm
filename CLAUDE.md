# agent-crm

Local-first CRM. One Python file, SQLite backend, zero dependencies.

## Quick reference

```bash
python crm.py add "Name" -e email -c Company -s status
python crm.py ls
python crm.py view <email_or_name>
python crm.py update <email_or_name> -s <status> -d <deal>
python crm.py log <email_or_name> <type> "summary"
python crm.py pipeline
python crm.py network             # relationship dashboard
python crm.py health              # relationship health scores
python crm.py intros <target>     # warm intro finder
python crm.py ingest all          # pull macOS data (Contacts, iMessage, Calendar, Mail)
python crm.py markdown            # dump for agent context
python crm.py json                # dump for programmatic use
```

## As a library

```python
from crm import CRM
crm = CRM("crm.db")
```

All methods return dicts. `get_contact()` accepts email or partial name match.

## Database

Default: `crm.db` in current directory. Override with `CRM_DB` env var or `--db` flag.

Tables: contacts, activity, deals, facts. Schema auto-creates on first run.

## Skills

Integration skills live in `skills/`. Each has a SKILL.md that Claude Code follows.

| Skill | What |
|-------|------|
| `skills/hubspot/` | Bidirectional sync with HubSpot CRM |
| `skills/salesforce/` | Bidirectional sync with Salesforce |
| `skills/evolve/` | Self-improvement loop. Analyzes pipeline, proposes experiments, tracks what works |

To use a skill, symlink it into your Claude Code skills directory:
```bash
ln -s $(pwd)/skills/hubspot ~/.claude/skills/agent-crm-hubspot
ln -s $(pwd)/skills/salesforce ~/.claude/skills/agent-crm-salesforce
ln -s $(pwd)/skills/evolve ~/.claude/skills/agent-crm-evolve
```
