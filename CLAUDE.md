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
python crm.py markdown    # dump for agent context
python crm.py json        # dump for programmatic use
```

## As a library

```python
from crm import CRM
crm = CRM("crm.db")
```

All methods return dicts. `get_contact()` accepts email or partial name match.

## Database

Default: `crm.db` in current directory. Override with `CRM_DB` env var or `--db` flag.

Tables: contacts, activity, deals. Schema auto-creates on first run.
