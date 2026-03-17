---
name: salesforce
description: Salesforce CRM sync via REST API. Pull contacts, opportunities, accounts, and activities into local agent-crm. Push local changes back. Use when user asks about Salesforce data, syncing CRM, importing deals, or pushing updates to Salesforce.
version: 1.0.0
tags:
  - salesforce
  - crm
  - sync
---

# Salesforce Sync Agent

> Bidirectional sync between Salesforce and local agent-crm (crm.py + SQLite).

## Auth

Salesforce uses OAuth 2.0. You need two values:

| Variable | Description |
|----------|-------------|
| `SF_ACCESS_TOKEN` | OAuth 2.0 access token (Bearer token) |
| `SF_INSTANCE_URL` | Your Salesforce instance, e.g. `https://yourorg.my.salesforce.com` |

```bash
# Set these before any operation
export SF_ACCESS_TOKEN="00D..."
export SF_INSTANCE_URL="https://yourorg.my.salesforce.com"
```

To obtain a token via Connected App (client_credentials or web server flow):

```bash
curl -s -X POST "https://login.salesforce.com/services/oauth2/token" \
  -d "grant_type=password" \
  -d "client_id=YOUR_CONSUMER_KEY" \
  -d "client_secret=YOUR_CONSUMER_SECRET" \
  -d "username=YOUR_USERNAME" \
  -d "password=YOUR_PASSWORD_PLUS_SECURITY_TOKEN"
```

Response gives `access_token` and `instance_url`.

---

## API Endpoints

Base: `$SF_INSTANCE_URL/services/data/v59.0`

All requests require: `-H "Authorization: Bearer $SF_ACCESS_TOKEN"`

### Query Contacts

```bash
curl -s "$SF_INSTANCE_URL/services/data/v59.0/query/" \
  --data-urlencode "q=SELECT Id, Name, Email, Title, Account.Name, LeadSource, Description FROM Contact ORDER BY LastModifiedDate DESC LIMIT 200" \
  -H "Authorization: Bearer $SF_ACCESS_TOKEN"
```

### Query Opportunities (with stages)

```bash
curl -s "$SF_INSTANCE_URL/services/data/v59.0/query/" \
  --data-urlencode "q=SELECT Id, Name, Amount, StageName, CloseDate, Account.Name, ContactId, Description FROM Opportunity ORDER BY LastModifiedDate DESC LIMIT 200" \
  -H "Authorization: Bearer $SF_ACCESS_TOKEN"
```

### Query Accounts

```bash
curl -s "$SF_INSTANCE_URL/services/data/v59.0/query/" \
  --data-urlencode "q=SELECT Id, Name, Industry, Website, Phone, BillingCity, BillingState FROM Account ORDER BY LastModifiedDate DESC LIMIT 200" \
  -H "Authorization: Bearer $SF_ACCESS_TOKEN"
```

### Query Tasks (Activities)

```bash
curl -s "$SF_INSTANCE_URL/services/data/v59.0/query/" \
  --data-urlencode "q=SELECT Id, Subject, Description, Status, ActivityDate, Who.Name, Who.Email, Type FROM Task ORDER BY LastModifiedDate DESC LIMIT 200" \
  -H "Authorization: Bearer $SF_ACCESS_TOKEN"
```

### Query Events (Meetings)

```bash
curl -s "$SF_INSTANCE_URL/services/data/v59.0/query/" \
  --data-urlencode "q=SELECT Id, Subject, Description, StartDateTime, EndDateTime, Who.Name, Location FROM Event ORDER BY LastModifiedDate DESC LIMIT 200" \
  -H "Authorization: Bearer $SF_ACCESS_TOKEN"
```

### Create Contact

```bash
curl -s -X POST "$SF_INSTANCE_URL/services/data/v59.0/sobjects/Contact/" \
  -H "Authorization: Bearer $SF_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "FirstName": "Jane",
    "LastName": "Doe",
    "Email": "jane@example.com",
    "Title": "VP Engineering",
    "AccountId": "001XXXXXXXXXXXXXXX"
  }'
```

### Update Contact

```bash
curl -s -X PATCH "$SF_INSTANCE_URL/services/data/v59.0/sobjects/Contact/003XXXXXXXXXXXXXXX" \
  -H "Authorization: Bearer $SF_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "Email": "jane.new@example.com",
    "Title": "CTO"
  }'
```

### Create Opportunity

```bash
curl -s -X POST "$SF_INSTANCE_URL/services/data/v59.0/sobjects/Opportunity/" \
  -H "Authorization: Bearer $SF_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "Name": "Acme Corp - Enterprise Deal",
    "StageName": "Prospecting",
    "CloseDate": "2026-06-30",
    "Amount": 50000,
    "AccountId": "001XXXXXXXXXXXXXXX"
  }'
```

### Update Opportunity

```bash
curl -s -X PATCH "$SF_INSTANCE_URL/services/data/v59.0/sobjects/Opportunity/006XXXXXXXXXXXXXXX" \
  -H "Authorization: Bearer $SF_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "StageName": "Negotiation/Review",
    "Amount": 75000
  }'
```

---

## Field Mapping

### Contacts: Salesforce -> crm.py

| Salesforce Field | crm.py Field | Notes |
|------------------|-------------|-------|
| `Name` | `name` | FirstName + LastName combined |
| `Email` | `email` | Primary key for matching |
| `Title` | `title` | |
| `Account.Name` | `company` | From related Account |
| `LeadSource` | `source` | |
| `Description` | `notes` | |
| `Id` | stored via `observe()` | As `salesforce_id` fact |

### Opportunities: Salesforce -> crm.py deals

| Salesforce Field | crm.py Field | Notes |
|------------------|-------------|-------|
| `Name` | deal `name` | |
| `Amount` | deal `value` | |
| `StageName` | deal `stage` | Mapped via stage table below |
| `Description` | deal `notes` | |
| `Id` | stored via `observe()` | As `salesforce_opportunity_id` fact |

### Stage Mapping: Salesforce -> crm.py

| Salesforce StageName | crm.py status/stage |
|---------------------|---------------------|
| `Prospecting` | `prospect` |
| `Qualification` | `prospect` |
| `Needs Analysis` | `lead` |
| `Value Proposition` | `lead` |
| `Id. Decision Makers` | `lead` |
| `Perception Analysis` | `qualified` |
| `Proposal/Price Quote` | `qualified` |
| `Negotiation/Review` | `negotiation` |
| `Closed Won` | `customer` |
| `Closed Lost` | `lost` |

Reverse mapping when pushing to Salesforce:

| crm.py status | Salesforce StageName |
|---------------|---------------------|
| `prospect` | `Prospecting` |
| `lead` | `Needs Analysis` |
| `qualified` | `Proposal/Price Quote` |
| `negotiation` | `Negotiation/Review` |
| `customer` | `Closed Won` |
| `lost` | `Closed Lost` |

### Activity Type Mapping

| Salesforce Type | crm.py activity type |
|----------------|---------------------|
| `Task` | `task` |
| `Event` | `meeting` |
| `Call` (Task.Type) | `call` |
| `Email` (Task.Type) | `email` |

---

## Sync to Local CRM

### Full import workflow

```python
import json, subprocess

from crm import CRM
crm = CRM("crm.db")

SF_INSTANCE_URL = "https://yourorg.my.salesforce.com"
SF_ACCESS_TOKEN = "00D..."

def sf_query(soql):
    result = subprocess.run(
        ["curl", "-s", f"{SF_INSTANCE_URL}/services/data/v59.0/query/",
         "--data-urlencode", f"q={soql}",
         "-H", f"Authorization: Bearer {SF_ACCESS_TOKEN}"],
        capture_output=True, text=True
    )
    return json.loads(result.stdout).get("records", [])

# --- Import Contacts ---
for c in sf_query("SELECT Id, Name, Email, Title, Account.Name, LeadSource, Description FROM Contact"):
    name = c["Name"]
    email = c.get("Email")
    account_name = (c.get("Account") or {}).get("Name")
    title = c.get("Title")
    source = c.get("LeadSource")
    notes = c.get("Description")

    existing = crm.get_contact(email) if email else None
    if existing:
        crm.update_contact(email, company=account_name, title=title, source=source, notes=notes)
    else:
        crm.add_contact(name, email=email, company=account_name, title=title, source=source, notes=notes)

    # Store Salesforce ID as a fact for bidirectional sync
    entity = f"contact:{name.lower()}"
    crm.observe(entity, "salesforce_id", c["Id"], source="salesforce")

# --- Import Opportunities as Deals ---
STAGE_MAP = {
    "Prospecting": "prospect", "Qualification": "prospect",
    "Needs Analysis": "lead", "Value Proposition": "lead", "Id. Decision Makers": "lead",
    "Perception Analysis": "qualified", "Proposal/Price Quote": "qualified",
    "Negotiation/Review": "negotiation",
    "Closed Won": "customer", "Closed Lost": "lost",
}

for opp in sf_query("SELECT Id, Name, Amount, StageName, ContactId, Account.Name, Description FROM Opportunity"):
    stage = STAGE_MAP.get(opp.get("StageName"), "prospect")
    account_name = (opp.get("Account") or {}).get("Name")
    amount = opp.get("Amount")

    # Find the associated contact
    if opp.get("ContactId"):
        contact_rec = sf_query(f"SELECT Name, Email FROM Contact WHERE Id = '{opp['ContactId']}'")
        if contact_rec:
            identifier = contact_rec[0].get("Email") or contact_rec[0]["Name"]
            crm.add_deal(identifier, opp["Name"], value=amount, stage=stage, notes=opp.get("Description"))
            crm.observe(f"deal:{opp['Name'].lower()}", "salesforce_opportunity_id", opp["Id"], source="salesforce")

# --- Import Activities ---
ACTIVITY_TYPE_MAP = {"Call": "call", "Email": "email"}

for task in sf_query("SELECT Id, Subject, Description, Who.Name, Who.Email, Type FROM Task"):
    who = task.get("Who") or {}
    identifier = who.get("Email") or who.get("Name")
    if not identifier:
        continue
    activity_type = ACTIVITY_TYPE_MAP.get(task.get("Type"), "task")
    summary = task.get("Subject") or task.get("Description") or "Salesforce task"
    crm.log_activity(identifier, activity_type, summary)

for event in sf_query("SELECT Id, Subject, Description, Who.Name, Who.Email FROM Event"):
    who = event.get("Who") or {}
    identifier = who.get("Email") or who.get("Name")
    if not identifier:
        continue
    crm.log_activity(identifier, "meeting", event.get("Subject") or "Salesforce event")
```

---

## Push to Salesforce

### Sync local changes back

```python
import json, subprocess

from crm import CRM
crm = CRM("crm.db")

SF_INSTANCE_URL = "https://yourorg.my.salesforce.com"
SF_ACCESS_TOKEN = "00D..."

REVERSE_STAGE_MAP = {
    "prospect": "Prospecting",
    "lead": "Needs Analysis",
    "qualified": "Proposal/Price Quote",
    "negotiation": "Negotiation/Review",
    "customer": "Closed Won",
    "lost": "Closed Lost",
}

def sf_get_fact(entity, key):
    """Retrieve a stored Salesforce ID from the facts table."""
    facts = crm.recall(entity)
    for f in facts:
        if f["key"] == key:
            return f["value"]
    return None

def sf_patch(sobject, sf_id, data):
    subprocess.run(
        ["curl", "-s", "-X", "PATCH",
         f"{SF_INSTANCE_URL}/services/data/v59.0/sobjects/{sobject}/{sf_id}",
         "-H", f"Authorization: Bearer {SF_ACCESS_TOKEN}",
         "-H", "Content-Type: application/json",
         "-d", json.dumps(data)],
        capture_output=True, text=True
    )

def sf_create(sobject, data):
    result = subprocess.run(
        ["curl", "-s", "-X", "POST",
         f"{SF_INSTANCE_URL}/services/data/v59.0/sobjects/{sobject}/",
         "-H", f"Authorization: Bearer {SF_ACCESS_TOKEN}",
         "-H", "Content-Type: application/json",
         "-d", json.dumps(data)],
        capture_output=True, text=True
    )
    return json.loads(result.stdout)

# --- Push Contacts ---
for contact in crm.list_contacts():
    entity = f"contact:{contact['name'].lower()}"
    sf_id = sf_get_fact(entity, "salesforce_id")

    names = contact["name"].split(" ", 1)
    first_name = names[0]
    last_name = names[1] if len(names) > 1 else names[0]

    payload = {
        "FirstName": first_name,
        "LastName": last_name,
        "Email": contact.get("email"),
        "Title": contact.get("title"),
    }
    # Remove None values
    payload = {k: v for k, v in payload.items() if v is not None}

    if sf_id:
        sf_patch("Contact", sf_id, payload)
    else:
        result = sf_create("Contact", payload)
        if result.get("id"):
            crm.observe(entity, "salesforce_id", result["id"], source="salesforce")

# --- Push Deals as Opportunities ---
for deal in crm.list_deals():
    entity = f"deal:{deal['name'].lower()}"
    sf_id = sf_get_fact(entity, "salesforce_opportunity_id")

    sf_stage = REVERSE_STAGE_MAP.get(deal.get("stage"), "Prospecting")

    payload = {
        "Name": deal["name"],
        "StageName": sf_stage,
        "Amount": deal.get("value"),
        "CloseDate": "2026-12-31",  # Default; adjust as needed
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    if sf_id:
        sf_patch("Opportunity", sf_id, payload)
    else:
        result = sf_create("Opportunity", payload)
        if result.get("id"):
            crm.observe(entity, "salesforce_opportunity_id", result["id"], source="salesforce")
```

---

## Workflows

### "Import all Salesforce data"
1. Verify auth: check `SF_ACCESS_TOKEN` and `SF_INSTANCE_URL` are set
2. Query contacts, opportunities, tasks, events via SOQL
3. Map fields and stages per tables above
4. Upsert into local CRM via `add_contact` / `update_contact` / `add_deal` / `log_activity`
5. Store all Salesforce IDs as facts via `observe()` for future sync

### "Push local updates to Salesforce"
1. Iterate `crm.list_contacts()` and `crm.list_deals()`
2. Look up Salesforce IDs from facts (`salesforce_id`, `salesforce_opportunity_id`)
3. If ID exists: PATCH the Salesforce record
4. If no ID: POST to create, then store returned ID via `observe()`

### "Sync a single contact"
1. `crm.get_contact(email)` to get local data
2. `crm.recall(f"contact:{name.lower()}")` to get Salesforce ID
3. If Salesforce ID exists: PATCH to update Salesforce
4. If not: query Salesforce by email, then create or link

### "Reconcile conflicts"
1. Pull Salesforce record by ID
2. Compare `LastModifiedDate` with local `updated_at`
3. Most recent write wins (or prompt user to choose)

---

## Error Handling

| Error | Meaning | Solution |
|-------|---------|----------|
| `401 Unauthorized` | Token expired or invalid | Re-authenticate via OAuth flow |
| `400 INVALID_SESSION_ID` | Session expired | Refresh token or re-auth |
| `403 REQUEST_LIMIT_EXCEEDED` | API limit hit | Wait or check API usage in Salesforce Setup |
| `404 NOT_FOUND` | Record ID invalid | Re-query to get current ID |
| `DUPLICATES_DETECTED` | Duplicate rules triggered | Check existing records before creating |
| `REQUIRED_FIELD_MISSING` | Missing required field | Ensure LastName (Contact) or CloseDate+StageName (Opportunity) are set |
