# HubSpot CRM Sync

Bidirectional sync between HubSpot CRM and local agent-crm. Pulls contacts, deals, and activities from HubSpot REST API v3 into the local SQLite CRM, and pushes local changes back.

## When to Use

Triggers on: hubspot, hubspot sync, import from hubspot, push to hubspot, crm sync, hubspot contacts, hubspot deals, hubspot pipeline

## Auth

HubSpot API v3 supports three auth methods. Pick one.

### Bearer Token (Private App)
```bash
HS_TOKEN="your-private-app-token"
# Header: -H "Authorization: Bearer $HS_TOKEN"
```
Create at: HubSpot > Settings > Integrations > Private Apps

### API Key (legacy, deprecated)
```bash
HS_API_KEY="your-api-key"
# Append: ?hapikey=$HS_API_KEY
```

### OAuth 2.0
For production integrations. Requires client_id, client_secret, redirect_uri, and refresh token flow. Use Bearer token for local/agent use.

**Test auth:**
```bash
curl -s "https://api.hubapi.com/crm/v3/objects/contacts?limit=1" \
  -H "Authorization: Bearer $HS_TOKEN"
```

---

## HubSpot Endpoints

Base: `https://api.hubapi.com`

All requests require: `-H "Authorization: Bearer $HS_TOKEN"`

### List Contacts
```bash
curl -s "https://api.hubapi.com/crm/v3/objects/contacts?limit=100&properties=firstname,lastname,email,company,jobtitle,lifecyclestage,hs_lead_status,notes_last_contacted" \
  -H "Authorization: Bearer $HS_TOKEN"
```

Paginate with `&after=<paging.next.after>` from the response.

### Search Contacts
```bash
curl -s -X POST "https://api.hubapi.com/crm/v3/objects/contacts/search" \
  -H "Authorization: Bearer $HS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "filterGroups": [{
      "filters": [{
        "propertyName": "company",
        "operator": "CONTAINS_TOKEN",
        "value": "Acme"
      }]
    }],
    "properties": ["firstname", "lastname", "email", "company", "jobtitle", "lifecyclestage"],
    "limit": 100
  }'
```

### List Deals
```bash
curl -s "https://api.hubapi.com/crm/v3/objects/deals?limit=100&properties=dealname,amount,dealstage,pipeline,closedate,hs_lastmodifieddate&associations=contacts" \
  -H "Authorization: Bearer $HS_TOKEN"
```

### List Deal Pipeline Stages
```bash
curl -s "https://api.hubapi.com/crm/v3/pipelines/deals" \
  -H "Authorization: Bearer $HS_TOKEN"
```

Returns all pipelines with their stages. Use `stages[].label` for human-readable names and `stages[].id` for API values.

### List Activities / Engagements
```bash
# Notes
curl -s "https://api.hubapi.com/crm/v3/objects/notes?limit=100&properties=hs_note_body,hs_timestamp&associations=contacts" \
  -H "Authorization: Bearer $HS_TOKEN"

# Emails
curl -s "https://api.hubapi.com/crm/v3/objects/emails?limit=100&properties=hs_email_subject,hs_email_text,hs_timestamp,hs_email_direction&associations=contacts" \
  -H "Authorization: Bearer $HS_TOKEN"

# Calls
curl -s "https://api.hubapi.com/crm/v3/objects/calls?limit=100&properties=hs_call_body,hs_call_title,hs_timestamp,hs_call_duration&associations=contacts" \
  -H "Authorization: Bearer $HS_TOKEN"

# Meetings
curl -s "https://api.hubapi.com/crm/v3/objects/meetings?limit=100&properties=hs_meeting_title,hs_meeting_body,hs_timestamp&associations=contacts" \
  -H "Authorization: Bearer $HS_TOKEN"

# Tasks
curl -s "https://api.hubapi.com/crm/v3/objects/tasks?limit=100&properties=hs_task_subject,hs_task_body,hs_timestamp,hs_task_status&associations=contacts" \
  -H "Authorization: Bearer $HS_TOKEN"
```

### Create / Update Contact
```bash
# Create
curl -s -X POST "https://api.hubapi.com/crm/v3/objects/contacts" \
  -H "Authorization: Bearer $HS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {
      "firstname": "Jane",
      "lastname": "Doe",
      "email": "jane@acme.com",
      "company": "Acme",
      "jobtitle": "VP Sales",
      "lifecyclestage": "lead"
    }
  }'

# Update (by HubSpot ID)
curl -s -X PATCH "https://api.hubapi.com/crm/v3/objects/contacts/{contactId}" \
  -H "Authorization: Bearer $HS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {
      "lifecyclestage": "customer",
      "company": "Acme Corp"
    }
  }'
```

### Create / Update Deal
```bash
# Create
curl -s -X POST "https://api.hubapi.com/crm/v3/objects/deals" \
  -H "Authorization: Bearer $HS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {
      "dealname": "Acme Enterprise",
      "amount": "50000",
      "dealstage": "qualifiedtobuy",
      "pipeline": "default"
    }
  }'

# Associate deal with contact
curl -s -X PUT "https://api.hubapi.com/crm/v3/objects/deals/{dealId}/associations/contacts/{contactId}/deal_to_contact" \
  -H "Authorization: Bearer $HS_TOKEN"

# Update deal stage
curl -s -X PATCH "https://api.hubapi.com/crm/v3/objects/deals/{dealId}" \
  -H "Authorization: Bearer $HS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "properties": {
      "dealstage": "closedwon",
      "amount": "55000"
    }
  }'
```

---

## Field Mapping: HubSpot to crm.py

| HubSpot Field | crm.py Field | Notes |
|---|---|---|
| `firstname` + `lastname` | `name` | Concatenate with space |
| `email` | `email` | Direct map |
| `company` | `company` | Direct map |
| `jobtitle` | `title` | Direct map |
| `lifecyclestage` | `status` | Map values (see below) |
| `amount` (on deal) | `deal_size` | Format as currency string |
| `hs_lead_status` | `status` | Alternative status source |
| `notes_last_contacted` | `last_contacted` | Date format |
| `dealname` | deal `name` | For `add_deal()` |
| `dealstage` | deal `stage` | Map values (see below) |
| Contact `id` | fact `hubspot_id` | Store via `observe()` |

### Status Mapping

| HubSpot `lifecyclestage` | crm.py `status` |
|---|---|
| `subscriber` | `prospect` |
| `lead` | `prospect` |
| `marketingqualifiedlead` | `prospect` |
| `salesqualifiedlead` | `lead` |
| `opportunity` | `negotiation` |
| `customer` | `active_customer` |
| `evangelist` | `active_customer` |
| `other` | `prospect` |

### Deal Stage Mapping

| HubSpot `dealstage` | crm.py deal `stage` |
|---|---|
| `appointmentscheduled` | `prospect` |
| `qualifiedtobuy` | `lead` |
| `presentationscheduled` | `negotiation` |
| `decisionmakerboughtin` | `negotiation` |
| `contractsent` | `negotiation` |
| `closedwon` | `closed_won` |
| `closedlost` | `closed_lost` |

Adapt these if the HubSpot account uses custom pipeline stages. Fetch actual stages from the pipelines endpoint first.

---

## Sync to Local CRM

Full pull workflow. Run this to import HubSpot data into agent-crm.

```python
import json, subprocess

HS_TOKEN = "your-token"  # or os.environ["HUBSPOT_TOKEN"]

def hs_get(endpoint, params=""):
    """Fetch from HubSpot API."""
    url = f"https://api.hubapi.com{endpoint}?{params}&limit=100"
    result = subprocess.run(
        ["curl", "-s", url, "-H", f"Authorization: Bearer {HS_TOKEN}"],
        capture_output=True, text=True
    )
    return json.loads(result.stdout)

# --- Status mapping ---
LIFECYCLE_MAP = {
    "subscriber": "prospect", "lead": "prospect",
    "marketingqualifiedlead": "prospect", "salesqualifiedlead": "lead",
    "opportunity": "negotiation", "customer": "active_customer",
    "evangelist": "active_customer", "other": "prospect",
}

DEALSTAGE_MAP = {
    "appointmentscheduled": "prospect", "qualifiedtobuy": "lead",
    "presentationscheduled": "negotiation", "decisionmakerboughtin": "negotiation",
    "contractsent": "negotiation", "closedwon": "closed_won", "closedlost": "closed_lost",
}

from crm import CRM
crm = CRM("crm.db")

# --- 1. Sync contacts ---
contacts_data = hs_get("/crm/v3/objects/contacts",
    "properties=firstname,lastname,email,company,jobtitle,lifecyclestage,hs_lead_status")

for c in contacts_data.get("results", []):
    props = c["properties"]
    name = f'{props.get("firstname", "")} {props.get("lastname", "")}'.strip()
    email = props.get("email")
    if not name:
        continue

    lifecycle = props.get("lifecyclestage", "other") or "other"
    mapped_status = LIFECYCLE_MAP.get(lifecycle, "prospect")

    # Upsert: try update first, add if not found
    existing = crm.get_contact(email) if email else crm.get_contact(name)
    if existing:
        crm.update_contact(existing["email"], company=props.get("company"),
                          title=props.get("jobtitle"), status=mapped_status)
    else:
        crm.add_contact(name, email=email, company=props.get("company"),
                       title=props.get("jobtitle"), status=mapped_status,
                       source="hubspot")

    # Store HubSpot ID as a fact for back-sync
    crm.observe(f"contact:{name.lower()}", "hubspot_id", str(c["id"]), source="hubspot")

# Handle pagination
while contacts_data.get("paging", {}).get("next"):
    after = contacts_data["paging"]["next"]["after"]
    contacts_data = hs_get("/crm/v3/objects/contacts",
        f"properties=firstname,lastname,email,company,jobtitle,lifecyclestage&after={after}")
    # ... repeat contact processing above ...

# --- 2. Sync deals ---
deals_data = hs_get("/crm/v3/objects/deals",
    "properties=dealname,amount,dealstage,pipeline,closedate&associations=contacts")

for d in deals_data.get("results", []):
    props = d["properties"]
    deal_name = props.get("dealname", "Untitled Deal")
    amount = props.get("amount")
    stage = DEALSTAGE_MAP.get(props.get("dealstage", ""), "prospect")

    # Find associated contact
    assoc_contacts = (d.get("associations", {}).get("contacts", {}).get("results", []))
    if assoc_contacts:
        hs_contact_id = str(assoc_contacts[0]["id"])
        # Look up local contact by hubspot_id fact
        local = crm.conn.execute(
            "SELECT value FROM facts WHERE key='hubspot_id' AND value=? AND source='hubspot'",
            (hs_contact_id,)
        ).fetchone()
        if local:
            entity = crm.conn.execute(
                "SELECT entity FROM facts WHERE key='hubspot_id' AND value=? AND source='hubspot'",
                (hs_contact_id,)
            ).fetchone()
            if entity:
                contact_name = entity[0].replace("contact:", "")
                amount_str = f"${int(float(amount)):,}" if amount else None
                crm.add_deal(contact_name, deal_name, value=amount_str, stage=stage)
                crm.observe(f"deal:{deal_name.lower()}", "hubspot_deal_id",
                           str(d["id"]), source="hubspot")

# --- 3. Sync activities ---
for obj_type, type_label, body_prop in [
    ("notes", "note", "hs_note_body"),
    ("emails", "email", "hs_email_subject"),
    ("calls", "call", "hs_call_title"),
    ("meetings", "meeting", "hs_meeting_title"),
]:
    data = hs_get(f"/crm/v3/objects/{obj_type}",
        f"properties={body_prop},hs_timestamp&associations=contacts")
    for item in data.get("results", []):
        props = item["properties"]
        summary = props.get(body_prop, "")
        if not summary:
            continue
        assoc = (item.get("associations", {}).get("contacts", {}).get("results", []))
        if assoc:
            hs_cid = str(assoc[0]["id"])
            entity_row = crm.conn.execute(
                "SELECT entity FROM facts WHERE key='hubspot_id' AND value=? AND source='hubspot'",
                (hs_cid,)
            ).fetchone()
            if entity_row:
                contact_name = entity_row[0].replace("contact:", "")
                crm.log_activity(contact_name, type_label,
                                f"[hubspot] {summary[:200]}")
```

---

## Push to HubSpot

Sync local changes back to HubSpot. Finds contacts with a stored `hubspot_id` fact and pushes updates.

```python
import json, subprocess

HS_TOKEN = "your-token"

REVERSE_LIFECYCLE = {
    "prospect": "lead", "lead": "salesqualifiedlead",
    "negotiation": "opportunity", "active_customer": "customer",
    "churned": "other", "lost": "other",
}

REVERSE_DEALSTAGE = {
    "prospect": "appointmentscheduled", "lead": "qualifiedtobuy",
    "negotiation": "presentationscheduled", "closed_won": "closedwon",
    "closed_lost": "closedlost",
}

def hs_patch(endpoint, data):
    """PATCH to HubSpot API."""
    result = subprocess.run(
        ["curl", "-s", "-X", "PATCH", f"https://api.hubapi.com{endpoint}",
         "-H", f"Authorization: Bearer {HS_TOKEN}",
         "-H", "Content-Type: application/json",
         "-d", json.dumps(data)],
        capture_output=True, text=True
    )
    return json.loads(result.stdout)

from crm import CRM
crm = CRM("crm.db")

# --- Push contact updates ---
contacts = crm.list_contacts()
for c in contacts:
    name_key = f"contact:{c['name'].lower()}"
    hs_id_row = crm.conn.execute(
        "SELECT value FROM facts WHERE entity=? AND key='hubspot_id' AND source='hubspot'",
        (name_key,)
    ).fetchone()
    if not hs_id_row:
        continue  # No HubSpot link, skip

    hs_id = hs_id_row[0]
    name_parts = c["name"].split(" ", 1)
    hs_lifecycle = REVERSE_LIFECYCLE.get(c["status"], "lead")

    properties = {
        "firstname": name_parts[0],
        "lastname": name_parts[1] if len(name_parts) > 1 else "",
        "email": c["email"] or "",
        "company": c["company"] or "",
        "jobtitle": c["title"] or "",
        "lifecyclestage": hs_lifecycle,
    }

    hs_patch(f"/crm/v3/objects/contacts/{hs_id}", {"properties": properties})

# --- Push deal updates ---
deals = crm.list_deals()
for d in deals:
    deal_key = f"deal:{d['name'].lower()}"
    hs_deal_row = crm.conn.execute(
        "SELECT value FROM facts WHERE entity=? AND key='hubspot_deal_id' AND source='hubspot'",
        (deal_key,)
    ).fetchone()
    if not hs_deal_row:
        continue

    hs_deal_id = hs_deal_row[0]
    hs_stage = REVERSE_DEALSTAGE.get(d["stage"], "appointmentscheduled")
    amount = d["value"].replace("$", "").replace(",", "") if d.get("value") else None

    properties = {"dealname": d["name"], "dealstage": hs_stage}
    if amount:
        properties["amount"] = amount

    hs_patch(f"/crm/v3/objects/deals/{hs_deal_id}", {"properties": properties})
```

---

## Gotchas

- **Pagination**: HubSpot returns max 100 results. Always check `paging.next.after` and loop.
- **Rate limits**: 100 requests per 10 seconds (private apps). Batch where possible.
- **Lifecycle stage is one-way**: HubSpot does not allow setting `lifecyclestage` backwards (e.g., customer -> lead) via API without clearing it first. To downgrade: PATCH with `""` first, then PATCH with the new value.
- **Deal associations**: Creating a deal does not auto-associate it. You must call the associations endpoint separately.
- **Email dedup**: HubSpot uses email as the unique key for contacts. The `add_contact` endpoint will 409 if the email already exists -- use search + update instead.
- **Custom properties**: If the HubSpot account has custom properties, fetch them with `GET /crm/v3/properties/contacts` and add to the field mapping.
- **Archived records**: Default list endpoints exclude archived records. Add `&archived=true` to include them.
