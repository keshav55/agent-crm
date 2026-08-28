---
name: evolve
description: Self-improvement loop for agent-crm. Analyzes CRM data to find what's working, proposes one experiment, and tracks results via the facts table. Use when the user says evolve, improve, self-improve, optimize, what's working, experiment, or strategy.
version: 1.0.0
tags:
  - crm
  - self-improvement
  - experiments
  - strategy
  - analytics
---

# Evolve

One improvement cycle per run. Read the CRM, find what's working, propose one change, measure it.

Inspired by Karpathy's autoresearch: instead of optimizing code, optimize your GTM by running experiments against real CRM data.

## When to activate

- "evolve" / "improve" / "self-improve"
- "optimize" / "what's working" / "experiment"
- "strategy" (in the context of outreach or GTM)
- "what should I try next?"
- "why aren't deals closing?"

## Prerequisites

The CRM database must exist. Default location: `crm.db` in the project root, or set via `CRM_DB` env var.

```python
from crm import CRM
crm = CRM("crm.db")
```

The `evolve.py` script and `strategy.md` file in the agent-crm project root provide additional context. Run `python evolve.py init` if they don't exist yet.

---

## Workflow: One Improvement Cycle

### Step 1: Read the CRM state

Start with experiment history. The loop's own track record is the first input
to every new proposal: do not repeat a loss unchanged or start a duplicate of a
running experiment.

```python
from crm import CRM
crm = CRM("crm.db")

# Experiment history and track record — always read these first
past_experiments = crm.experiment_history()
wins = [experiment for experiment in past_experiments if experiment["verdict"] == "win"]
losses = [experiment for experiment in past_experiments if experiment["verdict"] == "loss"]
running = [experiment for experiment in past_experiments if experiment["status"] == "running"]
decided = len(wins) + len(losses)
experiment_track_record = {
    "wins": wins,
    "losses": losses,
    "running": running,
    "win_rate": round(len(wins) / decided * 100, 1) if decided else 0.0,
}

# Pipeline overview
pipeline = crm.pipeline()
# Returns: [{"status": "lead", "count": 12, "names": "Alice, Bob, ..."}]

# Full stats
stats = crm.stats()
# Returns: {"total_contacts": N, "contacted_last_7d": N, "stale_14d": N, "by_status": [...]}

# All activity (look at last 50 events across the CRM)
contacts = crm.list_contacts()

# Stale contacts — relationships that are fading
stale = crm.stale_contacts(days=14)

# Facts graph — other observed patterns
past_learnings = crm.search_graph("learning")
```

### Step 2: Analyze what's working and what's not

Run these four analyses. Each one answers a specific question.

**Which outreach gets replies?**
```python
# Look at contacts who moved from 'lead' or 'contacted' to 'interested' or beyond
for c in contacts:
    if c["status"] in ("interested", "negotiating", "active_customer"):
        acts = crm.get_activity(c["email"] or c["name"], limit=20)
        timeline = crm.timeline(c["email"] or c["name"])
        # What type of first touch? How long between touches?
        # What channel? What did the outreach say?
```

**Which deals are stuck?**
```python
# Contacts in negotiating or interested that haven't moved
for c in contacts:
    if c["status"] in ("interested", "negotiating"):
        velocity = crm.velocity(c["email"] or c["name"])
        # velocity["trend"] tells you: "accelerating", "stable", "decaying", "cold"
        if velocity and velocity["trend"] in ("decaying", "cold"):
            # This deal is stuck. Note why.
            pass
```

**Which relationships are fading?**
```python
# stale_contacts already handles this
stale = crm.stale_contacts(days=14)
# Sort by deal_size to prioritize high-value fading relationships
stale_sorted = sorted(stale, key=lambda c: c.get("deal_size") or 0, reverse=True)
```

**What patterns correlate with closed deals?**
```python
customers = crm.list_contacts(status="active_customer")
for c in customers:
    acts = crm.get_activity(c["email"] or c["name"], limit=20)
    journey = " -> ".join(f"{a['type']}" for a in reversed(acts))
    # Track: source, number of touches, time to close, channel mix
```

### Step 3: Propose ONE specific change

Based on the analysis, propose exactly one change. Not two. Not a redesign. One variable.

The change must be one of:
- **Targeting**: Who you reach out to (industry, company size, role, signal)
- **Messaging**: What you say (subject line, opening line, CTA, proof point)
- **Timing**: When you send (day of week, time of day, cadence)
- **Channel**: How you reach them (email, Twitter DM, LinkedIn, warm intro)

Format the proposal clearly:

```
EXPERIMENT: [short name]
CHANGE: [exactly what's different from current strategy]
HYPOTHESIS: [what you expect to happen and why]
MEASURE: [how you'll know if it worked — specific metric + threshold]
BATCH SIZE: [how many contacts to test on — usually 5-10]
```

### Step 4: Record the experiment in the facts table

Use the facts table to track experiments across sessions. This is the persistent memory.

```python
slug = crm.log_experiment(
    "Tuesday timing",
    "Tuesday emails get 2x replies vs Thursday",
    "reply rate",
    "12.5",
)
batch_id = f"experiment:{slug}"

# Tag each contact in the batch
for contact_email in batch_contacts:
    crm.observe(batch_id, "contact", contact_email, source="evolve")
```

Also record in `evolve.py`'s results file if it exists:
```bash
python evolve.py record --batch N --sent 8 --replies 0 --meetings 0 --notes "timing experiment: Tuesday send"
```

### Step 5: Record outcomes (when results come in)

After enough time has passed (typically 3-7 days for email), record what happened:

```python
# Numeric results derive win/loss/neutral from the stored baseline
experiment = crm.conclude_experiment(slug, "37.5")

# If the experiment won, record a learning
import datetime
crm.observe("learning:timing", "insight", "Tuesday 9-11am gets 3x reply rate vs Thursday", source="evolve")
crm.observe("learning:timing", "confidence", "1 batch, needs replication", source="evolve")
crm.observe("learning:timing", "discovered_at", datetime.date.today().isoformat(), source="evolve")
```

If the experiment lost:
```python
experiment = crm.conclude_experiment(slug, "10")  # verdict derives to "loss"
crm.observe("learning:timing", "insight", "Thursday is not worse than Tuesday — keep default", source="evolve")
```

For non-numeric metrics, pass an explicit verdict:
```python
crm.conclude_experiment(slug, "Customers preferred version B", verdict="win")
```

### Step 6: Update strategy.md with learnings

If an experiment produced a clear win, update `strategy.md` in the agent-crm project root. Move proven learnings from "Open Experiments" to "What Converts (proven)" or "What Fails (proven)".

---

## Reviewing experiment history

To see all past experiments and learnings stored in the facts table:

```python
# All experiments
experiments = crm.experiment_history()
for e in experiments:
    print(f"{e['slug']} | {e['status']} | {e['verdict'] or '-'}")

# All learnings
learnings = crm.search_graph("learning:")
for l in learnings:
    print(f"{l['entity']} | {l['key']}: {l['value']}")

# Specific experiment details
details = crm.facts_about("experiment:tuesday-timing")
```

Or via the CLI:
```bash
python crm.py experiments
python crm.py experiments --status running
python crm.py experiment log "Tuesday timing" "Tuesday doubles replies" "reply rate" 12.5
python crm.py experiment conclude tuesday-timing 37.5
```

---

## Connecting to Atris for cross-session memory

For learnings that should persist beyond the local CRM (across projects, across machines), push them to Atris.

### Get token

```bash
# From ~/.atris/credentials.json
TOKEN=$(python3 -c "import json,os; print(json.load(open(os.path.expanduser('~/.atris/credentials.json')))['token'])")
```

### Push a learning to Atris

```bash
curl -s -X POST "https://api.atris.ai/api/memory" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "content": "CRM evolve: Tuesday 9-11am emails get 3x reply rate vs Thursday baseline. Tested on 8 contacts, 3 replied.",
    "tags": ["crm", "evolve", "timing", "experiment-result"],
    "source": "agent-crm/evolve"
  }'
```

### Retrieve past learnings from Atris

```bash
# Search for all CRM evolve learnings
curl -s "https://api.atris.ai/api/memory/search?q=crm+evolve&limit=20" \
  -H "Authorization: Bearer $TOKEN"
```

### When to use Atris vs local facts table

- **Local facts table**: Experiment tracking, batch details, contact-level data. This is the working memory.
- **Atris**: Proven learnings, cross-project patterns, insights you want available in every session. This is the long-term memory.

Rule of thumb: push to Atris only after an experiment has a clear conclusion. Don't push hypotheses or in-progress experiments.

---

## Example: Full cycle

User says: "/evolve"

1. Read CRM: 47 contacts, 12 leads, 8 contacted, 3 interested, 2 negotiating. 6 stale.
2. Analyze:
   - 2 of 3 interested contacts came from warm intros (already known)
   - Cold email reply rate: 2/16 (12.5%) — all sent Thursday afternoon
   - The 2 replies were both to emails that mentioned a specific pain point from their LinkedIn
   - 2 deals in negotiating haven't had activity in 11 days
3. Propose:
   ```
   EXPERIMENT: personalized_pain_point
   CHANGE: Every cold email must reference one specific pain point found on their LinkedIn or website (not generic industry pain)
   HYPOTHESIS: Specific pain references drove both replies so far. Doubling down should beat 12.5% baseline.
   MEASURE: Reply rate > 25% on next 8 cold emails
   BATCH SIZE: 8
   ```
4. Record: `slug = crm.log_experiment("personalized pain", "...", "reply rate", 12.5)`
5. Execute the batch, wait 5 days
6. Record: `crm.conclude_experiment(slug, 37.5)`. Push the winning learning to Atris.
7. Update strategy.md: move "specific pain point reference" from experiment to proven.

---

## Troubleshooting

### No experiment history found
The facts table may be empty. Run `crm.experiment_history()` to check. If empty, this is the first cycle — focus on establishing a baseline.

### Not enough data to analyze
If the CRM has fewer than 10 contacts or fewer than 5 activity entries, the analysis step won't yield meaningful patterns. In this case, propose a baseline measurement experiment: send 10 outreach messages using the current strategy and record reply rates.

### Experiment results are ambiguous
If a batch is too small to support a win or loss, conclude it with an explicit
`neutral` verdict, then increase the batch size or try a different variable.
Don't declare a winner on thin data.
