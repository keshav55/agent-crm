# Roadmap

How this repo self-improves, and what to build next.

## The loop

Two RSI loops, both already live:

**Code loop.** `benchmark.py` is the fitness function. Every tick adds a capability
plus tests, and the score only moves when the tests pass. History: 320 → 326 → 344 →
353 → 362 → 368 → 377. The score is saturated at 377/377, which means the next tick
must grow the benchmark, not just pass it.

**Data loop.** `skills/evolve/` reads real pipeline data, proposes one experiment,
tracks the result in the facts table. It improves the GTM, not the code.

Rule for every tick: one bounded change, tests included, score printed in the
commit message, no regressions. Dispatch builds through `/engines`; verify and
land here.

## Next ticks (code loop)

### [x] One. Deal close probability
Use `win_loss_analysis()` history to score open deals: given status, cadence,
activity recency, and source, estimate close likelihood. New method
`deal_probability(identifier)`, CLI `crm.py forecast`, MCP tool. ~15 tests.

### [x] Two. Next-best-action queue
Combine reminders, `optimal_cadence()`, health scores, and stale-deal detection
into one ranked "do this today" list. `next_actions(limit=10)`, CLI
`crm.py today`. This is the single highest-leverage daily entry point. ~12 tests.

### [x] Three. Import/export round-trip
CSV and vCard import, full JSON export/import that round-trips losslessly
(contacts + activity + deals + facts). Makes the engine adoptable and makes
backups real. ~15 tests.

### [x] Four. Multi-hop intro paths
`intros` today finds direct warm paths. Extend the facts graph to walk 2–3 hops
(`introduced_by`, `knows`, shared company/domain) with path scoring by
relationship strength. ~10 tests.

### [x] Five. Ingest hardening
The macOS connectors (Contacts, iMessage, Calendar, Mail) have no benchmark
coverage because they touch live system data. Add a fixture layer (fake
chat.db / AddressBook snapshots) so ingest logic gets scored too. ~15 tests.

## Next ticks (data loop)

### [x] Six. Experiment outcome tracking
`evolve()` proposes experiments; nothing yet closes the loop on whether past
experiments worked. Store proposal → result → verdict in the facts table and
have the next `evolve()` run read its own track record first. This is the tick
that makes the data loop actually recursive.

### [x] Seven. Cadence auto-tuning
Feed `optimal_cadence()` output back into reminder defaults per status, so the
system's own analysis changes its own behavior.

## Meta

### [x] Eight. Benchmark integrity
As the benchmark grows past ~400 tests, add a perf-regression gate (fail if any
op exceeds threshold) and a property-based pass over CRUD so score inflation
stays honest. The benchmark is the fitness function — it must be harder to game
than to satisfy.

## Wave two — the loop turns inward

Wave one gave the system predictions, a daily queue, experiment memory, and
self-tuned cadence. Wave two makes it measure and modify itself.

### [x] Nine. Forecast calibration + learned weights
Every `deal_probability()` call records its prediction as a fact. When a deal
closes, score the prediction (Brier score). `calibration_report()` shows how
good the forecasts actually are. When enough closed outcomes exist, fit the
signal weights from history (pure-stdlib logistic fit) instead of the
hardcoded 0.45/0.20/... — the model retrains itself from its own track record.

### [x] Ten. Autonomous data hygiene
`suggest_merges()` finds duplicates; nothing acts on them. Add confidence
scoring, auto-merge above a threshold with full undo (merge log in facts),
entity-key canonicalization across name/email variants, and a `doctor`
command that finds and fixes inconsistencies in one pass.

### [x] Eleven. Self-proposing roadmap
`propose_ticks()` — the CRM introspects itself: which public methods have no
benchmark coverage, which next_actions signals never fire on real data, which
CLI commands error most, where perf timings trend worse. Output: ranked
candidate ticks in roadmap.md format. The system writes its own wave three.

### [x] Twelve. Outcome-driven action tuning
`next_actions` urgency weights are hand-set. Log which surfaced actions get
acted on (activity within N days of surfacing) and whether acted-on contacts
advance stage; shift urgency weights toward action types that actually move
deals. The daily queue learns from being used.

## Order

One and Two first (they compound: forecast feeds the action queue). Then Six —
it makes the data loop self-referential. Then Three, Four, Five, Seven, Eight.

## Wave three — self-proposed

### [x] Thirteen. Benchmark find_intros capability
Add focused benchmark cases for `CRM.find_intros()`; proposed by `propose_ticks()` and now covered.

### [x] Fourteen. Benchmark network_summary capability
Add focused benchmark cases for `CRM.network_summary()`; proposed by `propose_ticks()` and now covered.

### [x] Fifteen. Benchmark unified_search capability
Add focused benchmark cases for `CRM.unified_search()`; proposed by `propose_ticks()` and now covered.
