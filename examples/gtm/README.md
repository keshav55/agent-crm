# examples/gtm

Two scripts that use agent-crm for outbound sales automation.

## `logged.py`

Run after sending a cold email by hand. Logs the send and transitions the contact from `prospect` to `contacted`. Creates the contact if it does not exist.

    python logged.py jane@acme.com "built something, want your take"
    python logged.py jane@acme.com "subject" --name "Jane Doe" --company "Acme"
    python logged.py jane@acme.com "quick nudge" --followup

## `roi.py`

One-screen pipeline health: MRR, ARR, target gap, pipeline by status, 7-day activity, active-customer breakdown.

    python roi.py
    python roi.py --json
    python roi.py --target-arr 500000

## Environment

Both scripts honor `CRM_DB` for the database path and default to `./crm.db`.

    export CRM_DB=pipeline.db

## Typical daily loop

    morning:     python roi.py                              # see where you stand
    outbound:    send 10 cold emails by hand
    after each:  python logged.py <email> "<subject>"
    end of day:  python roi.py                              # see what moved

Both scripts are standard-library only. Copy, modify, replace.
