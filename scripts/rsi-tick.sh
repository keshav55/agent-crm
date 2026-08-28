#!/bin/zsh
# Nightly RSI tick for agent-crm. Launched by com.atris.agent-crm-rsi (launchd).
# One bounded improvement round: propose -> build -> verify -> land if score rises.

set -euo pipefail
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
cd /Users/keshavrao/arena/agent-crm

LOG_DIR="atris/logs/rsi"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/$(date +%Y-%m-%d).md"

claude -p --permission-mode acceptEdits --allowedTools "Bash,Read,Edit,Write,Grep,Glob" "
You are running one unattended self-improvement tick for agent-crm
(/Users/keshavrao/arena/agent-crm). Read the memory file
~/.claude/projects/-Users-keshavrao-arena-agent-crm/memory/project_rsi_loop.md
first and follow that loop exactly. Rules:

1. git status first. Work only on branch member/codex-deal-probability
   (create nothing new, never touch master, never push, never destructive git).
2. Run: python3 benchmark.py. If it does not pass cleanly, STOP and write
   why to $LOG. Never build on a broken base.
3. Run: python3 crm.py propose. Pick the single highest-ranked actionable
   proposal (skip data-dependent 'signal never fires' items). If none are
   actionable, run one hardening pass instead: pick one public CRM method,
   read it, and add 3-5 sharper benchmark tests for its edge cases.
4. Implement the one bounded change with tests, directly (no codex dispatch
   in unattended mode; keep the slice small enough to write yourself).
5. Re-run python3 benchmark.py. Commit ONLY if all tests pass and the score
   is >= the starting score, with the score delta in the commit message and
   no AI/Co-Authored-By references. Otherwise git checkout -- . and log why.
6. Append a 3-line receipt to $LOG: what changed, score before/after,
   commit hash or 'reverted'.
" >> "$LOG" 2>&1

echo "\n---\ntick finished $(date '+%Y-%m-%d %H:%M')" >> "$LOG"
