# ADR 0007 — Configurable quorum policy for checkpoint barriers

**Status:** Accepted (2026-08)

## Context

A checkpoint barrier must decide when "enough" ranks have uploaded to commit. Always
requiring all ranks makes a single straggler block every checkpoint; always allowing
partial commits silently produces unrestorable state for models whose shards are all
required.

Whether a partial checkpoint is restorable depends on the *parallelism scheme*
(fully-sharded state: every rank required; redundantly-replicated optimizer state:
a fraction suffices) — something only the deployment knows.

## Decision

Quorum is **deployment configuration**, not runtime heuristics:

```yaml
checkpoint:
  quorum:
    policy: all_ranks          # or: fraction
    # value: 0.9               # for fraction
  barrier_timeout_ms: 600000
```

- `QuorumPolicy::required(expected)` computes the threshold — fraction uses
  ceil-then-clamp so 0.5 of 7 ranks is 4 (a strict majority) and no fraction can ever
  require zero ranks.
- The barrier is a state machine `Pending → Ready → Committed`, with `Aborted`
  reachable only from Pending/Ready. **Deadlines are enforced lazily** on every
  barrier access — no timer task to leak or lose; a stalled barrier aborts the moment
  anyone observes it past the deadline.
- `expected` comes from the max `world_size` reported by live workers — not the live
  count — so a barrier opened while one rank restarts still expects the full world and
  the *policy* decides commit-ability, not membership luck.
- Commit is **client-polled** (`PENDING / COMMITTED / ABORTED`), never a blocking RPC:
  no pinned connections across multi-minute barrier windows, and every rank converges
  on the same terminal state.

## Consequences

- ✅ Straggler behavior is an explicit, per-run choice with safe default (`all_ranks`).
- ✅ Deadline abort means a wedged rank costs one checkpoint interval, never the job.
- ⚠️ Fractional quorum shifts restorability responsibility to the operator — the
  values file documents that it is only sound for redundantly-sharded state.

Related: [ADR 0004](0004-manifest-based-atomic-commit.md),
[checkpoint commit sequence](../diagrams/checkpoint-commit-sequence.html).
