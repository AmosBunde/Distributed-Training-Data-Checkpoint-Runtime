# ADR 0005 — TTL lease-based shard scheduling with fencing

**Status:** Accepted (2026-08)

## Context

Each epoch, N dataset shards must be consumed exactly once across W workers, where any
worker can die, stall, or partition at any time. The classic static assignment
(rank r reads shards r, r+W, r+2W, …) requires either a full epoch restart or manual
intervention when a rank dies, and cannot serve elastic worker counts.

## Decision

Shard access is granted through **renewable, TTL-bound leases** over contiguous
half-open ranges `[begin, end)`:

- Each `(dataset, epoch)` lazily materializes a pool of shard indices; `acquire` draws
  the longest contiguous run from the pool minimum (contiguity keeps reads sequential
  for the prefetcher).
- Leases expire after `lease_ttl_ms` unless renewed; expired shards **return to the
  pool** for any live worker to acquire — a dead worker's shards are re-consumed
  without restarting the epoch.
- **Fencing:** `renew` on a lapsed lease returns `renewed=false` (a signal, not an
  error). A partitioned worker that comes back learns it must stop reading *before* it
  can double-consume shards now owned by another worker. Session-token rotation on
  re-registration fences zombie processes the same way.
- Worker eviction (missed heartbeats) cascades: `sweep_expired → revoke_worker → shards
  back to pool`, run by the server's background sweeper every heartbeat interval.

Config invariant (enforced at boot): `lease_ttl_ms ≥ heartbeat_interval_ms`, so a
healthy worker can never lose a lease between heartbeats.

## Consequences

- ✅ Worker failure is a routine event: shards reflow automatically, epoch completes.
- ✅ Elastic worker counts work naturally — the pool doesn't care about W.
- ✅ At-least-once shard delivery with fencing against concurrent double-reads.
- ⚠️ Exactly-once is *not* guaranteed across failure: a worker that consumed shards
  but died before `release(completed=true)` causes re-reads. Training tolerates
  duplicate samples; jobs needing strict exactly-once must checkpoint consumption
  offsets with model state.
- ⚠️ Lease state is in-memory ([ADR 0006](0006-in-memory-coordinator-state.md)).

Related: [worker & lease lifecycle diagram](../diagrams/worker-lease-lifecycle.html).
