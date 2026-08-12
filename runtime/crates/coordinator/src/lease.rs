//! Shard leasing: TTL-bound, renewable assignment of shard ranges to workers.
//!
//! Each (dataset, epoch) owns a pool of shard indices. Workers draw contiguous
//! ranges from the pool; expired leases return their shards for reassignment,
//! which is what makes worker failure survivable without restarting the epoch.

use dtr_common::types::{DatasetId, LeaseId, ShardRange, WorkerId};
use dtr_common::DtrError;
use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;
use tokio::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ShardLease {
    pub lease_id: LeaseId,
    pub worker_id: WorkerId,
    pub dataset_id: DatasetId,
    pub epoch: u64,
    pub shards: ShardRange,
    pub expires_at: Instant,
}

#[derive(Debug, Clone)]
struct DatasetState {
    num_shards: u32,
    /// Pool of unassigned shard indices per epoch. Populated lazily on first
    /// acquire for that epoch.
    pools: HashMap<u64, BTreeSet<u32>>,
}

/// Lease table with TTL expiry and shard-pool reassignment.
pub struct LeaseManager {
    datasets: Mutex<HashMap<DatasetId, DatasetState>>,
    leases: Mutex<HashMap<LeaseId, ShardLease>>,
    ttl: Duration,
    default_shards_per_lease: u32,
}

impl LeaseManager {
    pub fn new(lease_ttl_ms: u64, default_shards_per_lease: u32) -> Self {
        Self {
            datasets: Mutex::new(HashMap::new()),
            leases: Mutex::new(HashMap::new()),
            ttl: Duration::from_millis(lease_ttl_ms),
            default_shards_per_lease: default_shards_per_lease.max(1),
        }
    }

    pub fn ttl_ms(&self) -> u64 {
        self.ttl.as_millis() as u64
    }

    /// Register a dataset (idempotent). Returns (dataset_id, already_registered).
    pub fn register_dataset(&self, uri: &str, num_shards: u32) -> (DatasetId, bool) {
        // Deterministic id from the URI so all ranks resolve the same handle.
        let id = DatasetId::new(format!("ds-{:016x}", fxhash(uri.as_bytes())));
        let mut datasets = self.datasets.lock().unwrap();
        let existed = datasets.contains_key(&id);
        datasets.entry(id.clone()).or_insert_with(|| DatasetState {
            num_shards,
            pools: HashMap::new(),
        });
        (id, existed)
    }

    /// Acquire up to `requested` contiguous shards for (dataset, epoch).
    /// Returns None when the epoch pool is exhausted.
    pub fn acquire(
        &self,
        worker_id: WorkerId,
        dataset_id: &DatasetId,
        epoch: u64,
        requested: u32,
    ) -> Result<Option<ShardLease>, DtrError> {
        self.expire_due_leases();

        let mut datasets = self.datasets.lock().unwrap();
        let ds = datasets
            .get_mut(dataset_id)
            .ok_or_else(|| DtrError::UnknownDataset(dataset_id.to_string()))?;

        let num_shards = ds.num_shards;
        let pool = ds
            .pools
            .entry(epoch)
            .or_insert_with(|| (0..num_shards).collect());

        if pool.is_empty() {
            return Ok(None);
        }

        let want = if requested == 0 {
            self.default_shards_per_lease
        } else {
            requested
        };

        // Take the longest contiguous run starting at the pool's minimum,
        // capped at `want`. Contiguity keeps range reads sequential.
        let start = *pool.iter().next().unwrap();
        let mut end = start;
        while end - start < want && pool.contains(&end) {
            end += 1;
        }
        for shard in start..end {
            pool.remove(&shard);
        }

        let lease = ShardLease {
            lease_id: LeaseId::new(format!("lease-{}", Uuid::new_v4())),
            worker_id,
            dataset_id: dataset_id.clone(),
            epoch,
            shards: ShardRange::new(start, end).expect("start <= end by construction"),
            expires_at: Instant::now() + self.ttl,
        };
        self.leases
            .lock()
            .unwrap()
            .insert(lease.lease_id.clone(), lease.clone());
        Ok(Some(lease))
    }

    /// Renew a lease. Returns the new expiry, or LeaseExpired if it already
    /// lapsed (its shards may belong to someone else now — the caller must
    /// stop reading them).
    pub fn renew(&self, lease_id: &LeaseId) -> Result<Instant, DtrError> {
        self.expire_due_leases();
        let mut leases = self.leases.lock().unwrap();
        match leases.get_mut(lease_id) {
            Some(lease) => {
                lease.expires_at = Instant::now() + self.ttl;
                Ok(lease.expires_at)
            }
            None => Err(DtrError::LeaseExpired(lease_id.to_string())),
        }
    }

    /// Release a lease. If not `completed`, the shards return to the pool.
    pub fn release(&self, lease_id: &LeaseId, completed: bool) -> Result<(), DtrError> {
        let lease = self
            .leases
            .lock()
            .unwrap()
            .remove(lease_id)
            .ok_or_else(|| DtrError::UnknownLease(lease_id.to_string()))?;
        if !completed {
            self.return_to_pool(&lease);
        }
        Ok(())
    }

    /// Expire all due leases, returning their shards to their pools.
    /// Called opportunistically on every acquire/renew and by the sweeper task.
    pub fn expire_due_leases(&self) -> Vec<ShardLease> {
        let now = Instant::now();
        let mut leases = self.leases.lock().unwrap();
        let due: Vec<LeaseId> = leases
            .values()
            .filter(|l| l.expires_at <= now)
            .map(|l| l.lease_id.clone())
            .collect();
        let expired: Vec<ShardLease> = due.iter().filter_map(|id| leases.remove(id)).collect();
        drop(leases);
        for lease in &expired {
            self.return_to_pool(lease);
        }
        expired
    }

    /// Drop all leases held by a worker (on eviction), returning shards.
    pub fn revoke_worker(&self, worker_id: &WorkerId) -> Vec<ShardLease> {
        let mut leases = self.leases.lock().unwrap();
        let held: Vec<LeaseId> = leases
            .values()
            .filter(|l| &l.worker_id == worker_id)
            .map(|l| l.lease_id.clone())
            .collect();
        let revoked: Vec<ShardLease> = held.iter().filter_map(|id| leases.remove(id)).collect();
        drop(leases);
        for lease in &revoked {
            self.return_to_pool(lease);
        }
        revoked
    }

    pub fn active_lease_count(&self) -> usize {
        self.leases.lock().unwrap().len()
    }

    fn return_to_pool(&self, lease: &ShardLease) {
        let mut datasets = self.datasets.lock().unwrap();
        if let Some(ds) = datasets.get_mut(&lease.dataset_id) {
            if let Some(pool) = ds.pools.get_mut(&lease.epoch) {
                for shard in lease.shards.begin..lease.shards.end {
                    pool.insert(shard);
                }
            }
        }
    }
}

/// Small stable hash (FNV-1a) for deterministic dataset ids across restarts.
fn fxhash(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn acquire_grants_contiguous_disjoint_ranges() {
        let lm = LeaseManager::new(60_000, 4);
        let (ds, existed) = lm.register_dataset("s3://d/x", 10);
        assert!(!existed);

        let a = lm.acquire("w0".into(), &ds, 0, 4).unwrap().unwrap();
        let b = lm.acquire("w1".into(), &ds, 0, 4).unwrap().unwrap();
        let c = lm.acquire("w2".into(), &ds, 0, 4).unwrap().unwrap();
        assert_eq!((a.shards.begin, a.shards.end), (0, 4));
        assert_eq!((b.shards.begin, b.shards.end), (4, 8));
        assert_eq!((c.shards.begin, c.shards.end), (8, 10)); // partial tail

        // Pool exhausted.
        assert!(lm.acquire("w3".into(), &ds, 0, 4).unwrap().is_none());
        // Next epoch has a fresh pool.
        assert!(lm.acquire("w3".into(), &ds, 1, 4).unwrap().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn dataset_registration_is_idempotent_and_deterministic() {
        let lm = LeaseManager::new(60_000, 1);
        let (id1, e1) = lm.register_dataset("s3://d/x", 10);
        let (id2, e2) = lm.register_dataset("s3://d/x", 10);
        assert_eq!(id1, id2);
        assert!(!e1);
        assert!(e2);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_lease_returns_shards_and_renew_fails() {
        let lm = LeaseManager::new(1_000, 4);
        let (ds, _) = lm.register_dataset("s3://d/x", 4);

        let lease = lm.acquire("w0".into(), &ds, 0, 4).unwrap().unwrap();
        assert!(lm.acquire("w1".into(), &ds, 0, 4).unwrap().is_none());

        tokio::time::advance(std::time::Duration::from_millis(1_500)).await;

        // w1 can now take over the reassigned shards.
        let takeover = lm.acquire("w1".into(), &ds, 0, 4).unwrap().unwrap();
        assert_eq!((takeover.shards.begin, takeover.shards.end), (0, 4));

        // Original holder's renew is refused with the fencing error.
        assert!(matches!(
            lm.renew(&lease.lease_id),
            Err(DtrError::LeaseExpired(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn renew_extends_ttl() {
        let lm = LeaseManager::new(1_000, 1);
        let (ds, _) = lm.register_dataset("s3://d/x", 1);
        let lease = lm.acquire("w0".into(), &ds, 0, 1).unwrap().unwrap();

        tokio::time::advance(std::time::Duration::from_millis(800)).await;
        lm.renew(&lease.lease_id).unwrap();
        tokio::time::advance(std::time::Duration::from_millis(800)).await;
        // Would have expired without the renewal.
        assert!(lm.renew(&lease.lease_id).is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn incomplete_release_returns_shards() {
        let lm = LeaseManager::new(60_000, 2);
        let (ds, _) = lm.register_dataset("s3://d/x", 2);
        let lease = lm.acquire("w0".into(), &ds, 0, 2).unwrap().unwrap();
        lm.release(&lease.lease_id, false).unwrap();

        let again = lm.acquire("w1".into(), &ds, 0, 2).unwrap().unwrap();
        assert_eq!((again.shards.begin, again.shards.end), (0, 2));
    }

    #[tokio::test(start_paused = true)]
    async fn revoke_worker_frees_all_its_leases() {
        let lm = LeaseManager::new(60_000, 1);
        let (ds, _) = lm.register_dataset("s3://d/x", 3);
        lm.acquire("w0".into(), &ds, 0, 1).unwrap().unwrap();
        lm.acquire("w0".into(), &ds, 0, 1).unwrap().unwrap();
        lm.acquire("w1".into(), &ds, 0, 1).unwrap().unwrap();

        let revoked = lm.revoke_worker(&"w0".into());
        assert_eq!(revoked.len(), 2);
        assert_eq!(lm.active_lease_count(), 1);

        // Both freed shards are acquirable again.
        assert!(lm.acquire("w2".into(), &ds, 0, 1).unwrap().is_some());
        assert!(lm.acquire("w2".into(), &ds, 0, 1).unwrap().is_some());
        assert!(lm.acquire("w2".into(), &ds, 0, 1).unwrap().is_none());
    }
}
