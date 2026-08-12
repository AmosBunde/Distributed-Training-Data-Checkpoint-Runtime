//! Worker membership: registration, session tokens, heartbeat deadlines.

use dtr_common::types::WorkerId;
use dtr_common::DtrError;
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub worker_id: WorkerId,
    pub rank: u32,
    pub world_size: u32,
    pub session_token: String,
    pub labels: HashMap<String, String>,
    pub last_heartbeat: Instant,
    pub current_step: u64,
}

/// Registry of live workers with heartbeat-deadline eviction.
pub struct Membership {
    workers: Mutex<HashMap<WorkerId, WorkerInfo>>,
    heartbeat_interval: Duration,
    missed_allowed: u32,
}

impl Membership {
    pub fn new(heartbeat_interval_ms: u64, missed_allowed: u32) -> Self {
        Self {
            workers: Mutex::new(HashMap::new()),
            heartbeat_interval: Duration::from_millis(heartbeat_interval_ms),
            missed_allowed,
        }
    }

    pub fn heartbeat_interval_ms(&self) -> u64 {
        self.heartbeat_interval.as_millis() as u64
    }

    pub fn missed_heartbeats_allowed(&self) -> u32 {
        self.missed_allowed
    }

    /// Register (or re-register) a worker. Idempotent: re-registering the same
    /// worker_id rotates its session token and resets its deadline.
    pub fn register(
        &self,
        worker_id: WorkerId,
        rank: u32,
        world_size: u32,
        labels: HashMap<String, String>,
    ) -> WorkerInfo {
        let info = WorkerInfo {
            worker_id: worker_id.clone(),
            rank,
            world_size,
            session_token: Uuid::new_v4().to_string(),
            labels,
            last_heartbeat: Instant::now(),
            current_step: 0,
        };
        self.workers.lock().unwrap().insert(worker_id, info.clone());
        info
    }

    /// Validate a (worker, token) pair, refusing evicted or unknown workers.
    pub fn authenticate(&self, worker_id: &WorkerId, token: &str) -> Result<(), DtrError> {
        let workers = self.workers.lock().unwrap();
        let info = workers
            .get(worker_id)
            .ok_or_else(|| DtrError::UnknownWorker(worker_id.to_string()))?;
        if info.session_token != token {
            return Err(DtrError::InvalidSession(worker_id.to_string()));
        }
        Ok(())
    }

    /// Record a heartbeat. Returns Err(UnknownWorker) after eviction so the
    /// gRPC layer can tell the client to re-register.
    pub fn heartbeat(
        &self,
        worker_id: &WorkerId,
        token: &str,
        current_step: u64,
    ) -> Result<(), DtrError> {
        let mut workers = self.workers.lock().unwrap();
        let info = workers
            .get_mut(worker_id)
            .ok_or_else(|| DtrError::UnknownWorker(worker_id.to_string()))?;
        if info.session_token != token {
            return Err(DtrError::InvalidSession(worker_id.to_string()));
        }
        info.last_heartbeat = Instant::now();
        info.current_step = current_step;
        Ok(())
    }

    /// Evict every worker whose deadline (interval * (missed_allowed + 1))
    /// has passed. Returns the evicted workers so lease/barrier state can react.
    pub fn sweep_expired(&self) -> Vec<WorkerInfo> {
        let deadline = self.heartbeat_interval * (self.missed_allowed + 1);
        let now = Instant::now();
        let mut workers = self.workers.lock().unwrap();
        let expired: Vec<WorkerId> = workers
            .values()
            .filter(|w| now.duration_since(w.last_heartbeat) > deadline)
            .map(|w| w.worker_id.clone())
            .collect();
        expired.iter().filter_map(|id| workers.remove(id)).collect()
    }

    pub fn live_count(&self) -> usize {
        self.workers.lock().unwrap().len()
    }

    /// Snapshot of one worker's info (None if unknown/evicted).
    pub fn get(&self, worker_id: &WorkerId) -> Option<WorkerInfo> {
        self.workers.lock().unwrap().get(worker_id).cloned()
    }

    /// World size as reported by the most recently registered worker (0 if none).
    pub fn expected_world_size(&self) -> u32 {
        self.workers
            .lock()
            .unwrap()
            .values()
            .map(|w| w.world_size)
            .max()
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn register_authenticate_heartbeat() {
        let m = Membership::new(1_000, 2);
        let info = m.register("rank-0".into(), 0, 2, HashMap::new());
        assert!(m
            .authenticate(&"rank-0".into(), &info.session_token)
            .is_ok());
        assert!(matches!(
            m.authenticate(&"rank-0".into(), "bogus"),
            Err(DtrError::InvalidSession(_))
        ));
        assert!(matches!(
            m.authenticate(&"rank-9".into(), "x"),
            Err(DtrError::UnknownWorker(_))
        ));
        assert!(m
            .heartbeat(&"rank-0".into(), &info.session_token, 42)
            .is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn eviction_after_missed_heartbeats() {
        let m = Membership::new(1_000, 2); // deadline = 3s
        let a = m.register("rank-0".into(), 0, 2, HashMap::new());
        m.register("rank-1".into(), 1, 2, HashMap::new());

        tokio::time::advance(std::time::Duration::from_millis(2_500)).await;
        // rank-0 heartbeats, rank-1 stays silent.
        m.heartbeat(&"rank-0".into(), &a.session_token, 1).unwrap();

        tokio::time::advance(std::time::Duration::from_millis(1_000)).await;
        let evicted = m.sweep_expired();
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].worker_id.as_str(), "rank-1");
        assert_eq!(m.live_count(), 1);

        // Evicted worker's heartbeat now instructs re-registration.
        assert!(matches!(
            m.heartbeat(&"rank-1".into(), "old-token", 1),
            Err(DtrError::UnknownWorker(_))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn reregistration_rotates_token() {
        let m = Membership::new(1_000, 2);
        let first = m.register("rank-0".into(), 0, 1, HashMap::new());
        let second = m.register("rank-0".into(), 0, 1, HashMap::new());
        assert_ne!(first.session_token, second.session_token);
        assert!(m
            .authenticate(&"rank-0".into(), &first.session_token)
            .is_err());
        assert_eq!(m.live_count(), 1);
    }
}
