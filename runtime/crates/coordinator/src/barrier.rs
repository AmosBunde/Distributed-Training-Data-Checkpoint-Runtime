//! Checkpoint barriers: per-step rendezvous with quorum policy and deadline.
//!
//! A barrier is opened by the first `BeginCheckpoint(step)` and joined by every
//! subsequent one (idempotent token). Ranks report completion; commit is
//! allowed once the quorum is satisfied, and the barrier aborts if the
//! deadline passes first.

use dtr_common::types::{CheckpointToken, QuorumPolicy, TargetId};
use dtr_common::DtrError;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use tokio::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BarrierStatus {
    /// Waiting for more ranks (commit would return PENDING).
    Pending,
    /// Quorum satisfied; commit may verify + publish.
    Ready,
    /// Deadline passed before quorum, or explicitly aborted.
    Aborted,
    /// Manifest published.
    Committed,
}

#[derive(Debug, Clone)]
pub struct CheckpointBarrier {
    pub token: CheckpointToken,
    pub target_id: TargetId,
    pub step: u64,
    pub expected: u32,
    pub required: u32,
    pub deadline: Instant,
    pub completed_ranks: HashSet<u32>,
    pub status: BarrierStatus,
    pub abort_reason: Option<String>,
}

/// Barrier registry keyed by (target, step) with idempotent open-or-join.
pub struct BarrierManager {
    barriers: Mutex<HashMap<CheckpointToken, CheckpointBarrier>>,
    by_step: Mutex<HashMap<(TargetId, u64), CheckpointToken>>,
    quorum: QuorumPolicy,
    timeout: Duration,
}

impl BarrierManager {
    pub fn new(quorum: QuorumPolicy, barrier_timeout_ms: u64) -> Self {
        Self {
            barriers: Mutex::new(HashMap::new()),
            by_step: Mutex::new(HashMap::new()),
            quorum,
            timeout: Duration::from_millis(barrier_timeout_ms),
        }
    }

    /// Open the barrier for (target, step), or join it if already open.
    pub fn begin(
        &self,
        target_id: TargetId,
        step: u64,
        expected_participants: u32,
    ) -> CheckpointBarrier {
        let mut by_step = self.by_step.lock().unwrap();
        let mut barriers = self.barriers.lock().unwrap();

        if let Some(token) = by_step.get(&(target_id.clone(), step)) {
            if let Some(existing) = barriers.get(token) {
                return existing.clone();
            }
        }

        let barrier = CheckpointBarrier {
            token: CheckpointToken::new(format!("ckpt-{}-{}", step, Uuid::new_v4())),
            target_id: target_id.clone(),
            step,
            expected: expected_participants,
            required: self.quorum.required(expected_participants),
            deadline: Instant::now() + self.timeout,
            completed_ranks: HashSet::new(),
            status: BarrierStatus::Pending,
            abort_reason: None,
        };
        by_step.insert((target_id, step), barrier.token.clone());
        barriers.insert(barrier.token.clone(), barrier.clone());
        barrier
    }

    /// Mark a rank complete. Returns the number of completed ranks.
    pub fn rank_complete(&self, token: &CheckpointToken, rank: u32) -> Result<u32, DtrError> {
        let mut barriers = self.barriers.lock().unwrap();
        let barrier = barriers
            .get_mut(token)
            .ok_or_else(|| DtrError::UnknownCheckpoint(token.to_string()))?;

        Self::check_deadline(barrier);
        match barrier.status {
            BarrierStatus::Aborted => {
                return Err(DtrError::CheckpointAborted {
                    token: token.to_string(),
                    reason: barrier
                        .abort_reason
                        .clone()
                        .unwrap_or_else(|| "barrier deadline exceeded".into()),
                })
            }
            BarrierStatus::Committed => {
                return Err(DtrError::CheckpointNotReady {
                    token: token.to_string(),
                    reason: "checkpoint already committed".into(),
                })
            }
            _ => {}
        }

        barrier.completed_ranks.insert(rank);
        if barrier.completed_ranks.len() as u32 >= barrier.required {
            barrier.status = BarrierStatus::Ready;
        }
        Ok(barrier.completed_ranks.len() as u32)
    }

    /// Current status, applying the deadline lazily.
    pub fn status(&self, token: &CheckpointToken) -> Result<BarrierStatus, DtrError> {
        let mut barriers = self.barriers.lock().unwrap();
        let barrier = barriers
            .get_mut(token)
            .ok_or_else(|| DtrError::UnknownCheckpoint(token.to_string()))?;
        Self::check_deadline(barrier);
        Ok(barrier.status)
    }

    pub fn get(&self, token: &CheckpointToken) -> Result<CheckpointBarrier, DtrError> {
        let mut barriers = self.barriers.lock().unwrap();
        let barrier = barriers
            .get_mut(token)
            .ok_or_else(|| DtrError::UnknownCheckpoint(token.to_string()))?;
        Self::check_deadline(barrier);
        Ok(barrier.clone())
    }

    /// Transition Ready -> Committed. Refused while Pending or after abort.
    pub fn mark_committed(&self, token: &CheckpointToken) -> Result<(), DtrError> {
        let mut barriers = self.barriers.lock().unwrap();
        let barrier = barriers
            .get_mut(token)
            .ok_or_else(|| DtrError::UnknownCheckpoint(token.to_string()))?;
        Self::check_deadline(barrier);
        match barrier.status {
            BarrierStatus::Ready | BarrierStatus::Committed => {
                barrier.status = BarrierStatus::Committed;
                Ok(())
            }
            BarrierStatus::Pending => Err(DtrError::CheckpointNotReady {
                token: token.to_string(),
                reason: format!(
                    "{}/{} required ranks reported",
                    barrier.completed_ranks.len(),
                    barrier.required
                ),
            }),
            BarrierStatus::Aborted => Err(DtrError::CheckpointAborted {
                token: token.to_string(),
                reason: barrier
                    .abort_reason
                    .clone()
                    .unwrap_or_else(|| "barrier deadline exceeded".into()),
            }),
        }
    }

    /// Explicit abort (client-initiated or verification failure).
    pub fn abort(
        &self,
        token: &CheckpointToken,
        reason: impl Into<String>,
    ) -> Result<(), DtrError> {
        let mut barriers = self.barriers.lock().unwrap();
        let barrier = barriers
            .get_mut(token)
            .ok_or_else(|| DtrError::UnknownCheckpoint(token.to_string()))?;
        if barrier.status != BarrierStatus::Committed {
            barrier.status = BarrierStatus::Aborted;
            barrier.abort_reason = Some(reason.into());
        }
        Ok(())
    }

    fn check_deadline(barrier: &mut CheckpointBarrier) {
        if barrier.status == BarrierStatus::Pending && Instant::now() > barrier.deadline {
            barrier.status = BarrierStatus::Aborted;
            barrier.abort_reason = Some("barrier deadline exceeded".into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn begin_is_idempotent_per_step() {
        let bm = BarrierManager::new(QuorumPolicy::AllRanks, 10_000);
        let a = bm.begin(TargetId::new("t"), 100, 4);
        let b = bm.begin(TargetId::new("t"), 100, 4);
        assert_eq!(a.token, b.token);
        // Different step opens a different barrier.
        let c = bm.begin(TargetId::new("t"), 200, 4);
        assert_ne!(a.token, c.token);
    }

    #[tokio::test(start_paused = true)]
    async fn all_ranks_quorum_lifecycle() {
        let bm = BarrierManager::new(QuorumPolicy::AllRanks, 10_000);
        let barrier = bm.begin(TargetId::new("t"), 1, 3);
        assert_eq!(barrier.required, 3);

        bm.rank_complete(&barrier.token, 0).unwrap();
        bm.rank_complete(&barrier.token, 1).unwrap();
        // Commit before quorum is refused with a progress message.
        assert!(matches!(
            bm.mark_committed(&barrier.token),
            Err(DtrError::CheckpointNotReady { .. })
        ));

        // Duplicate report from the same rank does not double count.
        bm.rank_complete(&barrier.token, 1).unwrap();
        assert_eq!(bm.status(&barrier.token).unwrap(), BarrierStatus::Pending);

        bm.rank_complete(&barrier.token, 2).unwrap();
        assert_eq!(bm.status(&barrier.token).unwrap(), BarrierStatus::Ready);
        bm.mark_committed(&barrier.token).unwrap();
        assert_eq!(bm.status(&barrier.token).unwrap(), BarrierStatus::Committed);
    }

    #[tokio::test(start_paused = true)]
    async fn fractional_quorum_commits_without_stragglers() {
        let bm = BarrierManager::new(QuorumPolicy::Fraction(0.5), 10_000);
        let barrier = bm.begin(TargetId::new("t"), 1, 4);
        assert_eq!(barrier.required, 2);
        bm.rank_complete(&barrier.token, 0).unwrap();
        bm.rank_complete(&barrier.token, 3).unwrap();
        assert_eq!(bm.status(&barrier.token).unwrap(), BarrierStatus::Ready);
        bm.mark_committed(&barrier.token).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_aborts_pending_barrier() {
        let bm = BarrierManager::new(QuorumPolicy::AllRanks, 5_000);
        let barrier = bm.begin(TargetId::new("t"), 1, 2);
        bm.rank_complete(&barrier.token, 0).unwrap();

        tokio::time::advance(std::time::Duration::from_millis(6_000)).await;
        assert_eq!(bm.status(&barrier.token).unwrap(), BarrierStatus::Aborted);
        assert!(matches!(
            bm.rank_complete(&barrier.token, 1),
            Err(DtrError::CheckpointAborted { .. })
        ));
        assert!(matches!(
            bm.mark_committed(&barrier.token),
            Err(DtrError::CheckpointAborted { .. })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn committed_barrier_cannot_be_aborted() {
        let bm = BarrierManager::new(QuorumPolicy::AllRanks, 10_000);
        let barrier = bm.begin(TargetId::new("t"), 1, 1);
        bm.rank_complete(&barrier.token, 0).unwrap();
        bm.mark_committed(&barrier.token).unwrap();
        bm.abort(&barrier.token, "too late").unwrap();
        assert_eq!(bm.status(&barrier.token).unwrap(), BarrierStatus::Committed);
    }
}
