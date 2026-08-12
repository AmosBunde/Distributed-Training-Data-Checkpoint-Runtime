//! Core domain identifiers and value types.
//!
//! Newtypes keep worker/dataset/lease/checkpoint identifiers from being mixed
//! up at call sites; `ShardRange` carries the half-open range invariant.

use serde::{Deserialize, Serialize};
use std::fmt;

macro_rules! id_newtype {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                Self(s.to_owned())
            }
        }
    };
}

id_newtype!(
    /// Stable worker identity chosen by the trainer (e.g. "rank-0").
    WorkerId
);
id_newtype!(
    /// Coordinator-assigned dataset handle.
    DatasetId
);
id_newtype!(
    /// Handle for one TTL-bound shard lease.
    LeaseId
);
id_newtype!(
    /// Barrier token shared by all ranks of one checkpoint step.
    CheckpointToken
);
id_newtype!(
    /// Handle for a registered checkpoint destination.
    TargetId
);

/// A contiguous half-open range of shard indices `[begin, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardRange {
    pub begin: u32,
    pub end: u32,
}

impl ShardRange {
    /// Construct a range, enforcing `begin <= end`.
    pub fn new(begin: u32, end: u32) -> Result<Self, crate::DtrError> {
        if begin > end {
            return Err(crate::DtrError::InvalidArgument(format!(
                "shard range begin ({begin}) must be <= end ({end})"
            )));
        }
        Ok(Self { begin, end })
    }

    pub fn len(&self) -> u32 {
        self.end - self.begin
    }

    pub fn is_empty(&self) -> bool {
        self.begin == self.end
    }

    pub fn contains(&self, shard: u32) -> bool {
        shard >= self.begin && shard < self.end
    }
}

/// Quorum policy for checkpoint barriers.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "policy", content = "value")]
pub enum QuorumPolicy {
    /// Every expected participant must report before commit.
    AllRanks,
    /// A fraction (0.0, 1.0] of expected participants suffices.
    Fraction(f64),
}

impl QuorumPolicy {
    /// Number of participants required to commit given the expected set size.
    pub fn required(&self, expected: u32) -> u32 {
        match self {
            QuorumPolicy::AllRanks => expected,
            QuorumPolicy::Fraction(f) => {
                let req = (f * expected as f64).ceil() as u32;
                req.clamp(1, expected)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_range_enforces_order() {
        assert!(ShardRange::new(5, 3).is_err());
        let r = ShardRange::new(3, 7).unwrap();
        assert_eq!(r.len(), 4);
        assert!(r.contains(3) && r.contains(6));
        assert!(!r.contains(7));
    }

    #[test]
    fn empty_range_is_valid() {
        let r = ShardRange::new(4, 4).unwrap();
        assert!(r.is_empty());
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn quorum_all_ranks() {
        assert_eq!(QuorumPolicy::AllRanks.required(8), 8);
    }

    #[test]
    fn quorum_fraction_rounds_up_and_clamps() {
        assert_eq!(QuorumPolicy::Fraction(0.5).required(7), 4); // ceil(3.5)
        assert_eq!(QuorumPolicy::Fraction(0.01).required(8), 1); // floor at 1
        assert_eq!(QuorumPolicy::Fraction(1.0).required(8), 8);
    }

    #[test]
    fn ids_are_distinct_types() {
        // Compile-time property; runtime sanity that display round-trips.
        let w = WorkerId::new("rank-0");
        assert_eq!(w.to_string(), "rank-0");
        assert_eq!(w.as_str(), "rank-0");
    }
}
