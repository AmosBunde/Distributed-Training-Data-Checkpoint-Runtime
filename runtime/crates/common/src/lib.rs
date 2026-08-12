//! Shared foundation for the DTR runtime: typed configuration, the error
//! taxonomy, and core domain identifiers used by every other crate.

pub mod config;
pub mod error;
pub mod types;

pub use config::RuntimeConfig;
pub use error::DtrError;
