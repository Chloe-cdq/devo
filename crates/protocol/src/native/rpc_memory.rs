//! Params and result types for the General Persistent Memory API.

use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

/// Parameters for `memory/status`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryStatusParams {}

/// Safe, aggregate memory health information exposed to Native clients.
///
/// Counts intentionally exclude row contents and error details. The server
/// may report `unavailable` while ordinary session operations continue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryStatus {
    /// Effective global memory state after applying the global feature gate.
    pub enabled: bool,
    /// Redacted storage health classification, such as `healthy` or
    /// `unavailable`.
    pub storage_health: String,
    pub entry_count: u64,
    pub candidate_count: u64,
    pub pending_job_count: u64,
    pub retrying_job_count: u64,
    pub error_job_count: u64,
}
