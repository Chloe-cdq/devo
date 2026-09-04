//! Params and result types for the General Persistent Memory API.

use chrono::DateTime;
use chrono::Utc;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use ts_rs::TS;

use super::ids::{ItemId, MemoryEntryId};
use super::page::Page;

/// Parameters for `memory/status`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryStatusParams {}

/// Scope that owns a memory entry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScope {
    /// Memory shared by the current user across projects.
    #[default]
    User,
    /// Memory associated with the current project identity.
    Project,
}

/// Approved semantic kind for a memory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    Preference,
    Feedback,
    Fact,
    Reference,
}

/// Lifecycle state of a memory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum MemoryState {
    Active,
    Stale,
    Conflicted,
    Retired,
}

/// Provenance summary safe to return with a memory entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryProvenance {
    pub source_session_id: Option<String>,
    pub source_turn_id: Option<String>,
    pub source_user_item_id: Option<ItemId>,
}

/// Canonical, readable projection of one memory entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryEntry {
    pub entry_id: MemoryEntryId,
    pub scope: MemoryScope,
    pub scope_id: String,
    pub kind: MemoryKind,
    pub normalized_key: String,
    pub body: String,
    pub origin: MemoryOrigin,
    pub state: MemoryState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub replacement_entry_id: Option<MemoryEntryId>,
    pub provenance: Vec<MemoryProvenance>,
}

/// Origin of a memory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "snake_case")]
pub enum MemoryOrigin {
    ExplicitUser,
    InferredSession,
}

/// Parameters for an explicit user memory write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryRememberParams {
    pub text: String,
    #[serde(default)]
    pub scope: MemoryScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<MemoryKind>,
    pub source_user_item_id: ItemId,
}

/// Parameters for safe memory inspection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
pub struct MemoryListParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<MemoryScope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<MemoryKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<MemoryState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<MemoryOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Result returned by `memory/list`.
pub type MemoryListResult = Page<MemoryEntry>;

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
    /// Completion time of the most recent successful source scan.
    pub last_successful_scan_at: Option<DateTime<Utc>>,
    /// Distinct redacted failure classifications for current error jobs.
    pub error_classes: Vec<String>,
}
