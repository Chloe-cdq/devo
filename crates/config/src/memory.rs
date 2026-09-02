//! Configuration for the server-owned General Persistent Memory runtime.

use devo_protocol::native::session::MemorySetting;
use serde::Deserialize;
use serde::Serialize;

/// Global General Persistent Memory settings.
///
/// The `enabled` gate is authoritative: when it is `false`, both effective
/// recall and contribution are disabled even if their configured defaults are
/// `on`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Master feature gate. Defaults to disabled for privacy and safety.
    #[serde(default)]
    pub enabled: bool,
    /// Default recall setting for sessions whose setting is `inherit`.
    #[serde(default = "default_memory_setting_on")]
    pub default_recall: MemorySetting,
    /// Default contribution setting for sessions whose setting is `inherit`.
    #[serde(default = "default_memory_setting_on")]
    pub default_contribution: MemorySetting,
    /// Minimum age of a source session before it can be scanned.
    #[serde(default = "default_min_source_idle_hours")]
    pub min_source_idle_hours: u64,
    /// Maximum age of source sessions considered for extraction.
    #[serde(default = "default_source_window_days")]
    pub source_window_days: u64,
    /// Age after which inferred entries become stale.
    #[serde(default = "default_inferred_stale_after_days")]
    pub inferred_stale_after_days: u64,
    /// Retention period for candidates and jobs.
    #[serde(default = "default_candidate_and_job_retention_days")]
    pub candidate_and_job_retention_days: u64,
    /// Maximum source sessions considered by one background scan.
    #[serde(default = "default_max_sources_per_scan")]
    pub max_sources_per_scan: u32,
    /// Maximum memory entries injected into one turn.
    #[serde(default = "default_max_entries_per_turn")]
    pub max_entries_per_turn: u32,
    /// Maximum prompt tokens reserved for memory recall.
    #[serde(default = "default_max_prompt_tokens")]
    pub max_prompt_tokens: u32,
    /// Minimum remaining provider rate-limit percentage required for scans.
    #[serde(default = "default_min_rate_limit_remaining_percent")]
    pub min_rate_limit_remaining_percent: u8,
    /// Optional model binding used by future extraction jobs.
    #[serde(default)]
    pub extract_model: Option<String>,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_recall: MemorySetting::On,
            default_contribution: MemorySetting::On,
            min_source_idle_hours: default_min_source_idle_hours(),
            source_window_days: default_source_window_days(),
            inferred_stale_after_days: default_inferred_stale_after_days(),
            candidate_and_job_retention_days: default_candidate_and_job_retention_days(),
            max_sources_per_scan: default_max_sources_per_scan(),
            max_entries_per_turn: default_max_entries_per_turn(),
            max_prompt_tokens: default_max_prompt_tokens(),
            min_rate_limit_remaining_percent: default_min_rate_limit_remaining_percent(),
            extract_model: None,
        }
    }
}

impl MemoryConfig {
    /// Resolves the global recall setting, applying the authoritative gate.
    pub fn effective_recall(&self) -> MemorySetting {
        if self.enabled {
            self.default_recall
        } else {
            MemorySetting::Off
        }
    }

    /// Resolves the global contribution setting, applying the authoritative
    /// gate.
    pub fn effective_contribution(&self) -> MemorySetting {
        if self.enabled {
            self.default_contribution
        } else {
            MemorySetting::Off
        }
    }
}

const fn default_memory_setting_on() -> MemorySetting {
    MemorySetting::On
}

const fn default_min_source_idle_hours() -> u64 {
    6
}

const fn default_source_window_days() -> u64 {
    30
}

const fn default_inferred_stale_after_days() -> u64 {
    90
}

const fn default_candidate_and_job_retention_days() -> u64 {
    30
}

const fn default_max_sources_per_scan() -> u32 {
    2
}

const fn default_max_entries_per_turn() -> u32 {
    12
}

const fn default_max_prompt_tokens() -> u32 {
    2_000
}

const fn default_min_rate_limit_remaining_percent() -> u8 {
    25
}
