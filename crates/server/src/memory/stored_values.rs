use chrono::{DateTime, Utc};
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryOrigin, MemoryScope, MemoryState};

use super::MemoryError;

pub(super) fn parse_scope(value: &str) -> Result<MemoryScope, MemoryError> {
    match value {
        "user" => Ok(MemoryScope::User),
        "project" => Ok(MemoryScope::Project),
        _ => Err(MemoryError::InvalidStoredValue(value.into())),
    }
}

pub(super) fn parse_kind(value: &str) -> Result<MemoryKind, MemoryError> {
    match value {
        "preference" => Ok(MemoryKind::Preference),
        "feedback" => Ok(MemoryKind::Feedback),
        "fact" => Ok(MemoryKind::Fact),
        "reference" => Ok(MemoryKind::Reference),
        _ => Err(MemoryError::InvalidStoredValue(value.into())),
    }
}

pub(super) fn parse_state(value: &str) -> Result<MemoryState, MemoryError> {
    match value {
        "active" => Ok(MemoryState::Active),
        "stale" => Ok(MemoryState::Stale),
        "conflicted" => Ok(MemoryState::Conflicted),
        "retired" => Ok(MemoryState::Retired),
        "restored" => Ok(MemoryState::Restored),
        _ => Err(MemoryError::InvalidStoredValue(value.into())),
    }
}

pub(super) fn parse_origin(value: &str) -> Result<MemoryOrigin, MemoryError> {
    match value {
        "explicit_user" => Ok(MemoryOrigin::ExplicitUser),
        "inferred_session" => Ok(MemoryOrigin::InferredSession),
        _ => Err(MemoryError::InvalidStoredValue(value.into())),
    }
}

pub(super) fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, MemoryError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| MemoryError::InvalidTimestamp(value.into()))
}
