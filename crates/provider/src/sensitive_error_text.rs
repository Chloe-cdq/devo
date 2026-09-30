//! Private error text with safe default representations.

use serde::{Deserialize, Serialize, Serializer};
use std::fmt;

/// Provider response text intended only for explicit user-facing projections.
/// Formatting and serialization redact it, including when embedded in errors.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct SensitiveErrorText(String);

impl SensitiveErrorText {
    /// Expose private text for a user-facing payload, never a log.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl From<String> for SensitiveErrorText {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SensitiveErrorText {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for SensitiveErrorText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

impl fmt::Debug for SensitiveErrorText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[redacted]")
    }
}

impl Serialize for SensitiveErrorText {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("[redacted]")
    }
}
