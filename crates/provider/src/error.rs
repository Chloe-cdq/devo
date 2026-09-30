//! Structured provider error classification.
//!
//! Implements L3-BEH-PROVIDER-001 §B6. Classifies provider failures into
//! recoverable and non-recoverable categories with retry hints.

use crate::SensitiveErrorText;
use crate::diagnostic::{DiagnosticError, ErrorClass};
use serde::{Deserialize, Serialize};

/// Structured error from a model provider invocation.
#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "error_kind", rename_all = "snake_case")]
pub enum ProviderError {
    /// An SDK failure with isolated private details and safe diagnostic sources.
    #[error(transparent)]
    Diagnostic(DiagnosticError),
    #[error("authentication failed: {message}")]
    AuthenticationError {
        message: SensitiveErrorText,
        provider_name: Option<String>,
        status_code: Option<u16>,
    },

    #[error("rate limited: {message}")]
    RateLimitError {
        message: SensitiveErrorText,
        retry_after_seconds: Option<u64>,
        provider_name: Option<String>,
    },

    #[error("provider server error ({status_code:?}): {message}")]
    ProviderServerError {
        message: SensitiveErrorText,
        status_code: Option<u16>,
        provider_name: Option<String>,
    },

    #[error("provider timeout: {message}")]
    ProviderTimeoutError {
        message: SensitiveErrorText,
        provider_name: Option<String>,
    },

    #[error("context limit exceeded: {message}")]
    ContextLimitError {
        message: SensitiveErrorText,
        current_tokens: Option<u64>,
        limit: Option<u64>,
    },

    #[error("model not found: {model_name:?} — {message}")]
    ModelNotFoundError {
        message: SensitiveErrorText,
        model_name: Option<SensitiveErrorText>,
    },

    #[error("quota exceeded: {message}")]
    QuotaExceededError {
        message: SensitiveErrorText,
        provider_name: Option<String>,
    },

    #[error("content filtered: {message}")]
    ContentFilteredError {
        message: SensitiveErrorText,
        finish_reason: Option<SensitiveErrorText>,
    },

    #[error("invalid request: {message}")]
    InvalidRequestError {
        message: SensitiveErrorText,
        details: Option<SensitiveErrorText>,
    },

    #[error("stream error: {message}")]
    StreamError {
        message: SensitiveErrorText,
        bytes_received: Option<u64>,
    },

    #[error("unknown provider error: {message}")]
    UnknownError {
        message: SensitiveErrorText,
        status_code: Option<u16>,
    },
}

impl ProviderError {
    /// Full details for a user-facing error payload. Never use in diagnostic logs.
    pub fn user_message(&self) -> String {
        match self {
            Self::Diagnostic(error) => error.user_message(),
            Self::AuthenticationError { message, .. } => {
                format!("authentication failed: {}", message.expose())
            }
            Self::RateLimitError { message, .. } => format!("rate limited: {}", message.expose()),
            Self::ProviderServerError {
                message,
                status_code,
                ..
            } => format!(
                "provider server error ({status_code:?}): {}",
                message.expose()
            ),
            Self::ProviderTimeoutError { message, .. } => {
                format!("provider timeout: {}", message.expose())
            }
            Self::ContextLimitError { message, .. } => {
                format!("context limit exceeded: {}", message.expose())
            }
            Self::ModelNotFoundError {
                message,
                model_name,
            } => {
                let model_name = model_name.as_ref().map(SensitiveErrorText::expose);
                format!("model not found: {model_name:?} — {}", message.expose())
            }
            Self::QuotaExceededError { message, .. } => {
                format!("quota exceeded: {}", message.expose())
            }
            Self::ContentFilteredError { message, .. } => {
                format!("content filtered: {}", message.expose())
            }
            Self::InvalidRequestError { message, .. } => {
                format!("invalid request: {}", message.expose())
            }
            Self::StreamError { message, .. } => format!("stream error: {}", message.expose()),
            Self::UnknownError { message, .. } => {
                format!("unknown provider error: {}", message.expose())
            }
        }
    }

    /// Whether retrying the request may succeed.
    pub fn is_recoverable(&self) -> bool {
        if let Self::Diagnostic(error) = self {
            return error.source.as_deref().map_or(
                matches!(
                    error.class,
                    ErrorClass::RateLimit | ErrorClass::ServerError | ErrorClass::NetworkError
                ),
                Self::is_recoverable,
            );
        }
        matches!(
            self,
            Self::RateLimitError { .. }
                | Self::ProviderServerError { .. }
                | Self::ProviderTimeoutError { .. }
                | Self::StreamError { .. }
        )
    }

    /// Whether this is a transient error that should be retried with backoff.
    pub fn is_transient(&self) -> bool {
        if let Self::Diagnostic(error) = self {
            return error.source.as_deref().map_or(
                matches!(
                    error.class,
                    ErrorClass::RateLimit | ErrorClass::ServerError | ErrorClass::NetworkError
                ),
                Self::is_transient,
            );
        }
        matches!(
            self,
            Self::RateLimitError { .. }
                | Self::ProviderTimeoutError { .. }
                | Self::ProviderServerError {
                    status_code: Some(429),
                    ..
                }
                | Self::ProviderServerError {
                    status_code: Some(502),
                    ..
                }
                | Self::ProviderServerError {
                    status_code: Some(503),
                    ..
                }
                | Self::ProviderServerError {
                    status_code: Some(504),
                    ..
                }
        )
    }

    /// Suggested retry delay in seconds from the provider.
    pub fn retry_after_seconds(&self) -> Option<u64> {
        match self {
            Self::Diagnostic(error) => error.source.as_deref().and_then(Self::retry_after_seconds),
            Self::RateLimitError {
                retry_after_seconds,
                ..
            } => *retry_after_seconds,
            _ => None,
        }
    }

    /// Whether the error should be surfaced to the user.
    pub fn is_user_facing(&self) -> bool {
        if let Self::Diagnostic(error) = self {
            return error.source.as_deref().is_none_or(Self::is_user_facing);
        }
        !matches!(self, Self::StreamError { .. })
    }

    /// Machine-readable error code.
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::Diagnostic(error) => error.error_code(),
            Self::AuthenticationError { .. } => "AUTHENTICATION_ERROR",
            Self::RateLimitError { .. } => "RATE_LIMIT_ERROR",
            Self::ProviderServerError { .. } => "PROVIDER_SERVER_ERROR",
            Self::ProviderTimeoutError { .. } => "PROVIDER_TIMEOUT_ERROR",
            Self::ContextLimitError { .. } => "CONTEXT_LIMIT_ERROR",
            Self::ModelNotFoundError { .. } => "MODEL_NOT_FOUND_ERROR",
            Self::QuotaExceededError { .. } => "QUOTA_EXCEEDED_ERROR",
            Self::ContentFilteredError { .. } => "CONTENT_FILTERED_ERROR",
            Self::InvalidRequestError { .. } => "INVALID_REQUEST_ERROR",
            Self::StreamError { .. } => "STREAM_ERROR",
            Self::UnknownError { .. } => "UNKNOWN_ERROR",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypedFailureKind {
    ContextLimit,
    Authentication,
    RateLimit,
    Server,
}

pub(crate) fn typed_failure_kind(
    error_kind: Option<&str>,
    error_code: Option<&str>,
) -> Option<TypedFailureKind> {
    [error_code, error_kind]
        .into_iter()
        .flatten()
        .find_map(|value| {
            let value = value.to_ascii_lowercase();
            if value.contains("context_length_exceeded") || value.contains("context_too_long") {
                Some(TypedFailureKind::ContextLimit)
            } else if value.contains("authentication_error")
                || value.contains("invalid_api_key")
                || value.contains("unauthorized")
            {
                Some(TypedFailureKind::Authentication)
            } else if value.contains("rate_limit") || value.contains("too_many_requests") {
                Some(TypedFailureKind::RateLimit)
            } else if value.contains("server_error") || value.contains("internal_error") {
                Some(TypedFailureKind::Server)
            } else {
                None
            }
        })
}

pub(crate) fn context_limit_error(
    message: String,
    status_code: Option<u16>,
    typed_kind: Option<TypedFailureKind>,
) -> Option<ProviderError> {
    if status_code.is_some_and(|status| !matches!(status, 400 | 413 | 422)) {
        return None;
    }
    let normalized_message = message.to_ascii_lowercase();
    let message_matches = normalized_message.contains("maximum context length")
        || normalized_message.contains("context_length_exceeded")
        || normalized_message.contains("context_too_long")
        || (normalized_message.contains("context window")
            && (normalized_message.contains("exceeded")
                || normalized_message.contains("too long")));
    (typed_kind == Some(TypedFailureKind::ContextLimit)
        || (typed_kind.is_none() && message_matches))
        .then_some(ProviderError::ContextLimitError {
            message: message.into(),
            current_tokens: None,
            limit: None,
        })
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn rate_limit_is_recoverable() {
        let err = ProviderError::RateLimitError {
            message: "slow down".into(),
            retry_after_seconds: Some(30),
            provider_name: Some("anthropic".into()),
        };
        assert!(err.is_recoverable());
        assert!(err.is_transient());
        assert_eq!(err.retry_after_seconds(), Some(30));
    }

    #[test]
    fn auth_error_is_not_recoverable() {
        let err = ProviderError::AuthenticationError {
            message: "bad api key".into(),
            provider_name: Some("openai".into()),
            status_code: Some(401),
        };
        assert!(!err.is_recoverable());
        assert!(!err.is_transient());
        assert_eq!(err.error_code(), "AUTHENTICATION_ERROR");
    }

    #[test]
    fn server_error_503_is_transient() {
        let err = ProviderError::ProviderServerError {
            message: "service unavailable".into(),
            status_code: Some(503),
            provider_name: None,
        };
        assert!(err.is_transient());
    }

    #[test]
    fn context_limit_is_user_facing() {
        let err = ProviderError::ContextLimitError {
            message: "too many tokens".into(),
            current_tokens: Some(250000),
            limit: Some(200000),
        };
        assert!(err.is_user_facing());
        assert!(!err.is_recoverable());
    }

    #[test]
    fn stream_error_is_not_user_facing() {
        let err = ProviderError::StreamError {
            message: "connection reset".into(),
            bytes_received: Some(1024),
        };
        assert!(!err.is_user_facing());
        assert!(err.is_recoverable());
    }

    #[test]
    fn all_variants_have_distinct_codes() {
        let mut codes = std::collections::HashSet::new();
        let errors = vec![
            ProviderError::AuthenticationError {
                message: "".into(),
                provider_name: None,
                status_code: None,
            },
            ProviderError::RateLimitError {
                message: "".into(),
                retry_after_seconds: None,
                provider_name: None,
            },
            ProviderError::ProviderServerError {
                message: "".into(),
                status_code: None,
                provider_name: None,
            },
            ProviderError::ProviderTimeoutError {
                message: "".into(),
                provider_name: None,
            },
            ProviderError::ContextLimitError {
                message: "".into(),
                current_tokens: None,
                limit: None,
            },
            ProviderError::ModelNotFoundError {
                message: "".into(),
                model_name: None,
            },
            ProviderError::QuotaExceededError {
                message: "".into(),
                provider_name: None,
            },
            ProviderError::ContentFilteredError {
                message: "".into(),
                finish_reason: None,
            },
            ProviderError::InvalidRequestError {
                message: "".into(),
                details: None,
            },
            ProviderError::StreamError {
                message: "".into(),
                bytes_received: None,
            },
            ProviderError::UnknownError {
                message: "".into(),
                status_code: None,
            },
        ];
        for err in &errors {
            assert!(
                codes.insert(err.error_code()),
                "duplicate code: {}",
                err.error_code()
            );
        }
    }

    #[test]
    fn provider_error_serde_roundtrip() {
        let err = ProviderError::RateLimitError {
            message: "slow down".into(),
            retry_after_seconds: Some(30),
            provider_name: Some("anthropic".into()),
        };
        let json = serde_json::to_string(&err).expect("serialize");
        let restored: ProviderError = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored.error_code(), "RATE_LIMIT_ERROR");
    }
}
