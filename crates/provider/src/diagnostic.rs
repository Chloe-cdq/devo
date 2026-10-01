//! Safe provider failure boundaries and stable diagnostic classification.

use crate::SensitiveErrorText;
use crate::error::ProviderError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::ErrorKind;

/// Stable diagnostic category, computed before private error text is isolated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ErrorClass {
    ContextTooLong,
    ParameterError,
    FileContentAnomaly,
    AuthenticationFailure,
    FeatureUnavailable,
    TaskNotFound,
    RateLimit,
    NoApiPermission,
    FileTooLarge,
    ServerError,
    NetworkError,
    Unretryable,
}

/// An SDK failure with isolated private details and safe diagnostic sources.
/// Classification and recovery guidance are captured once at the boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticError {
    pub(crate) class: ErrorClass,
    details: SensitiveErrorText,
    recovery_hint: Option<SensitiveErrorText>,
    pub(crate) source: Option<Box<ProviderError>>,
}

impl fmt::Display for DiagnosticError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "provider failure ({:?})", self.class)
    }
}

impl std::error::Error for DiagnosticError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|error| error as &dyn std::error::Error)
    }
}

impl DiagnosticError {
    pub(crate) fn user_message(&self) -> String {
        self.details.expose().to_string()
    }
    pub(crate) fn recovery_hint(&self) -> Option<&str> {
        self.recovery_hint.as_ref().map(SensitiveErrorText::expose)
    }
    pub(crate) fn error_code(&self) -> &'static str {
        self.source
            .as_deref()
            .map_or("UNKNOWN_ERROR", ProviderError::error_code)
    }
}

/// Read private provider details for a user-facing payload. Never use in logs.
pub fn user_message_for_error(error: &anyhow::Error) -> String {
    let mut messages = Vec::new();
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<ProviderError>() {
            messages.push(error.user_message());
            break;
        }
        messages.push(cause.to_string());
    }
    messages.join(": ")
}

/// Normalize a failure without retaining unsafe context or source formatting.
pub(crate) fn normalize_error(error: anyhow::Error) -> ProviderError {
    if let Some(source) = error
        .chain()
        .next()
        .and_then(|cause| cause.downcast_ref::<ProviderError>())
        && !matches!(source, ProviderError::UnknownError { .. })
    {
        return source.clone();
    }
    ProviderError::Diagnostic(DiagnosticError {
        class: classify_error(&error),
        recovery_hint: crate::recovery_hint_for_anyhow(&error).map(Into::into),
        details: user_message_for_error(&error).into(),
        source: error
            .chain()
            .find_map(|cause| cause.downcast_ref::<ProviderError>())
            .cloned()
            .map(Box::new),
    })
}

/// Isolate SDK failures, including stream items. Default formatting and the
/// complete source chain are safe to log; details require an explicit projection.
pub fn sanitize_error(error: anyhow::Error) -> anyhow::Error {
    anyhow::Error::new(normalize_error(error))
}

/// Classify structured errors; compatibility text is read only at the boundary.
pub fn classify_error(e: &anyhow::Error) -> ErrorClass {
    for cause in e.chain() {
        let Some(provider_error) = cause.downcast_ref::<ProviderError>() else {
            continue;
        };
        match provider_error {
            ProviderError::Diagnostic(error) => return error.class,
            ProviderError::AuthenticationError { .. } => return ErrorClass::AuthenticationFailure,
            ProviderError::RateLimitError { .. } => return ErrorClass::RateLimit,
            ProviderError::ProviderServerError {
                status_code: Some(429),
                ..
            } => return ErrorClass::RateLimit,
            ProviderError::ProviderServerError {
                status_code: Some(408),
                ..
            }
            | ProviderError::ProviderTimeoutError { .. }
            | ProviderError::StreamError { .. } => return ErrorClass::NetworkError,
            ProviderError::ProviderServerError { .. } => return ErrorClass::ServerError,
            ProviderError::ContextLimitError { .. } => return ErrorClass::ContextTooLong,
            ProviderError::ModelNotFoundError { .. } => return ErrorClass::TaskNotFound,
            ProviderError::InvalidRequestError { .. } => return ErrorClass::ParameterError,
            ProviderError::QuotaExceededError { .. }
            | ProviderError::ContentFilteredError { .. } => {
                return ErrorClass::Unretryable;
            }
            ProviderError::UnknownError {
                status_code: Some(429),
                ..
            } => return ErrorClass::RateLimit,
            ProviderError::UnknownError {
                status_code: Some(408),
                ..
            } => return ErrorClass::NetworkError,
            ProviderError::UnknownError {
                status_code: Some(500..=599),
                ..
            } => return ErrorClass::ServerError,
            ProviderError::UnknownError { .. } => {}
        }
    }

    for cause in e.chain() {
        let Some(error) = cause.downcast_ref::<reqwest::Error>() else {
            continue;
        };
        if error.is_status()
            && let Some(status) = error.status()
        {
            return match status.as_u16() {
                401 | 403 => ErrorClass::AuthenticationFailure,
                404 => ErrorClass::TaskNotFound,
                408 => ErrorClass::NetworkError,
                429 => ErrorClass::RateLimit,
                500..=599 => ErrorClass::ServerError,
                400..=499 => ErrorClass::ParameterError,
                _ => ErrorClass::Unretryable,
            };
        }
        if error.is_timeout() || error.is_connect() || error.is_decode() || error.is_body() {
            return ErrorClass::NetworkError;
        }
    }

    if e.chain().any(|cause| {
        cause.downcast_ref::<std::io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                ErrorKind::TimedOut
                    | ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::NotConnected
                    | ErrorKind::BrokenPipe
                    | ErrorKind::UnexpectedEof
            )
        })
    }) {
        return ErrorClass::NetworkError;
    }

    // Response JSON failures are transport/decode failures. Their private values
    // must not impersonate status codes or credentials in compatibility text.
    if e.chain()
        .any(|cause| cause.downcast_ref::<serde_json::Error>().is_some())
    {
        return ErrorClass::NetworkError;
    }

    let msg = user_message_for_error(e).to_lowercase();
    // TODO: Expand the error of ContextTooLong
    if msg.contains("context_too_long")
        || msg.contains("context_length_exceeded")
        || msg.contains("maximum context length")
    {
        ErrorClass::ContextTooLong
    } else if msg.contains("401")
        || msg.contains("authentication failure")
        || msg.contains("token timeout")
        || msg.contains("unauthorized")
        || msg.contains("api key")
    {
        ErrorClass::AuthenticationFailure
    } else if msg.contains("404")
        && (msg.contains("feature not available")
            || msg.contains("fine-tuning feature not available"))
    {
        ErrorClass::FeatureUnavailable
    } else if msg.contains("404")
        && (msg.contains("task does not exist")
            || msg.contains("does not exist")
            || msg.contains("not found"))
    {
        ErrorClass::TaskNotFound
    } else if msg.contains("429") || msg.contains("rate limit") {
        ErrorClass::RateLimit
    } else if msg.contains("434") || msg.contains("no api permission") || msg.contains("beta phase")
    {
        ErrorClass::NoApiPermission
    } else if msg.contains("435")
        || msg.contains("file size exceeds 100mb")
        || msg.contains("smaller than 100mb")
    {
        ErrorClass::FileTooLarge
    } else if msg.contains("400")
        && (msg.contains("file content anomaly")
            || msg.contains("jsonl file content")
            || msg.contains("jsonl"))
    {
        ErrorClass::FileContentAnomaly
    } else if msg.contains("408")
        || msg.contains("request timeout")
        || msg.contains("request timed out")
        || msg.contains("operation timed out")
        || msg.contains("timed out")
        || msg.contains("deadline has elapsed")
        || msg.contains("deadline exceeded")
        || msg.contains("provider timeout")
        || msg.contains("stream idle timeout")
        || msg.contains("network error")
        || msg.contains("network is unreachable")
        || msg.contains("network unreachable")
        || msg.contains("host unreachable")
        || msg.contains("destination unreachable")
        || msg.contains("unreachable host")
        || msg.contains("no route to host")
        || msg.contains("connection refused")
        || msg.contains("connection reset")
        || msg.contains("connection closed")
        || msg.contains("connection aborted")
        || msg.contains("connection timed out")
        || msg.contains("connection failure")
        || msg.contains("connection failed")
        || msg.contains("failed to connect")
        || msg.contains("connect error")
        || msg.contains("error trying to connect")
        || msg.contains("error sending request")
        || msg.contains("dns error")
        || msg.contains("failed to lookup address information")
        || msg.contains("temporary failure in name resolution")
        || msg.contains("name or service not known")
        || msg.contains("nodename nor servname")
        || msg.contains("could not resolve host")
        || msg.contains("unexpected eof")
        || msg.contains("invalidcontenttype")
        || msg.contains("invalid content-type")
        || msg.contains("invalid header value")
        || msg.contains("text/event-stream")
        // Stream-level decode/decrypt errors (e.g. TLS decrypt failure, chunk
        // deserialization).  These are typically transient — the proxy or
        // TLS-terminator that sits between us and the provider may have had a
        // hiccup; retrying usually succeeds.
        || msg.contains("error decoding")
        || msg.contains("decoding response")
        || msg.contains("cannot decrypt")
        || msg.contains("decrypt error")
        || msg.contains("decrypterror")
        || msg.contains("stream error")
        || msg.contains("failed to decode")
    {
        ErrorClass::NetworkError
    } else if msg.contains("400")
        || msg.contains("parameter error")
        || msg.contains("invalid parameter")
        || msg.contains("bad request")
    {
        ErrorClass::ParameterError
    } else if msg.starts_with('5')
        || msg.contains("500")
        || msg.contains("502")
        || msg.contains("503")
        || msg.contains("504")
        || msg.contains("internal server error")
        || msg.contains("server error occurred while processing the request")
    {
        ErrorClass::ServerError
    } else {
        ErrorClass::Unretryable
    }
}
