//! Provider diagnostics must stay safe when callers format or serialize errors.

use devo_provider::error::ProviderError;
use pretty_assertions::assert_eq;

const PRIVATE_TEXT: &str = "Quoted memory: Use tabs. Private earlier conversation";

#[test]
fn provider_error_default_representations_omit_sensitive_details() {
    let errors = [
        ProviderError::AuthenticationError {
            message: PRIVATE_TEXT.into(),
            provider_name: Some("openai".into()),
            status_code: Some(401),
        },
        ProviderError::RateLimitError {
            message: PRIVATE_TEXT.into(),
            retry_after_seconds: Some(30),
            provider_name: Some("openai".into()),
        },
        ProviderError::ProviderServerError {
            message: PRIVATE_TEXT.into(),
            status_code: Some(500),
            provider_name: Some("openai".into()),
        },
        ProviderError::ProviderTimeoutError {
            message: PRIVATE_TEXT.into(),
            provider_name: Some("openai".into()),
        },
        ProviderError::ContextLimitError {
            message: PRIVATE_TEXT.into(),
            current_tokens: Some(200_000),
            limit: Some(128_000),
        },
        ProviderError::ModelNotFoundError {
            message: PRIVATE_TEXT.into(),
            model_name: Some(PRIVATE_TEXT.into()),
        },
        ProviderError::QuotaExceededError {
            message: PRIVATE_TEXT.into(),
            provider_name: Some("openai".into()),
        },
        ProviderError::ContentFilteredError {
            message: PRIVATE_TEXT.into(),
            finish_reason: Some(PRIVATE_TEXT.into()),
        },
        ProviderError::InvalidRequestError {
            message: PRIVATE_TEXT.into(),
            details: Some(PRIVATE_TEXT.into()),
        },
        ProviderError::StreamError {
            message: PRIVATE_TEXT.into(),
            bytes_received: Some(128),
        },
        ProviderError::UnknownError {
            message: PRIVATE_TEXT.into(),
            status_code: Some(400),
        },
    ];
    let mut leaks = Vec::new();
    for error in errors {
        assert!(error.user_message().contains(PRIVATE_TEXT));
        let code = error.error_code();
        let json = serde_json::to_string(&error).expect("serialize provider error");
        let error = anyhow::Error::new(error).context("provider request failed");
        for (representation, output) in [
            ("display", format!("{error}")),
            ("display_chain", format!("{error:#}")),
            ("debug", format!("{error:?}")),
            ("debug_struct", format!("{error:#?}")),
            ("json", json),
        ] {
            if output.contains(PRIVATE_TEXT) {
                leaks.push((code, representation));
            }
        }
    }
    assert_eq!(leaks, Vec::new());
}

#[test]
fn normalization_is_log_safe_idempotent_and_preserves_diagnostics() {
    use devo_provider::diagnostic::{
        ErrorClass, classify_error, sanitize_error, user_message_for_error,
    };
    use devo_provider::{AUTH_HINT, NETWORK_PROXY_HINT, recovery_hint_for_anyhow};

    let cases = [
        (
            anyhow::Error::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                PRIVATE_TEXT,
            ))
            .context("private source boundary"),
            ErrorClass::NetworkError,
            None,
            "UNKNOWN_ERROR",
        ),
        (
            anyhow::Error::new(ProviderError::AuthenticationError {
                message: PRIVATE_TEXT.into(),
                provider_name: Some("openai".into()),
                status_code: Some(401),
            })
            .context("private provider boundary"),
            ErrorClass::AuthenticationFailure,
            Some(AUTH_HINT),
            "AUTHENTICATION_ERROR",
        ),
        (
            anyhow::anyhow!("connection reset: {PRIVATE_TEXT}"),
            ErrorClass::NetworkError,
            Some(NETWORK_PROXY_HINT),
            "UNKNOWN_ERROR",
        ),
        (
            anyhow::anyhow!("500 internal server error: {PRIVATE_TEXT}"),
            ErrorClass::ServerError,
            None,
            "UNKNOWN_ERROR",
        ),
        (
            anyhow::anyhow!("context_length_exceeded: {PRIVATE_TEXT}"),
            ErrorClass::ContextTooLong,
            None,
            "UNKNOWN_ERROR",
        ),
    ];
    for (raw_error, class, hint, code) in cases {
        let expected_details = user_message_for_error(&raw_error);
        assert_eq!(
            (
                classify_error(&raw_error),
                recovery_hint_for_anyhow(&raw_error)
            ),
            (class, hint.map(str::to_string))
        );
        let error = sanitize_error(sanitize_error(raw_error));
        let provider_error = error
            .downcast_ref::<ProviderError>()
            .expect("safe provider failure");
        assert_eq!(
            (
                classify_error(&error),
                recovery_hint_for_anyhow(&error),
                provider_error.error_code(),
                user_message_for_error(&error)
            ),
            (class, hint.map(str::to_string), code, expected_details)
        );
        let outputs = [
            format!("{error}"),
            format!("{error:#}"),
            format!("{error:?}"),
            format!("{error:#?}"),
            serde_json::to_string(provider_error).expect("safe serialization"),
        ];
        assert_eq!(
            outputs
                .iter()
                .filter(|text| text.contains(PRIVATE_TEXT)
                    || text.contains("private source boundary")
                    || text.contains("private provider boundary"))
                .collect::<Vec<_>>(),
            Vec::<&String>::new()
        );
    }
}

#[test]
fn normalization_preserves_structured_recovery_metadata() {
    use devo_provider::diagnostic::sanitize_error;
    for original in [
        ProviderError::RateLimitError {
            message: PRIVATE_TEXT.into(),
            provider_name: Some("openai".into()),
            retry_after_seconds: Some(30),
        },
        ProviderError::StreamError {
            message: PRIVATE_TEXT.into(),
            bytes_received: Some(128),
        },
    ] {
        let expected = (
            original.error_code(),
            original.is_recoverable(),
            original.is_transient(),
            original.retry_after_seconds(),
            original.is_user_facing(),
        );
        let error =
            sanitize_error(anyhow::Error::new(original).context("private provider boundary"));
        let normalized = error
            .downcast_ref::<ProviderError>()
            .expect("normalized provider error");
        assert_eq!(
            (
                normalized.error_code(),
                normalized.is_recoverable(),
                normalized.is_transient(),
                normalized.retry_after_seconds(),
                normalized.is_user_facing()
            ),
            expected
        );
    }
}
