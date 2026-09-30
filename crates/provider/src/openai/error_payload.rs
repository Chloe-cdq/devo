use devo_protocol::ModelRequest;
use serde::Deserialize;
use serde_json::Value;

use crate::error::ProviderError;
use crate::error::{TypedFailureKind, context_limit_error, typed_failure_kind};

#[derive(Debug, Deserialize)]
struct OpenAIErrorEnvelope {
    error: OpenAIErrorPayload,
}

#[derive(Debug, Deserialize)]
struct OpenAIErrorPayload {
    #[serde(default)]
    message: Option<String>,
    #[serde(rename = "type", default)]
    kind: Option<String>,
    #[serde(default)]
    code: Option<Value>,
}

pub(super) fn provider_error_from_payload(
    value: &Value,
    request: &ModelRequest,
) -> Option<ProviderError> {
    let envelope = serde_json::from_value::<OpenAIErrorEnvelope>(value.clone()).ok()?;
    let payload = envelope.error;
    let message = payload
        .message
        .filter(|message| !message.trim().is_empty())
        .unwrap_or_else(|| "OpenAI-compatible provider returned an error".to_string());
    let status_code = payload.code.as_ref().and_then(status_code_from_value);
    let provider_name = Some("openai".to_string());
    let typed_kind = typed_failure_kind(
        payload.kind.as_deref(),
        payload.code.as_ref().and_then(Value::as_str),
    );
    if let Some(error) = context_limit_error(message.clone(), status_code, typed_kind) {
        return Some(error);
    }

    let message = message.into();
    Some(match status_code {
        Some(401 | 403) => ProviderError::AuthenticationError {
            message,
            provider_name,
            status_code,
        },
        Some(404) => ProviderError::ModelNotFoundError {
            message,
            model_name: Some(request.model.clone().into()),
        },
        Some(408) => ProviderError::ProviderTimeoutError {
            message,
            provider_name,
        },
        Some(429) => ProviderError::RateLimitError {
            message,
            retry_after_seconds: None,
            provider_name,
        },
        Some(500..=599) => ProviderError::ProviderServerError {
            message,
            status_code,
            provider_name,
        },
        Some(400..=499) => ProviderError::InvalidRequestError {
            message,
            details: error_details(payload.kind.as_deref(), payload.code.as_ref()).map(Into::into),
        },
        Some(_) => ProviderError::UnknownError {
            message,
            status_code,
        },
        None if typed_kind == Some(TypedFailureKind::Authentication) => {
            ProviderError::AuthenticationError {
                message,
                provider_name,
                status_code: None,
            }
        }
        None if typed_kind == Some(TypedFailureKind::Server) => {
            ProviderError::ProviderServerError {
                message,
                status_code: None,
                provider_name,
            }
        }
        None if typed_kind == Some(TypedFailureKind::RateLimit) => ProviderError::RateLimitError {
            message,
            retry_after_seconds: None,
            provider_name,
        },
        None => ProviderError::UnknownError {
            message,
            status_code: None,
        },
    })
}

fn status_code_from_value(value: &Value) -> Option<u16> {
    value
        .as_u64()
        .and_then(|status| u16::try_from(status).ok())
        .or_else(|| value.as_str()?.parse::<u16>().ok())
}

fn error_details(kind: Option<&str>, code: Option<&Value>) -> Option<String> {
    let kind = kind.filter(|kind| !kind.trim().is_empty());
    let code = code.and_then(|code| match code {
        Value::String(code) if !code.trim().is_empty() => Some(code.clone()),
        Value::Number(code) => Some(code.to_string()),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) | Value::String(_) => {
            None
        }
    });
    match (kind, code) {
        (Some(kind), Some(code)) => Some(format!("type={kind}, code={code}")),
        (Some(kind), None) => Some(format!("type={kind}")),
        (None, Some(code)) => Some(format!("code={code}")),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use devo_protocol::ModelProfileKey;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_numeric_and_string_error_codes() {
        let request = ModelRequest {
            model_slug: ModelProfileKey::Generic,
            model: "provider-model".to_string(),
            system: None,
            messages: Vec::new(),
            max_tokens: 16,
            tools: None,
            hosted_tools: Vec::new(),
            sampling: Default::default(),
            request_thinking: None,
            reasoning_effort: None,
            extra_body: None,
        };

        let errors = [
            provider_error_from_payload(
                &json!({"error": {"message": "failed", "code": 500}}),
                &request,
            ),
            provider_error_from_payload(
                &json!({"error": {"message": "busy", "code": "429"}}),
                &request,
            ),
        ]
        .into_iter()
        .map(|error| serde_json::to_value(error.expect("provider error")).expect("serialize"))
        .collect::<Vec<_>>();

        assert_eq!(
            errors,
            vec![
                json!({
                    "error_kind": "provider_server_error",
                    "message": "[redacted]",
                    "status_code": 500,
                    "provider_name": "openai"
                }),
                json!({
                    "error_kind": "rate_limit_error",
                    "message": "[redacted]",
                    "retry_after_seconds": null,
                    "provider_name": "openai"
                }),
            ]
        );
    }

    #[test]
    fn classifies_context_length_error_payload() {
        let request = ModelRequest {
            model_slug: ModelProfileKey::Generic,
            model: "provider-model".to_string(),
            system: None,
            messages: Vec::new(),
            max_tokens: 384_000,
            tools: None,
            hosted_tools: Vec::new(),
            sampling: Default::default(),
            request_thinking: None,
            reasoning_effort: None,
            extra_body: None,
        };
        let message = "This model's maximum context length is 1048565 tokens. However, you request 1058357 tokens (674357 in the messages, 384000 in the completion). Please reduce the length of the messages or completion.";

        let error = provider_error_from_payload(
            &json!({
                "error": {
                    "message": message,
                    "type": "invalid_request_error",
                    "code": "context_length_exceeded"
                }
            }),
            &request,
        )
        .expect("provider error");

        assert_eq!(
            serde_json::to_value(&error).expect("serialize provider error"),
            json!({
                "error_kind": "context_limit_error",
                "message": "[redacted]",
                "current_tokens": null,
                "limit": null
            })
        );
        assert_eq!(
            error.user_message(),
            format!("context limit exceeded: {message}")
        );
    }

    /// Trace: L2-DES-MEM-001 Rev 4 Failure/Observability
    /// Verifies: typed SSE status and kind cannot become context-limit errors through echoed text.
    #[test]
    fn typed_sse_error_code_overrides_echoed_context_text() {
        let request = ModelRequest {
            model_slug: ModelProfileKey::Generic,
            model: "provider-model".to_string(),
            system: None,
            messages: Vec::new(),
            max_tokens: 16,
            tools: None,
            hosted_tools: Vec::new(),
            sampling: Default::default(),
            request_thinking: None,
            reasoning_effort: None,
            extra_body: None,
        };
        let error = provider_error_from_payload(
            &json!({"error": {
                "code": 401,
                "type": "authentication_error",
                "message": "invalid API key; echoed prompt says maximum context length"
            }}),
            &request,
        )
        .expect("provider error");
        assert_eq!(
            serde_json::to_value(&error).expect("serialize provider error"),
            json!({
                "error_kind": "authentication_error",
                "message": "[redacted]",
                "provider_name": "openai",
                "status_code": 401
            })
        );
        assert_eq!(
            crate::diagnostic::classify_error(&anyhow::Error::new(error)),
            crate::diagnostic::ErrorClass::AuthenticationFailure
        );
        for (kind, expected) in [
            (
                "authentication_error",
                json!({
                    "error_kind": "authentication_error",
                    "message": "[redacted]",
                    "provider_name": "openai",
                    "status_code": null
                }),
            ),
            (
                "rate_limit_error",
                json!({
                    "error_kind": "rate_limit_error",
                    "message": "[redacted]",
                    "retry_after_seconds": null,
                    "provider_name": "openai"
                }),
            ),
        ] {
            let error = provider_error_from_payload(
                &json!({"error": {
                    "type": kind,
                    "message": "echoed prompt says maximum context length"
                }}),
                &request,
            )
            .expect("provider error");
            assert_eq!(
                serde_json::to_value(error).expect("serialize provider error"),
                expected
            );
        }
    }
}
