//! Provider error classification and retry policy for the query loop.

use std::time::Duration;

use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::AgentError;
pub(crate) use devo_provider::diagnostic::{ErrorClass, classify_error};

use super::event::EventCallback;
use super::event::ProviderRetryStatus;
use super::event::QueryEvent;
use super::event::QueryProviderRetryPhase;
use super::event::emit_query_event;

const MAX_RETRIES: usize = 5;
const INITIAL_RETRY_BACKOFF_MS: u64 = 250;
const RATE_LIMIT_RETRY_DELAY: Duration = Duration::from_secs(60);

pub(crate) enum ProviderRetryDecision {
    RetryAfter(Duration),
    CompactAndRetry,
    Fail,
}

pub(crate) fn provider_retry_decision(
    error: &anyhow::Error,
    retry_count: &mut usize,
    context_compacted: &mut bool,
) -> ProviderRetryDecision {
    match classify_error(error) {
        ErrorClass::ContextTooLong => {
            if *context_compacted {
                ProviderRetryDecision::Fail
            } else {
                *context_compacted = true;
                ProviderRetryDecision::CompactAndRetry
            }
        }
        ErrorClass::RateLimit => {
            if *retry_count >= MAX_RETRIES {
                ProviderRetryDecision::Fail
            } else {
                *retry_count += 1;
                ProviderRetryDecision::RetryAfter(RATE_LIMIT_RETRY_DELAY)
            }
        }
        ErrorClass::ServerError | ErrorClass::NetworkError => {
            if *retry_count >= MAX_RETRIES {
                ProviderRetryDecision::Fail
            } else {
                *retry_count += 1;
                ProviderRetryDecision::RetryAfter(retry_backoff_duration(*retry_count))
            }
        }
        ErrorClass::ParameterError
        | ErrorClass::FileContentAnomaly
        | ErrorClass::AuthenticationFailure
        | ErrorClass::FeatureUnavailable
        | ErrorClass::TaskNotFound
        | ErrorClass::NoApiPermission
        | ErrorClass::FileTooLarge
        | ErrorClass::Unretryable => ProviderRetryDecision::Fail,
    }
}

pub(crate) async fn wait_for_provider_retry(
    on_event: &Option<EventCallback>,
    cancel_token: Option<&CancellationToken>,
    provider: &str,
    model: &str,
    attempt: usize,
    backoff: Duration,
    reason: &str,
) -> Result<(), AgentError> {
    let backoff_ms = backoff.as_millis().min(u128::from(u64::MAX)) as u64;
    let reason = reason.trim();
    let reason = if reason.is_empty() {
        "Provider request failed"
    } else {
        reason
    };
    emit_query_event(
        on_event,
        QueryEvent::ProviderRetryStatus(ProviderRetryStatus {
            provider: provider.to_string(),
            model: model.to_string(),
            attempt,
            max_attempts: MAX_RETRIES,
            backoff_ms,
            phase: QueryProviderRetryPhase::Scheduled,
            // Failure cause for UI disclosure; countdown is carried by backoff_ms.
            message: reason.to_string(),
        }),
    )
    .await;

    if let Some(cancel_token) = cancel_token {
        tokio::select! {
            biased;
            () = cancel_token.cancelled() => return Err(AgentError::Aborted),
            () = sleep(backoff) => {}
        }
    } else {
        sleep(backoff).await;
    }

    emit_query_event(
        on_event,
        QueryEvent::ProviderRetryStatus(ProviderRetryStatus {
            provider: provider.to_string(),
            model: model.to_string(),
            attempt,
            max_attempts: MAX_RETRIES,
            backoff_ms: 0,
            phase: QueryProviderRetryPhase::Resumed,
            message: reason.to_string(),
        }),
    )
    .await;

    Ok(())
}

fn retry_backoff_duration(attempt: usize) -> Duration {
    let exponent = attempt.saturating_sub(1).min(10) as u32;
    let multiplier = 2u64.pow(exponent);
    Duration::from_millis(INITIAL_RETRY_BACKOFF_MS.saturating_mul(multiplier))
}
