//! Passive extraction orchestration; all blocking storage and journal work
//! runs on the blocking pool, outside the foreground session and turn tasks.
use super::MemoryRuntime;
use super::extraction::{build_extraction_request, parse_candidates};
use super::jobs::{JobFailure, MAX_ATTEMPTS};
use super::source::{ExtractableSource, read_source};
use async_trait::async_trait;
use devo_protocol::SessionId;
use devo_protocol::native::session::MemorySetting;
use devo_provider::ModelProviderSDK;
use devo_provider::error::ProviderError;
use std::sync::Arc;
use std::time::Duration;

/// Live source activity supplied by the session runtime. Implementations must
/// treat missing runtime ownership as active and must not mutate source actors.
#[async_trait]
pub(crate) trait SourceActivity: Send + Sync {
    async fn is_active(&self, session_id: SessionId) -> bool;
}

pub(crate) struct ScanContext {
    pub(crate) db: Arc<crate::db::Database>,
    pub(crate) usage_ledger: crate::usage_ledger::UsageLedger,
    pub(crate) triggering_session: SessionId,
    pub(crate) model_context: Arc<crate::session_context::SessionRuntimeContext>,
    pub(crate) activity: Arc<dyn SourceActivity>,
}

impl MemoryRuntime {
    pub(crate) async fn run_background_scan(
        self: Arc<Self>,
        context: ScanContext,
    ) -> anyhow::Result<()> {
        if !self.config.enabled || self.config.max_sources_per_scan == 0 {
            return Ok(());
        }
        let configured_small = context
            .model_context
            .config_store
            .lock()
            .map_err(|_| anyhow::anyhow!("configuration unavailable"))?
            .effective_config()
            .provider_catalog_config()
            .small_model;
        let selection = self
            .config
            .extract_model
            .clone()
            .or(configured_small)
            .or_else(|| {
                devo_core::resolve_small_model(
                    context.model_context.model_catalog.as_ref(),
                    &context.model_context.default_model,
                )
            });
        let Some(selection) = selection else {
            return Ok(());
        };
        if context
            .model_context
            .model_catalog
            .get(&selection)
            .is_none()
            && context
                .model_context
                .provider_catalog_snapshot
                .resolve_model(Some(&selection))
                .is_err()
        {
            return Ok(());
        }
        let turn_config = context
            .model_context
            .resolve_turn_config(Some(&selection), /*reasoning_effort_selection*/ None);
        let catalog_model = turn_config
            .model
            .resolve_reasoning_effort_selection(/*selection*/ None)
            .request_model;
        let request_model = turn_config.provider_request_model(&catalog_model);
        let model_slug = turn_config.model.slug.clone();
        let provider = context.usage_ledger.instrumented_provider(
            context
                .model_context
                .provider_for_route(turn_config.provider_route),
            context.triggering_session,
            /*turn_id*/ None,
            devo_protocol::native::usage::UsagePurpose::MemoryExtraction,
        );
        let initialization_failure = provider.initialization_error().map(|error| {
            if matches!(error, ProviderError::AuthenticationError { .. }) {
                JobFailure::Credentials
            } else {
                JobFailure::PermanentProvider
            }
        });
        if initialization_failure.is_none() && !self.quota_allows(provider.as_ref()) {
            return Ok(());
        }
        let db = Arc::clone(&context.db);
        let indexes = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            db.list_root_sessions()?
                .into_iter()
                .map(|session| db.get_session_index(&session.session_id))
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .await??;
        let mut admitted = 0;
        for index in indexes.into_iter().flatten() {
            if admitted >= self.config.max_sources_per_scan
                || (initialization_failure.is_none() && !self.quota_allows(provider.as_ref()))
            {
                break;
            }
            let session_id = index.metadata.session_id;
            let Some(path) = index.rollout_path else {
                continue;
            };
            if context.activity.is_active(session_id).await {
                continue;
            }
            let read_path = path.clone();
            let source = tokio::task::spawn_blocking(move || read_source(&read_path))
                .await?
                .ok()
                .flatten();
            let Some(source) = source else {
                continue;
            };
            if source.session_id.as_str() != session_id.to_string() {
                continue;
            }
            let memory = Arc::clone(&self);
            let claim_source = source.clone();
            let claim = tokio::task::spawn_blocking(move || {
                memory.claim_source(&claim_source, chrono::Utc::now())
            })
            .await??;
            let Some(mut claim) = claim else {
                continue;
            };
            admitted += 1;
            if let Some(failure) = initialization_failure {
                let memory = Arc::clone(&self);
                tokio::task::spawn_blocking(move || {
                    memory.fail_job(&claim, failure, chrono::Utc::now())
                })
                .await??;
                continue;
            }
            let source_still_eligible = |latest: &ExtractableSource| {
                latest.watermark == source.watermark
                    && self
                        .config
                        .resolve_contribution(latest.session_contribution)
                        == MemorySetting::On
            };
            loop {
                if context.activity.is_active(session_id).await
                    || !self.quota_allows(provider.as_ref())
                {
                    let memory = Arc::clone(&self);
                    tokio::task::spawn_blocking(move || memory.release_job(&claim)).await??;
                    break;
                }
                let read_path = path.clone();
                let latest = tokio::task::spawn_blocking(move || read_source(&read_path))
                    .await?
                    .ok()
                    .flatten();
                if !latest.as_ref().is_some_and(source_still_eligible) {
                    let memory = Arc::clone(&self);
                    let source = source.clone();
                    tokio::task::spawn_blocking(move || {
                        memory.commit_extraction(&claim, &source, &[], chrono::Utc::now())
                    })
                    .await??;
                    break;
                }
                let mut request =
                    build_extraction_request(model_slug.clone(), request_model.clone(), &source);
                request.extra_body = devo_core::add_model_request_headers(
                    devo_core::merge_model_request_body(
                        turn_config.provider_request_models.request_defaults(),
                        request.extra_body,
                    ),
                    turn_config.provider_request_models.request_headers(),
                );
                // Defaults cannot override the fixed input, output bound, or
                // tool-free extraction profile when adapters merge extra_body.
                if let Some(serde_json::Value::Object(extra)) = &mut request.extra_body {
                    for key in [
                        "model",
                        "system",
                        "messages",
                        "input",
                        "instructions",
                        "previous_response_id",
                        "conversation",
                        "prompt",
                        "max_tokens",
                        "max_completion_tokens",
                        "max_output_tokens",
                        "tools",
                        "tool_choice",
                        "parallel_tool_calls",
                        "functions",
                        "function_call",
                        "web_search_options",
                        "thinking",
                        "reasoning",
                        "reasoning_effort",
                        "stream",
                        "stream_options",
                    ] {
                        extra.remove(key);
                    }
                }
                let response =
                    tokio::time::timeout(Duration::from_secs(60), provider.completion(request))
                        .await;
                let result = match response {
                    Ok(Ok(response)) => parse_candidates(&response.content, &source)
                        .map_err(|_| JobFailure::InvalidOutput),
                    Ok(Err(error)) => Err(classify_provider_failure(&error)),
                    Err(_) => Err(JobFailure::TransientProvider),
                };
                if let Ok(candidates) = result {
                    let read_path = path.clone();
                    let latest = tokio::task::spawn_blocking(move || read_source(&read_path))
                        .await?
                        .ok()
                        .flatten();
                    let still_eligible = latest.as_ref().is_some_and(source_still_eligible)
                        && !context.activity.is_active(session_id).await;
                    let candidates = if still_eligible {
                        candidates
                    } else {
                        Vec::new()
                    };
                    let memory = Arc::clone(&self);
                    let source = source.clone();
                    let commit_claim = claim.clone();
                    let commit = tokio::task::spawn_blocking(move || {
                        memory.commit_extraction(
                            &commit_claim,
                            &source,
                            &candidates,
                            chrono::Utc::now(),
                        )
                    })
                    .await?;
                    if commit.is_err() {
                        let memory = Arc::clone(&self);
                        tokio::task::spawn_blocking(move || {
                            memory.fail_job(&claim, JobFailure::Storage, chrono::Utc::now())
                        })
                        .await??;
                    }
                    break;
                }
                let failure = result.expect_err("successful extraction handled above");
                let memory = Arc::clone(&self);
                let failed_claim = claim.clone();
                tokio::task::spawn_blocking(move || {
                    memory.fail_job(&failed_claim, failure, chrono::Utc::now())
                })
                .await??;
                if !matches!(failure, JobFailure::TransientProvider)
                    || claim.attempt >= MAX_ATTEMPTS
                {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(30 * (1_u64 << (claim.attempt - 1)))).await;
                if !self.quota_allows(provider.as_ref()) {
                    break;
                }
                let memory = Arc::clone(&self);
                let source = source.clone();
                let next = tokio::task::spawn_blocking(move || {
                    memory.claim_source(&source, chrono::Utc::now())
                })
                .await??;
                let Some(next) = next else {
                    break;
                };
                claim = next;
            }
        }
        Ok(())
    }

    fn quota_allows(&self, provider: &dyn ModelProviderSDK) -> bool {
        provider
            .remaining_quota_percent()
            .is_some_and(|remaining| remaining >= self.config.min_rate_limit_remaining_percent)
    }
}

fn classify_provider_failure(error: &anyhow::Error) -> JobFailure {
    if let Some(error) = error.downcast_ref::<ProviderError>() {
        return match error {
            ProviderError::AuthenticationError { .. } => JobFailure::Credentials,
            ProviderError::RateLimitError { .. }
            | ProviderError::ProviderServerError { .. }
            | ProviderError::ProviderTimeoutError { .. }
            | ProviderError::StreamError { .. }
            | ProviderError::UnknownError {
                status_code: None | Some(429 | 500..=599),
                ..
            } => JobFailure::TransientProvider,
            ProviderError::ContextLimitError { .. }
            | ProviderError::ModelNotFoundError { .. }
            | ProviderError::QuotaExceededError { .. }
            | ProviderError::ContentFilteredError { .. }
            | ProviderError::InvalidRequestError { .. }
            | ProviderError::UnknownError {
                status_code: Some(_),
                ..
            } => JobFailure::PermanentProvider,
        };
    }
    JobFailure::TransientProvider
}
