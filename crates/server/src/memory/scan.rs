//! Passive extraction orchestration; all blocking storage and journal work
//! runs on the blocking pool, outside the foreground session and turn tasks.
use super::MemoryRuntime;
use super::extraction::{build_extraction_request, parse_candidates};
use super::jobs::{JobFailure, MAX_ATTEMPTS, SourceJobAction};
use super::rebuild::ScanTarget;
use super::source::{ExtractableSource, read_source};
use async_trait::async_trait;
use devo_protocol::SessionId;
use devo_protocol::native::rpc_memory::MemorySourceExclusionReason as SourceExclusion;
use devo_protocol::native::session::MemorySetting;
use devo_provider::ModelProviderSDK;
use devo_provider::error::ProviderError;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Live source activity supplied by the session runtime. Implementations must
/// treat missing runtime ownership as active and must not mutate source actors.
#[async_trait]
pub(crate) trait SourceActivity: Send + Sync {
    async fn is_active(&self, session_id: SessionId) -> bool;
}

#[derive(Clone, Copy)]
pub(crate) enum ScanTrigger {
    SessionStart,
    ExplicitRebuild,
}

pub(crate) struct ScanContext {
    pub(crate) trigger: ScanTrigger,
    pub(crate) db: Arc<crate::db::Database>,
    pub(crate) usage_ledger: crate::usage_ledger::UsageLedger,
    pub(crate) triggering_session: SessionId,
    pub(crate) model_context: Arc<crate::session_context::SessionRuntimeContext>,
    pub(crate) activity: Arc<dyn SourceActivity>,
}

pub(crate) enum MemorySourceWork {
    Scan(ScanContext),
    Reconcile,
    /// Commit source cleanup on the storage worker and report its canonical outcome.
    DeleteSources {
        sources: Vec<SessionId>,
        related_memory: devo_protocol::native::rpc_session::RelatedMemoryDeletion,
        reply: tokio::sync::oneshot::Sender<
            Result<Vec<devo_protocol::native::rpc_memory::MemoryEntry>, super::MemoryError>,
        >,
    },
}

impl MemoryRuntime {
    pub(crate) async fn run_background_scan(
        self: Arc<Self>,
        context: ScanContext,
    ) -> anyhow::Result<()> {
        let result: anyhow::Result<()> = async {
            let memory = Arc::clone(&self);
            tokio::task::spawn_blocking(move || memory.prune_expired((memory.clock)())).await??;
            if !self.config.enabled || self.config.max_sources_per_scan == 0 {
                return Ok(());
            }
            // Ordinary learning must not wait for another scope's deferred rebuild.
            // An explicit rebuild invocation never authorizes an ordinary scan.
            if matches!(context.trigger, ScanTrigger::SessionStart) {
                self.run_source_scan(&context, ScanTarget::Automatic)
                    .await?;
            }
            let memory = Arc::clone(&self);
            let rebuilds = tokio::task::spawn_blocking(move || memory.pending_rebuilds()).await??;
            for request in rebuilds {
                loop {
                    let (admitted, complete) = self
                        .run_source_scan(&context, ScanTarget::Rebuild(request.clone()))
                        .await?;
                    if complete || admitted == 0 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
            Ok(())
        }
        .await;
        if result.is_err() {
            // Maintenance can fail before any job exists; retain only safe health state.
            self.storage_failed.store(true, Ordering::Relaxed);
        }
        result
    }

    async fn run_source_scan(
        self: &Arc<Self>,
        context: &ScanContext,
        target: ScanTarget,
    ) -> anyhow::Result<(u32, bool)> {
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
        let model_unavailable = selection.as_ref().is_none_or(|selection| {
            context.model_context.model_catalog.get(selection).is_none()
                && context
                    .model_context
                    .provider_catalog_snapshot
                    .resolve_model(Some(selection))
                    .is_err()
        });
        let turn_config = context.model_context.resolve_turn_config(
            selection.as_deref(),
            /*reasoning_effort_selection*/ None,
        );
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
        let initialization_failure = if model_unavailable {
            Some(JobFailure::ProviderUnavailable)
        } else {
            provider.initialization_error().map(|error| {
                if matches!(error, ProviderError::AuthenticationError { .. }) {
                    JobFailure::Credentials
                } else {
                    JobFailure::PermanentProvider
                }
            })
        };
        if matches!(target, ScanTarget::Automatic)
            && initialization_failure.is_none()
            && !self.quota_allows(provider.as_ref())
        {
            return Ok((0, false));
        }
        let db = Arc::clone(&context.db);
        let indexes = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            db.list_sessions()?
                .into_iter()
                .map(|session| db.get_session_index(&session.session_id))
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .await??;
        let mut admitted = 0;
        let mut deferred = false;
        for index in indexes.into_iter().flatten() {
            let budget_available = admitted < self.config.max_sources_per_scan
                && (initialization_failure.is_some() || self.quota_allows(provider.as_ref()));
            if matches!(target, ScanTarget::Automatic) && !budget_available {
                break;
            }
            if let ScanTarget::Rebuild(request) = &target {
                if index.metadata.created_at > request.requested_at {
                    continue;
                }
                let memory = Arc::clone(self);
                let workspace = index.metadata.cwd.clone();
                let scope = request.scope;
                let scope_id =
                    tokio::task::spawn_blocking(move || memory.scope_id(scope, &workspace)).await?;
                if !scope_id.is_ok_and(|id| id == request.scope_id) {
                    self.cancel_rebuild_source(&request.id, &index.metadata.session_id.to_string())
                        .await?;
                    continue;
                }
            }
            let session_id = index.metadata.session_id;
            let source_id = session_id.to_string();
            let (path, source) = match self.read_scan_source(context, index).await? {
                super::scan_source::SourceAdmission::Ready { path, source } => (path, source),
                super::scan_source::SourceAdmission::Deferred => {
                    deferred = true;
                    continue;
                }
                super::scan_source::SourceAdmission::Excluded => {
                    if let ScanTarget::Rebuild(request) = &target {
                        self.cancel_rebuild_source(&request.id, &source_id).await?;
                    }
                    continue;
                }
            };
            if let ScanTarget::Rebuild(request) = &target {
                let memory = Arc::clone(self);
                let workspace = source.workspace_root.clone();
                let scope = request.scope;
                let scope_id =
                    tokio::task::spawn_blocking(move || memory.scope_id(scope, &workspace)).await?;
                if !scope_id.is_ok_and(|id| id == request.scope_id)
                    || self
                        .config
                        .resolve_contribution(source.session_contribution)
                        != MemorySetting::On
                {
                    self.cancel_rebuild_source(&request.id, &source_id).await?;
                    continue;
                }
                if chrono::Utc::now().signed_duration_since(source.observed_at)
                    < self.minimum_source_idle()
                {
                    deferred = true;
                    continue;
                }
            }
            let memory = Arc::clone(self);
            let claim_source = source.clone();
            let claim_target = target.clone();
            let claim = tokio::task::spawn_blocking(move || match &claim_target {
                ScanTarget::Automatic => memory.claim_source(&claim_source, chrono::Utc::now()),
                ScanTarget::Rebuild(_) => memory.source_job(
                    &claim_source,
                    chrono::Utc::now(),
                    &claim_target,
                    if budget_available {
                        SourceJobAction::Claim
                    } else {
                        SourceJobAction::Queue
                    },
                ),
            })
            .await??;
            let Some(mut claim) = claim else {
                continue;
            };
            admitted += 1;
            if let Some(failure) = initialization_failure {
                let memory = Arc::clone(self);
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
                    || self.scan_source_has_intent(&source_id).await
                    || !self.quota_allows(provider.as_ref())
                {
                    let memory = Arc::clone(self);
                    tokio::task::spawn_blocking(move || memory.release_job(&claim)).await??;
                    break;
                }
                let read_path = path.clone();
                let latest = tokio::task::spawn_blocking(move || read_source(&read_path))
                    .await?
                    .ok()
                    .flatten();
                if !latest.as_ref().is_some_and(source_still_eligible) {
                    let memory = Arc::clone(self);
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
                // The awaited snapshot read may have overlapped a new turn or
                // external-context intent. Revalidate before sending any text.
                let active = context.activity.is_active(session_id).await;
                let fenced = self.scan_source_has_intent(&source_id).await;
                if active || fenced {
                    self.note_source_exclusion(if fenced {
                        SourceExclusion::SourceFenced
                    } else {
                        SourceExclusion::Active
                    });
                    let memory = Arc::clone(self);
                    tokio::task::spawn_blocking(move || memory.release_job(&claim)).await??;
                    break;
                }
                let memory = Arc::clone(self);
                let dispatch_claim = claim.clone();
                let dispatch_source = source.clone();
                if !tokio::task::spawn_blocking(move || {
                    memory.dispatch_authorized(
                        &dispatch_claim,
                        &dispatch_source,
                        chrono::Utc::now(),
                    )
                })
                .await??
                {
                    break;
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
                        && !context.activity.is_active(session_id).await
                        && !self.scan_source_has_intent(&source_id).await;
                    let candidates = if still_eligible {
                        candidates
                    } else {
                        Vec::new()
                    };
                    let memory = Arc::clone(self);
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
                        let memory = Arc::clone(self);
                        tokio::task::spawn_blocking(move || {
                            memory.fail_job(&claim, JobFailure::Storage, chrono::Utc::now())
                        })
                        .await??;
                    }
                    break;
                }
                let failure = result.expect_err("successful extraction handled above");
                let memory = Arc::clone(self);
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
                let memory = Arc::clone(self);
                let source = source.clone();
                let retry_target = target.clone();
                let next = tokio::task::spawn_blocking(move || {
                    memory.source_job(
                        &source,
                        chrono::Utc::now(),
                        &retry_target,
                        SourceJobAction::Claim,
                    )
                })
                .await??;
                let Some(next) = next else {
                    break;
                };
                claim = next;
            }
        }
        let complete = match target {
            ScanTarget::Automatic => true,
            ScanTarget::Rebuild(request) => {
                let memory = Arc::clone(self);
                tokio::task::spawn_blocking(move || memory.finish_rebuild_pass(&request, deferred))
                    .await??
            }
        };
        Ok((admitted, complete))
    }

    fn quota_allows(&self, provider: &dyn ModelProviderSDK) -> bool {
        provider
            .remaining_quota_percent()
            .is_some_and(|remaining| remaining >= self.config.min_rate_limit_remaining_percent)
    }

    pub(crate) async fn scan_source_has_intent(self: &Arc<Self>, source: &str) -> bool {
        let memory = Arc::clone(self);
        let source = source.to_owned();
        match tokio::task::spawn_blocking(move || memory.source_has_intent(&source)).await {
            Ok(blocked) => blocked,
            Err(_) => {
                tracing::warn!(
                    error_class = "worker_error",
                    "memory source intent check task failed"
                );
                true
            }
        }
    }
}

fn classify_provider_failure(error: &anyhow::Error) -> JobFailure {
    if let Some(provider_error) = error.downcast_ref::<ProviderError>() {
        return match provider_error {
            ProviderError::Diagnostic(_) => {
                use devo_provider::diagnostic::ErrorClass;
                match devo_provider::diagnostic::classify_error(error) {
                    ErrorClass::AuthenticationFailure => JobFailure::Credentials,
                    ErrorClass::RateLimit | ErrorClass::ServerError | ErrorClass::NetworkError => {
                        JobFailure::TransientProvider
                    }
                    ErrorClass::ContextTooLong
                    | ErrorClass::ParameterError
                    | ErrorClass::FileContentAnomaly
                    | ErrorClass::FeatureUnavailable
                    | ErrorClass::TaskNotFound
                    | ErrorClass::NoApiPermission
                    | ErrorClass::FileTooLarge
                    | ErrorClass::Unretryable => JobFailure::PermanentProvider,
                }
            }
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
