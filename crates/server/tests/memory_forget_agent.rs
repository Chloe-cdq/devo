use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use async_trait::async_trait;
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::{
    MemoryEntry, MemoryForgetResult, MemoryScope, MemorySearchResult, MemoryState,
};
use devo_protocol::{
    ModelRequest, ModelResponse, RequestContent, ResponseContent, ResponseMetadata, SessionId,
    StopReason, StreamEvent, Usage,
};
use devo_provider::ModelProviderSDK;
use devo_server::ServerRuntime;
use devo_server::memory::{
    MemoryCommand, MemoryCommandResult, MemoryError, MemoryForgetExecutor, MemoryForgetRequest,
    MemoryRuntime,
};
use futures::Stream;
use pretty_assertions::assert_eq;
use tempfile::TempDir;
use tokio::sync::{Notify, mpsc};
use tokio::time::{Duration, timeout};

#[path = "support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

use support::{
    build_runtime_with_workspace_config,
    build_runtime_with_workspace_config_and_memory_forget_executor, initialize_connection,
    start_parent_session, start_turn_with_approval_policy, wait_for_parent_turn_completed,
};

struct BlockingFirstMemoryForgetExecutor {
    calls: AtomicUsize,
    mutation_started: Notify,
    release_mutation: Notify,
}

impl BlockingFirstMemoryForgetExecutor {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            mutation_started: Notify::new(),
            release_mutation: Notify::new(),
        }
    }

    async fn wait_until_started(&self) -> Result<()> {
        timeout(Duration::from_secs(5), self.mutation_started.notified())
            .await
            .context("first memory forget mutation did not start")?;
        Ok(())
    }

    fn release(&self) {
        self.release_mutation.notify_one();
    }
}

#[async_trait]
impl MemoryForgetExecutor for BlockingFirstMemoryForgetExecutor {
    async fn execute(
        &self,
        memory: &MemoryRuntime,
        request: MemoryForgetRequest,
    ) -> Result<MemoryCommandResult, MemoryError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.mutation_started.notify_one();
            self.release_mutation.notified().await;
        }
        memory.execute_command(MemoryCommand::Forget(request)).await
    }
}

enum ProviderAction {
    Search(&'static str),
    CaptureSearch,
    ForgetSearchCandidate(usize),
    ForgetTarget,
    Complete(&'static str),
}

struct MemoryAgentProvider {
    actions: Mutex<VecDeque<ProviderAction>>,
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    search_result: Mutex<Option<MemorySearchResult>>,
    target: Mutex<Option<MemoryEntryId>>,
}

impl MemoryAgentProvider {
    fn new(actions: impl IntoIterator<Item = ProviderAction>) -> Self {
        Self {
            actions: Mutex::new(actions.into_iter().collect()),
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            search_result: Mutex::new(None),
            target: Mutex::new(None),
        }
    }

    fn set_target(&self, entry_id: MemoryEntryId) {
        *self.target.lock().expect("target lock") = Some(entry_id);
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().expect("requests lock").clone()
    }

    fn search_result(&self) -> MemorySearchResult {
        self.search_result
            .lock()
            .expect("search result lock")
            .clone()
            .expect("captured memory search result")
    }

    fn capture_search(&self, request: &ModelRequest) -> Result<MemorySearchResult> {
        let result: MemorySearchResult = serde_json::from_str(
            tool_result(request, "memory-search").context("memory search result")?,
        )?;
        *self.search_result.lock().expect("search result lock") = Some(result.clone());
        Ok(result)
    }
}

#[async_trait]
impl ModelProviderSDK for MemoryAgentProvider {
    async fn completion(&self, _request: ModelRequest) -> Result<ModelResponse> {
        anyhow::bail!("memory agent test uses streaming completion")
    }

    async fn completion_stream(
        &self,
        request: ModelRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamEvent>> + Send>>> {
        self.requests
            .lock()
            .expect("requests lock")
            .push(request.clone());
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let action = self
            .actions
            .lock()
            .expect("actions lock")
            .pop_front()
            .with_context(|| format!("provider action exhausted at call {call}"))?;
        let events = match action {
            ProviderAction::Search(query) => tool_call_events(
                "memory-search",
                "memory_search",
                serde_json::json!({ "query": query }),
            ),
            ProviderAction::CaptureSearch => {
                self.capture_search(&request)?;
                text_events("reply with: Confirm forget memory entry <stable ID>")
            }
            ProviderAction::ForgetSearchCandidate(index) => {
                let result = self.capture_search(&request)?;
                let entry_id = result
                    .data
                    .get(index)
                    .with_context(|| format!("memory search candidate {index}"))?
                    .entry_id
                    .clone();
                tool_call_events(
                    "memory-forget",
                    "memory_forget",
                    serde_json::json!({ "entry_id": entry_id }),
                )
            }
            ProviderAction::ForgetTarget => {
                let entry_id = self
                    .target
                    .lock()
                    .expect("target lock")
                    .clone()
                    .context("configured memory forget target")?;
                tool_call_events(
                    "memory-forget",
                    "memory_forget",
                    serde_json::json!({ "entry_id": entry_id }),
                )
            }
            ProviderAction::Complete(text) => text_events(text),
        };
        Ok(Box::pin(futures::stream::iter(events.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        "memory-agent-provider"
    }
}

struct MemoryAgentHarness {
    _data_root: TempDir,
    runtime: Arc<ServerRuntime>,
    connection_id: u64,
    notifications_rx: mpsc::Receiver<serde_json::Value>,
    session_id: SessionId,
}

impl MemoryAgentHarness {
    async fn new(provider: Arc<MemoryAgentProvider>) -> Result<Self> {
        Self::open(provider, /*memory_forget_executor*/ None).await
    }

    async fn new_with_forget_executor(
        provider: Arc<MemoryAgentProvider>,
        memory_forget_executor: Arc<dyn MemoryForgetExecutor>,
    ) -> Result<Self> {
        Self::open(provider, Some(memory_forget_executor)).await
    }

    async fn open(
        provider: Arc<MemoryAgentProvider>,
        memory_forget_executor: Option<Arc<dyn MemoryForgetExecutor>>,
    ) -> Result<Self> {
        let data_root = TempDir::new()?;
        std::fs::create_dir_all(data_root.path().join(".devo"))?;
        std::fs::write(
            data_root.path().join(".devo").join("config.toml"),
            "[memory]\nenabled = true\n",
        )?;
        let runtime = if let Some(memory_forget_executor) = memory_forget_executor {
            build_runtime_with_workspace_config_and_memory_forget_executor(
                data_root.path(),
                Arc::clone(&provider) as _,
                memory_forget_executor,
            )?
        } else {
            build_runtime_with_workspace_config(data_root.path(), Arc::clone(&provider) as _)?
        };
        let session = MemoryAgentSession::start(&runtime, data_root.path()).await?;
        Ok(Self {
            _data_root: data_root,
            runtime,
            connection_id: session.connection_id,
            notifications_rx: session.notifications_rx,
            session_id: session.session_id,
        })
    }

    async fn additional_session(&self) -> Result<MemoryAgentSession> {
        MemoryAgentSession::start(&self.runtime, self._data_root.path()).await
    }

    async fn remember(&self, text: &str, scope: MemoryScope) -> Result<MemoryEntry> {
        let response = self
            .runtime
            .handle_incoming(
                self.connection_id,
                serde_json::json!({
                    "id": 4,
                    "method": "memory/remember",
                    "params": { "text": text, "scope": scope }
                }),
            )
            .await
            .context("memory/remember response")?;
        serde_json::from_value::<MemoryEntry>(response["result"].clone())
            .with_context(|| format!("decode memory/remember response: {response}"))
    }

    async fn run_turn(&mut self, text: &str) -> Result<()> {
        start_turn_with_approval_policy(
            &self.runtime,
            self.connection_id,
            self.session_id,
            text,
            Some("never"),
        )
        .await?;
        wait_for_parent_turn_completed(&mut self.notifications_rx, self.session_id).await
    }

    async fn list(&self, scope: MemoryScope, state: MemoryState) -> Result<Page<MemoryEntry>> {
        let response = self
            .runtime
            .handle_incoming(
                self.connection_id,
                serde_json::json!({
                    "id": 6,
                    "method": "memory/list",
                    "params": { "scope": scope, "state": state }
                }),
            )
            .await
            .context("memory/list response")?;
        serde_json::from_value::<Page<MemoryEntry>>(response["result"].clone())
            .with_context(|| format!("decode memory/list response: {response}"))
    }
}

struct MemoryAgentSession {
    connection_id: u64,
    notifications_rx: mpsc::Receiver<serde_json::Value>,
    session_id: SessionId,
}

impl MemoryAgentSession {
    async fn start(runtime: &Arc<ServerRuntime>, workspace_root: &std::path::Path) -> Result<Self> {
        let (connection_id, notifications_rx) = initialize_connection(runtime).await?;
        let session_id = start_parent_session(runtime, connection_id, workspace_root).await?;
        let response = runtime
            .handle_incoming(
                connection_id,
                serde_json::json!({
                    "id": 3,
                    "method": "subscription/create",
                    "params": {
                        "selectors": [{ "kind": "session", "sessionId": session_id }],
                        "includeSnapshot": false
                    }
                }),
            )
            .await
            .context("subscription/create response")?;
        anyhow::ensure!(
            response.get("result").is_some(),
            "subscription/create failed: {response}"
        );
        Ok(Self {
            connection_id,
            notifications_rx,
            session_id,
        })
    }

    async fn run_turn(&mut self, runtime: &Arc<ServerRuntime>, text: &str) -> Result<()> {
        start_turn_with_approval_policy(
            runtime,
            self.connection_id,
            self.session_id,
            text,
            Some("never"),
        )
        .await?;
        wait_for_parent_turn_completed(&mut self.notifications_rx, self.session_id).await
    }
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: an ambiguous natural-language search cannot authorize an exact-ID forget in the same turn.
#[tokio::test]
async fn ambiguous_search_cannot_delete_in_same_turn() -> Result<()> {
    let provider = Arc::new(MemoryAgentProvider::new([
        ProviderAction::Search("tabs"),
        ProviderAction::ForgetSearchCandidate(0),
        ProviderAction::Complete("selection required"),
    ]));
    let mut harness = MemoryAgentHarness::new(Arc::clone(&provider)).await?;
    let rust = harness
        .remember("I prefer tabs for Rust", MemoryScope::User)
        .await?;
    let python = harness
        .remember("I prefer tabs for Python", MemoryScope::User)
        .await?;

    harness.run_turn("Forget that I prefer tabs").await?;

    assert_forget_rejected(
        &provider.requests(),
        2,
        "requires a subsequent user selection",
    )?;
    assert_eq!(
        harness.list(MemoryScope::User, MemoryState::Active).await?,
        Page {
            data: vec![python, rust],
            next_cursor: None,
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: a follow-up selection cannot retire an ID outside the server-recorded candidate set.
#[tokio::test]
async fn pending_selection_rejects_an_id_outside_its_candidates() -> Result<()> {
    let provider = Arc::new(MemoryAgentProvider::new([
        ProviderAction::Search("tabs"),
        ProviderAction::CaptureSearch,
        ProviderAction::ForgetTarget,
        ProviderAction::Complete("outside candidate rejected"),
    ]));
    let mut harness = MemoryAgentHarness::new(Arc::clone(&provider)).await?;
    let candidate = harness.remember("I prefer tabs", MemoryScope::User).await?;
    let outside = harness
        .remember("I prefer spaces", MemoryScope::User)
        .await?;
    provider.set_target(outside.entry_id.clone());

    harness.run_turn("Forget my indentation preference").await?;
    harness
        .run_turn(&format!("Confirm forget memory entry {}", outside.entry_id))
        .await?;

    assert_eq!(
        provider.search_result(),
        Page {
            data: vec![search_entry(&candidate)],
            next_cursor: None,
        }
    );
    assert_forget_rejected(&provider.requests(), 3, "not one of the pending candidates")?;
    assert_eq!(
        harness.list(MemoryScope::User, MemoryState::Active).await?,
        Page {
            data: vec![outside, candidate],
            next_cursor: None,
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: a later user turn can retire the exact ID selected from the pending candidate set.
#[tokio::test]
async fn pending_selection_accepts_a_later_candidate_id() -> Result<()> {
    let provider = Arc::new(MemoryAgentProvider::new([
        ProviderAction::Search("tabs"),
        ProviderAction::CaptureSearch,
        ProviderAction::ForgetTarget,
        ProviderAction::Complete("selected candidate forgotten"),
    ]));
    let mut harness = MemoryAgentHarness::new(Arc::clone(&provider)).await?;
    let selected = harness.remember("I prefer tabs", MemoryScope::User).await?;
    provider.set_target(selected.entry_id.clone());

    harness.run_turn("Forget my indentation preference").await?;
    harness
        .run_turn(&format!(
            "Confirm forget memory entry {}",
            selected.entry_id
        ))
        .await?;

    let requests = provider.requests();
    let result: MemoryForgetResult = serde_json::from_str(
        tool_result(&requests[3], "memory-forget").context("memory forget result")?,
    )?;
    let forgotten = result.forgotten.clone().context("forgotten entry")?;
    assert_eq!(
        result,
        MemoryForgetResult {
            forgotten: Some(MemoryEntry {
                state: MemoryState::Retired,
                updated_at: forgotten.updated_at,
                ..selected
            }),
            candidates: Vec::new(),
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: a failed storage mutation releases its in-flight candidate reservation for a later retry.
#[tokio::test]
async fn failed_pending_mutation_can_retry_the_same_candidate() -> Result<()> {
    let provider = Arc::new(MemoryAgentProvider::new([
        ProviderAction::Search("tabs"),
        ProviderAction::CaptureSearch,
        ProviderAction::ForgetTarget,
        ProviderAction::Complete("first mutation failed safely"),
        ProviderAction::ForgetTarget,
        ProviderAction::Complete("retry succeeded"),
    ]));
    let mut harness = MemoryAgentHarness::new(Arc::clone(&provider)).await?;
    let selected = harness.remember("I prefer tabs", MemoryScope::User).await?;
    provider.set_target(selected.entry_id.clone());
    let confirmation = format!("Confirm forget memory entry {}", selected.entry_id);

    harness.run_turn("Forget my tab preference").await?;
    let fault_connection = rusqlite::Connection::open(
        harness
            ._data_root
            .path()
            .join("memory")
            .join("memory.sqlite3"),
    )?;
    fault_connection.execute(
        "ALTER TABLE memory_revocations RENAME TO memory_revocations_unavailable",
        [],
    )?;
    harness.run_turn(&confirmation).await?;
    let requests = provider.requests();
    let failed_result =
        tool_result(&requests[3], "memory-forget").context("failed memory forget tool result")?;
    anyhow::ensure!(
        failed_result.contains("memory operation is unavailable"),
        "unexpected failed memory forget result: {failed_result}"
    );
    assert_eq!(
        harness.list(MemoryScope::User, MemoryState::Active).await?,
        Page {
            data: vec![selected.clone()],
            next_cursor: None,
        }
    );
    fault_connection.execute(
        "ALTER TABLE memory_revocations_unavailable RENAME TO memory_revocations",
        [],
    )?;

    harness.run_turn(&confirmation).await?;
    let retired = harness
        .list(MemoryScope::User, MemoryState::Retired)
        .await?;
    let retired_entry = retired.data.first().context("retired entry")?;
    assert_eq!(
        retired,
        Page {
            data: vec![MemoryEntry {
                state: MemoryState::Retired,
                updated_at: retired_entry.updated_at,
                ..selected
            }],
            next_cursor: None,
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: an explicit exact stable-ID command can retire that identity without a prior search.
#[tokio::test]
async fn exact_stable_id_command_can_delete_directly() -> Result<()> {
    let provider = Arc::new(MemoryAgentProvider::new([
        ProviderAction::ForgetTarget,
        ProviderAction::Complete("exact memory forgotten"),
    ]));
    let mut harness = MemoryAgentHarness::new(Arc::clone(&provider)).await?;
    let entry = harness.remember("I prefer tabs", MemoryScope::User).await?;
    provider.set_target(entry.entry_id.clone());

    harness
        .run_turn(&format!("Forget memory entry {}", entry.entry_id))
        .await?;

    let requests = provider.requests();
    let result: MemoryForgetResult = serde_json::from_str(
        tool_result(&requests[1], "memory-forget").context("memory forget result")?,
    )?;
    let forgotten = result.forgotten.clone().context("forgotten entry")?;
    assert_eq!(
        result,
        MemoryForgetResult {
            forgotten: Some(MemoryEntry {
                state: MemoryState::Retired,
                updated_at: forgotten.updated_at,
                ..entry
            }),
            candidates: Vec::new(),
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: a prior unrelated search does not prevent a later strict direct exact-ID deletion.
#[tokio::test]
async fn pending_search_does_not_shadow_direct_exact_id_command() -> Result<()> {
    let provider = Arc::new(MemoryAgentProvider::new([
        ProviderAction::Search("tabs"),
        ProviderAction::CaptureSearch,
        ProviderAction::ForgetTarget,
        ProviderAction::Complete("direct memory forgotten"),
    ]));
    let mut harness = MemoryAgentHarness::new(Arc::clone(&provider)).await?;
    let pending = harness.remember("I prefer tabs", MemoryScope::User).await?;
    let direct = harness
        .remember("My timezone is UTC", MemoryScope::User)
        .await?;
    provider.set_target(direct.entry_id.clone());

    harness.run_turn("Find my tab preference").await?;
    assert_eq!(
        provider.search_result(),
        Page {
            data: vec![search_entry(&pending)],
            next_cursor: None,
        }
    );
    harness
        .run_turn(&format!("Forget memory entry {}", direct.entry_id))
        .await?;

    let requests = provider.requests();
    let result: MemoryForgetResult = serde_json::from_str(
        tool_result(&requests[3], "memory-forget").context("memory forget result")?,
    )?;
    let forgotten = result.forgotten.clone().context("forgotten entry")?;
    assert_eq!(
        result,
        MemoryForgetResult {
            forgotten: Some(MemoryEntry {
                state: MemoryState::Retired,
                updated_at: forgotten.updated_at,
                ..direct
            }),
            candidates: Vec::new(),
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: a global direct mutation lease rejects a confirmation from another session until storage completes.
#[tokio::test]
async fn direct_first_blocks_confirmation() -> Result<()> {
    let provider = Arc::new(MemoryAgentProvider::new([
        ProviderAction::Search("tabs"),
        ProviderAction::CaptureSearch,
        ProviderAction::ForgetTarget,
        ProviderAction::ForgetSearchCandidate(0),
        ProviderAction::Complete("confirmation blocked"),
        ProviderAction::Complete("direct memory forgotten"),
    ]));
    let executor = Arc::new(BlockingFirstMemoryForgetExecutor::new());
    let mut harness = MemoryAgentHarness::new_with_forget_executor(
        Arc::clone(&provider),
        Arc::clone(&executor) as _,
    )
    .await?;
    let mut confirmation_session = harness.additional_session().await?;
    let pending = harness.remember("I prefer tabs", MemoryScope::User).await?;
    let direct = harness
        .remember("My timezone is UTC", MemoryScope::User)
        .await?;
    provider.set_target(direct.entry_id.clone());
    confirmation_session
        .run_turn(&harness.runtime, "Find my tab preference")
        .await?;
    assert_eq!(
        provider.search_result(),
        Page {
            data: vec![search_entry(&pending)],
            next_cursor: None,
        }
    );

    let direct_text = format!("Forget memory entry {}", direct.entry_id);
    let runtime = Arc::clone(&harness.runtime);
    let direct_task = tokio::spawn(async move {
        harness.run_turn(&direct_text).await?;
        Ok::<_, anyhow::Error>(harness)
    });
    executor.wait_until_started().await?;
    confirmation_session
        .run_turn(
            &runtime,
            &format!("Confirm forget memory entry {}", pending.entry_id),
        )
        .await?;
    let requests = provider.requests();
    let confirmation_result = assert_forget_rejected(&requests, 4, "already in flight");
    executor.release();
    let harness = direct_task.await??;
    confirmation_result?;
    assert_eq!(
        harness.list(MemoryScope::User, MemoryState::Active).await?,
        Page {
            data: vec![pending],
            next_cursor: None,
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: Native and agent deletion share one global lease in both arrival orders, including across sessions.
#[tokio::test]
async fn native_and_agent_forget_are_mutually_exclusive() -> Result<()> {
    {
        let provider = Arc::new(MemoryAgentProvider::new([
            ProviderAction::ForgetTarget,
            ProviderAction::Complete("agent memory forgotten"),
        ]));
        let executor = Arc::new(BlockingFirstMemoryForgetExecutor::new());
        let mut harness = MemoryAgentHarness::new_with_forget_executor(
            Arc::clone(&provider),
            Arc::clone(&executor) as _,
        )
        .await?;
        let native_session = harness.additional_session().await?;
        let entry = harness.remember("I prefer tabs", MemoryScope::User).await?;
        provider.set_target(entry.entry_id.clone());
        let runtime = Arc::clone(&harness.runtime);
        let direct_text = format!("Forget memory entry {}", entry.entry_id);
        let agent_task = tokio::spawn(async move {
            harness.run_turn(&direct_text).await?;
            Ok::<_, anyhow::Error>(harness)
        });
        executor.wait_until_started().await?;
        let native_response = runtime
            .handle_incoming(
                native_session.connection_id,
                serde_json::json!({
                    "id": 20,
                    "method": "memory/forget",
                    "params": { "entryId": entry.entry_id }
                }),
            )
            .await
            .context("concurrent Native memory/forget response")?;
        executor.release();
        let _harness = agent_task.await??;
        anyhow::ensure!(
            native_response["error"]["message"]
                .as_str()
                .is_some_and(|message| message.contains("already in flight")),
            "Native mutation bypassed the agent lease: {native_response}"
        );
    }

    {
        let provider = Arc::new(MemoryAgentProvider::new([
            ProviderAction::ForgetTarget,
            ProviderAction::Complete("agent mutation blocked"),
        ]));
        let executor = Arc::new(BlockingFirstMemoryForgetExecutor::new());
        let mut harness = MemoryAgentHarness::new_with_forget_executor(
            Arc::clone(&provider),
            Arc::clone(&executor) as _,
        )
        .await?;
        let native_session = harness.additional_session().await?;
        let entry = harness
            .remember("I prefer spaces", MemoryScope::User)
            .await?;
        provider.set_target(entry.entry_id.clone());
        let runtime = Arc::clone(&harness.runtime);
        let native_entry_id = entry.entry_id.clone();
        let native_task = tokio::spawn(async move {
            runtime
                .handle_incoming(
                    native_session.connection_id,
                    serde_json::json!({
                        "id": 21,
                        "method": "memory/forget",
                        "params": { "entryId": native_entry_id }
                    }),
                )
                .await
                .context("Native memory/forget response")
        });
        executor.wait_until_started().await?;
        harness
            .run_turn(&format!("Forget memory entry {}", entry.entry_id))
            .await?;
        let requests = provider.requests();
        let agent_result = assert_forget_rejected(&requests, 1, "already in flight");
        executor.release();
        let native_response = native_task.await??;
        agent_result?;
        anyhow::ensure!(
            native_response.get("result").is_some(),
            "Native mutation did not complete after release: {native_response}"
        );
    }
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-6, DD-12
/// Verifies: punctuation, whitespace, connectors, and bilingual task rewrites never authorize an arbitrary stable ID.
#[tokio::test]
async fn ordinary_task_rewrites_cannot_authorize_forget() -> Result<()> {
    let variants = [
        "Please forget that I work on tests and implement docs",
        "Please forget that I work on tests. Implement docs",
        "Please forget that I work on tests\nImplement docs",
        "PLEASE FORGET THAT I WORK ON TESTS AND IMPLEMENT DOCS",
        "请忘记我在写测试，然后实现文档",
        "请删除我不需要的文件，并实现文档",
    ];
    let actions = variants.iter().flat_map(|_| {
        [
            ProviderAction::ForgetTarget,
            ProviderAction::Complete("ordinary task preserved"),
        ]
    });
    let provider = Arc::new(MemoryAgentProvider::new(actions));
    let mut harness = MemoryAgentHarness::new(Arc::clone(&provider)).await?;
    let entry = harness.remember("I prefer tabs", MemoryScope::User).await?;
    provider.set_target(entry.entry_id.clone());

    for variant in variants {
        harness.run_turn(variant).await?;
    }

    let requests = provider.requests();
    for (index, variant) in variants.iter().enumerate() {
        assert_forget_rejected(&requests, index * 2 + 1, "exact stable-ID command")
            .with_context(|| format!("unsafe variant: {variant}"))?;
    }
    assert_eq!(
        harness.list(MemoryScope::User, MemoryState::Active).await?,
        Page {
            data: vec![entry],
            next_cursor: None,
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-10, DD-12
/// Verifies: default root-agent search includes explicitly restored identities.
#[tokio::test]
async fn default_search_includes_restored_entries() -> Result<()> {
    let provider = Arc::new(MemoryAgentProvider::new([
        ProviderAction::Search("dark mode"),
        ProviderAction::CaptureSearch,
    ]));
    let mut harness = MemoryAgentHarness::new(Arc::clone(&provider)).await?;
    let original = harness
        .remember("I prefer dark mode", MemoryScope::User)
        .await?;
    let response = harness
        .runtime
        .handle_incoming(
            harness.connection_id,
            serde_json::json!({
                "id": 5,
                "method": "memory/forget",
                "params": { "entryId": original.entry_id }
            }),
        )
        .await
        .context("memory/forget response")?;
    serde_json::from_value::<MemoryForgetResult>(response["result"].clone())
        .with_context(|| format!("decode memory/forget response: {response}"))?;
    let restored = harness
        .remember("I prefer dark mode", MemoryScope::User)
        .await?;
    let expected_restored = MemoryEntry {
        state: MemoryState::Restored,
        updated_at: restored.updated_at,
        ..original
    };
    assert_eq!(restored, expected_restored);

    harness.run_turn("Find my dark mode memory").await?;

    assert_eq!(
        provider.search_result(),
        Page {
            data: vec![search_entry(&expected_restored)],
            next_cursor: None,
        }
    );
    Ok(())
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-12
/// Verifies: a User-scope pending selection cannot authorize a Project-scope identity.
#[tokio::test]
async fn pending_selection_does_not_cross_memory_scopes() -> Result<()> {
    let provider = Arc::new(MemoryAgentProvider::new([
        ProviderAction::Search("tabs"),
        ProviderAction::CaptureSearch,
        ProviderAction::ForgetTarget,
        ProviderAction::Complete("cross-scope target rejected"),
    ]));
    let mut harness = MemoryAgentHarness::new(Arc::clone(&provider)).await?;
    let user_entry = harness.remember("I prefer tabs", MemoryScope::User).await?;
    let project_entry = harness
        .remember("The project uses tabs", MemoryScope::Project)
        .await?;
    provider.set_target(project_entry.entry_id.clone());

    harness.run_turn("Forget my tabs preference").await?;
    harness
        .run_turn(&format!(
            "Confirm forget memory entry {}",
            project_entry.entry_id
        ))
        .await?;

    assert_eq!(
        provider.search_result(),
        Page {
            data: vec![search_entry(&user_entry)],
            next_cursor: None,
        }
    );
    assert_forget_rejected(&provider.requests(), 3, "not one of the pending candidates")?;
    assert_eq!(
        harness
            .list(MemoryScope::Project, MemoryState::Active)
            .await?,
        Page {
            data: vec![project_entry],
            next_cursor: None,
        }
    );
    Ok(())
}

fn assert_forget_rejected(
    requests: &[ModelRequest],
    request_index: usize,
    expected_message: &str,
) -> Result<()> {
    let result = tool_result(
        requests
            .get(request_index)
            .with_context(|| format!("provider request {request_index}"))?,
        "memory-forget",
    )
    .context("memory forget result")?;
    anyhow::ensure!(
        result.contains(expected_message),
        "expected {expected_message:?} in memory_forget result: {result}"
    );
    Ok(())
}

fn search_entry(entry: &MemoryEntry) -> devo_protocol::native::rpc_memory::MemorySearchEntry {
    devo_protocol::native::rpc_memory::MemorySearchEntry {
        entry_id: entry.entry_id.clone(),
        scope: entry.scope,
        kind: entry.kind,
        state: entry.state,
        summary: entry.body.clone(),
    }
}

fn tool_call_events(id: &str, name: &str, input: serde_json::Value) -> Vec<StreamEvent> {
    vec![
        StreamEvent::ToolCallStart {
            index: 0,
            id: id.to_string(),
            name: name.to_string(),
            input: input.clone(),
        },
        StreamEvent::MessageDone {
            response: ModelResponse {
                id: format!("response-{id}"),
                content: vec![ResponseContent::ToolUse {
                    id: id.to_string(),
                    name: name.to_string(),
                    input,
                }],
                stop_reason: Some(StopReason::ToolUse),
                usage: Usage::default(),
                metadata: ResponseMetadata::default(),
            },
        },
    ]
}

fn text_events(text: &str) -> Vec<StreamEvent> {
    vec![
        StreamEvent::TextDelta {
            index: 0,
            text: text.to_string(),
        },
        StreamEvent::MessageDone {
            response: ModelResponse {
                id: "response-final".to_string(),
                content: vec![ResponseContent::Text(text.to_string())],
                stop_reason: Some(StopReason::EndTurn),
                usage: Usage::default(),
                metadata: ResponseMetadata::default(),
            },
        },
    ]
}

fn tool_result<'a>(request: &'a ModelRequest, tool_use_id: &str) -> Option<&'a str> {
    request
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|content| {
            let RequestContent::ToolResult {
                tool_use_id: result_id,
                content,
                ..
            } = content
            else {
                return None;
            };
            (result_id == tool_use_id).then_some(content.as_str())
        })
}
