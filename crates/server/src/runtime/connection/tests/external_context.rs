use super::*;
use devo_core::{InternalRecordV2, ParsedRolloutLine, RolloutLineV2, parse_rollout_line};
use pretty_assertions::assert_eq;

struct ToolSearchProvider {
    calls: std::sync::atomic::AtomicUsize,
}

struct HostedWebProvider {
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl ModelProviderSDK for HostedWebProvider {
    async fn completion(&self, _request: ModelRequest) -> Result<ModelResponse> {
        anyhow::bail!("streaming only")
    }

    async fn completion_stream(
        &self,
        _request: ModelRequest,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent>> + Send>>> {
        let first = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
        let response = ModelResponse {
            id: "hosted".into(),
            content: if first {
                vec![devo_protocol::ResponseContent::HostedToolUse {
                    id: "hosted-1".into(),
                    name: "web_search".into(),
                    input: serde_json::json!({"query":"docs"}),
                    output: Some(serde_json::json!({"results":[]})),
                    status: Some("completed".into()),
                }]
            } else {
                vec![devo_protocol::ResponseContent::Text("done".into())]
            },
            stop_reason: Some(if first {
                devo_protocol::StopReason::ToolUse
            } else {
                devo_protocol::StopReason::EndTurn
            }),
            usage: devo_protocol::Usage::default(),
            metadata: devo_protocol::ResponseMetadata::default(),
        };
        let mut events = Vec::new();
        if first {
            events.push(Ok(StreamEvent::HostedToolCallStart {
                index: 0,
                id: "hosted-1".into(),
                name: "web_search".into(),
                input: serde_json::json!({"query":"docs"}),
            }));
        }
        events.push(Ok(StreamEvent::MessageDone { response }));
        Ok(Box::pin(futures::stream::iter(events)))
    }

    fn name(&self) -> &str {
        "hosted-web-provider"
    }
}

async fn completed_turn(
    runtime: &Arc<ServerRuntime>,
    turn_id: TurnId,
) -> Result<TerminalTurnSnapshot> {
    let receiver = runtime.subscribe_terminal_turn_status(turn_id).await;
    if let Some(terminal) = runtime.recent_terminal_turn_status(turn_id).await {
        return Ok(terminal);
    }
    Ok(tokio::time::timeout(Duration::from_secs(5), receiver).await??)
}

fn external_context_fact_count(path: &std::path::Path) -> Result<usize> {
    Ok(std::fs::read_to_string(path)?
        .lines()
        .filter_map(|line| parse_rollout_line(line).ok())
        .filter(|line| {
            matches!(line, ParsedRolloutLine::V2(v2)
            if matches!(v2.as_ref(), RolloutLineV2::Internal {
                entry: InternalRecordV2::ExternalContextUsed, ..
            }))
        })
        .count())
}

fn build_runtime_with_default_tools(
    data_root: &std::path::Path,
    provider: Arc<dyn ModelProviderSDK>,
) -> Arc<ServerRuntime> {
    let db = Arc::new(
        crate::db::Database::open(data_root.join("external_context.db"))
            .expect("open test database"),
    );
    ServerRuntime::with_protocols(
        data_root.to_path_buf(),
        ServerRuntimeDependencies::new(
            Arc::clone(&provider),
            Arc::new(SingleProviderRouter::new(provider)),
            Arc::new(devo_core::tools::create_default_tool_registry()),
            crate::empty_mcp_manager(),
            "test-model".to_string(),
            Arc::new(PresetModelCatalog::default()),
            Box::new(FileSystemSkillCatalog::new(SkillsConfig {
                bundled: Some(BundledSkillsConfig { enabled: false }),
                ..SkillsConfig::default()
            })),
            devo_core::AgentsMdConfig::default(),
            db,
            Arc::new(std::sync::Mutex::new(
                AppConfigStore::load(data_root.to_path_buf(), /*workspace_root*/ None)
                    .expect("load app config store"),
            )),
        ),
        ProtocolSet::all(),
    )
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: a failed rollout marker write still leaves a durable, non-destructive source exclusion.
#[tokio::test]
async fn failed_external_marker_write_keeps_source_excluded() -> Result<()> {
    let root = TempDir::new()?;
    let runtime = build_runtime(root.path());
    let session_id = SessionId::new();
    let blocked_path = root.path().join("blocked-rollout");
    std::fs::create_dir(&blocked_path)?;

    assert!(
        runtime
            .mark_external_context_used(
                Some(blocked_path),
                session_id,
                /*parent_session_id*/ None
            )
            .await
            .is_err()
    );
    assert_eq!(
        runtime.deps.db.pending_external_context_sources()?,
        vec![session_id]
    );
    assert!(
        runtime
            .deps
            .db
            .has_external_context_source(&session_id.to_string())?
    );
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-6.
/// Verifies: a failed middle marker cannot leave an unloaded ancestor eligible.
#[tokio::test]
async fn failed_parent_marker_still_excludes_entire_durable_ancestor_chain() -> Result<()> {
    let root = TempDir::new()?;
    let runtime = build_runtime(root.path());
    let connection_id = initialized_connection(&runtime).await;
    let parent_id = start_durable_session(&runtime, connection_id, root.path()).await?;
    let mut parent = runtime
        .deps
        .db
        .get_session(&parent_id)?
        .context("parent metadata")?;
    let grandparent_id = SessionId::new();
    let mut grandparent = parent.clone();
    grandparent.session_id = grandparent_id;
    grandparent.parent_session_id = None;
    runtime
        .deps
        .db
        .upsert_session(&grandparent, /*rollout_path*/ None)?;
    parent.parent_session_id = Some(grandparent_id);
    let blocked_path = root.path().join("blocked-parent-rollout");
    std::fs::create_dir(&blocked_path)?;
    runtime
        .deps
        .db
        .upsert_session(&parent, Some(&blocked_path))?;

    let child_id = SessionId::new();
    assert!(
        runtime
            .mark_external_context_used(/*rollout_path*/ None, child_id, Some(parent_id))
            .await
            .is_err()
    );
    assert_eq!(
        (
            runtime
                .deps
                .db
                .has_external_context_source(&child_id.to_string())?,
            runtime
                .deps
                .db
                .has_external_context_source(&parent_id.to_string())?,
            runtime
                .deps
                .db
                .has_external_context_source(&grandparent_id.to_string())?,
        ),
        (true, true, true)
    );
    Ok(())
}

#[async_trait]
impl ModelProviderSDK for ToolSearchProvider {
    async fn completion(&self, _request: ModelRequest) -> Result<ModelResponse> {
        anyhow::bail!("streaming only")
    }

    async fn completion_stream(
        &self,
        _request: ModelRequest,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = Result<StreamEvent>> + Send>>> {
        let first = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
        let response = if first {
            ModelResponse {
                id: "search-tool".into(),
                content: vec![devo_protocol::ResponseContent::ToolUse {
                    id: "tool-1".into(),
                    name: "ToolSearch".into(),
                    input: serde_json::json!({"query":"select:read"}),
                }],
                stop_reason: Some(devo_protocol::StopReason::ToolUse),
                usage: devo_protocol::Usage::default(),
                metadata: devo_protocol::ResponseMetadata::default(),
            }
        } else {
            ModelResponse {
                id: "done".into(),
                content: vec![devo_protocol::ResponseContent::Text("done".into())],
                stop_reason: Some(devo_protocol::StopReason::EndTurn),
                usage: devo_protocol::Usage::default(),
                metadata: devo_protocol::ResponseMetadata::default(),
            }
        };
        Ok(Box::pin(futures::stream::iter(vec![Ok(
            StreamEvent::MessageDone { response },
        )])))
    }

    fn name(&self) -> &str {
        "tool-search-provider"
    }
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: observed hosted Web use persists a session-wide provenance fact.
#[tokio::test]
async fn durable_turn_persists_observed_hosted_web_marker() -> Result<()> {
    let root = TempDir::new()?;
    std::fs::write(
        root.path().join("config.toml"),
        "[tools.web_search]\nmode = 'provider'\n",
    )?;
    let runtime = build_runtime_with_provider(
        root.path(),
        Arc::new(HostedWebProvider {
            calls: std::sync::atomic::AtomicUsize::new(0),
        }),
    );
    let connection_id = initialized_connection(&runtime).await;
    let session_id = start_durable_session(&runtime, connection_id, root.path()).await?;
    let record = runtime
        .session(session_id)
        .await
        .context("session handle")?
        .record()
        .await
        .flatten()
        .context("durable record")?;

    let turn_id = start_turn(&runtime, connection_id, session_id, "Search the web").await?;
    assert_eq!(
        completed_turn(&runtime, turn_id).await?.status,
        TurnStatus::Completed
    );
    assert_eq!(external_context_fact_count(&record.rollout_path)?, 1);
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: the production local-tool path records Tool Search use.
#[tokio::test]
async fn durable_turn_persists_tool_search_marker_on_dispatch() -> Result<()> {
    let root = TempDir::new()?;
    std::fs::write(
        root.path().join("config.toml"),
        "[tools.web_search]\nmode = 'disabled'\n[tools.web_fetch]\nmode = 'disabled'\n",
    )?;
    let provider = Arc::new(ToolSearchProvider {
        calls: std::sync::atomic::AtomicUsize::new(0),
    });
    let runtime = build_runtime_with_default_tools(root.path(), provider);
    let connection_id = initialized_connection(&runtime).await;
    let session_id = start_durable_session(&runtime, connection_id, root.path()).await?;
    let record = runtime
        .session(session_id)
        .await
        .context("session handle")?
        .record()
        .await
        .flatten()
        .context("durable record")?;

    let turn_id = start_turn(&runtime, connection_id, session_id, "Find a tool").await?;
    assert_eq!(
        completed_turn(&runtime, turn_id).await?.status,
        TurnStatus::Completed
    );
    assert_eq!(external_context_fact_count(&record.rollout_path)?, 1);
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: offered provider-hosted Web leaves a text-only durable turn eligible.
#[tokio::test]
async fn durable_text_only_turn_does_not_persist_external_context_marker() -> Result<()> {
    let root = TempDir::new()?;
    let provider = Arc::new(ToolSearchProvider {
        calls: std::sync::atomic::AtomicUsize::new(1),
    });
    let runtime = build_runtime_with_provider(root.path(), provider);
    let connection_id = initialized_connection(&runtime).await;
    let session_id = start_durable_session(&runtime, connection_id, root.path()).await?;
    let record = runtime
        .session(session_id)
        .await
        .context("session handle")?
        .record()
        .await
        .flatten()
        .context("durable record")?;
    let turn_id = start_turn(&runtime, connection_id, session_id, "Hello").await?;
    assert_eq!(
        completed_turn(&runtime, turn_id).await?.status,
        TurnStatus::Completed
    );
    assert_eq!(external_context_fact_count(&record.rollout_path)?, 0);
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: subagent external use taints its parent before returning content.
#[tokio::test]
async fn subagent_external_context_marks_parent_session() -> Result<()> {
    let root = TempDir::new()?;
    let runtime = build_runtime(root.path());
    let connection_id = initialized_connection(&runtime).await;
    let parent_id = start_durable_session(&runtime, connection_id, root.path()).await?;
    let record = runtime
        .session(parent_id)
        .await
        .context("parent handle")?
        .record()
        .await
        .flatten()
        .context("parent durable record")?;
    runtime
        .mark_external_context_used(
            /*rollout_path*/ None,
            SessionId::new(),
            Some(parent_id),
        )
        .await
        .map_err(anyhow::Error::msg)?;
    assert_eq!(external_context_fact_count(&record.rollout_path)?, 1);
    Ok(())
}
