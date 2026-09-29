//! Prepared recall stays immutable across both query-loop compaction paths.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use devo_core::tools::{ToolRegistry, ToolRuntime};
use devo_core::{
    Message, Model, ModelRequest, ModelResponse, QueryOptions, ResponseContent, SessionConfig,
    SessionState, StopReason, StreamEvent, TurnConfig, Usage, query,
};
use devo_protocol::RequestContent;
use devo_provider::ModelProviderSDK;
use devo_provider::error::ProviderError;
use futures::Stream;
use pretty_assertions::assert_eq;

const MEMORY: &str = "<advisory_memory>Quoted memory: Use tabs.</advisory_memory>";

#[derive(Clone, Copy)]
enum CompactionTrigger {
    Auto,
    ContextLimit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestKind {
    Compaction,
    Query,
}

struct CompactionProvider {
    trigger: CompactionTrigger,
    stream_attempts: AtomicUsize,
    requests: Mutex<Vec<(RequestKind, ModelRequest)>>,
}

#[async_trait]
impl ModelProviderSDK for CompactionProvider {
    async fn completion(&self, request: ModelRequest) -> Result<ModelResponse> {
        self.requests
            .lock()
            .expect("requests")
            .push((RequestKind::Compaction, request));
        Ok(ModelResponse {
            id: "summary".into(),
            content: vec![ResponseContent::Text("Earlier work summary".into())],
            stop_reason: Some(StopReason::EndTurn),
            usage: Usage::default(),
            metadata: Default::default(),
        })
    }

    async fn completion_stream(
        &self,
        request: ModelRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamEvent>> + Send>>> {
        self.requests
            .lock()
            .expect("requests")
            .push((RequestKind::Query, request));
        if self.stream_attempts.fetch_add(1, Ordering::SeqCst) == 0
            && matches!(self.trigger, CompactionTrigger::ContextLimit)
        {
            return Err(ProviderError::ContextLimitError {
                message: "maximum context length exceeded".into(),
                current_tokens: None,
                limit: None,
            }
            .into());
        }
        Ok(Box::pin(futures::stream::iter([Ok(
            StreamEvent::MessageDone {
                response: ModelResponse {
                    id: "done".into(),
                    content: vec![ResponseContent::Text("done".into())],
                    stop_reason: Some(StopReason::EndTurn),
                    usage: Usage::default(),
                    metadata: Default::default(),
                },
            },
        )])))
    }

    fn name(&self) -> &str {
        "memory-compaction-provider"
    }
}

async fn verify_compaction_recall(trigger: CompactionTrigger) -> Result<()> {
    let provider = Arc::new(CompactionProvider {
        trigger,
        stream_attempts: AtomicUsize::new(/*v*/ 0),
        requests: Mutex::new(Vec::new()),
    });
    let registry = Arc::new(ToolRegistry::new());
    let runtime = ToolRuntime::new_without_permissions(registry.clone());
    let workspace = tempfile::tempdir()?;
    let mut session = SessionState::new(SessionConfig::default(), workspace.path().to_path_buf());
    session.push_message(Message::user("x".repeat(/*n*/ 80_004)));
    session.push_message(Message::assistant_text("Earlier response"));
    session.push_message(Message::user("Use tabs"));
    if matches!(trigger, CompactionTrigger::Auto) {
        session.total_input_tokens = 200_000;
        session.last_turn_tokens = 200_000;
    }
    query(
        &mut session,
        &TurnConfig::new(Model::default(), /*reasoning_effort_selection*/ None),
        provider.clone(),
        registry,
        &runtime,
        /*on_event*/ None,
        QueryOptions {
            prepared_memory: Some(Arc::from(MEMORY)),
            ..QueryOptions::default()
        },
    )
    .await?;
    let actual = provider
        .requests
        .lock()
        .expect("requests")
        .iter()
        .map(|(kind, request)| {
            let memory = request
                .messages
                .iter()
                .filter(|message| {
                    message.content.iter().any(|content|
                        matches!(content, RequestContent::Text { text } if text.starts_with("<advisory_memory>")))
                })
                .map(|message| serde_json::to_value(message).expect("memory message"))
                .collect::<Vec<_>>();
            (*kind, memory)
        })
        .collect::<Vec<_>>();
    let memory = serde_json::json!({
        "role": "user",
        "content": [{"type": "text", "text": MEMORY}]
    });
    let expected = match trigger {
        CompactionTrigger::Auto => vec![
            (RequestKind::Compaction, vec![memory.clone()]),
            (RequestKind::Query, vec![memory]),
        ],
        CompactionTrigger::ContextLimit => vec![
            (RequestKind::Query, vec![memory.clone()]),
            (RequestKind::Compaction, vec![memory.clone()]),
            (RequestKind::Query, vec![memory]),
        ],
    };
    assert_eq!(actual, expected);
    assert!(
        session.prompt_source_messages().iter().all(|message| {
            !serde_json::to_string(message)
                .expect("message")
                .contains("advisory_memory")
        }),
        "recall must stay outside persisted prompt history"
    );
    Ok(())
}

#[tokio::test]
async fn auto_compaction_receives_the_same_recall_as_the_first_query() -> Result<()> {
    verify_compaction_recall(CompactionTrigger::Auto).await
}

#[tokio::test]
async fn context_limit_compaction_and_retry_receive_the_original_recall() -> Result<()> {
    verify_compaction_recall(CompactionTrigger::ContextLimit).await
}
