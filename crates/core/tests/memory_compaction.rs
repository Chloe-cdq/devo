//! Prepared recall stays immutable across both query-loop compaction paths.

#[path = "support/memory_log_privacy.rs"]
mod log_support;

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
use devo_protocol::{ModelProfileKey, RequestContent, RequestMessage, SamplingControls};
use devo_provider::error::ProviderError;
use devo_provider::openai::OpenAIProvider;
use devo_provider::{ModelProviderSDK, ProviderHttpOptions};
use futures::{Stream, StreamExt};
use pretty_assertions::assert_eq;
use tokio::net::TcpListener;
use tracing::instrument::WithSubscriber;

use log_support::{log_subscriber, read_http_request, write_http_response};

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
    let logs = Arc::new(Mutex::new(Vec::new()));
    let subscriber = log_subscriber(Arc::clone(&logs), tracing::Level::DEBUG);
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
    .with_subscriber(subscriber)
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
    let logs = String::from_utf8(logs.lock().expect("logs").clone())?;
    assert!(logs.contains("sending LLM compaction request"));
    assert!(logs.contains("received LLM compaction response"));
    let leaked_content = [
        "Quoted memory: Use tabs.",
        "Earlier response",
        "Earlier work summary",
    ]
    .into_iter()
    .filter(|private_content| logs.contains(private_content))
    .collect::<Vec<_>>();
    assert_eq!(leaked_content, Vec::<&str>::new());
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

#[derive(Clone, Copy)]
enum ProviderRequestMode {
    Streaming,
    CompletionError,
    StreamingError,
}

async fn verify_provider_log_privacy(mode: ProviderRequestMode) -> Result<()> {
    const RESPONSE_BODY: &str = "private provider response text";
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let (mut socket, request) = read_http_request(&listener).await?;
        let (status, content_type, body) = match mode {
            ProviderRequestMode::Streaming => {
                let chunk = serde_json::json!({
                    "id": "memory-response",
                    "choices": [{"index": 0, "delta": {"content": RESPONSE_BODY}, "finish_reason": "stop"}]
                });
                ("200 OK", "text/event-stream", format!("data: {chunk}\n\ndata: [DONE]\n\n"))
            }
            ProviderRequestMode::CompletionError | ProviderRequestMode::StreamingError => (
                "400 Bad Request",
                "application/json",
                serde_json::json!({"error": {"message": RESPONSE_BODY, "type": "invalid_request_error"}}).to_string(),
            ),
        };
        write_http_response(&mut socket, status, content_type, &body).await?;
        Ok::<_, anyhow::Error>(request)
    });
    let provider = OpenAIProvider::new(format!("http://{address}/v1")).with_http_options(
        ProviderHttpOptions::from_raw_with_no_proxy(
            /*proxy_url*/ None,
            Some("127.0.0.1".into()),
            /*headers*/ None,
        )?,
    )?;
    let request = ModelRequest {
        model_slug: ModelProfileKey::CatalogSlug("gpt-4o".into()),
        model: "gpt-4o".into(),
        system: None,
        messages: vec![RequestMessage {
            role: "user".into(),
            content: vec![RequestContent::Text {
                text: MEMORY.into(),
            }],
        }],
        max_tokens: 128,
        tools: None,
        hosted_tools: Vec::new(),
        sampling: SamplingControls::default(),
        request_thinking: None,
        reasoning_effort: None,
        extra_body: None,
    };
    let logs = Arc::new(Mutex::new(Vec::new()));
    let subscriber = log_subscriber(Arc::clone(&logs), tracing::Level::TRACE);
    async {
        match mode {
            ProviderRequestMode::CompletionError => {
                assert!(provider.completion(request).await.is_err());
            }
            ProviderRequestMode::Streaming | ProviderRequestMode::StreamingError => {
                let events = provider
                    .completion_stream(request)
                    .await?
                    .collect::<Vec<_>>()
                    .await;
                match mode {
                    ProviderRequestMode::Streaming => {
                        let events = events.into_iter().collect::<Result<Vec<_>>>()?;
                        let text = events
                            .into_iter()
                            .filter_map(|event| {
                                if let StreamEvent::TextDelta { text, .. } = event {
                                    Some(text)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>();
                        assert_eq!(text, vec![RESPONSE_BODY.to_string()]);
                    }
                    ProviderRequestMode::StreamingError => {
                        assert!(events.iter().any(Result::is_err))
                    }
                    ProviderRequestMode::CompletionError => {
                        unreachable!("completion handled above")
                    }
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .with_subscriber(subscriber)
    .await?;
    let wire_request = server.await??;
    assert_eq!(
        wire_request["messages"],
        serde_json::json!([
            {"role": "user", "content": MEMORY}
        ])
    );
    let logs = String::from_utf8(logs.lock().expect("logs").clone())?;
    match mode {
        ProviderRequestMode::Streaming => {
            assert!(logs.contains("sending openai streaming request"));
            assert!(logs.contains("openai chat completions raw stream event"));
        }
        ProviderRequestMode::CompletionError | ProviderRequestMode::StreamingError => {
            assert!(logs.contains("provider request failed"));
        }
    }
    let leaked_content = ["Quoted memory: Use tabs.", RESPONSE_BODY]
        .into_iter()
        .filter(|private_content| logs.contains(private_content))
        .collect::<Vec<_>>();
    assert_eq!(leaked_content, Vec::<&str>::new());
    Ok(())
}

#[tokio::test]
async fn provider_stream_logs_omit_recall_and_response_bodies() -> Result<()> {
    verify_provider_log_privacy(ProviderRequestMode::Streaming).await
}

#[tokio::test]
async fn provider_completion_error_logs_omit_recall_and_response_bodies() -> Result<()> {
    verify_provider_log_privacy(ProviderRequestMode::CompletionError).await
}

#[tokio::test]
async fn provider_stream_error_logs_omit_recall_and_response_bodies() -> Result<()> {
    verify_provider_log_privacy(ProviderRequestMode::StreamingError).await
}
