//! Provider errors must not reintroduce recall bodies in query-loop logs.

#[path = "support/memory_log_privacy.rs"]
mod log_support;

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use devo_core::tools::{ToolRegistry, ToolRuntime};
use devo_core::{
    AgentError, Message, Model, QueryEvent, QueryOptions, SessionConfig, SessionState, TurnConfig,
    query,
};
use devo_provider::openai::OpenAIProvider;
use devo_provider::{ModelProviderSDK, ProviderHttpOptions};
use pretty_assertions::assert_eq;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::instrument::WithSubscriber;

use log_support::{EagerHttpErrorProvider, log_subscriber, read_http_request, write_http_response};

const MEMORY: &str = "<advisory_memory>Quoted memory: Use tabs.</advisory_memory>";
const CONVERSATION: &str = "Private earlier conversation";

#[derive(Clone, Copy)]
enum ErrorPath {
    StreamCreation,
    StreamEvent,
    Compaction,
}

async fn verify_query_error_log_privacy(path: ErrorPath) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (finished_tx, mut finished_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        loop {
            let (mut socket, request) = tokio::select! {
                request = read_http_request(&listener) => request?,
                _ = &mut finished_rx => return Ok::<_, anyhow::Error>(requests),
            };
            let streaming = request["stream"].as_bool().unwrap_or(false);
            let query_can_complete = matches!(path, ErrorPath::Compaction) && streaming;
            if query_can_complete {
                let chunk = serde_json::json!({
                    "id": "done",
                    "choices": [{"index": 0, "delta": {"content": "done"}, "finish_reason": "stop"}]
                });
                write_http_response(
                    &mut socket,
                    "200 OK",
                    "text/event-stream",
                    &format!("data: {chunk}\n\ndata: [DONE]\n\n"),
                )
                .await?;
            } else {
                let body = serde_json::json!({
                    "error": {
                        "message": format!("Rejected context: {MEMORY}\n{CONVERSATION}"),
                        "type": "invalid_request_error"
                    }
                });
                write_http_response(
                    &mut socket,
                    "400 Bad Request",
                    "application/json",
                    &body.to_string(),
                )
                .await?;
            }
            requests.push(request);
        }
    });
    let http_provider = OpenAIProvider::new(format!("http://{address}/v1")).with_http_options(
        // Pin loopback routing independently of machine proxy settings.
        ProviderHttpOptions::from_raw_with_no_proxy(
            /*proxy_url*/ Some(format!("http://{address}")),
            Some("127.0.0.1".into()),
            /*headers*/ None,
        )?,
    )?;
    let provider: Arc<dyn ModelProviderSDK> = match path {
        ErrorPath::StreamCreation => Arc::new(EagerHttpErrorProvider(http_provider)),
        ErrorPath::StreamEvent | ErrorPath::Compaction => Arc::new(http_provider),
    };
    let registry = Arc::new(ToolRegistry::new());
    let runtime = ToolRuntime::new_without_permissions(registry.clone());
    let workspace = tempfile::tempdir()?;
    let mut session = SessionState::new(SessionConfig::default(), workspace.path().to_path_buf());
    if matches!(path, ErrorPath::Compaction) {
        session.push_message(Message::user("x".repeat(/*n*/ 80_004)));
        session.push_message(Message::assistant_text(CONVERSATION));
        session.total_input_tokens = 200_000;
        session.last_turn_tokens = 200_000;
    }
    session.push_message(Message::user("Use tabs"));
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_callback = Arc::clone(&events);
    let callback = Arc::new(move |event| {
        events_for_callback.lock().expect("events").push(event);
        Box::pin(async {}) as Pin<Box<dyn futures::Future<Output = ()> + Send>>
    });
    let logs = Arc::new(Mutex::new(Vec::new()));
    let subscriber = log_subscriber(Arc::clone(&logs), tracing::Level::TRACE);
    let model = Model {
        slug: "gpt-4o".into(),
        ..Model::default()
    };
    let outcome = query(
        &mut session,
        &TurnConfig::new(model, /*reasoning_effort_selection*/ None),
        provider,
        registry,
        &runtime,
        Some(callback),
        QueryOptions {
            prepared_memory: Some(Arc::from(MEMORY)),
            ..QueryOptions::default()
        },
    )
    .with_subscriber(subscriber)
    .await;
    let _ = finished_tx.send(());
    let requests = server.await??;
    let recalled_messages = requests
        .iter()
        .map(|request| {
            request["messages"]
                .as_array()
                .expect("wire messages")
                .iter()
                .filter(|message| message["content"] == MEMORY)
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        recalled_messages,
        vec![vec![serde_json::json!({"role": "user", "content": MEMORY})]; requests.len()]
    );
    match path {
        ErrorPath::StreamCreation | ErrorPath::StreamEvent => {
            let Err(AgentError::Provider(error)) = outcome else {
                anyhow::bail!("query must surface the provider error");
            };
            assert!(devo_provider::diagnostic::user_message_for_error(&error).contains(MEMORY));
            assert!(
                devo_provider::diagnostic::user_message_for_error(&error).contains(CONVERSATION)
            );
        }
        ErrorPath::Compaction => {
            outcome?;
            let failures = events
                .lock()
                .expect("events")
                .iter()
                .filter_map(|event| {
                    if let QueryEvent::ContextCompactionFailed { message } = event {
                        Some(message.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(failures.len(), 1);
            assert!(failures[0].contains(MEMORY));
            assert!(failures[0].contains(CONVERSATION));
            assert!(requests.iter().any(|request| request["stream"] != true));
            assert_eq!(requests.last().expect("query request")["stream"], true);
        }
    }
    let logs = String::from_utf8(logs.lock().expect("logs").clone())?;
    let event_name = match path {
        ErrorPath::StreamCreation => "failed to create provider stream",
        ErrorPath::StreamEvent => "stream error",
        ErrorPath::Compaction => "LLM compaction failed",
    };
    assert!(logs.contains(event_name));
    let leaked_content = ["Quoted memory: Use tabs.", CONVERSATION]
        .into_iter()
        .filter(|private_content| logs.contains(private_content))
        .collect::<Vec<_>>();
    assert_eq!(leaked_content, Vec::<&str>::new());
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 Failure/Observability
/// Verifies: stream creation failures do not leak private provider text into logs.
#[tokio::test]
async fn query_stream_creation_failure_logs_omit_sensitive_error_bodies() -> Result<()> {
    verify_query_error_log_privacy(ErrorPath::StreamCreation).await
}

/// Trace: L2-DES-MEM-001 Rev 4 Failure/Observability
/// Verifies: stream event failures do not leak private provider text into logs.
#[tokio::test]
async fn query_stream_event_failure_logs_omit_sensitive_error_bodies() -> Result<()> {
    verify_query_error_log_privacy(ErrorPath::StreamEvent).await
}

/// Trace: L2-DES-MEM-001 Rev 4 Failure/Observability
/// Verifies: compaction failures do not leak private provider text into logs.
#[tokio::test]
async fn query_compaction_failure_logs_omit_sensitive_error_bodies() -> Result<()> {
    verify_query_error_log_privacy(ErrorPath::Compaction).await
}

/// Trace: L2-DES-MEM-001 Rev 4 Failure/Observability
/// Verifies: compaction errors redact private text in default representations.
#[test]
fn compaction_error_default_representations_omit_private_text() {
    use devo_core::history::compaction::CompactionError;
    let message = format!("{MEMORY}: {CONVERSATION}");
    let error = CompactionError::SummarizationFailed {
        message: message.clone().into(),
    };
    assert_eq!(
        error.user_message(),
        format!("summarization failed: {message}")
    );
    let outputs = [
        error.to_string(),
        format!("{error:?}"),
        serde_json::to_string(&error).expect("serialize compaction error"),
    ];
    assert_eq!(
        outputs
            .iter()
            .filter(|text| text.contains(MEMORY) || text.contains(CONVERSATION))
            .collect::<Vec<_>>(),
        Vec::<&String>::new()
    );
}
