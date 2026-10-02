use std::sync::Arc;

use devo_protocol::{ModelProfileKey, ModelRequest, RequestContent, RequestMessage};
use devo_provider::{
    ModelProviderSDK,
    anthropic::AnthropicProvider,
    openai::{OpenAIProvider, OpenAIResponsesProvider},
};
use futures::StreamExt;
use pretty_assertions::assert_eq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[derive(Clone, Copy)]
enum Adapter {
    Chat,
    Responses,
    Anthropic,
}

impl Adapter {
    fn provider(self, base_url: String) -> Arc<dyn ModelProviderSDK> {
        match self {
            Self::Chat => Arc::new(OpenAIProvider::new(base_url)),
            Self::Responses => Arc::new(OpenAIResponsesProvider::new(base_url)),
            Self::Anthropic => Arc::new(AnthropicProvider::new(base_url)),
        }
    }

    fn headers(self) -> &'static str {
        match self {
            Self::Chat | Self::Responses => concat!(
                "x-ratelimit-limit-requests: 100\r\n",
                "x-ratelimit-remaining-requests: 80\r\n",
                "x-ratelimit-limit-tokens: 1000\r\n",
                "x-ratelimit-remaining-tokens: 359\r\n",
            ),
            Self::Anthropic => concat!(
                "anthropic-ratelimit-requests-limit: 100\r\n",
                "anthropic-ratelimit-requests-remaining: 80\r\n",
                "anthropic-ratelimit-input-tokens-limit: 1000\r\n",
                "anthropic-ratelimit-input-tokens-remaining: 359\r\n",
                "anthropic-ratelimit-output-tokens-limit: 1000\r\n",
                "anthropic-ratelimit-output-tokens-remaining: 900\r\n",
            ),
        }
    }

    fn body(self) -> &'static str {
        match self {
            Self::Chat => {
                r#"{"id":"chat-test","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"OK"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}"#
            }
            Self::Responses => {
                r#"{"id":"responses-test","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"OK"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}"#
            }
            Self::Anthropic => {
                r#"{"id":"anthropic-test","content":[{"type":"text","text":"OK"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}"#
            }
        }
    }

    fn stream_body(self) -> &'static str {
        match self {
            Self::Chat | Self::Responses => "data: [DONE]\n\n",
            Self::Anthropic => "event: message_stop\ndata: {}\n\n",
        }
    }
}

fn request() -> ModelRequest {
    ModelRequest {
        model_slug: ModelProfileKey::Generic,
        model: "quota-test".to_string(),
        system: None,
        messages: vec![RequestMessage {
            role: "user".to_string(),
            content: vec![RequestContent::Text {
                text: "private-extraction-content".to_string(),
            }],
        }],
        max_tokens: 16,
        tools: None,
        hosted_tools: Vec::new(),
        sampling: Default::default(),
        request_thinking: None,
        reasoning_effort: None,
        extra_body: None,
    }
}

async fn server(responses: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
    let address = listener.local_addr().expect("server address");
    let task = tokio::spawn(async move {
        let mut requests = Vec::new();
        for response in responses {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut bytes = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let count = socket.read(&mut buffer).await.expect("read request");
                assert!(count > 0, "request must finish before connection closes");
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(boundary) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..boundary]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().expect("content length"))
                        })
                        .unwrap_or_default();
                    if bytes.len() >= boundary + 4 + length {
                        break;
                    }
                }
            }
            requests.push(String::from_utf8(bytes).expect("request UTF-8"));
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        }
        requests
    });
    (format!("http://{address}"), task)
}

fn response(status: &str, content_type: &str, headers: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\n{headers}content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: completed requests report minimum quota window rounded down.
#[tokio::test]
async fn completed_requests_report_minimum_quota_window_rounded_down() {
    let mut observed = Vec::new();
    for adapter in [Adapter::Chat, Adapter::Responses, Adapter::Anthropic] {
        let (url, capture) = server(vec![response(
            "200 OK",
            "application/json",
            adapter.headers(),
            adapter.body(),
        )])
        .await;
        let provider = adapter.provider(url);
        assert_eq!(provider.remaining_quota_percent(), None);
        provider
            .completion(request())
            .await
            .expect("complete request");
        observed.push(provider.remaining_quota_percent());
        capture.await.expect("server task");
    }
    assert_eq!(observed, vec![Some(35), Some(35), Some(35)]);
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: streaming requests capture quota from success response headers.
#[tokio::test]
async fn streaming_requests_capture_quota_from_success_response_headers() {
    let mut observed = Vec::new();
    for adapter in [Adapter::Chat, Adapter::Responses, Adapter::Anthropic] {
        let (url, capture) = server(vec![response(
            "200 OK",
            "text/event-stream",
            adapter.headers(),
            adapter.stream_body(),
        )])
        .await;
        let provider = adapter.provider(url);
        let mut stream = provider
            .completion_stream(request())
            .await
            .expect("start stream");
        while let Some(event) = stream.next().await {
            event.expect("stream event");
        }
        observed.push(provider.remaining_quota_percent());
        capture.await.expect("server task");
    }
    assert_eq!(observed, vec![Some(35), Some(35), Some(35)]);
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: response without quota headers clears previous observation.
#[tokio::test]
async fn response_without_quota_headers_clears_previous_observation() {
    let mut observed = Vec::new();
    for adapter in [Adapter::Chat, Adapter::Responses, Adapter::Anthropic] {
        let (url, capture) = server(vec![
            response(
                "200 OK",
                "application/json",
                adapter.headers(),
                adapter.body(),
            ),
            response("200 OK", "application/json", "", adapter.body()),
        ])
        .await;
        let provider = adapter.provider(url);
        provider
            .completion(request())
            .await
            .expect("first completion");
        observed.push(provider.remaining_quota_percent());
        provider
            .completion(request())
            .await
            .expect("second completion");
        observed.push(provider.remaining_quota_percent());
        capture.await.expect("server task");
    }
    assert_eq!(
        observed,
        vec![Some(35), None, Some(35), None, Some(35), None]
    );
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: background http errors redact response content and strip internal marker.
#[tokio::test]
async fn background_http_errors_redact_response_content_and_strip_internal_marker() {
    for adapter in [Adapter::Chat, Adapter::Responses, Adapter::Anthropic] {
        let (url, capture) = server(vec![response(
            "429 Too Many Requests",
            "application/json",
            adapter.headers(),
            r#"{"error":{"message":"private-extraction-content"}}"#,
        )])
        .await;
        let provider = adapter.provider(url);
        let mut background = request();
        background.extra_body = Some(serde_json::json!({"__devo_background_request": true}));
        let error = provider
            .completion(background)
            .await
            .expect_err("rate limit failure");
        assert!(
            !error.to_string().contains("private-extraction-content"),
            "background error must redact echoed content: {error}"
        );
        let requests = capture.await.expect("server task");
        let body: serde_json::Value =
            serde_json::from_str(requests[0].split_once("\r\n\r\n").expect("request body").1)
                .expect("JSON request");
        assert_eq!(body.get("__devo_background_request"), None);
        assert_eq!(provider.remaining_quota_percent(), Some(35));
    }
}
/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: background stream diagnostics do not record request or response content.
#[tokio::test]
async fn background_stream_diagnostics_do_not_record_request_or_response_content() {
    use std::{
        io::{self, Write},
        sync::Mutex,
    };

    struct Capture(Arc<Mutex<Vec<u8>>>);
    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("log capture").extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    let logs = Arc::new(Mutex::new(Vec::new()));
    let writer = Arc::clone(&logs);
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .without_time()
        .with_ansi(/*ansi*/ false)
        .with_writer(move || Capture(Arc::clone(&writer)))
        .finish();
    use tracing::instrument::WithSubscriber;
    async {
    let body = concat!(
        "data: {\"id\":\"test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"private-extraction-response\"}}]}\n\n",
        "data: [DONE]\n\n",
    );
    let (url, capture) = server(vec![
        response("200 OK", "text/event-stream", "", body),
        response(
            "429 Too Many Requests",
            "application/json",
            "",
            r#"{"error":{"message":"private-extraction-response"}}"#,
        ),
    ])
    .await;
    let provider = OpenAIProvider::new(url);
    let mut background = request();
    background.extra_body = Some(serde_json::json!({"__devo_background_request": true}));
    let mut stream = provider
        .completion_stream(background.clone())
        .await
        .expect("stream request");
    while let Some(event) = stream.next().await {
        event.expect("stream event");
    }
    provider
        .completion(background)
        .await
        .expect_err("background HTTP failure");
    capture.await.expect("server task");
    }.with_subscriber(subscriber).await;
    let logs = String::from_utf8(logs.lock().expect("log capture").clone()).expect("UTF-8 logs");
    assert!(
        !logs.is_empty(),
        "subscriber must capture provider diagnostics"
    );
    assert!(
        !logs.contains("private-extraction-content"),
        "request text must be absent from logs: {logs}"
    );
    assert!(
        !logs.contains("private-extraction-response"),
        "response text must be absent from logs: {logs}"
    );
}
/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: transport failure clears previous quota observation.
#[tokio::test]
async fn transport_failure_clears_previous_quota_observation() {
    let mut observed = Vec::new();
    for adapter in [Adapter::Chat, Adapter::Responses, Adapter::Anthropic] {
        let (url, capture) = server(vec![response(
            "200 OK",
            "application/json",
            adapter.headers(),
            adapter.body(),
        )])
        .await;
        let provider = adapter.provider(url);
        provider
            .completion(request())
            .await
            .expect("first completion");
        capture.await.expect("closed server");
        observed.push(provider.remaining_quota_percent());
        provider
            .completion(request())
            .await
            .expect_err("closed transport");
        observed.push(provider.remaining_quota_percent());
    }
    assert_eq!(
        observed,
        vec![Some(35), None, Some(35), None, Some(35), None]
    );
}
/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: background stream http errors capture quota without echoing response content.
#[tokio::test]
async fn background_stream_http_errors_capture_quota_without_echoing_response_content() {
    let mut observed = Vec::new();
    for adapter in [Adapter::Chat, Adapter::Responses, Adapter::Anthropic] {
        let (url, capture) = server(vec![response(
            "429 Too Many Requests",
            "application/json",
            adapter.headers(),
            r#"{"error":{"message":"private-extraction-response"}}"#,
        )])
        .await;
        let provider = adapter.provider(url);
        let mut background = request();
        background.extra_body = Some(serde_json::json!({"__devo_background_request": true}));
        let mut stream = provider
            .completion_stream(background)
            .await
            .expect("start stream");
        let error = stream
            .next()
            .await
            .expect("stream error")
            .expect_err("HTTP failure");
        assert!(
            !error.to_string().contains("private-extraction-response"),
            "background error must redact echoed content: {error}"
        );
        observed.push(provider.remaining_quota_percent());
        capture.await.expect("server task");
    }
    assert_eq!(observed, vec![Some(35), Some(35), Some(35)]);
}
/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: background failures keep retry classification through provider router.
#[tokio::test]
async fn background_failures_keep_retry_classification_through_provider_router() {
    use devo_provider::{ProviderRoute, ProviderRouter, SingleProviderRouter};

    let cases = [
        (
            "429 Too Many Requests",
            serde_json::json!({"error_kind":"rate_limit_error","message":"Background provider request was rate limited","retry_after_seconds":2,"provider_name":"openai"}),
        ),
        (
            "503 Service Unavailable",
            serde_json::json!({"error_kind":"provider_server_error","message":"Background provider request failed","status_code":503,"provider_name":"openai"}),
        ),
        (
            "401 Unauthorized",
            serde_json::json!({"error_kind":"authentication_error","message":"Background provider request authentication failed","status_code":401,"provider_name":"openai"}),
        ),
        (
            "403 Forbidden",
            serde_json::json!({"error_kind":"authentication_error","message":"Background provider request authentication failed","status_code":403,"provider_name":"openai"}),
        ),
        (
            "400 Bad Request",
            serde_json::json!({"error_kind":"invalid_request_error","message":"Background provider request was rejected","details":"HTTP status 400"}),
        ),
    ];
    let mut observed = Vec::new();
    let mut expected = Vec::new();
    for (status, mut wanted) in cases {
        let (url, capture) = server(vec![response(
            status,
            "application/json",
            "retry-after: 2\r\n",
            r#"{"error":{"message":"private-extraction-response"}}"#,
        )])
        .await;
        let router = SingleProviderRouter::new(Arc::new(OpenAIProvider::new(url)));
        let mut background = request();
        background.extra_body = Some(serde_json::json!({"__devo_background_request": true}));
        let error = router
            .complete(ProviderRoute::Default, background)
            .await
            .expect_err("background failure");
        assert!(
            error
                .user_message()
                .contains(wanted["message"].as_str().unwrap())
        );
        wanted["message"] = serde_json::json!("[redacted]");
        if wanted.get("details").is_some() {
            wanted["details"] = serde_json::json!("[redacted]");
        }
        observed.push(serde_json::to_value(error).expect("serialize error"));
        expected.push(wanted);
        capture.await.expect("server task");
    }
    assert_eq!(observed, expected);
}
