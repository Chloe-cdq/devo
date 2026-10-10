//! Shared log capture and local HTTP fixtures for memory privacy regressions.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl Write for CapturedLogs {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("logs").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn log_subscriber(
    logs: Arc<Mutex<Vec<u8>>>,
    max_level: tracing::Level,
) -> impl tracing::Subscriber + Send + Sync {
    tracing_subscriber::fmt()
        .with_ansi(/*ansi*/ false)
        .without_time()
        .with_max_level(max_level)
        .with_writer(move || CapturedLogs(Arc::clone(&logs)))
        .finish()
}

pub async fn read_http_request(listener: &TcpListener) -> Result<(TcpStream, serde_json::Value)> {
    let (mut socket, _) = tokio::time::timeout(
        std::time::Duration::from_secs(/*secs*/ 10),
        listener.accept(),
    )
    .await
    .context("local HTTP fixture did not receive a request")??;
    let mut bytes = Vec::new();
    let (header_end, content_length) = loop {
        let mut chunk = [0_u8; 4096];
        let count = socket.read(&mut chunk).await?;
        anyhow::ensure!(count > 0, "request headers ended early");
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let headers = std::str::from_utf8(&bytes[..end])?;
            let length = headers
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim().parse::<usize>())
                .transpose()?
                .expect("request content length");
            break (end + 4, length);
        }
    };
    while bytes.len() < header_end + content_length {
        let mut chunk = [0_u8; 4096];
        let count = socket.read(&mut chunk).await?;
        anyhow::ensure!(count > 0, "request body ended early");
        bytes.extend_from_slice(&chunk[..count]);
    }
    let request = serde_json::from_slice(&bytes[header_end..header_end + content_length])?;
    Ok((socket, request))
}

pub async fn write_http_response(
    socket: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> Result<()> {
    let content_length = body.len();
    socket.write_all(format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n{body}"
    ).as_bytes()).await?;
    Ok(())
}

// Exercise adapters that finish an HTTP exchange before returning their stream.
#[allow(dead_code)]
pub struct EagerHttpErrorProvider(pub devo_provider::openai::OpenAIProvider);

#[async_trait::async_trait]
impl devo_provider::ModelProviderSDK for EagerHttpErrorProvider {
    async fn completion(
        &self,
        request: devo_protocol::ModelRequest,
    ) -> anyhow::Result<devo_protocol::ModelResponse> {
        self.0.completion(request).await
    }

    async fn completion_stream(
        &self,
        request: devo_protocol::ModelRequest,
    ) -> anyhow::Result<
        std::pin::Pin<
            Box<dyn futures::Stream<Item = anyhow::Result<devo_protocol::StreamEvent>> + Send>,
        >,
    > {
        use futures::StreamExt;
        let mut stream = self.0.completion_stream(request).await?;
        let error = stream
            .next()
            .await
            .context("HTTP error stream ended without an error")?
            .expect_err("test gateway must reject the request");
        // Legacy third-party adapters may flatten safe SDK failures back into text.
        // Exercise the independent router and query boundaries with that raw error.
        Err(anyhow::anyhow!(
            devo_provider::diagnostic::user_message_for_error(&error)
        ))
    }

    fn name(&self) -> &str {
        "eager-http-error-provider"
    }
}
