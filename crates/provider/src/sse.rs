//! One-shot SSE transport with response-header quota capture.
//!
//! Adapters stop on the first transport error, so EventSource reconnection was
//! never observed. Keep its event/error surface, lazy request, and cancellation
//! while inspecting headers before handing the body to the same SSE parser.

use std::pin::Pin;

use eventsource_stream::Eventsource;
use futures::{Stream, StreamExt};
use reqwest::{
    RequestBuilder, StatusCode,
    header::{ACCEPT, CONTENT_TYPE, HeaderValue},
};
use reqwest_eventsource::{CannotCloneRequestError, Error, Event};

use crate::quota::{QuotaHeaderFamily, QuotaTelemetry};

type EventStream = Pin<Box<dyn Stream<Item = Result<Event, Error>> + Send>>;

pub(crate) fn quota_event_source(
    builder: RequestBuilder,
    quota: QuotaTelemetry,
    family: QuotaHeaderFamily,
) -> Result<EventStream, CannotCloneRequestError> {
    let builder = builder
        .header(ACCEPT, "text/event-stream")
        .try_clone()
        .ok_or(CannotCloneRequestError)?;
    let stream = async_stream::try_stream! {
        let request_generation = quota.begin_request();
        let response = builder.send().await.map_err(Error::Transport)?;
        quota.update(request_generation, response.headers(), family);
        let status = response.status();
        let response = if status == StatusCode::OK {
            response
        } else {
            Err(Error::InvalidStatusCode(status, response))?
        };
        let content_type = response.headers().get(CONTENT_TYPE)
            .cloned().unwrap_or_else(|| HeaderValue::from_static(""));
        let is_sse = content_type.to_str().ok().is_some_and(|value| {
            value.split(';').next().is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("text/event-stream"))
        });
        let response = if is_sse {
            response
        } else {
            Err(Error::InvalidContentType(content_type, response))?
        };
        yield Event::Open;
        let messages = response.bytes_stream().eventsource();
        futures::pin_mut!(messages);
        while let Some(message) = messages.next().await {
            yield Event::Message(message.map_err(Error::from)?);
        }
        Err(Error::StreamEnded)?;
    };
    Ok(Box::pin(stream))
}
