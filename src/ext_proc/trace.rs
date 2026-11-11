//! Spans for each ext_proc stream and handler call.

use std::sync::Arc;

use futures::{Stream, stream};
use tracing::{Span, field::Empty};

use crate::envoy::{Head, PseudoHeader, Reply};

pub(super) type SpanInitializer = Arc<dyn Fn(&Span, &http::HeaderMap) + Send + Sync>;

pub(super) fn stream_span() -> Span {
    tracing::info_span!(
        "ext_proc",
        otel.kind = "server",
        otel.status_code = Empty,
        request_id = Empty,
        http.request.method = Empty,
        url.path = Empty,
        http.response.status_code = Empty,
    )
}

/// Tonic runs the processing loop after `process` returns; enter the request span so its logs belong to that request.
/// Exit whenever the loop yields or waits, so unrelated work does not inherit the span.
pub(super) fn instrument_stream<S>(stream: S, span: Span) -> impl Stream<Item = S::Item> + Send
where
    S: Stream + Send + 'static,
{
    let mut stream = Box::pin(stream);
    stream::poll_fn(move |cx| {
        let _entered = span.enter();
        stream.as_mut().poll_next(cx)
    })
}

/// `detail` is an optional application-defined classification recorded by the handler.
pub(super) fn handler_span(parent: &Span, head: &Head) -> Span {
    match head {
        Head::Request(_) => {
            tracing::info_span!(parent: parent, "handle_request", otel.status_code = Empty, detail = Empty)
        }
        Head::Response(_) => {
            tracing::info_span!(parent: parent, "handle_response", otel.status_code = Empty, detail = Empty)
        }
    }
}

/// Records the request line or the response status on the stream span.
pub(super) fn record_head(span: &Span, head: &Head) {
    match head {
        Head::Request(parts) => {
            span.record("http.request.method", parts.method.as_str());
            span.record("url.path", parts.uri.path());
            if let Some(id) = parts.headers.get("x-request-id") {
                span.record("request_id", id.to_str().unwrap_or_default());
            }
        }
        Head::Response(parts) => {
            span.record("http.response.status_code", parts.status.as_u16());
        }
    }
}

pub(super) fn mark_error(span: &Span) {
    span.record("otel.status_code", "ERROR");
}

/// Records status changes only when they are sent to Envoy.
pub(super) fn record_reply(span: &Span, reply: &Reply) {
    let status = match reply {
        Reply::ImmediateResponse(response) => Some(response.status),
        Reply::HeadersNoBody { mutation, .. }
        | Reply::HeadersReplaceBody { mutation, .. }
        | Reply::LastBody { mutation, .. } => match mutation.pseudo {
            Some(PseudoHeader::Status(status)) => Some(status),
            _ => None,
        },
        _ => None,
    };
    if let Some(status) = status {
        span.record("http.response.status_code", status.as_u16());
    }
}
