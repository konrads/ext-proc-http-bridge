//! In-memory tests of the real processing loop and its asynchronous tasks.

use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroUsize,
    pin::pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::{BufMut, BytesMut};
use envoy_types::pb::envoy::{
    config::core::v3::{HeaderMap, HeaderValue},
    extensions::filters::http::ext_proc::v3::{
        ProcessingMode,
        processing_mode::{BodySendMode, HeaderSendMode},
    },
    service::ext_proc::v3::{
        BodyResponse, CommonResponse, HeadersResponse, HttpBody, HttpHeaders, HttpTrailers,
        ImmediateResponse as EnvoyImmediateResponse, TrailersResponse, processing_request,
        processing_response,
    },
};
use http_body_util::StreamBody;
use tonic::metadata::MetadataMap;
use tonic_prost::{ProstDecoder, prost::Message};
use tracing::{
    Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{
    Layer, Registry,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};

use super::*;
use crate::{
    BodyError,
    envoy::Direction,
    handler::{HandlerAction, RequestContinue, ResponseContinue},
};

#[derive(Debug, Clone, Copy)]
enum Mode {
    Collect,
    DropStream,
    Blocked,
    Immediate,
    Stream,
}

struct TestHandler {
    mode: Mode,
    received: Mutex<Option<Result<Bytes, BodyError>>>,
    dropped: AtomicBool,
}

type Input = mpsc::Sender<Result<ProcessingRequest, Status>>;

struct DropSignal<'a>(&'a AtomicBool);

struct FailingHandler;

#[test]
fn stream_span_is_current_only_during_poll() {
    use std::task::{Context, Poll};

    let _subscriber = test_subscriber(Registry::default());
    let caller = tracing::info_span!("caller");
    let _entered = caller.enter();
    let request = trace::stream_span();
    let expected = request.id();
    assert!(expected.is_some());
    let mut first = true;
    let stream = futures::stream::poll_fn(move |_| {
        assert_eq!(Span::current().id(), expected);
        let was_first = first;
        first = false;
        if was_first {
            Poll::Pending
        } else {
            Poll::Ready(Some(()))
        }
    });
    let mut stream = pin!(trace::instrument_stream(stream, request));
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Pending);
    assert_eq!(Span::current().id(), caller.id());
    assert_eq!(stream.as_mut().poll_next(&mut cx), Poll::Ready(Some(())));
    assert_eq!(Span::current().id(), caller.id());
}

/// Collects each span with its parent and fields when it closes.
#[derive(Clone, Default)]
struct SpanLog(Arc<Mutex<Vec<SpanRecord>>>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct SpanRecord {
    name: &'static str,
    parent: Option<&'static str>,
    fields: BTreeMap<String, String>,
}

struct FieldVisitor<'a>(&'a mut BTreeMap<String, String>);

#[tokio::test(start_paused = true)]
async fn collection_requests_body_before_waiting_for_its_bytes() {
    let handler = Arc::new(TestHandler::new(Mode::Collect));
    let (input, mut output) = start(handler.clone(), ExtProcConfig::default()).await;
    input
        .send(Ok(headers(Direction::Request, false)))
        .await
        .unwrap();
    assert_eq!(
        reply(&mut output).await.unwrap(),
        headers_reply(Direction::Request, BodySendMode::Buffered)
    );
    input
        .send(Ok(body(Direction::Request, b"payload", true)))
        .await
        .unwrap();
    assert_eq!(
        reply(&mut output).await.unwrap(),
        body_reply(Direction::Request)
    );
    assert_eq!(
        *handler.received.lock().unwrap(),
        Some(Ok(Bytes::from_static(b"payload")))
    );
    drop(input);
    assert!(output.next().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn body_request_precedes_an_immediately_returned_handler_result() {
    let (input, mut output) = start(
        Arc::new(TestHandler::new(Mode::DropStream)),
        ExtProcConfig::default(),
    )
    .await;
    input
        .send(Ok(headers(Direction::Request, false)))
        .await
        .unwrap();
    assert_eq!(
        reply(&mut output).await.unwrap(),
        headers_reply(Direction::Request, BodySendMode::Streamed)
    );
    input
        .send(Ok(body(Direction::Request, b"ignored", true)))
        .await
        .unwrap();
    assert_eq!(
        reply(&mut output).await.unwrap(),
        body_reply(Direction::Request)
    );
    drop(input);
    assert!(output.next().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn terminal_error_cancels_the_handler_before_yielding() {
    let handler = Arc::new(TestHandler::new(Mode::Collect));
    let (input, mut output) = start(handler.clone(), ExtProcConfig::default()).await;
    input
        .send(Ok(headers(Direction::Request, false)))
        .await
        .unwrap();
    assert_eq!(
        reply(&mut output).await.unwrap(),
        headers_reply(Direction::Request, BodySendMode::Buffered)
    );
    input
        .send(Err(Status::unavailable("read failed")))
        .await
        .unwrap();
    let error = reply(&mut output).await.unwrap_err();
    assert_eq!(
        (error.code(), error.message()),
        (tonic::Code::Unavailable, "read failed")
    );
    assert!(handler.dropped.load(Ordering::SeqCst));
    assert!(output.next().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn dropping_output_cancels_backpressured_delivery_and_the_handler() {
    let handler = Arc::new(TestHandler::new(Mode::Blocked));
    let config = ExtProcConfig {
        body: BodyConfig {
            channel_capacity: NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        },
        ..Default::default()
    };
    let (input, mut output) = start(handler.clone(), config).await;
    input
        .send(Ok(headers(Direction::Request, false)))
        .await
        .unwrap();
    assert_eq!(
        reply(&mut output).await.unwrap(),
        headers_reply(Direction::Request, BodySendMode::Streamed)
    );
    input
        .send(Ok(body(Direction::Request, b"first", false)))
        .await
        .unwrap();
    assert_eq!(
        reply(&mut output).await.unwrap(),
        body_reply(Direction::Request)
    );
    input
        .send(Ok(body(Direction::Request, b"blocked", false)))
        .await
        .unwrap();
    input
        .send(Err(Status::unavailable("must not be read yet")))
        .await
        .unwrap();
    assert!(futures::poll!(output.next()).is_pending());
    drop(output);
    assert!(handler.dropped.load(Ordering::SeqCst));
    assert!(input.is_closed());
}

#[tokio::test]
async fn send_waits_for_room_in_a_full_channel() {
    let (tx, mut rx) = mpsc::channel(1);
    let mut task = send_chunk(tx, Bytes::from_static(b"a"), true);
    assert!(futures::poll!(&mut task).is_pending());
    assert!(matches!(rx.recv().await, Some(BodyMessage::Frame(Ok(_)))));
    let completion = task.await;
    assert!(matches!(completion.event, Event::ChunkSent { last: true }));
    assert!(matches!(rx.recv().await, Some(BodyMessage::End)));
}

#[tokio::test]
async fn send_to_a_dropped_body_completes() {
    for full in [false, true] {
        let (tx, rx) = mpsc::channel(1);
        if full {
            tx.try_send(BodyMessage::End).unwrap();
        }
        let mut task = send_chunk(tx, Bytes::new(), true);
        if full {
            assert!(futures::poll!(&mut task).is_pending());
        }
        drop(rx);
        assert!(matches!(task.await.event, Event::ChunkSent { last: true }));
    }
}

#[tokio::test(start_paused = true)]
async fn immediate_response_cancels_pending_reads_before_yielding() {
    let (input, mut output) = start(
        Arc::new(TestHandler::new(Mode::Immediate)),
        ExtProcConfig::default(),
    )
    .await;
    input
        .send(Ok(headers(Direction::Request, false)))
        .await
        .unwrap();
    assert_eq!(
        reply(&mut output).await.unwrap(),
        headers_reply(Direction::Request, BodySendMode::Streamed)
    );
    let expected = ProcessingResponse {
        response: Some(processing_response::Response::ImmediateResponse(
            EnvoyImmediateResponse {
                status: Some(envoy_types::pb::envoy::r#type::v3::HttpStatus { code: 403 }),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    assert_eq!(reply(&mut output).await.unwrap(), expected);
    assert!(input.is_closed());
    assert!(output.next().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn response_collection_preserves_context_and_delivers_body() {
    let direction = Direction::Response;
    let (handler, input, mut output) = start_phase(Mode::Collect, direction).await;
    input.send(Ok(headers(direction, false))).await.unwrap();
    assert_eq!(
        reply(&mut output).await.unwrap(),
        headers_reply(direction, BodySendMode::Buffered)
    );
    input
        .send(Ok(body(direction, b"response", true)))
        .await
        .unwrap();
    assert_eq!(reply(&mut output).await.unwrap(), body_reply(direction));
    assert_eq!(
        *handler.received.lock().unwrap(),
        Some(Ok(Bytes::from_static(b"response")))
    );
    drop(input);
    assert!(
        time::timeout(time::Duration::from_secs(1), output.next())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn trailers_end_a_read_body_and_pass_through_unchanged() {
    for direction in [Direction::Request, Direction::Response] {
        for (mode, body_mode) in [
            (Mode::Stream, BodySendMode::Streamed),
            (Mode::Collect, BodySendMode::Buffered),
        ] {
            for payload in [&b"payload"[..], b""] {
                let (handler, input, mut output) = start_phase(mode, direction).await;
                input.send(Ok(headers(direction, false))).await.unwrap();
                assert_eq!(
                    reply(&mut output).await.unwrap(),
                    headers_reply(direction, body_mode),
                    "{direction:?} {mode:?}: headers"
                );
                // An empty payload means the trailers follow the headers directly.
                if !payload.is_empty() {
                    input
                        .send(Ok(body(direction, payload, false)))
                        .await
                        .unwrap();
                    assert_eq!(
                        reply(&mut output).await.unwrap(),
                        body_reply(direction),
                        "{direction:?} {mode:?}: body reply"
                    );
                }
                input.send(Ok(trailers(direction))).await.unwrap();
                assert_eq!(
                    reply(&mut output).await.unwrap(),
                    trailers_reply(direction),
                    "{direction:?} {mode:?}"
                );
                assert_eq!(
                    *handler.received.lock().unwrap(),
                    Some(Ok(Bytes::copy_from_slice(payload))),
                    "{direction:?} {mode:?}: body bytes"
                );
                drop(input);
                assert!(
                    time::timeout(time::Duration::from_secs(1), output.next())
                        .await
                        .unwrap()
                        .is_none(),
                    "{direction:?} {mode:?}: stream did not end"
                );
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn clean_envoy_eof_mid_body_cancels_both_request_and_response_handlers() {
    for direction in [Direction::Request, Direction::Response] {
        let (handler, input, mut output) = start_phase(Mode::Stream, direction).await;
        input.send(Ok(headers(direction, false))).await.unwrap();
        assert_eq!(
            reply(&mut output).await.unwrap(),
            headers_reply(direction, BodySendMode::Streamed),
            "{direction:?}: headers"
        );
        input
            .send(Ok(body(direction, b"partial", false)))
            .await
            .unwrap();
        assert_eq!(
            reply(&mut output).await.unwrap(),
            body_reply(direction),
            "{direction:?}: body reply"
        );
        drop(input);
        assert!(
            time::timeout(time::Duration::from_secs(1), output.next())
                .await
                .unwrap()
                .is_none(),
            "{direction:?}: stream did not end"
        );
        assert!(
            handler.dropped.load(Ordering::SeqCst),
            "{direction:?}: handler was not cancelled"
        );
        assert_eq!(
            *handler.received.lock().unwrap(),
            None,
            "{direction:?}: partial body completed"
        );
    }
}

// helpers

impl TestHandler {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            received: Mutex::new(None),
            dropped: AtomicBool::new(false),
        }
    }

    async fn read_body(&self, body: &LazyBody) -> anyhow::Result<()> {
        let _signal = DropSignal(&self.dropped);
        match self.mode {
            Mode::Collect => {
                let result = body.collect().await;
                *self.received.lock().unwrap() = Some(result.clone());
                result?;
            }
            Mode::Stream => {
                let mut stream = pin!(body.stream()?);
                let mut bytes = BytesMut::new();
                while let Some(frame) = stream.next().await {
                    let data = frame?
                        .into_data()
                        .map_err(|_| anyhow::anyhow!("handler received a non-data frame"))?;
                    bytes.extend_from_slice(&data);
                }
                *self.received.lock().unwrap() = Some(Ok(bytes.freeze()));
            }
            Mode::DropStream | Mode::Immediate => drop(body.stream()?),
            Mode::Blocked => {
                let stream = body.stream()?;
                future::pending::<()>().await;
                drop(stream);
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler for TestHandler {
    async fn handle_request(
        &self,
        req: &http::Request<LazyBody>,
        context: &mut http::Extensions,
    ) -> anyhow::Result<HandlerAction<RequestContinue>> {
        self.read_body(req.body()).await?;
        if matches!(self.mode, Mode::Immediate) {
            return Ok(HandlerAction::ImmediateResponse(
                crate::handler::ImmediateResponse::new(http::StatusCode::FORBIDDEN),
            ));
        }
        context.insert(42_u32);
        Ok(HandlerAction::Continue(RequestContinue::default()))
    }

    async fn handle_response(
        &self,
        resp: &http::Response<LazyBody>,
        context: &http::Extensions,
    ) -> anyhow::Result<HandlerAction<ResponseContinue>> {
        assert_eq!(context.get::<u32>(), Some(&42));
        self.read_body(resp.body()).await?;
        Ok(HandlerAction::Continue(Default::default()))
    }
}

impl Drop for DropSignal<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn start(handler: Arc<dyn Handler>, config: ExtProcConfig) -> (Input, ProcessStream) {
    start_with(ExtProc::new(handler, config), MetadataMap::new()).await
}

async fn start_with(ext_proc: ExtProc, metadata: MetadataMap) -> (Input, ProcessStream) {
    let (tx, rx) = mpsc::channel::<Result<ProcessingRequest, Status>>(8);
    let frames = futures::stream::unfold(rx, |mut rx| async move {
        let message = rx.recv().await?;
        let frame = message.map(|message| {
            let len = message.encoded_len();
            let mut bytes = BytesMut::with_capacity(5 + len);
            bytes.put_u8(0);
            bytes.put_u32(u32::try_from(len).unwrap());
            message.encode(&mut bytes).unwrap();
            Frame::data(bytes.freeze())
        });
        Some((frame, rx))
    });
    let input =
        Streaming::new_request(ProstDecoder::default(), StreamBody::new(frames), None, None);
    let mut request = Request::new(input);
    *request.metadata_mut() = metadata;
    let output = ext_proc.process(request).await.unwrap().into_inner();
    (tx, output)
}

async fn reply(output: &mut ProcessStream) -> Result<ProcessingResponse, Status> {
    time::timeout(time::Duration::from_secs(1), output.next())
        .await
        .expect("processing loop stalled")
        .expect("processing stream ended")
}

fn headers(direction: Direction, end_of_stream: bool) -> ProcessingRequest {
    let (name, value) = match direction {
        Direction::Request => (":method", "POST"),
        Direction::Response => (":status", "200"),
    };
    let headers = HttpHeaders {
        headers: Some(HeaderMap {
            headers: vec![HeaderValue {
                key: name.into(),
                raw_value: value.as_bytes().to_vec(),
                ..Default::default()
            }],
        }),
        end_of_stream,
        ..Default::default()
    };
    ProcessingRequest {
        request: Some(match direction {
            Direction::Request => processing_request::Request::RequestHeaders(headers),
            Direction::Response => processing_request::Request::ResponseHeaders(headers),
        }),
        ..Default::default()
    }
}

fn body(direction: Direction, data: &'static [u8], end_of_stream: bool) -> ProcessingRequest {
    let body = HttpBody {
        body: data.to_vec(),
        end_of_stream,
        ..Default::default()
    };
    ProcessingRequest {
        request: Some(match direction {
            Direction::Request => processing_request::Request::RequestBody(body),
            Direction::Response => processing_request::Request::ResponseBody(body),
        }),
        ..Default::default()
    }
}

fn headers_reply(direction: Direction, mode: BodySendMode) -> ProcessingResponse {
    let headers = HeadersResponse {
        response: Some(CommonResponse::default()),
    };
    let mut processing = ProcessingMode {
        request_header_mode: HeaderSendMode::Send as i32,
        response_header_mode: HeaderSendMode::Send as i32,
        request_body_mode: BodySendMode::None as i32,
        response_body_mode: BodySendMode::None as i32,
        request_trailer_mode: HeaderSendMode::Skip as i32,
        response_trailer_mode: HeaderSendMode::Skip as i32,
    };
    // Trailers are asked for only with a body, which they can end.
    let trailers = if mode == BodySendMode::None {
        HeaderSendMode::Skip
    } else {
        HeaderSendMode::Send
    };
    let response = match direction {
        Direction::Request => {
            processing.request_body_mode = mode as i32;
            processing.request_trailer_mode = trailers as i32;
            processing_response::Response::RequestHeaders(headers)
        }
        Direction::Response => {
            processing.response_body_mode = mode as i32;
            processing.response_trailer_mode = trailers as i32;
            processing_response::Response::ResponseHeaders(headers)
        }
    };
    ProcessingResponse {
        response: Some(response),
        mode_override: Some(processing),
        ..Default::default()
    }
}

fn body_reply(direction: Direction) -> ProcessingResponse {
    let body = BodyResponse {
        response: Some(CommonResponse::default()),
    };
    ProcessingResponse {
        response: Some(match direction {
            Direction::Request => processing_response::Response::RequestBody(body),
            Direction::Response => processing_response::Response::ResponseBody(body),
        }),
        ..Default::default()
    }
}

async fn start_phase(mode: Mode, direction: Direction) -> (Arc<TestHandler>, Input, ProcessStream) {
    start_phase_with(mode, direction, ExtProcConfig::default()).await
}

async fn start_phase_with(
    mode: Mode,
    direction: Direction,
    config: ExtProcConfig,
) -> (Arc<TestHandler>, Input, ProcessStream) {
    let handler = Arc::new(TestHandler::new(mode));
    let (input, mut output) = start(handler.clone(), config).await;
    if direction == Direction::Response {
        input
            .send(Ok(headers(Direction::Request, true)))
            .await
            .unwrap();
        assert_eq!(
            reply(&mut output).await.unwrap(),
            headers_reply(Direction::Request, BodySendMode::None)
        );
        handler.received.lock().unwrap().take();
        handler.dropped.store(false, Ordering::SeqCst);
    }
    (handler, input, output)
}

fn trailers(direction: Direction) -> ProcessingRequest {
    let trailers = HttpTrailers {
        trailers: Some(HeaderMap {
            headers: vec![HeaderValue {
                key: "x-trailer".into(),
                raw_value: b"original".to_vec(),
                ..Default::default()
            }],
        }),
    };
    ProcessingRequest {
        request: Some(match direction {
            Direction::Request => processing_request::Request::RequestTrailers(trailers),
            Direction::Response => processing_request::Request::ResponseTrailers(trailers),
        }),
        ..Default::default()
    }
}

fn trailers_reply(direction: Direction) -> ProcessingResponse {
    let trailers = TrailersResponse::default();
    ProcessingResponse {
        response: Some(match direction {
            Direction::Request => processing_response::Response::RequestTrailers(trailers),
            Direction::Response => processing_response::Response::ResponseTrailers(trailers),
        }),
        ..Default::default()
    }
}

#[tokio::test(start_paused = true)]
async fn spans_record_the_exchange() {
    let (spans, _guard) = capture_spans();
    let (input, mut output) = start(
        Arc::new(TestHandler::new(Mode::Collect)),
        ExtProcConfig::default(),
    )
    .await;
    let mut request = headers(Direction::Request, true);
    let Some(processing_request::Request::RequestHeaders(h)) = &mut request.request else {
        unreachable!()
    };
    h.headers.as_mut().unwrap().headers.push(HeaderValue {
        key: "x-request-id".into(),
        raw_value: b"abc".to_vec(),
        ..Default::default()
    });
    input.send(Ok(request)).await.unwrap();
    reply(&mut output).await.unwrap();
    input
        .send(Ok(headers(Direction::Response, true)))
        .await
        .unwrap();
    reply(&mut output).await.unwrap();
    drop(input);
    assert!(output.next().await.is_none());
    drop(output);

    let fields = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    };
    assert_eq!(
        spans.sorted(),
        [
            SpanRecord {
                name: "ext_proc",
                parent: None,
                fields: fields(&[
                    ("http.request.method", "POST"),
                    ("http.response.status_code", "200"),
                    ("otel.kind", "server"),
                    ("request_id", "abc"),
                    ("url.path", "/"),
                ]),
            },
            SpanRecord {
                name: "handle_request",
                parent: Some("ext_proc"),
                fields: BTreeMap::new(),
            },
            SpanRecord {
                name: "handle_response",
                parent: Some("ext_proc"),
                fields: BTreeMap::new(),
            },
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn handler_failure_marks_both_spans_as_errors() {
    let (spans, _guard) = capture_spans();
    let (input, mut output) = start(Arc::new(FailingHandler), ExtProcConfig::default()).await;
    input
        .send(Ok(headers(Direction::Request, true)))
        .await
        .unwrap();
    assert!(reply(&mut output).await.is_err());
    assert!(output.next().await.is_none());
    drop((input, output));

    let status = |name| {
        spans
            .sorted()
            .into_iter()
            .find(|span| span.name == name)
            .and_then(|span| span.fields.get("otel.status_code").cloned())
    };
    assert_eq!(status("ext_proc").as_deref(), Some("ERROR"));
    assert_eq!(
        status("handle_request").as_deref(),
        Some("ERROR"),
        "{:?}",
        spans.sorted()
    );
}

#[tokio::test]
async fn grpc_context_controls_exported_trace_and_sampling() {
    use opentelemetry::{
        propagation::TextMapPropagator,
        trace::{SpanId, TraceContextExt, TraceId, TracerProvider},
    };
    use opentelemetry_sdk::{
        propagation::TraceContextPropagator,
        trace::{InMemorySpanExporter, Sampler, SdkTracerProvider},
    };
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    for (header, exported) in [
        (
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            true,
        ),
        (
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"),
            false,
        ),
        (Some("invalid"), true),
        (None, true),
    ] {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")));
        let _guard = test_subscriber(subscriber);
        let observed = Arc::new(Mutex::new(None));
        let captured = observed.clone();
        let processor = ExtProc::new(
            Arc::new(TestHandler::new(Mode::Collect)),
            ExtProcConfig::default(),
        )
        .with_span_initializer(move |span, metadata| {
            let parent = TraceContextPropagator::new()
                .extract(&opentelemetry_http::HeaderExtractor(metadata));
            span.set_parent(parent).unwrap();
            *captured.lock().unwrap() = Some(span.context().span().span_context().clone());
        });
        let mut metadata = MetadataMap::new();
        if let Some(header) = header {
            metadata.insert("traceparent", header.parse().unwrap());
            metadata.insert("tracestate", "vendor=value".parse().unwrap());
        }
        let (input, mut output) = start_with(processor, metadata).await;
        for direction in [Direction::Request, Direction::Response] {
            input.send(Ok(headers(direction, true))).await.unwrap();
            reply(&mut output).await.unwrap();
        }
        drop(input);
        assert!(output.next().await.is_none());
        drop(output);
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let context = observed.lock().unwrap().clone().unwrap();
        assert_eq!(context.is_sampled(), exported);
        if !exported {
            assert!(spans.is_empty());
            assert_eq!(
                context.trace_id(),
                TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap()
            );
            continue;
        }
        assert_eq!(spans.len(), 3);
        let root = spans.iter().find(|s| s.name == "ext_proc").unwrap();
        if header.is_some_and(|h| h.starts_with("00-")) {
            assert_eq!(
                root.span_context.trace_id(),
                TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap()
            );
            assert_eq!(
                root.parent_span_id,
                SpanId::from_hex("00f067aa0ba902b7").unwrap()
            );
            assert_eq!(root.span_context.trace_state().get("vendor"), Some("value"));
        } else {
            assert_eq!(root.parent_span_id, SpanId::INVALID);
            assert!(root.span_context.is_valid());
        }
        for child in spans.iter().filter(|s| s.name != "ext_proc") {
            assert_eq!(child.span_context.trace_id(), root.span_context.trace_id());
            assert_eq!(child.parent_span_id, root.span_context.span_id());
        }
    }
}

#[tokio::test]
async fn request_immediate_response_records_status_without_response_headers() {
    let (spans, _guard) = capture_spans();
    let (input, mut output) = start(
        Arc::new(TestHandler::new(Mode::Immediate)),
        ExtProcConfig::default(),
    )
    .await;
    input
        .send(Ok(headers(Direction::Request, true)))
        .await
        .unwrap();
    reply(&mut output).await.unwrap();
    drop((input, output));
    let spans = spans.sorted();
    let span = spans.iter().find(|s| s.name == "ext_proc").unwrap();
    assert_eq!(span.fields["http.response.status_code"], "403");
    assert!(!spans.iter().any(|s| s.name == "handle_response"));
}

#[tokio::test]
async fn spans_record_immediate_and_overridden_status() {
    struct StatusHandler(bool);
    #[async_trait::async_trait]
    impl Handler for StatusHandler {
        async fn handle_response(
            &self,
            response: &http::Response<LazyBody>,
            _: &http::Extensions,
        ) -> anyhow::Result<HandlerAction<ResponseContinue>> {
            response.body().collect().await?;
            if self.0 {
                Ok(HandlerAction::ImmediateResponse(
                    crate::handler::ImmediateResponse::new(http::StatusCode::BAD_GATEWAY),
                ))
            } else {
                Ok(HandlerAction::Continue(ResponseContinue {
                    status_override: Some(http::StatusCode::CREATED),
                    ..Default::default()
                }))
            }
        }
    }
    for immediate in [false, true] {
        for has_body in [false, true] {
            let (spans, _guard) = capture_spans();
            let (input, mut output) =
                start(Arc::new(StatusHandler(immediate)), ExtProcConfig::default()).await;
            input
                .send(Ok(headers(Direction::Request, true)))
                .await
                .unwrap();
            reply(&mut output).await.unwrap();
            input
                .send(Ok(headers(Direction::Response, !has_body)))
                .await
                .unwrap();
            reply(&mut output).await.unwrap();
            if has_body {
                input
                    .send(Ok(body(Direction::Response, b"body", true)))
                    .await
                    .unwrap();
                reply(&mut output).await.unwrap();
            }
            drop((input, output));
            let spans = spans.sorted();
            let span = spans.iter().find(|s| s.name == "ext_proc").unwrap();
            assert_eq!(
                span.fields["http.response.status_code"],
                if immediate { "502" } else { "201" }
            );
        }
    }
}

#[async_trait::async_trait]
impl Handler for FailingHandler {
    async fn handle_request(
        &self,
        _req: &http::Request<LazyBody>,
        _context: &mut http::Extensions,
    ) -> anyhow::Result<HandlerAction<RequestContinue>> {
        anyhow::bail!("failed")
    }
}

impl SpanLog {
    fn sorted(&self) -> Vec<SpanRecord> {
        let mut spans = self.0.lock().unwrap().clone();
        spans.sort_by_key(|span| span.name);
        spans
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for SpanLog {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).unwrap();
        let mut record = SpanRecord {
            name: span.name(),
            parent: span.parent().map(|parent| parent.name()),
            fields: BTreeMap::new(),
        };
        attrs.record(&mut FieldVisitor(&mut record.fields));
        span.extensions_mut().insert(record);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).unwrap();
        if let Some(record) = span.extensions_mut().get_mut::<SpanRecord>() {
            values.record(&mut FieldVisitor(&mut record.fields));
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).unwrap();
        let record = span.extensions_mut().remove::<SpanRecord>();
        self.0.lock().unwrap().extend(record);
    }
}

impl Visit for FieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

/// Spans closed while the guard lives; tests run on one thread, so the default subscriber covers them.
fn capture_spans() -> (SpanLog, tracing::subscriber::DefaultGuard) {
    let spans = SpanLog::default();
    let guard = test_subscriber(Registry::default().with(spans.clone()));
    (spans, guard)
}

fn test_subscriber(
    subscriber: impl Subscriber + Send + Sync + 'static,
) -> tracing::subscriber::DefaultGuard {
    // Keep two dispatchers registered so uncaptured threads cannot disable shared callsites.
    static BASELINE: std::sync::LazyLock<tracing::Dispatch> =
        std::sync::LazyLock::new(|| tracing::Dispatch::new(Registry::default()));
    std::sync::LazyLock::force(&BASELINE);
    tracing::subscriber::set_default(subscriber)
}
