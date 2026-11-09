//! In-memory tests of the real processing loop and its asynchronous tasks.

use std::{
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
use tonic_prost::{ProstDecoder, prost::Message};

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
    let output = ExtProc::new(handler, config)
        .process(Request::new(input))
        .await
        .unwrap()
        .into_inner();
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
