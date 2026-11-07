//! Lazy request/response body.

use std::{fmt, mem, num::NonZeroUsize, time::Duration};

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, stream};
use http_body::Frame;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time,
};

pub struct LazyBody {
    config: BodyConfig,
    state: Mutex<BodyState>,
}

/// Body delivery settings.
#[derive(Debug, Clone, Copy)]
pub struct BodyConfig {
    /// Longest wait for the next chunk; `None` waits forever.
    pub chunk_timeout: Option<Duration>,
    /// Capacity of the per-body chunk channel.
    pub channel_capacity: NonZeroUsize,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BodyError {
    /// `stream()` was already called, or `collect()` was called after `stream()`.
    #[error("body already consumed")]
    AlreadyConsumed,
    /// The Envoy stream ended or failed before the end of the body.
    #[error("body aborted")]
    Aborted,
    /// No chunk arrived within `chunk_timeout`.
    #[error("timed out waiting for body chunk")]
    Timeout,
}

pub type BodyFrame = Result<Frame<Bytes>, BodyError>;

#[derive(Debug)]
pub(crate) enum BodyMessage {
    Frame(BodyFrame),
    End,
}

/// How Envoy sends a requested body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyMode {
    /// In chunks, as `stream()` reads them.
    Streamed,
    /// In one message, for `collect()`.
    Buffered,
}

/// Body request sent from `LazyBody` to the ext-proc loop.
#[derive(Debug)]
pub(crate) struct BodyRequest {
    pub(crate) mode: BodyMode,
    pub(crate) tx: mpsc::Sender<BodyMessage>,
}

enum BodyState {
    Pending(oneshot::Sender<BodyRequest>),
    Ready(Bytes),
    NoBody,
    Consumed,
    Collecting {
        receiver: mpsc::Receiver<BodyMessage>,
        data: CollectedBody,
    },
    Collected(Result<Bytes, BodyError>),
}

enum CollectedBody {
    Single(Bytes),
    Multiple(BytesMut),
}

impl LazyBody {
    #[must_use]
    pub(crate) fn new(config: BodyConfig, control_tx: oneshot::Sender<BodyRequest>) -> Self {
        Self {
            config,
            state: Mutex::new(BodyState::Pending(control_tx)),
        }
    }

    /// Body of a message that has none (headers with `end_of_stream`).
    #[must_use]
    pub fn empty() -> Self {
        Self {
            config: BodyConfig::default(),
            state: Mutex::new(BodyState::NoBody),
        }
    }

    /// Creates a body from bytes without an Envoy stream, for testing handlers.
    ///
    /// Supports both [`Self::collect`] and [`Self::stream`]. Construct a request
    /// or response with this body and pass it directly to your handler:
    ///
    /// ```
    /// use ext_proc_http_bridge::LazyBody;
    ///
    /// let request = http::Request::new(LazyBody::from_bytes("payload"));
    /// let response = http::Response::new(LazyBody::from_bytes("response"));
    /// ```
    #[must_use]
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self {
        Self {
            config: BodyConfig::default(),
            state: Mutex::new(BodyState::Ready(bytes.into())),
        }
    }

    /// Takes the body stream; `AlreadyConsumed` on a second call or after `collect()`.
    pub fn stream(&self) -> Result<impl Stream<Item = BodyFrame> + Send + 'static, BodyError> {
        // Lock is only held long-term by a running `collect()`, which consumes the body.
        let mut state = self
            .state
            .try_lock()
            .map_err(|_| BodyError::AlreadyConsumed)?;
        let (rx, initial) = match mem::replace(&mut *state, BodyState::Consumed) {
            BodyState::Pending(control_tx) => (
                Some(request_body(
                    control_tx,
                    BodyMode::Streamed,
                    self.config.channel_capacity,
                )?),
                None,
            ),
            BodyState::Ready(bytes) => (None, Some(bytes)),
            BodyState::NoBody => (None, None),
            other => {
                *state = other;
                return Err(BodyError::AlreadyConsumed);
            }
        };
        let initial = initial
            .filter(|bytes| !bytes.is_empty())
            .map(|bytes| Ok(Frame::data(bytes)));
        Ok(stream::iter(initial).chain(chunks(rx, self.config.chunk_timeout)))
    }

    /// Buffers and caches the body; cancelled calls can be resumed by calling again.
    pub async fn collect(&self) -> Result<Bytes, BodyError> {
        let mut state = self.state.lock().await;
        *state = match mem::replace(&mut *state, BodyState::Consumed) {
            BodyState::Pending(control_tx) => {
                match request_body(control_tx, BodyMode::Buffered, self.config.channel_capacity) {
                    Ok(receiver) => BodyState::Collecting {
                        receiver,
                        data: CollectedBody::Single(Bytes::new()),
                    },
                    Err(error) => BodyState::Collected(Err(error)),
                }
            }
            BodyState::Ready(bytes) => BodyState::Collected(Ok(bytes)),
            BodyState::NoBody => BodyState::Collected(Ok(Bytes::new())),
            other => other,
        };
        while let BodyState::Collecting { receiver, data } = &mut *state {
            match receive_message(receiver, self.config.chunk_timeout).await {
                Ok(BodyMessage::Frame(Ok(frame))) => {
                    if let Ok(chunk) = frame.into_data() {
                        data.push(chunk);
                    }
                }
                Ok(BodyMessage::End) => {
                    *state = BodyState::Collected(Ok(mem::replace(
                        data,
                        CollectedBody::Single(Bytes::new()),
                    )
                    .into_bytes()));
                }
                Ok(BodyMessage::Frame(Err(error))) | Err(error) => {
                    *state = BodyState::Collected(Err(error));
                }
            }
        }
        match &*state {
            BodyState::Collected(result) => result.clone(),
            _ => Err(BodyError::AlreadyConsumed),
        }
    }
}

impl fmt::Debug for LazyBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyBody").finish_non_exhaustive()
    }
}

impl Default for BodyConfig {
    fn default() -> Self {
        Self {
            chunk_timeout: None,
            channel_capacity: NonZeroUsize::new(8).expect("non-zero"),
        }
    }
}

// helpers

impl CollectedBody {
    fn push(&mut self, chunk: Bytes) {
        if chunk.is_empty() {
            return;
        }
        match self {
            Self::Single(first) if first.is_empty() => *first = chunk,
            Self::Single(first) => {
                let mut buffer = BytesMut::with_capacity(first.len() + chunk.len());
                buffer.extend_from_slice(first);
                buffer.extend_from_slice(&chunk);
                *self = Self::Multiple(buffer);
            }
            Self::Multiple(buffer) => buffer.extend_from_slice(&chunk),
        }
    }

    fn into_bytes(self) -> Bytes {
        match self {
            Self::Single(chunk) => chunk,
            Self::Multiple(buffer) => buffer.freeze(),
        }
    }
}

fn request_body(
    control_tx: oneshot::Sender<BodyRequest>,
    mode: BodyMode,
    capacity: NonZeroUsize,
) -> Result<mpsc::Receiver<BodyMessage>, BodyError> {
    let (tx, rx) = mpsc::channel(capacity.get());
    control_tx
        .send(BodyRequest { mode, tx })
        .map_err(|_| BodyError::Aborted)?;
    Ok(rx)
}

/// Stream over the body channel; only an explicit end message completes it successfully.
fn chunks(
    receiver: Option<mpsc::Receiver<BodyMessage>>,
    chunk_timeout: Option<Duration>,
) -> impl Stream<Item = BodyFrame> + Send + 'static {
    futures::stream::unfold(receiver, move |receiver| async move {
        let mut receiver = receiver?;
        match receive_message(&mut receiver, chunk_timeout).await {
            Ok(BodyMessage::Frame(Ok(frame))) => Some((Ok(frame), Some(receiver))),
            Ok(BodyMessage::End) => None,
            Ok(BodyMessage::Frame(Err(error))) | Err(error) => Some((Err(error), None)),
        }
    })
}

async fn receive_message(
    receiver: &mut mpsc::Receiver<BodyMessage>,
    chunk_timeout: Option<Duration>,
) -> Result<BodyMessage, BodyError> {
    let message = match chunk_timeout {
        Some(timeout) => time::timeout(timeout, receiver.recv())
            .await
            .map_err(|_| BodyError::Timeout)?,
        None => receiver.recv().await,
    };
    message.ok_or(BodyError::Aborted)
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn collect_single_chunk_reuses_storage() {
        let chunk = Bytes::from(vec![b'x'; 1024]);
        let pointer = chunk.as_ptr();
        let (body, handle) = body_with(None, BodyMode::Buffered, vec![Ok(Frame::data(chunk))]);
        let collected = body.collect().await.unwrap();
        assert_eq!(collected, Bytes::from(vec![b'x'; 1024]));
        assert_eq!(collected.as_ptr(), pointer);
        assert_eq!(body.collect().await.unwrap().as_ptr(), pointer);
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn collect_success_is_cached() {
        let (body, handle) = body_with(
            None,
            BodyMode::Buffered,
            vec![
                data(""),
                data("t"),
                data(""),
                data("e"),
                data("st"),
                data(""),
            ],
        );
        assert_eq!(body.collect().await.unwrap(), "test");
        handle.await.unwrap();
        assert_eq!(body.collect().await.unwrap(), "test");
    }

    #[tokio::test]
    async fn stream_success() {
        let (body, handle) = body_with(None, BodyMode::Streamed, vec![data("a"), data("b")]);
        let frames = collect_stream(body.stream().unwrap()).await;
        handle.await.unwrap();
        assert_eq!(
            frames,
            [Ok(Bytes::from_static(b"a")), Ok(Bytes::from_static(b"b"))]
        );
    }

    #[tokio::test]
    async fn stream_ends_after_error() {
        let (body, handle) = body_with(
            None,
            BodyMode::Streamed,
            vec![data("a"), Err(BodyError::Aborted), data("b")],
        );
        let frames = collect_stream(body.stream().unwrap()).await;
        handle.await.ok();
        assert_eq!(
            frames,
            [Ok(Bytes::from_static(b"a")), Err(BodyError::Aborted)]
        );
    }

    #[tokio::test]
    async fn already_consumed() {
        let (body, _handle) = body_with(None, BodyMode::Streamed, vec![]);
        let _stream = body.stream().unwrap();
        assert_eq!(body.stream().err(), Some(BodyError::AlreadyConsumed));
        assert_eq!(body.collect().await, Err(BodyError::AlreadyConsumed));

        let (body, _handle) = body_with(None, BodyMode::Buffered, vec![]);
        assert_eq!(body.collect().await.unwrap(), "");
        assert_eq!(body.stream().err(), Some(BodyError::AlreadyConsumed));
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_is_cached() {
        let (control_tx, control_rx) = oneshot::channel::<BodyRequest>();
        let handle = tokio::spawn(async move {
            let msg = control_rx.await.unwrap();
            time::sleep(Duration::from_millis(500)).await;
            drop(msg);
        });
        let body = LazyBody::new(timeout_config(Some(Duration::from_millis(100))), control_tx);
        assert_eq!(body.collect().await, Err(BodyError::Timeout));
        assert_eq!(body.collect().await, Err(BodyError::Timeout));
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn control_dropped_is_aborted() {
        let (control_tx, control_rx) = oneshot::channel::<BodyRequest>();
        drop(control_rx);
        let body = LazyBody::new(BodyConfig::default(), control_tx);
        assert_eq!(body.collect().await, Err(BodyError::Aborted));
    }

    #[tokio::test]
    async fn cancelled_collect_preserves_partial_body() {
        let (control_tx, control_rx) = oneshot::channel();
        let body = LazyBody::new(BodyConfig::default(), control_tx);
        let mut collection = Box::pin(body.collect());
        assert!(futures::poll!(&mut collection).is_pending());
        let request = control_rx.await.unwrap();
        request
            .tx
            .send(BodyMessage::Frame(data("first")))
            .await
            .unwrap();
        assert!(futures::poll!(&mut collection).is_pending());
        drop(collection);

        request
            .tx
            .send(BodyMessage::Frame(data("second")))
            .await
            .unwrap();
        request.tx.send(BodyMessage::End).await.unwrap();
        assert_eq!(body.collect().await.unwrap(), "firstsecond");
        assert_eq!(body.collect().await.unwrap(), "firstsecond");
    }

    #[tokio::test]
    async fn stream_outlives_body() {
        let (control_tx, control_rx) = oneshot::channel();
        let stream = {
            let body = LazyBody::new(BodyConfig::default(), control_tx);
            body.stream().unwrap()
        };
        let request = control_rx.await.unwrap();
        request
            .tx
            .send(BodyMessage::Frame(data("body")))
            .await
            .unwrap();
        request.tx.send(BodyMessage::End).await.unwrap();
        let frames = collect_stream(stream).await;
        assert_eq!(frames, [Ok(Bytes::from_static(b"body"))]);
    }

    #[tokio::test]
    async fn closed_channel_aborts_stream_and_collection() {
        let (control_tx, control_rx) = oneshot::channel();
        let body = LazyBody::new(BodyConfig::default(), control_tx);
        let stream = body.stream().unwrap();
        let request = control_rx.await.unwrap();
        request
            .tx
            .send(BodyMessage::Frame(data("partial")))
            .await
            .unwrap();
        drop(request);
        let frames = collect_stream(stream).await;
        assert_eq!(
            frames,
            [Ok(Bytes::from_static(b"partial")), Err(BodyError::Aborted)]
        );

        let (control_tx, control_rx) = oneshot::channel();
        let body = LazyBody::new(BodyConfig::default(), control_tx);
        let producer = async {
            let request = control_rx.await.unwrap();
            request
                .tx
                .send(BodyMessage::Frame(data("partial")))
                .await
                .unwrap();
        };
        let (result, ()) = tokio::join!(body.collect(), producer);
        assert_eq!(result, Err(BodyError::Aborted));
        assert_eq!(body.collect().await, Err(BodyError::Aborted));
    }

    #[tokio::test]
    async fn no_body() {
        let body = LazyBody::empty();
        assert!(collect_stream(body.stream().unwrap()).await.is_empty());
        assert_eq!(body.collect().await, Err(BodyError::AlreadyConsumed));

        let body = LazyBody::empty();
        assert_eq!(body.collect().await.unwrap(), "");
    }

    #[tokio::test]
    async fn bytes_backed_body_supports_cached_collection() {
        for bytes in [Bytes::new(), Bytes::from_static(b"payload")] {
            let body = LazyBody::from_bytes(bytes.clone());
            assert_eq!(body.collect().await.unwrap(), bytes);
            assert_eq!(body.collect().await.unwrap(), bytes);
            assert_eq!(body.stream().err(), Some(BodyError::AlreadyConsumed));
        }
    }

    #[tokio::test]
    async fn bytes_backed_body_streams_once_and_outlives_its_body() {
        for bytes in [Bytes::new(), Bytes::from_static(b"payload")] {
            let stream = {
                let body = LazyBody::from_bytes(bytes.clone());
                let stream = body.stream().unwrap();
                assert_eq!(body.stream().err(), Some(BodyError::AlreadyConsumed));
                assert_eq!(body.collect().await, Err(BodyError::AlreadyConsumed));
                stream
            };
            let expected: Vec<_> = (!bytes.is_empty())
                .then_some(Ok(bytes))
                .into_iter()
                .collect();
            assert_eq!(collect_stream(stream).await, expected);
        }
    }

    #[test]
    fn debug_hides_state() {
        assert_eq!(
            format!("{:?}", LazyBody::from_bytes("secret")),
            "LazyBody { .. }"
        );
    }

    // helpers

    /// Body whose control side expects `mode` and replies with `frames`.
    fn body_with(
        timeout: Option<Duration>,
        mode: BodyMode,
        frames: Vec<BodyFrame>,
    ) -> (LazyBody, tokio::task::JoinHandle<()>) {
        let (control_tx, control_rx) = oneshot::channel::<BodyRequest>();
        let handle = tokio::spawn(async move {
            let msg = control_rx.await.unwrap();
            assert_eq!(msg.mode, mode);
            for frame in frames {
                if msg.tx.send(BodyMessage::Frame(frame)).await.is_err() {
                    return;
                }
            }
            let _ = msg.tx.send(BodyMessage::End).await;
        });
        (LazyBody::new(timeout_config(timeout), control_tx), handle)
    }

    fn timeout_config(chunk_timeout: Option<Duration>) -> BodyConfig {
        BodyConfig {
            chunk_timeout,
            ..BodyConfig::default()
        }
    }

    fn data(s: &'static str) -> BodyFrame {
        Ok(Frame::data(Bytes::from_static(s.as_bytes())))
    }

    async fn collect_stream(
        stream: impl Stream<Item = BodyFrame> + Send + 'static,
    ) -> Vec<Result<Bytes, BodyError>> {
        stream
            .map(|frame| frame.map(|frame| frame.into_data().expect("data frame")))
            .collect()
            .await
    }
}
