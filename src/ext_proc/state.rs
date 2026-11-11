//! State machine types for the ext-proc stream.

use bytes::Bytes;
use tokio::sync::mpsc;
use tonic::Status;

use crate::{
    body::{BodyMessage, BodyMode, BodyRequest},
    envoy::{BodyChunk, Direction, Headers, Message, Mutation},
    handler::HandlerAction,
};

pub(super) type Action = HandlerAction<ContinueAction>;

// One per stream; keep variants inline to avoid extra heap allocations.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(super) enum State {
    InitRequest,
    Headers(Direction),
    Body(BodyState),
    SendingBodyEnd {
        direction: Direction,
        mode: BodyMode,
        end: BodyEnd,
        pending: Option<ContinueAction>,
    },
    WaitingForHandler {
        direction: Direction,
        mode: BodyMode,
        end: BodyEnd,
    },
    /// Upstream answered before the request body ended; waits for the request handler.
    EarlyResponse(Headers),
    InitResponse(http::Extensions),
    ResponseDone,
}

impl State {
    /// State once the headers, body or trailers of `direction` are answered.
    pub(super) fn finished(direction: Direction, context: http::Extensions) -> Self {
        match direction {
            Direction::Request => Self::InitResponse(context),
            Direction::Response => Self::ResponseDone,
        }
    }

    /// Variant name, for logs.
    pub(super) const fn name(&self) -> &'static str {
        match self {
            Self::InitRequest => "InitRequest",
            Self::Headers(_) => "Headers",
            Self::Body(_) => "Body",
            Self::SendingBodyEnd { .. } => "SendingBodyEnd",
            Self::WaitingForHandler { .. } => "WaitingForHandler",
            Self::EarlyResponse(_) => "EarlyResponse",
            Self::InitResponse(_) => "InitResponse",
            Self::ResponseDone => "ResponseDone",
        }
    }
}

#[derive(Debug)]
pub(super) enum Event {
    Headers(Direction, Headers),
    Body(Direction, BodyChunk),
    Trailers(Direction),
    /// The stream closed (`None`) or failed.
    EnvoyEnd(Option<Status>),
    Handled(Action),
    /// Carries the context, which the response phase still needs after an aborted body.
    HandlerFailed {
        error: anyhow::Error,
        context: http::Extensions,
    },
    /// `None`: the body was dropped without being requested.
    FetchBody(Option<BodyRequest>),
    ChunkSent {
        last: bool,
    },
}

impl Event {
    /// Variant name, for logs.
    pub(super) const fn name(&self) -> &'static str {
        match self {
            Self::Headers(..) => "Headers",
            Self::Body(..) => "Body",
            Self::Trailers(_) => "Trailers",
            Self::EnvoyEnd(_) => "EnvoyEnd",
            Self::Handled(_) => "Handled",
            Self::HandlerFailed { .. } => "HandlerFailed",
            Self::FetchBody(_) => "FetchBody",
            Self::ChunkSent { .. } => "ChunkSent",
        }
    }
}

impl From<Message> for Event {
    fn from(message: Message) -> Self {
        match message {
            Message::Headers(direction, headers) => Self::Headers(direction, headers),
            Message::Body(direction, chunk) => Self::Body(direction, chunk),
            Message::Trailers(direction) => Self::Trailers(direction),
        }
    }
}

#[derive(Debug)]
pub(super) struct BodyState {
    pub(super) direction: Direction,
    pub(super) mode: BodyMode,
    pub(super) tx: mpsc::Sender<BodyMessage>,
    /// Handler's `Continue` received before the end of the body.
    pub(super) pending: Option<ContinueAction>,
}

/// Which message ended the body; its reply waits for the handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BodyEnd {
    LastChunk,
    Trailers,
}

/// A handler's `Continue`, the same for requests and responses.
#[derive(Debug)]
pub(super) struct ContinueAction {
    pub(super) mutation: Mutation,
    pub(super) body_override: Option<Bytes>,
    pub(super) context: http::Extensions,
}

impl ContinueAction {
    /// Header, pseudo-header or body changes, which STREAMED mode cannot apply.
    pub(super) fn has_overrides(&self) -> bool {
        self.mutation.changes_headers() || self.body_override.is_some()
    }
}
