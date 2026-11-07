//! Transition function of the ext-proc state machine; no side effects apart from logging.

use bytes::Bytes;
use tokio::sync::mpsc;
use tonic::Status;
use tracing::warn;

use super::{
    ExtProcConfig,
    state::{Action, BodyEnd, BodyState, ContinueAction, Event, State},
};
use crate::{
    body::{BodyError, BodyMessage, BodyMode, BodyRequest},
    envoy::{BodyChunk, Direction, Headers, Reply},
};

/// What one transition sends to Envoy and asks the loop to start.
#[derive(Debug, Default)]
pub(super) struct Transition {
    pub(super) next: Option<State>,
    pub(super) reply: Option<Reply>,
    pub(super) commands: Vec<Command>,
}

/// Work a transition asks the loop to start.
#[derive(Debug)]
pub(super) enum Command {
    /// Read the next Envoy message.
    Read,
    CallHandler {
        msg: Headers,
        context: http::Extensions,
    },
    SendChunk {
        tx: mpsc::Sender<BodyMessage>,
        data: Bytes,
        last: bool,
    },
    /// Deliver this event again as the next one.
    Requeue(Event),
    AbortBody(mpsc::Sender<BodyMessage>),
}

/// One transition, without side effects apart from logging; `next: None` ends the stream, `Err` ends it with that status.
pub(super) fn transition(
    config: ExtProcConfig,
    state: State,
    event: Event,
) -> Result<Transition, Status> {
    let mut out = Transition::default();
    let ExtProcConfig {
        override_message_timeout,
        processing_mode,
        ..
    } = config;
    // Each arm is one transition and evaluates to the next state.
    let next = match (state, event) {
        // Headers
        (State::InitRequest, Event::Headers(Direction::Request, msg)) => {
            out.commands.push(Command::CallHandler {
                msg,
                context: http::Extensions::new(),
            });
            if let Some(timeout) = override_message_timeout {
                out.reply = Some(Reply::MessageTimeout(timeout));
            }
            State::Headers(Direction::Request)
        }
        // Request headers come first unless `processing_mode` skips them.
        (State::InitRequest, Event::Headers(Direction::Response, msg)) => {
            out.commands.push(Command::CallHandler {
                msg,
                context: http::Extensions::new(),
            });
            if let Some(timeout) = override_message_timeout {
                out.reply = Some(Reply::MessageTimeout(timeout));
            }
            State::Headers(Direction::Response)
        }
        (State::InitResponse(context), Event::Headers(Direction::Response, msg)) => {
            out.commands.push(Command::CallHandler { msg, context });
            if let Some(timeout) = override_message_timeout {
                out.reply = Some(Reply::MessageTimeout(timeout));
            }
            State::Headers(Direction::Response)
        }
        (
            State::Headers(direction),
            Event::Handled(Action::Continue(ContinueAction {
                mutation,
                body_override,
                context,
            })),
        ) => {
            // A body replaced without reading it: Envoy discards the original.
            out.reply = Some(match body_override {
                None => Reply::HeadersNoBody {
                    direction,
                    mutation,
                    processing_mode,
                },
                Some(body) => Reply::HeadersReplaceBody {
                    direction,
                    mutation,
                    body,
                    processing_mode,
                },
            });
            out.commands.push(Command::Read);
            State::finished(direction, context)
        }
        (State::Headers(direction), Event::FetchBody(Some(BodyRequest { mode, tx }))) => {
            out.reply = Some(Reply::HeadersWithBody {
                direction,
                mode,
                processing_mode,
            });
            out.commands.push(Command::Read);
            State::Body(BodyState {
                direction,
                mode,
                tx,
                pending: None,
            })
        }
        // The handler finished without asking for the body.
        (state, Event::FetchBody(None)) => state,

        // The handler failed only because the body was cut off; the upstream's response goes on.
        (State::EarlyResponse(msg), Event::HandlerFailed { error, context })
            if is_aborted(&error) =>
        {
            out.commands
                .push(Command::Requeue(Event::Headers(Direction::Response, msg)));
            State::InitResponse(context)
        }

        // Any handler's early exit or error ends the stream.
        (_, Event::Handled(Action::ImmediateResponse(resp))) => {
            out.reply = Some(Reply::ImmediateResponse(resp));
            return Ok(out);
        }
        (_, Event::HandlerFailed { error: err, .. }) => {
            // Error text stays in local logs; Envoy logs what it receives.
            warn!(error = format!("{err:#}"), "Handler failed");
            return Err(Status::internal("handler failed"));
        }

        // Body messages are read only after the preceding delivery finishes.
        (
            State::Body(body),
            Event::Body(
                direction,
                BodyChunk {
                    data,
                    end_of_stream,
                },
            ),
        ) if direction == body.direction => {
            let last = end_of_stream || body.mode == BodyMode::Buffered;
            let (tx, next) = if last {
                (
                    body.tx,
                    State::SendingBodyEnd {
                        direction,
                        mode: body.mode,
                        end: BodyEnd::LastChunk,
                        pending: body.pending,
                    },
                )
            } else {
                (body.tx.clone(), State::Body(body))
            };
            out.commands.push(Command::SendChunk { tx, data, last });
            next
        }
        // Trailers only end the body; the handler does not see them.
        (State::Body(body), Event::Trailers(direction)) if direction == body.direction => {
            out.commands.push(Command::SendChunk {
                tx: body.tx,
                data: Bytes::new(),
                last: true,
            });
            State::SendingBodyEnd {
                direction,
                mode: body.mode,
                end: BodyEnd::Trailers,
                pending: body.pending,
            }
        }
        (
            State::Body(body),
            Event::ChunkSent {
                end_of_stream: false,
            },
        ) => {
            out.reply = Some(Reply::ContinueBody(body.direction));
            out.commands.push(Command::Read);
            State::Body(body)
        }
        (
            State::SendingBodyEnd {
                direction,
                mode,
                end,
                pending,
            },
            Event::ChunkSent {
                end_of_stream: true,
            },
        ) => match pending {
            Some(changes) => finish_body(direction, end, changes, &mut out)?,
            None => {
                if let Some(timeout) = override_message_timeout {
                    out.reply = Some(Reply::MessageTimeout(timeout));
                }
                State::WaitingForHandler {
                    direction,
                    mode,
                    end,
                }
            }
        },
        (State::Body(mut body), Event::Handled(Action::Continue(changes))) => {
            if body.mode == BodyMode::Streamed && changes.has_overrides() {
                return Err(overrides_after_stream());
            }
            body.pending = Some(changes);
            State::Body(body)
        }
        (
            State::SendingBodyEnd {
                direction,
                mode,
                end,
                pending: None,
            },
            Event::Handled(Action::Continue(changes)),
        ) => {
            if mode == BodyMode::Streamed && changes.has_overrides() {
                return Err(overrides_after_stream());
            }
            State::SendingBodyEnd {
                direction,
                mode,
                end,
                pending: Some(changes),
            }
        }
        (
            State::WaitingForHandler {
                direction,
                mode,
                end,
            },
            Event::Handled(Action::Continue(changes)),
        ) => {
            if mode == BodyMode::Streamed && changes.has_overrides() {
                return Err(overrides_after_stream());
            }
            finish_body(direction, end, changes, &mut out)?
        }

        // Upstream answered before the request body ended; the handler's request changes no longer apply.
        (
            State::Body(BodyState {
                direction: Direction::Request,
                tx,
                pending,
                ..
            }),
            Event::Headers(Direction::Response, msg),
        ) => {
            out.commands.push(Command::AbortBody(tx));
            match pending {
                Some(changes) => {
                    out.commands
                        .push(Command::Requeue(Event::Headers(Direction::Response, msg)));
                    State::InitResponse(changes.context)
                }
                None => State::EarlyResponse(msg),
            }
        }
        (
            State::EarlyResponse(msg),
            Event::Handled(Action::Continue(ContinueAction { context, .. })),
        ) => {
            out.commands
                .push(Command::Requeue(Event::Headers(Direction::Response, msg)));
            State::InitResponse(context)
        }

        // Trailers after a BUFFERED body, or sent because `processing_mode` asks for them: passed on unchanged.
        (
            state @ (State::InitRequest | State::InitResponse(_)),
            Event::Trailers(direction @ Direction::Request),
        )
        | (state @ State::ResponseDone, Event::Trailers(direction @ Direction::Response)) => {
            out.reply = Some(Reply::Trailers {
                direction,
                dynamic_metadata: serde_json::Map::new(),
            });
            out.commands.push(Command::Read);
            state
        }

        // Dropping the body sender reports Aborted unless completion was explicit.
        (_, Event::EnvoyEnd(None)) => return Ok(out),
        (_, Event::EnvoyEnd(Some(status))) => return Err(status),

        (State::SendingBodyEnd { .. } | State::WaitingForHandler { .. }, Event::Body(..)) => {
            return Err(Status::internal(
                "Envoy sent a body message after the body ended",
            ));
        }
        // A body the bridge never asked for: Envoy ignored mode_override.
        (_, Event::Body(..)) => {
            return Err(Status::failed_precondition(
                "Envoy sent a body that was not requested; set allow_mode_override: true and send_body_without_waiting_for_header_response: false",
            ));
        }
        (state, event) => {
            warn!(
                state = <&'static str>::from(&state),
                event = <&'static str>::from(&event),
                "Unexpected state/event combination"
            );
            return Err(Status::internal("Unexpected state/event combination"));
        }
    };
    out.next = Some(next);
    Ok(out)
}

/// Completes the body reply, or the trailers reply when trailers ended the body.
fn finish_body(
    direction: Direction,
    end: BodyEnd,
    changes: ContinueAction,
    out: &mut Transition,
) -> Result<State, Status> {
    match end {
        BodyEnd::LastChunk => {
            out.reply = Some(Reply::LastBody {
                direction,
                mutation: changes.mutation,
                body_override: changes.body_override,
            });
        }
        BodyEnd::Trailers => {
            if changes.has_overrides() {
                return Err(overrides_after_stream());
            }
            out.reply = Some(Reply::Trailers {
                direction,
                dynamic_metadata: changes.mutation.dynamic_metadata,
            });
        }
    }
    out.commands.push(Command::Read);
    Ok(State::finished(direction, changes.context))
}

// helpers

/// Whether the error comes from a body read cut off by `Command::AbortBody`.
fn is_aborted(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|e| e.downcast_ref::<BodyError>() == Some(&BodyError::Aborted))
}

fn overrides_after_stream() -> Status {
    Status::internal(
        "header or body changes are not supported after a streamed or trailers-only body",
    )
}

#[cfg(test)]
#[path = "fsm_tests.rs"]
mod tests;
