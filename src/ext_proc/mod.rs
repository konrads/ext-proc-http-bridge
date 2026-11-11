//! Ext-proc HTTP bridge implementation.

use std::{mem, pin::Pin, sync::Arc};

use async_stream::stream;
use bytes::Bytes;
use envoy_types::pb::envoy::service::ext_proc::v3::{
    ProcessingRequest, ProcessingResponse, external_processor_server::ExternalProcessor,
};
use futures::{Future, Stream, StreamExt, future, stream::FuturesUnordered};
use http_body::Frame;
use tokio::{
    sync::{mpsc, oneshot},
    time,
};
use tonic::{Request, Response, Status, Streaming};
use tracing::{Instrument, Span};

use self::{
    fsm::Command,
    state::{Action, ContinueAction, Event, State},
    trace::SpanInitializer,
};
use crate::{
    body::{BodyConfig, BodyMessage, BodyRequest, LazyBody},
    envoy::{self, Head, Headers, Mutation},
    handler::{Handler, HandlerAction, RequestContinue, ResponseContinue},
};

pub use crate::envoy::{HeaderMode, ProcessingMode};

mod fsm;
mod state;
mod trace;

type ProcessStream = Pin<Box<dyn Stream<Item = Result<ProcessingResponse, Status>> + Send>>;

type EnvoyStream = Streaming<ProcessingRequest>;

type Task = Pin<Box<dyn Future<Output = TaskCompletion> + Send>>;

/// Ext-proc settings.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExtProcConfig {
    pub body: BodyConfig,
    /// Sent as `override_message_timeout` while waiting on the handler; needs Envoy's `max_message_timeout`.
    pub override_message_timeout: Option<time::Duration>,
    /// Must match the header modes of Envoy's `processing_mode`.
    pub processing_mode: ProcessingMode,
}

/// Ext-proc implementation.
#[derive(Clone)]
pub struct ExtProc {
    handler: Arc<dyn Handler>,
    config: ExtProcConfig,
    span_initializer: Option<SpanInitializer>,
}

impl ExtProc {
    #[must_use]
    pub fn new(handler: Arc<dyn Handler>, config: ExtProcConfig) -> Self {
        Self {
            handler,
            config,
            span_initializer: None,
        }
    }

    /// Initializes each `ext_proc` span from gRPC metadata before it is entered or creates children.
    /// Without an initializer, the span inherits the current tracing span.
    #[must_use]
    pub fn with_span_initializer(
        mut self,
        span_initializer: impl Fn(&Span, &http::HeaderMap) + Send + Sync + 'static,
    ) -> Self {
        self.span_initializer = Some(Arc::new(span_initializer));
        self
    }
}

#[tonic::async_trait]
impl ExternalProcessor for ExtProc {
    type ProcessStream = ProcessStream;

    async fn process(
        &self,
        request: Request<Streaming<ProcessingRequest>>,
    ) -> Result<Response<Self::ProcessStream>, Status> {
        let (metadata, _, envoy) = request.into_parts();
        let span = trace::stream_span();
        if let Some(initialize) = &self.span_initializer {
            initialize(&span, &metadata.into_headers());
        }
        let mut envoy = Some(envoy);
        let this = self.clone();
        let stream_span = span.clone();

        let response_stream = stream! {
            let mut tasks = FuturesUnordered::new();
            let mut state = State::InitRequest;

            tasks.push(next_envoy_msg(&mut envoy));

            while let Some(TaskCompletion { event, stream }) = tasks.next().await {
                if let Some(stream) = stream {
                    envoy = Some(stream);
                }

                tracing::debug!(state = state.name(), event = event.name(), "Processing event");

                // Cancel pending work before yielding a terminal reply, which suspends the stream.
                let transition = match fsm::transition(this.config, state, event) {
                    Ok(transition) => transition,
                    Err(status) => {
                        trace::mark_error(&stream_span);
                        tasks.clear();
                        yield Err(status);
                        break;
                    }
                };
                for command in transition.commands {
                    match command {
                        Command::Read => tasks.push(next_envoy_msg(&mut envoy)),
                        Command::CallHandler { msg, context } => {
                            trace::record_head(&stream_span, &msg.head);
                            tasks.extend(call_handler(
                                &this.handler,
                                this.config.body,
                                &stream_span,
                                msg,
                                context,
                            ));
                        }
                        Command::SendChunk { tx, data, last } => tasks.push(send_chunk(tx, data, last)),
                        Command::Requeue(event) => tasks.push(task_ready(event)),
                        Command::AbortBody(tx) => drop(tx),
                    }
                }
                if transition.next.is_none() {
                    // The break runs only on the next poll, so drop pending tasks before yielding.
                    tasks.clear();
                }
                if let Some(reply) = transition.reply {
                    trace::record_reply(&stream_span, &reply);
                    yield Ok(ProcessingResponse::from(reply));
                }
                let Some(next) = transition.next else { break };
                state = next;
            }
        };

        Ok(Response::new(Box::pin(trace::instrument_stream(
            response_stream,
            span,
        ))))
    }
}

/// Starts the handler and the wait for its body request; the result is delivered only after that request resolves.
/// Returns them as separate tasks, as the body request must reach the FSM while the handler is still running.
fn call_handler(
    handler: &Arc<dyn Handler>,
    config: BodyConfig,
    stream_span: &Span,
    msg: Headers,
    mut context: http::Extensions,
) -> [Task; 2] {
    let Headers {
        head,
        end_of_stream,
        attributes,
        metadata,
    } = msg;
    let (body, control_rx) = if end_of_stream {
        (LazyBody::empty(), None)
    } else {
        let (control_tx, control_rx) = oneshot::channel::<BodyRequest>();
        (LazyBody::new(config, control_tx), Some(control_rx))
    };
    let handler = Arc::clone(handler);
    let handler_span = trace::handler_span(stream_span, &head);
    let error_span = handler_span.clone();
    // Dropping the message when the handler returns resolves an unused body request to `None`.
    let handled = async move {
        let result = match head {
            Head::Request(parts) => {
                let mut req = http::Request::from_parts(parts, body);
                req.extensions_mut().insert(attributes);
                req.extensions_mut().insert(metadata);
                let action = handler.handle_request(&req, &mut context).await;
                action.map(|action| request_action(action, mem::take(&mut context)))
            }
            Head::Response(parts) => {
                let mut resp = http::Response::from_parts(parts, body);
                resp.extensions_mut().insert(attributes);
                resp.extensions_mut().insert(metadata);
                let action = handler.handle_response(&resp, &context).await;
                action.map(|action| response_action(action, mem::take(&mut context)))
            }
        };
        match result {
            Ok(action) => Event::Handled(action),
            Err(error) => {
                trace::mark_error(&error_span);
                Event::HandlerFailed { error, context }
            }
        }
    }
    .instrument(handler_span);
    let (fetched_tx, fetched_rx) = oneshot::channel::<()>();
    [
        Box::pin(async {
            let msg = match control_rx {
                Some(rx) => rx.await.ok(),
                None => None,
            };
            let _ = fetched_tx.send(());
            Event::FetchBody(msg).into()
        }),
        Box::pin(async {
            let event = handled.await;
            let _ = fetched_rx.await;
            event.into()
        }),
    ]
}

fn request_action(action: HandlerAction<RequestContinue>, context: http::Extensions) -> Action {
    match action {
        HandlerAction::ImmediateResponse(resp) => Action::ImmediateResponse(resp),
        HandlerAction::Continue(RequestContinue {
            header_mutation,
            path_override,
            clear_route_cache,
            dynamic_metadata,
            body_override,
        }) => Action::Continue(ContinueAction {
            mutation: Mutation::request(
                header_mutation,
                path_override,
                clear_route_cache,
                dynamic_metadata,
            ),
            body_override,
            context,
        }),
    }
}

fn response_action(action: HandlerAction<ResponseContinue>, context: http::Extensions) -> Action {
    match action {
        HandlerAction::ImmediateResponse(resp) => Action::ImmediateResponse(resp),
        HandlerAction::Continue(ResponseContinue {
            header_mutation,
            status_override,
            dynamic_metadata,
            body_override,
        }) => Action::Continue(ContinueAction {
            mutation: Mutation::response(header_mutation, status_override, dynamic_metadata),
            body_override,
            context,
        }),
    }
}

/// Reads the next Envoy message and returns the stream with its completion.
fn next_envoy_msg(envoy: &mut Option<EnvoyStream>) -> Task {
    let stream = envoy.take();
    Box::pin(async move {
        // `None` only if two reads were requested at once.
        let Some(mut stream) = stream else {
            return Event::EnvoyEnd(Some(Status::internal("Envoy read already pending"))).into();
        };
        TaskCompletion {
            event: envoy_event(stream.next().await),
            stream: Some(stream),
        }
    })
}

/// Sends a chunk and marks successful completion after the last chunk.
fn send_chunk(tx: mpsc::Sender<BodyMessage>, data: Bytes, last: bool) -> Task {
    let frame = (!data.is_empty()).then(|| BodyMessage::Frame(Ok(Frame::data(data))));
    let end = last.then_some(BodyMessage::End);
    send_messages(tx, [frame, end], Event::ChunkSent { last })
}

/// Stops early if the receiver is gone.
fn send_messages(
    tx: mpsc::Sender<BodyMessage>,
    messages: [Option<BodyMessage>; 2],
    event: Event,
) -> Task {
    Box::pin(async move {
        for message in messages.into_iter().flatten() {
            if tx.send(message).await.is_err() {
                break;
            }
        }
        event.into()
    })
}

// helpers

/// Converts a read; an invalid message ends the stream like a failed read.
fn envoy_event(message: Option<Result<ProcessingRequest, Status>>) -> Event {
    match message {
        None => Event::EnvoyEnd(None),
        Some(Err(status)) => Event::EnvoyEnd(Some(status)),
        Some(Ok(message)) => envoy::Message::try_from(message)
            .map_or_else(|status| Event::EnvoyEnd(Some(status)), Event::from),
    }
}

/// What a task yields; a read hands the Envoy stream back with the message.
struct TaskCompletion {
    event: Event,
    stream: Option<EnvoyStream>,
}

impl From<Event> for TaskCompletion {
    fn from(event: Event) -> Self {
        Self {
            event,
            stream: None,
        }
    }
}

fn task_ready(event: Event) -> Task {
    Box::pin(future::ready(TaskCompletion::from(event)))
}

#[cfg(test)]
mod tests;
