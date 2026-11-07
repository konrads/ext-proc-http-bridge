//! Example and property tests for the ext-proc state machine.

use crate::{envoy::Mutation, ext_proc::request_action, handler::HandlerAction};

use super::*;

#[test]
fn body_completion_and_handler_result_can_arrive_in_either_order() {
    for handler_first in [false, true] {
        for end in [BodyEnd::LastChunk, BodyEnd::Trailers] {
            let mut state = sending_body_end(end);
            let sent = Event::ChunkSent {
                end_of_stream: true,
            };
            let events = if handler_first {
                [handled(), sent]
            } else {
                [sent, handled()]
            };
            let mut final_transition = None;
            for event in events {
                let mut output = transition(ExtProcConfig::default(), state, event).unwrap();
                state = output.next.take().unwrap();
                final_transition = Some(output);
            }
            let output = final_transition.unwrap();
            assert!(matches!(state, State::InitResponse(_)));
            assert!(matches!(output.commands.as_slice(), [Command::Read]));
            match end {
                BodyEnd::LastChunk => assert!(matches!(output.reply, Some(Reply::LastBody { .. }))),
                BodyEnd::Trailers => assert!(matches!(output.reply, Some(Reply::Trailers { .. }))),
            }
        }
    }
}

#[test]
fn no_envoy_read_is_pending_while_finishing_the_body() {
    for trailers in [false, true] {
        let (tx, _receiver) = mpsc::channel(1);
        let state = State::Body(BodyState {
            direction: Direction::Request,
            mode: BodyMode::Streamed,
            tx,
            pending: None,
        });
        let received = if trailers {
            Event::Trailers(Direction::Request)
        } else {
            Event::Body(
                Direction::Request,
                BodyChunk {
                    data: Bytes::new(),
                    end_of_stream: true,
                },
            )
        };
        let output = transition(ExtProcConfig::default(), state, received).unwrap();
        assert!(matches!(output.next, Some(State::SendingBodyEnd { .. })));
        assert!(matches!(
            output.commands.as_slice(),
            [Command::SendChunk { last: true, .. }]
        ));
        let sent = Event::ChunkSent {
            end_of_stream: true,
        };
        let output = transition(ExtProcConfig::default(), output.next.unwrap(), sent).unwrap();
        assert!(matches!(output.next, Some(State::WaitingForHandler { .. })));
        assert!(output.commands.is_empty());
    }
}

#[test]
fn streamed_overrides_fail_before_body_completion() {
    let (tx, mut receiver) = mpsc::channel(1);
    let state = State::Body(BodyState {
        direction: Direction::Request,
        mode: BodyMode::Streamed,
        tx,
        pending: None,
    });
    let changes = ContinueAction {
        mutation: Mutation::default(),
        body_override: Some(bytes::Bytes::from_static(b"replacement")),
        context: http::Extensions::new(),
    };
    assert!(
        transition(
            ExtProcConfig::default(),
            state,
            Event::Handled(Action::Continue(changes))
        )
        .is_err()
    );
    assert_eq!(
        receiver.try_recv().unwrap_err(),
        mpsc::error::TryRecvError::Disconnected
    );
}

#[test]
fn unrequested_body_is_rejected() {
    let state = State::InitResponse(http::Extensions::new());
    let status = status(transition(
        ExtProcConfig::default(),
        state,
        body_event(Direction::Request),
    ));
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn body_after_its_end_is_rejected() {
    let status = status(transition(
        ExtProcConfig::default(),
        sending_body_end(BodyEnd::LastChunk),
        body_event(Direction::Request),
    ));
    assert_eq!(status.code(), tonic::Code::Internal);
}

#[test]
fn handler_error_text_is_not_sent_to_envoy() {
    let status = status(transition(
        ExtProcConfig::default(),
        State::Headers(Direction::Request),
        Event::HandlerFailed {
            error: anyhow::anyhow!("token=secret"),
            context: http::Extensions::new(),
        },
    ));
    assert_eq!(
        (status.code(), status.message()),
        (tonic::Code::Internal, "handler failed")
    );
}

#[test]
fn unrequested_trailers_pass_through_unchanged() {
    for (state, direction) in [
        (State::InitRequest, Direction::Request),
        (State::InitResponse(Default::default()), Direction::Request),
        (State::ResponseDone, Direction::Response),
    ] {
        let name = <&'static str>::from(&state);
        let output =
            transition(ExtProcConfig::default(), state, Event::Trailers(direction)).unwrap();
        assert!(
            matches!(output.reply, Some(Reply::Trailers { ref dynamic_metadata, .. }) if dynamic_metadata.is_empty()),
            "{name}"
        );
        assert!(
            matches!(output.commands.as_slice(), [Command::Read]),
            "{name}"
        );
        assert_eq!(output.next.as_ref().map(<&'static str>::from), Some(name));
    }
}

#[test]
fn unexpected_trailers_are_rejected() {
    for (state, direction) in [
        (State::Headers(Direction::Request), Direction::Request),
        (State::InitResponse(Default::default()), Direction::Response),
        (State::ResponseDone, Direction::Request),
    ] {
        let error = status(transition(
            ExtProcConfig::default(),
            state,
            Event::Trailers(direction),
        ));
        assert_eq!(
            (error.code(), error.message()),
            (tonic::Code::Internal, "Unexpected state/event combination")
        );
    }
}

#[test]
fn trailers_reply_carries_the_handler_metadata() {
    let mut metadata = serde_json::Map::new();
    metadata.insert("ns".into(), "value".into());
    let mut state = sending_body_end(BodyEnd::Trailers);
    if let State::SendingBodyEnd { pending, .. } = &mut state {
        *pending = Some(ContinueAction {
            mutation: Mutation {
                dynamic_metadata: metadata.clone(),
                ..Mutation::default()
            },
            body_override: None,
            context: http::Extensions::new(),
        });
    }
    let output = transition(
        ExtProcConfig::default(),
        state,
        Event::ChunkSent {
            end_of_stream: true,
        },
    )
    .unwrap();
    assert!(
        matches!(output.reply, Some(Reply::Trailers { dynamic_metadata, .. }) if dynamic_metadata == metadata)
    );
}

#[test]
fn response_headers_come_first_when_request_headers_are_skipped() {
    let output = transition(
        ExtProcConfig::default(),
        State::InitRequest,
        Event::Headers(Direction::Response, response_headers()),
    )
    .unwrap();
    assert!(matches!(
        output.commands.as_slice(),
        [Command::CallHandler { context, .. }] if context.is_empty()
    ));
    assert!(matches!(
        output.next,
        Some(State::Headers(Direction::Response))
    ));
}

#[test]
fn headers_replies_repeat_the_configured_processing_mode() {
    let processing_mode = crate::ext_proc::ProcessingMode {
        request_header_mode: crate::ext_proc::HeaderMode::Skip,
        response_header_mode: crate::ext_proc::HeaderMode::Skip,
    };
    let config = ExtProcConfig {
        processing_mode,
        ..ExtProcConfig::default()
    };
    let output = transition(config, State::Headers(Direction::Request), handled()).unwrap();
    assert!(matches!(
        output.reply,
        Some(Reply::HeadersNoBody { processing_mode: mode, .. }) if mode == processing_mode
    ));
}

mod proptests {
    //! Property tests: a model of Envoy, the handlers and the task queue drives `transition` in random orders.

    use std::{collections::VecDeque, mem, time::Duration};

    use bytes::Bytes;
    use http_body::Frame;
    use proptest::{collection::vec, option, prelude::*};
    use tokio::sync::mpsc::{self, error::TryRecvError};
    use tonic::Status;

    use super::super::{Command, overrides_after_stream, transition};
    use crate::{
        body::{BodyError, BodyMessage, BodyMode, BodyRequest},
        envoy::{BodyChunk, Direction, Head, Headers, Mutation, Reply},
        ext_proc::{
            ExtProcConfig, HeaderMode, ProcessingMode,
            state::{Action, ContinueAction, Event, State},
        },
        handler::{HeaderMutation, ImmediateResponse},
    };

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Access {
        None,
        Stream,
        /// Calls `stream()`, then returns without reading.
        StreamPartly,
        Collect,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Outcome {
        Continue,
        Overrides,
        ReplaceBody,
        Immediate,
        Error,
    }

    /// One side of the exchange and how its handlers behave.
    #[derive(Debug, Clone)]
    struct Half {
        chunks: Vec<Vec<u8>>,
        trailers: bool,
        access: Access,
        outcome: Outcome,
    }

    #[derive(Debug, Clone)]
    struct Scenario {
        request: Half,
        response: Half,
        processing_mode: ProcessingMode,
        message_timeout: bool,
        /// Upstream answers after this many STREAMED request chunks.
        early_response: Option<usize>,
        /// Envoy closes (`false`) or fails (`true`) the stream at this read.
        envoy_end: Option<(usize, bool)>,
        /// Picks the next ready task at each step, modulo the number ready.
        schedule: Vec<usize>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Part {
        Headers,
        Body,
        Trailers,
    }

    type Kind = (Direction, Part);

    /// A message Envoy has queued, built into an `Event` when read.
    enum Message {
        Headers(Direction),
        Body(Direction, Vec<u8>, bool),
        Trailers(Direction),
    }

    /// Sends the scenario's messages, following the bridge's headers replies.
    struct Envoy<'a> {
        scenario: &'a Scenario,
        queue: VecDeque<Message>,
        next_headers: Option<Direction>,
        sent: Vec<Kind>,
        answered: usize,
        reads: usize,
        early_response: bool,
        immediate: bool,
        finished: bool,
    }

    /// Put in `context` by the request handler before it reads the body.
    #[derive(Clone)]
    struct Marker;

    struct HandlerTask {
        direction: Direction,
        has_body: bool,
        context: http::Extensions,
        fetched: bool,
        receiver: Option<mpsc::Receiver<BodyMessage>>,
        received: Vec<u8>,
        /// `Some(true)` after `End`, `Some(false)` after an error or a closed channel.
        ended: Option<bool>,
    }

    /// A task in the loop's queue.
    enum Pending {
        Read,
        Handler(Box<HandlerTask>),
        Chunk {
            tx: mpsc::Sender<BodyMessage>,
            data: Bytes,
            last: bool,
        },
        Event(Box<Event>),
    }

    /// Handlers and body channels, as the loop and `LazyBody` would run them.
    struct World<'a> {
        scenario: &'a Scenario,
        pending: Vec<Pending>,
        /// Body bytes each handler read, and how its body ended.
        received: [Option<(Vec<u8>, Option<bool>)>; 2],
    }

    #[derive(Debug)]
    enum End {
        Closed,
        Failed,
    }

    /// What the event that ended a run requires of its end.
    enum Cause {
        HandlerFailed,
        EnvoyFailed(Status),
        EnvoyClosed,
        Immediate,
        /// A handler's changes or the trailers that complete them, in this direction's body.
        Changes(Direction),
        Other,
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 4000,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn exchanges_follow_the_protocol(scenario in scenario()) {
            check(&scenario)?;
        }
    }

    /// Upstream answers before any of three empty STREAMED request chunks is delivered.
    #[test]
    fn early_response_during_empty_streamed_chunks() {
        check(&Scenario {
            request: Half {
                chunks: vec![vec![]; 3],
                access: Access::Stream,
                ..Half::default()
            },
            early_response: Some(0),
            ..Scenario::default()
        })
        .unwrap();
    }

    /// Overrides on a collected response body that is only trailers fail the stream.
    #[test]
    fn overrides_on_trailers_only_collected_response() {
        check(&Scenario {
            response: Half {
                trailers: true,
                access: Access::Collect,
                outcome: Outcome::Overrides,
                ..Half::default()
            },
            ..Scenario::default()
        })
        .unwrap();
    }

    /// Every run ends, as its last event requires; replies match Envoy's messages one to one; clean runs deliver whole bodies.
    fn check(scenario: &Scenario) -> Result<(), TestCaseError> {
        let (end, envoy, world) = run(scenario)?;
        if scenario.is_clean() {
            prop_assert!(matches!(end, End::Closed), "{:?}", end);
            prop_assert!(envoy.finished, "Envoy did not finish");
            prop_assert_eq!(envoy.answered, envoy.sent.len());
            for direction in [Direction::Request, Direction::Response] {
                let half = scenario.half(direction);
                let cut_off = direction == Direction::Request && envoy.early_response;
                if scenario.sends_headers(direction)
                    && matches!(half.access, Access::Stream | Access::Collect)
                    && half.has_body()
                    && !cut_off
                {
                    let (received, ended) = world.received[direction as usize]
                        .as_ref()
                        .ok_or_else(|| TestCaseError::fail("handler not called"))?;
                    prop_assert_eq!(*ended, Some(true));
                    prop_assert_eq!(received.as_slice(), half.chunks.concat());
                }
            }
        }
        Ok(())
    }

    fn run(scenario: &Scenario) -> Result<(End, Envoy<'_>, World<'_>), TestCaseError> {
        let config = ExtProcConfig {
            processing_mode: scenario.processing_mode,
            override_message_timeout: scenario.message_timeout.then_some(Duration::from_secs(1)),
            ..ExtProcConfig::default()
        };
        let mut envoy = Envoy::new(scenario);
        let mut world = World {
            scenario,
            pending: vec![Pending::Read],
            received: [None, None],
        };
        let mut state = State::InitRequest;
        let mut ready = Vec::new();
        for step in 0..1000 {
            ready.clear();
            ready.extend(
                world
                    .pending
                    .iter_mut()
                    .enumerate()
                    .filter_map(|(i, pending)| match pending {
                        Pending::Handler(task) => {
                            task.drain();
                            task.is_ready(scenario.half(task.direction).access)
                                .then_some(i)
                        }
                        _ => Some(i),
                    }),
            );
            prop_assert!(
                !ready.is_empty(),
                "stalled in {:?} with {} pending tasks",
                state,
                world.pending.len()
            );
            let pick = ready[scenario.schedule[step % scenario.schedule.len()] % ready.len()];
            let event = world.complete(pick, &mut envoy)?;
            let cause = Cause::of(&state, &event);
            let output = match transition(config, state, event) {
                Ok(output) => output,
                Err(status) => {
                    cause.check_failure(scenario, &status)?;
                    return Ok((End::Failed, envoy, world));
                }
            };
            if output.next.is_none() {
                cause.check_close(output.reply.as_ref())?;
            }
            if let Some(reply) = &output.reply {
                envoy.reply(reply)?;
            }
            for command in output.commands {
                world.execute(command)?;
            }
            match output.next {
                Some(next) => state = next,
                None => return Ok((End::Closed, envoy, world)),
            }
        }
        Err(TestCaseError::fail("no end after 1000 steps"))
    }

    impl Cause {
        fn of(state: &State, event: &Event) -> Self {
            let body_direction = match state {
                State::Body(body) => Some(body.direction),
                State::SendingBodyEnd { direction, .. }
                | State::WaitingForHandler { direction, .. } => Some(*direction),
                _ => None,
            };
            match event {
                Event::HandlerFailed { .. } => Self::HandlerFailed,
                Event::EnvoyEnd(Some(status)) => Self::EnvoyFailed(status.clone()),
                Event::EnvoyEnd(None) => Self::EnvoyClosed,
                Event::Handled(Action::ImmediateResponse(_)) => Self::Immediate,
                Event::Handled(Action::Continue(_))
                | Event::ChunkSent {
                    end_of_stream: true,
                } => body_direction.map_or(Self::Other, Self::Changes),
                _ => Self::Other,
            }
        }

        /// Only handler errors, Envoy errors and overrides after `stream()` may fail a run.
        fn check_failure(&self, scenario: &Scenario, status: &Status) -> Result<(), TestCaseError> {
            let expected = match self {
                Self::HandlerFailed => Status::internal("handler failed"),
                Self::EnvoyFailed(expected) => expected.clone(),
                Self::Changes(direction) if scenario.overrides_after_stream(*direction) => {
                    overrides_after_stream()
                }
                Self::Changes(_) | Self::Other | Self::EnvoyClosed | Self::Immediate => {
                    return Err(TestCaseError::fail(format!("unexpected failure: {status}")));
                }
            };
            prop_assert_eq!(
                (status.code(), status.message()),
                (expected.code(), expected.message())
            );
            Ok(())
        }

        /// Only Envoy closing the stream or an immediate response, sent last, may close a run.
        fn check_close(&self, reply: Option<&Reply>) -> Result<(), TestCaseError> {
            match self {
                Self::EnvoyClosed => prop_assert!(reply.is_none(), "reply after Envoy closed"),
                Self::Immediate => prop_assert!(
                    matches!(reply, Some(Reply::ImmediateResponse(_))),
                    "immediate response not sent last"
                ),
                _ => return Err(TestCaseError::fail("closed without a cause")),
            }
            Ok(())
        }
    }

    impl Default for Half {
        fn default() -> Self {
            Self {
                chunks: Vec::new(),
                trailers: false,
                access: Access::None,
                outcome: Outcome::Continue,
            }
        }
    }

    impl Default for Scenario {
        fn default() -> Self {
            Self {
                request: Half::default(),
                response: Half::default(),
                processing_mode: ProcessingMode::default(),
                message_timeout: false,
                early_response: None,
                envoy_end: None,
                schedule: vec![0],
            }
        }
    }

    impl Half {
        fn has_body(&self) -> bool {
            !self.chunks.is_empty() || self.trailers
        }
    }

    impl Scenario {
        /// Changes the reply cannot carry: after a streamed body, or a collected body that is only trailers.
        fn overrides_after_stream(&self, direction: Direction) -> bool {
            let half = self.half(direction);
            let ends_with_trailers = match half.access {
                Access::Stream | Access::StreamPartly => true,
                Access::Collect => half.chunks.is_empty() && half.trailers,
                Access::None => false,
            };
            ends_with_trailers && matches!(half.outcome, Outcome::Overrides | Outcome::ReplaceBody)
        }

        fn sends_headers(&self, direction: Direction) -> bool {
            let mode = match direction {
                Direction::Request => self.processing_mode.request_header_mode,
                Direction::Response => self.processing_mode.response_header_mode,
            };
            mode == HeaderMode::Send
        }

        /// The first headers Envoy sends after `after`, as `processing_mode` says.
        fn next_headers(&self, after: Option<Direction>) -> Option<Direction> {
            let candidates: &[Direction] = match after {
                None => &[Direction::Request, Direction::Response],
                Some(Direction::Request) => &[Direction::Response],
                Some(Direction::Response) => &[],
            };
            candidates
                .iter()
                .copied()
                .find(|direction| self.sends_headers(*direction))
        }

        const fn half(&self, direction: Direction) -> &Half {
            match direction {
                Direction::Request => &self.request,
                Direction::Response => &self.response,
            }
        }

        /// Envoy completes the exchange and every handler continues without header or body changes.
        fn is_clean(&self) -> bool {
            self.envoy_end.is_none()
                && [Direction::Request, Direction::Response]
                    .into_iter()
                    .filter(|direction| self.sends_headers(*direction))
                    .all(|direction| self.half(direction).outcome == Outcome::Continue)
        }
    }

    impl Message {
        const fn kind(&self) -> Kind {
            match self {
                Self::Headers(direction) => (*direction, Part::Headers),
                Self::Body(direction, ..) => (*direction, Part::Body),
                Self::Trailers(direction) => (*direction, Part::Trailers),
            }
        }

        fn event(self, scenario: &Scenario) -> Event {
            match self {
                Self::Headers(direction) => {
                    let head = match direction {
                        Direction::Request => Head::Request(http::Request::new(()).into_parts().0),
                        Direction::Response => {
                            Head::Response(http::Response::new(()).into_parts().0)
                        }
                    };
                    Event::Headers(
                        direction,
                        Headers {
                            head,
                            end_of_stream: !scenario.half(direction).has_body(),
                            attributes: Default::default(),
                            metadata: Default::default(),
                        },
                    )
                }
                Self::Body(direction, data, end_of_stream) => Event::Body(
                    direction,
                    BodyChunk {
                        data: Bytes::from(data),
                        end_of_stream,
                    },
                ),
                Self::Trailers(direction) => Event::Trailers(direction),
            }
        }
    }

    impl<'a> Envoy<'a> {
        fn new(scenario: &'a Scenario) -> Self {
            Self {
                scenario,
                queue: VecDeque::new(),
                next_headers: scenario.next_headers(None),
                sent: Vec::new(),
                answered: 0,
                reads: 0,
                early_response: false,
                immediate: false,
                finished: false,
            }
        }

        fn read(&mut self) -> Result<Event, TestCaseError> {
            prop_assert_eq!(
                self.answered,
                self.sent.len(),
                "read before the last message was answered"
            );
            if let Some((at, failed)) = self.scenario.envoy_end
                && at == self.reads
            {
                return Ok(Event::EnvoyEnd(
                    failed.then(|| Status::unavailable("Envoy failed")),
                ));
            }
            self.reads += 1;
            let message = match self.queue.pop_front() {
                Some(message) => message,
                None => match self.next_headers.take() {
                    Some(direction) => Message::Headers(direction),
                    None => {
                        self.finished = true;
                        return Ok(Event::EnvoyEnd(None));
                    }
                },
            };
            self.sent.push(message.kind());
            Ok(message.event(self.scenario))
        }

        fn reply(&mut self, reply: &Reply) -> Result<(), TestCaseError> {
            prop_assert!(!self.immediate, "reply after an immediate response");
            let Some(kind) = reply_kind(reply) else {
                // An immediate response may come at any time; Envoy can have messages in flight.
                if matches!(reply, Reply::ImmediateResponse(_)) {
                    prop_assert!(
                        !self.finished,
                        "immediate response after the exchange ended"
                    );
                    self.immediate = true;
                }
                return Ok(());
            };
            prop_assert!(
                self.answered < self.sent.len(),
                "reply without a message: {:?}",
                reply
            );
            prop_assert_eq!(kind, self.sent[self.answered], "reply to the wrong message");
            self.answered += 1;
            match reply {
                Reply::HeadersNoBody {
                    direction,
                    processing_mode,
                    ..
                }
                | Reply::HeadersReplaceBody {
                    direction,
                    processing_mode,
                    ..
                } => {
                    prop_assert_eq!(*processing_mode, self.scenario.processing_mode);
                    self.plan(*direction, None);
                }
                Reply::HeadersWithBody {
                    direction,
                    mode,
                    processing_mode,
                } => {
                    prop_assert_eq!(*processing_mode, self.scenario.processing_mode);
                    self.plan(*direction, Some(*mode));
                }
                _ => {}
            }
            Ok(())
        }

        /// Queues the body the headers reply asked for, and its trailers, which come only with a body.
        fn plan(&mut self, direction: Direction, body_mode: Option<BodyMode>) {
            let half = self.scenario.half(direction);
            match body_mode {
                Some(BodyMode::Streamed) => {
                    for (i, chunk) in half.chunks.iter().enumerate() {
                        let last = i + 1 == half.chunks.len();
                        self.queue.push_back(Message::Body(
                            direction,
                            chunk.clone(),
                            last && !half.trailers,
                        ));
                    }
                }
                Some(BodyMode::Buffered) if !half.chunks.is_empty() => {
                    self.queue.push_back(Message::Body(
                        direction,
                        half.chunks.concat(),
                        !half.trailers,
                    ));
                }
                _ => {}
            }
            if half.trailers && body_mode.is_some() {
                self.queue.push_back(Message::Trailers(direction));
            }
            self.next_headers = self.scenario.next_headers(Some(direction));
            if direction == Direction::Request
                && self.next_headers.is_some()
                && body_mode == Some(BodyMode::Streamed)
                && let Some(after) = self.scenario.early_response
                && after < half.chunks.len()
            {
                self.queue.truncate(after);
                self.queue.push_back(Message::Headers(Direction::Response));
                self.next_headers = None;
                self.early_response = true;
            }
        }
    }

    impl HandlerTask {
        fn drain(&mut self) {
            let Some(receiver) = &mut self.receiver else {
                return;
            };
            while self.ended.is_none() {
                match receiver.try_recv() {
                    Ok(BodyMessage::Frame(Ok(frame))) => {
                        if let Ok(data) = frame.into_data() {
                            self.received.extend_from_slice(&data);
                        }
                    }
                    Ok(BodyMessage::Frame(Err(_))) | Err(TryRecvError::Disconnected) => {
                        self.ended = Some(false);
                    }
                    Ok(BodyMessage::End) => self.ended = Some(true),
                    Err(TryRecvError::Empty) => break,
                }
            }
        }

        fn is_ready(&self, access: Access) -> bool {
            !self.fetched
                || self.receiver.is_none()
                || access == Access::StreamPartly
                || self.ended.is_some()
        }
    }

    impl World<'_> {
        fn execute(&mut self, command: Command) -> Result<(), TestCaseError> {
            match command {
                Command::Read => {
                    prop_assert!(
                        !self.pending.iter().any(|p| matches!(p, Pending::Read)),
                        "two Envoy reads pending"
                    );
                    self.pending.push(Pending::Read);
                }
                Command::CallHandler { msg, mut context } => {
                    let direction = match msg.head {
                        Head::Request(_) => Direction::Request,
                        Head::Response(_) => Direction::Response,
                    };
                    match direction {
                        Direction::Request => {
                            prop_assert!(context.is_empty(), "context invented");
                            context.insert(Marker);
                        }
                        Direction::Response => prop_assert_eq!(
                            context.get::<Marker>().is_some(),
                            self.scenario.sends_headers(Direction::Request),
                            "context lost or invented"
                        ),
                    }
                    self.pending.push(Pending::Handler(Box::new(HandlerTask {
                        direction,
                        has_body: !msg.end_of_stream,
                        context,
                        fetched: false,
                        receiver: None,
                        received: Vec::new(),
                        ended: None,
                    })));
                }
                Command::SendChunk { tx, data, last } => {
                    self.pending.push(Pending::Chunk { tx, data, last });
                }
                Command::Requeue(event) => self.pending.push(Pending::Event(Box::new(event))),
                Command::AbortBody(tx) => drop(tx),
            }
            Ok(())
        }

        /// Completes the task at `index`, like `FuturesUnordered` yielding it.
        fn complete(
            &mut self,
            index: usize,
            envoy: &mut Envoy<'_>,
        ) -> Result<Event, TestCaseError> {
            Ok(match self.pending.swap_remove(index) {
                Pending::Read => envoy.read()?,
                Pending::Handler(task) => self.handler_event(*task),
                Pending::Chunk { tx, data, last } => {
                    // Send errors mean the handler dropped its stream; the bridge ignores them too.
                    if !data.is_empty() {
                        let _ = tx.try_send(BodyMessage::Frame(Ok(Frame::data(data))));
                    }
                    if last {
                        let _ = tx.try_send(BodyMessage::End);
                    }
                    Event::ChunkSent {
                        end_of_stream: last,
                    }
                }
                Pending::Event(event) => *event,
            })
        }

        /// The handler's body request first, then its result, as `call_handler` orders them.
        fn handler_event(&mut self, mut task: HandlerTask) -> Event {
            let half = self.scenario.half(task.direction);
            if !task.fetched {
                task.fetched = true;
                let request = (half.access != Access::None && task.has_body).then(|| {
                    let (tx, receiver) = mpsc::channel(1024);
                    task.receiver = Some(receiver);
                    let mode = if half.access == Access::Collect {
                        BodyMode::Buffered
                    } else {
                        BodyMode::Streamed
                    };
                    BodyRequest { mode, tx }
                });
                self.pending.push(Pending::Handler(Box::new(task)));
                return Event::FetchBody(request);
            }
            let result = if task.ended == Some(false) && half.access != Access::StreamPartly {
                Err(anyhow::Error::new(BodyError::Aborted))
            } else {
                match half.outcome {
                    Outcome::Continue | Outcome::Overrides | Outcome::ReplaceBody => {
                        let mutation = if half.outcome == Outcome::Overrides {
                            Mutation {
                                overrides: header_mutation(),
                                ..Mutation::default()
                            }
                        } else {
                            Mutation::default()
                        };
                        Ok(Action::Continue(ContinueAction {
                            mutation,
                            body_override: (half.outcome == Outcome::ReplaceBody)
                                .then(|| Bytes::from_static(b"replaced")),
                            context: mem::take(&mut task.context),
                        }))
                    }
                    Outcome::Immediate => Ok(Action::ImmediateResponse(immediate())),
                    Outcome::Error => Err(anyhow::anyhow!("handler failed")),
                }
            };
            self.received[task.direction as usize] = Some((task.received, task.ended));
            match result {
                Ok(action) => Event::Handled(action),
                Err(error) => Event::HandlerFailed {
                    error,
                    context: mem::take(&mut task.context),
                },
            }
        }
    }

    // helpers

    fn half() -> impl Strategy<Value = Half> {
        let access = prop_oneof![
            Just(Access::None),
            Just(Access::Stream),
            Just(Access::StreamPartly),
            Just(Access::Collect),
        ];
        let outcome = || {
            prop_oneof![
                4 => Just(Outcome::Continue),
                1 => Just(Outcome::Overrides),
                1 => Just(Outcome::ReplaceBody),
                1 => Just(Outcome::Immediate),
                1 => Just(Outcome::Error),
            ]
        };
        (
            vec(vec(any::<u8>(), 0..3), 0..4),
            any::<bool>(),
            access,
            outcome(),
        )
            .prop_map(|(chunks, trailers, access, outcome)| Half {
                chunks,
                trailers,
                access,
                outcome,
            })
    }

    fn scenario() -> impl Strategy<Value = Scenario> {
        (
            half(),
            half(),
            (header_mode(), header_mode()),
            any::<bool>(),
            option::of(0..4usize),
            option::weighted(0.2, (0..12usize, any::<bool>())),
            vec(any::<usize>(), 1..32),
        )
            .prop_map(
                |(
                    request,
                    response,
                    (request_header_mode, response_header_mode),
                    message_timeout,
                    early_response,
                    envoy_end,
                    schedule,
                )| {
                    Scenario {
                        request,
                        response,
                        processing_mode: ProcessingMode {
                            request_header_mode,
                            response_header_mode,
                        },
                        message_timeout,
                        early_response,
                        envoy_end,
                        schedule,
                    }
                },
            )
    }

    fn header_mode() -> impl Strategy<Value = HeaderMode> {
        prop_oneof![4 => Just(HeaderMode::Send), 1 => Just(HeaderMode::Skip)]
    }

    /// `None` for an immediate response or a message timeout.
    const fn reply_kind(reply: &Reply) -> Option<Kind> {
        match reply {
            Reply::HeadersNoBody { direction, .. }
            | Reply::HeadersReplaceBody { direction, .. }
            | Reply::HeadersWithBody { direction, .. } => Some((*direction, Part::Headers)),
            Reply::ContinueBody(direction) | Reply::LastBody { direction, .. } => {
                Some((*direction, Part::Body))
            }
            Reply::Trailers { direction, .. } => Some((*direction, Part::Trailers)),
            Reply::ImmediateResponse(_) | Reply::MessageTimeout(_) => None,
        }
    }

    fn header_mutation() -> HeaderMutation {
        let mut set = http::HeaderMap::new();
        set.insert("x-changed", http::HeaderValue::from_static("1"));
        HeaderMutation::from(set)
    }

    fn immediate() -> ImmediateResponse {
        ImmediateResponse::new(http::StatusCode::FORBIDDEN)
    }
}

// helpers

fn handled() -> Event {
    Event::Handled(request_action(
        HandlerAction::Continue(Default::default()),
        http::Extensions::new(),
    ))
}

fn sending_body_end(end: BodyEnd) -> State {
    State::SendingBodyEnd {
        direction: Direction::Request,
        mode: BodyMode::Streamed,
        end,
        pending: None,
    }
}

fn response_headers() -> Headers {
    Headers {
        head: crate::envoy::Head::Response(http::Response::new(()).into_parts().0),
        end_of_stream: true,
        attributes: Default::default(),
        metadata: Default::default(),
    }
}

fn body_event(direction: Direction) -> Event {
    Event::Body(
        direction,
        BodyChunk {
            data: Bytes::new(),
            end_of_stream: false,
        },
    )
}

fn status(result: Result<Transition, Status>) -> Status {
    result.expect_err("transition should fail")
}
