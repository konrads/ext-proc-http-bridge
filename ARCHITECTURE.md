# Architecture

- **One state machine per HTTP request.** Each ext_proc gRPC stream is driven by `fsm::transition`, a single `match (state, event)` in `ext_proc/fsm.rs`. Every arm is one transition. The states (`state.rs`) follow Envoy's phases: request headers and body, then the same for the response.

- **Pure transition function.** `transition` has no side effects apart from logging: it does not call the handler, touch the Envoy stream or spawn tasks. It returns the next state, the replies to Envoy, and `Command`s describing the work to start (read the next message, call the handler, send a chunk, abort the body). The loop in `ext_proc/mod.rs` turns the commands into tasks. Transitions can be tested by feeding events and checking the returned state, replies and commands, without Envoy or a handler.

- **Envoy types stay at the edge.** Only `envoy/` and the gRPC service in `ext_proc/mod.rs` use `envoy-types`. The loop converts each Envoy message into an `Event` (`TryFrom<ProcessingRequest> for envoy::Message`) and each `Reply` from `transition` into a `ProcessingResponse` (`From<Reply>`).

- **Futures queue.** All pending work is held in a `FuturesUnordered`: the next Envoy message, the running handler, the handler's body request, and the chunk being delivered to the handler. The loop takes whichever finishes first as the next event. The Envoy stream is moved into each read and handed back with the message, so exactly one read is pending at a time.

- **Bodies on demand.** `LazyBody::stream()` and `LazyBody::collect()` send a body request to the loop. The loop replies to Envoy's headers message with a `mode_override`: `STREAMED` for `stream()`, `BUFFERED` for `collect()`, `NONE` if the body is never requested. Collection is cached; cancelling `collect()` preserves progress for the next call. An unfinished body whose channel closes returns `BodyError::Aborted`.

- **Trailers end bodies only.** With trailers skipped, Envoy never marks the end of a STREAMED body that has them, so trailers are requested with every body the handler reads. They end the body and are answered unchanged; handlers never see them.

- **`processing_mode`.** Envoy does not tell the bridge its `processing_mode`, and a `mode_override` replaces all of it, so every override repeats the header modes from `ExtProcConfig::processing_mode`. With request headers skipped, the first message is the response headers.

- **Backpressure and ordering.** Chunks reach the handler through a bounded channel (`BodyConfig::channel_capacity`, default 8). The bridge answers a chunk only once it is in the channel, and reads the next one only after that, so chunks stay in order and a slow handler slows Envoy. The handler's result is always processed after its body request.

## Tests

`cargo test` runs the unit and end-to-end tests. The end-to-end tests need `envoy` on `PATH`.

Coverage, with [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov):

```sh
cargo llvm-cov --summary-only
```
