# ext-proc-http-bridge

[![build](https://github.com/konrads/ext-proc-http-bridge/actions/workflows/build.yml/badge.svg)](https://github.com/konrads/ext-proc-http-bridge/actions/workflows/build.yml)

An Envoy [`ext_proc`](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/ext_proc_filter) gRPC service that calls your `Handler` with an `http::Request` and `http::Response`. The handler can change headers, body, path, status and metadata, or send an immediate response; trailers are not supported. Bodies are fetched from Envoy only if the handler reads them.

## Usage

Implement `Handler`, wrap it in `ExtProc`, and serve it with tonic. Both methods default to pass-through; implement only the methods you need:

```rust,no_run
use std::sync::Arc;

use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer;
use ext_proc_http_bridge::{
    ExtProc, ExtProcConfig, Handler, LazyBody,
    handler::{HandlerAction, RequestContinue},
};
use tonic::transport::Server;

struct MyHandler;

#[async_trait::async_trait]
impl Handler for MyHandler {
    async fn handle_request(
        &self,
        req: &http::Request<LazyBody>,
        _context: &mut http::Extensions,
    ) -> anyhow::Result<HandlerAction<RequestContinue>> {
        let body = req.body().collect().await?;
        let mut headers = http::HeaderMap::new();
        headers.insert("x-body-length", body.len().into());
        Ok(HandlerAction::Continue(RequestContinue {
            header_mutation: headers.into(),
            ..Default::default()
        }))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let processor = ExtProc::new(Arc::new(MyHandler), ExtProcConfig::default());
    Server::builder()
        .add_service(ExternalProcessorServer::new(processor))
        .serve("127.0.0.1:50051".parse()?)
        .await?;
    Ok(())
}
```

## Handler

- **Return** `HandlerAction::Continue(..)` with the changes, or `HandlerAction::ImmediateResponse(..)`. `Default::default()` means no changes.
- **Bodies:** `collect()` reads the whole body, `stream()` reads it chunk by chunk with backpressure.
- **`context`** carries values from the request handler to the response handler.

## Envoy configuration

```yaml
- name: envoy.filters.http.ext_proc
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
    grpc_service:
      envoy_grpc:
        cluster_name: ext_proc_service
    allow_mode_override: true
```

- `processing_mode` may skip request or response headers; set the same in `ExtProcConfig::processing_mode`, as each `mode_override` the bridge sends replaces Envoy's whole mode. Leave body and trailer modes unset: the bridge sets body modes per request.
- Don't set `send_body_without_waiting_for_header_response`, `observability_mode` or `allowed_override_modes`: they break `mode_override`, which the bridge needs to control body delivery.
- Envoy answers 504 if a reply takes longer than `message_timeout` (default 200ms); raise it for slow handlers.
- Optional: `request_attributes: ["request.protocol"]` sets the HTTP version (`HTTP/1.1` otherwise); `metadata_options` exchanges dynamic metadata with other filters.

## Limits

- No header, body, path or status changes after `stream()`, or on a body that is only trailers; use `collect()` when changes depend on the body.
- `collect()` is limited by Envoy's buffer limit and tonic's 4 MiB message size (`max_decoding_message_size`).
- Handlers cannot read or modify trailers; they pass through unchanged.
- `FULL_DUPLEX_STREAMED` is not supported.
