#![allow(unused_variables, dead_code)]

use std::{collections::HashMap, pin::pin, sync::Mutex, time::Duration};

use ext_proc_http_bridge::{
    body::LazyBody,
    handler::{
        EnvoyAttributes, EnvoyMetadata, Handler, HandlerAction, HeaderMutation, ImmediateResponse,
        RequestContinue, ResponseContinue,
    },
};
use futures::StreamExt;
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use tracing::info;

use super::Payload;

#[derive(Clone)]
pub struct MockRespHeaders(HashMap<String, String>);

#[derive(Clone)]
pub struct MockHandler;

/// Response trailers test: how the response handler reads the body.
#[derive(Clone)]
pub struct RespTrailers {
    pub mode: String,
}

/// Attribute the request handler saw, for the response handler to report.
#[derive(Clone)]
pub struct Meta {
    pub protocol: String,
}

/// Marks a request whose response handler replaces the status.
#[derive(Clone)]
pub struct StatusOverride;

/// Marks a request whose response handler replaces the body without reading it.
#[derive(Clone)]
pub struct RespReplaceUnread;

/// Marks a request whose response handler replaces the body after `collect()`.
#[derive(Clone)]
pub struct RespReplace;

/// Marks a request whose response handler streams the body and records `RESP_STREAM_STATS`.
#[derive(Clone)]
pub struct RespStream;

/// What the streaming response handler saw.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct RespStreamStats {
    pub chunks: usize,
    pub bytes: usize,
}

/// Set by the streaming response handler; taken by the test.
pub static RESP_STREAM_STATS: Mutex<Option<RespStreamStats>> = Mutex::new(None);

/// Marks a request whose response handler exits early.
#[derive(Clone)]
pub struct RespExit;

/// Marks a request whose response handler is slower than Envoy's message_timeout.
#[derive(Clone)]
pub struct SlowResp;

/// Marks a request whose response handler sets, appends and removes headers.
#[derive(Clone)]
pub struct MutateResp;

/// Marks a request whose response handler calls `stream()` and returns at once.
#[derive(Clone)]
pub struct RespStreamUnread;

/// Bodyless request: how both handlers read the empty body, and the request body's length.
#[derive(Clone)]
pub struct NoBody {
    pub mode: String,
    pub request_len: usize,
}

#[async_trait::async_trait]
impl Handler for MockHandler {
    async fn handle_request(
        &self,
        req: &http::Request<LazyBody>,
        context: &mut http::Extensions,
    ) -> anyhow::Result<HandlerAction<RequestContinue>> {
        let http_method = req.method();
        let path = req
            .uri()
            .path()
            .trim_matches('/')
            .split('/')
            .collect::<Vec<_>>();

        match (http_method, path.as_slice()) {
            (&Method::GET, ["666", "rest", ..]) => {
                let header_mutation = headers([("invalid-token", "666")]);
                Ok(HandlerAction::ImmediateResponse(ImmediateResponse {
                    body: bytes::Bytes::from_static(b"invalid token"),
                    status: http::StatusCode::UNAUTHORIZED,
                    header_mutation: header_mutation.into(),
                }))
            }
            // Sets, appends and removes request headers; the response handler does the same.
            (&Method::GET, [_, "rest", "mutate"]) => {
                context.insert(MutateResp);
                Ok(HandlerAction::Continue(RequestContinue {
                    header_mutation: HeaderMutation {
                        set: headers([("x-client", "new")]),
                        append: headers([("x-multi", "a"), ("x-multi", "b")]),
                        remove: vec![HeaderName::from_static("x-remove-me")],
                    },
                    ..Default::default()
                }))
            }
            // The response handler replaces the body, or streams it.
            (&Method::GET, [_, "rest", "resp-replace"]) => {
                context.insert(RespReplace);
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // Response trailers, with the response body streamed, collected or not read.
            (&Method::POST, ["h2", "resp-trailers", mode]) => {
                context.insert(RespTrailers {
                    mode: (*mode).to_string(),
                });
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            (&Method::POST, ["h2", "early"]) => {
                let mut stream = pin!(req.body().stream()?);
                // Fails on the cut-off body, as most handlers would.
                while let Some(frame) = stream.next().await {
                    frame?;
                }
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // Collects a body that is only trailers, then changes a header.
            (&Method::POST, ["h2", "trailers-only-collect"]) => {
                req.body().collect().await?;
                Ok(HandlerAction::Continue(RequestContinue {
                    header_mutation: headers([("x-collected", "1")]).into(),
                    ..Default::default()
                }))
            }
            // Request trailers, with the body streamed, collected or not read.
            (&Method::POST, ["h2", "req-trailers", mode]) => {
                match *mode {
                    "stream" => {
                        let mut stream = pin!(req.body().stream()?);
                        while stream.next().await.is_some() {}
                    }
                    "collect" => {
                        req.body().collect().await?;
                    }
                    _ => {}
                }
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            (&Method::GET, ["h2", ..]) | (&Method::POST, [_, "resp-stream"]) => {
                context.insert(RespStream);
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // The response handler exits early, or is slower than message_timeout.
            (&Method::GET, [_, "rest", "resp-exit"]) => {
                context.insert(RespExit);
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            (&Method::GET, [_, "rest", "slow-resp"]) => {
                context.insert(SlowResp);
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // The response handler requests the body stream and returns at once.
            (&Method::GET, [_, "rest", "unread"]) => {
                context.insert(RespStreamUnread);
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            (&Method::GET, [_, "rest", "resp-replace-unread"]) => {
                context.insert(RespReplaceUnread);
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // Reads the absent request body with `collect()` or `stream()`.
            (&Method::GET, [_, "rest", "no-body", mode]) => {
                let request_len = read_len(req.body(), mode).await?;
                context.insert(NoBody {
                    mode: (*mode).to_string(),
                    request_len,
                });
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // Empty header value must reach the upstream.
            (&Method::GET, [_, "rest", "empty-header"]) => {
                Ok(HandlerAction::Continue(RequestContinue {
                    header_mutation: headers([("x-client", "")]).into(),
                    ..Default::default()
                }))
            }
            // Sets the header the h2 route matches on, with and without a new route match.
            (&Method::GET, [_, "rest", kind @ ("header-route" | "header-route-no-clear")]) => {
                Ok(HandlerAction::Continue(RequestContinue {
                    header_mutation: headers([("x-route", "h2")]).into(),
                    clear_route_cache: *kind == "header-route",
                    ..Default::default()
                }))
            }
            // Rewrites the path to one routed to the h2 upstream.
            (&Method::GET, [_, "rest", "reroute"]) => {
                Ok(HandlerAction::Continue(RequestContinue {
                    path_override: Some(http::uri::PathAndQuery::from_static("/h2/rerouted")),
                    ..Default::default()
                }))
            }
            // The response handler replaces the upstream's status.
            (&Method::GET, [_, "rest", "status"]) => {
                context.insert(StatusOverride);
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // Reads an Envoy attribute and emits dynamic metadata for the response phase.
            (&Method::GET, [_, "rest", "meta"]) => {
                let protocol = req
                    .extensions()
                    .get::<EnvoyAttributes>()
                    .and_then(|a| a.0.get("request.protocol"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("<absent>")
                    .to_string();
                let serde_json::Value::Object(dynamic_metadata) =
                    serde_json::json!({ "bridge": { "user": "alice" } })
                else {
                    unreachable!()
                };
                context.insert(Meta { protocol });
                Ok(HandlerAction::Continue(RequestContinue {
                    dynamic_metadata,
                    ..Default::default()
                }))
            }
            (&Method::GET, [token, "rest", ..]) => {
                let header_mutation = HeaderMap::<HeaderValue>::from_iter([(
                    HeaderName::from_static("valid-token"),
                    token.parse()?,
                )]);
                context.insert(MockRespHeaders(HashMap::from_iter([(
                    "http-method".to_string(),
                    "GET".to_string(),
                )])));
                Ok(HandlerAction::Continue(RequestContinue {
                    header_mutation: header_mutation.into(),
                    ..Default::default()
                }))
            }
            (&Method::GET, _) => Ok(HandlerAction::ImmediateResponse(ImmediateResponse::new(
                http::StatusCode::UNAUTHORIZED,
            ))),
            (&Method::POST, ["666", "rpc"]) => {
                let header_mutation = headers([("invalid-token", "666")]);
                Ok(HandlerAction::ImmediateResponse(ImmediateResponse {
                    body: bytes::Bytes::new(),
                    status: http::StatusCode::UNAUTHORIZED,
                    header_mutation: header_mutation.into(),
                }))
            }
            (&Method::POST, [token, "rpc"]) => {
                info!("Fetching request body");
                let payload = serde_json::from_slice::<Payload>(&req.body().collect().await?)?;
                let header_mutation = HeaderMap::<HeaderValue>::from_iter([(
                    HeaderName::from_static("valid-token"),
                    token.parse()?,
                )]);
                context.insert(MockRespHeaders(HashMap::from_iter([
                    ("rpc-method".to_string(), payload.method),
                    ("rpc-value".to_string(), payload.value.to_string()),
                ])));
                Ok(HandlerAction::Continue(RequestContinue {
                    header_mutation: header_mutation.into(),
                    ..Default::default()
                }))
            }
            (&Method::POST, [_, "stream"]) => {
                let mut stream = pin!(req.body().stream()?);
                let (mut chunks, mut bytes) = (0usize, 0usize);
                while let Some(frame) = stream.next().await {
                    if let Ok(data) = frame?.into_data() {
                        chunks += 1;
                        bytes += data.len();
                    }
                }
                context.insert(MockRespHeaders(HashMap::from_iter([
                    ("stream-chunks".to_string(), chunks.to_string()),
                    ("stream-bytes".to_string(), bytes.to_string()),
                ])));
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // Reads one chunk, then returns overrides that STREAMED cannot apply.
            (&Method::POST, [_, "stream-override"]) => {
                let mut stream = pin!(req.body().stream()?);
                stream.next().await;
                Ok(HandlerAction::Continue(RequestContinue {
                    header_mutation: headers([("too-late", "1")]).into(),
                    ..Default::default()
                }))
            }
            // Slower than Envoy's 200ms message_timeout, faster than the 600ms override.
            (&Method::POST, [_, "slow"]) => {
                tokio::time::sleep(Duration::from_millis(400)).await;
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // Slower than the bridge's 600ms override_message_timeout.
            (&Method::POST, [_, "too-slow"]) => {
                tokio::time::sleep(Duration::from_millis(800)).await;
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // Requests the stream and returns without reading it.
            (&Method::POST, [_, "stream-unread"]) => {
                drop(req.body().stream()?);
                Ok(HandlerAction::Continue(RequestContinue::default()))
            }
            // Body replaced without reading it.
            (&Method::POST, [_, "replace-unread"]) => {
                Ok(HandlerAction::Continue(RequestContinue {
                    body_override: Some(bytes::Bytes::from_static(b"replaced-unread")),
                    ..Default::default()
                }))
            }
            (&Method::POST, [_, "replace"]) => {
                let original = req.body().collect().await?;
                let header_mutation = HeaderMap::<HeaderValue>::from_iter([(
                    HeaderName::from_static("original-length"),
                    original.len().into(),
                )]);
                Ok(HandlerAction::Continue(RequestContinue {
                    header_mutation: header_mutation.into(),
                    body_override: Some(bytes::Bytes::from_static(b"replaced-body")),
                    ..Default::default()
                }))
            }
            _ => Ok(HandlerAction::Continue(RequestContinue::default())),
        }
    }

    async fn handle_response(
        &self,
        resp: &http::Response<LazyBody>,
        context: &http::Extensions,
    ) -> anyhow::Result<HandlerAction<ResponseContinue>> {
        if let Some(RespTrailers { mode, .. }) = context.get::<RespTrailers>() {
            match mode.as_str() {
                "stream" => {
                    let mut stream = pin!(resp.body().stream()?);
                    while stream.next().await.is_some() {}
                }
                "collect" => {
                    resp.body().collect().await?;
                }
                _ => {}
            }
            return Ok(HandlerAction::Continue(ResponseContinue::default()));
        }
        if let Some(NoBody { mode, request_len }) = context.get::<NoBody>() {
            let response_len = read_len(resp.body(), mode).await?;
            let set = HeaderMap::from_iter([
                (
                    HeaderName::from_static("req-body-length"),
                    request_len.to_owned().into(),
                ),
                (
                    HeaderName::from_static("resp-body-length"),
                    response_len.into(),
                ),
            ]);
            return Ok(HandlerAction::Continue(ResponseContinue {
                header_mutation: set.into(),
                ..Default::default()
            }));
        }
        if let Some(Meta { protocol }) = context.get::<Meta>() {
            let response_code = resp
                .extensions()
                .get::<EnvoyAttributes>()
                .and_then(|a| a.0.get("response.code"))
                .map_or_else(|| "<absent>".to_string(), ToString::to_string);
            let user = resp
                .extensions()
                .get::<EnvoyMetadata>()
                .and_then(|m| m.0.get("bridge"))
                .and_then(|ns| ns.get("user"))
                .and_then(|v| v.as_str())
                .unwrap_or("<absent>")
                .to_string();
            let set = HeaderMap::from_iter([
                (HeaderName::from_static("seen-protocol"), protocol.parse()?),
                (
                    HeaderName::from_static("seen-response-code"),
                    response_code.parse()?,
                ),
                (HeaderName::from_static("seen-user"), user.parse()?),
            ]);
            return Ok(HandlerAction::Continue(ResponseContinue {
                header_mutation: set.into(),
                ..Default::default()
            }));
        }
        if context.get::<StatusOverride>().is_some() {
            return Ok(HandlerAction::Continue(ResponseContinue {
                status_override: Some(http::StatusCode::NON_AUTHORITATIVE_INFORMATION),
                ..Default::default()
            }));
        }
        if context.get::<RespReplaceUnread>().is_some() {
            return Ok(HandlerAction::Continue(ResponseContinue {
                body_override: Some(bytes::Bytes::from_static(b"resp-replaced-unread")),
                ..Default::default()
            }));
        }
        if context.get::<RespReplace>().is_some() {
            resp.body().collect().await?;
            return Ok(HandlerAction::Continue(ResponseContinue {
                body_override: Some(bytes::Bytes::from_static(b"resp-replaced")),
                ..Default::default()
            }));
        }
        if context.get::<RespStream>().is_some() {
            let mut stream = pin!(resp.body().stream()?);
            let mut stats = RespStreamStats::default();
            while let Some(frame) = stream.next().await {
                if let Ok(data) = frame?.into_data() {
                    stats.chunks += 1;
                    stats.bytes += data.len();
                }
            }
            *RESP_STREAM_STATS.lock().unwrap() = Some(stats);
            return Ok(HandlerAction::Continue(ResponseContinue::default()));
        }
        if context.get::<RespExit>().is_some() {
            return Ok(HandlerAction::ImmediateResponse(ImmediateResponse {
                body: bytes::Bytes::new(),
                status: http::StatusCode::IM_A_TEAPOT,
                header_mutation: headers([("x-resp-exit", "1")]).into(),
            }));
        }
        if context.get::<SlowResp>().is_some() {
            tokio::time::sleep(Duration::from_millis(400)).await;
            return Ok(HandlerAction::Continue(ResponseContinue::default()));
        }
        if context.get::<MutateResp>().is_some() {
            let mut append = HeaderMap::new();
            append.append(http::header::SET_COOKIE, HeaderValue::from_static("a=1"));
            append.append(http::header::SET_COOKIE, HeaderValue::from_static("b=2"));
            return Ok(HandlerAction::Continue(ResponseContinue {
                header_mutation: HeaderMutation {
                    set: HeaderMap::from_iter([(
                        http::header::CONTENT_TYPE,
                        HeaderValue::from_static("text/x-overridden"),
                    )]),
                    append,
                    remove: vec![HeaderName::from_static("x-upstream")],
                },
                ..Default::default()
            }));
        }
        if context.get::<RespStreamUnread>().is_some() {
            drop(resp.body().stream()?);
            return Ok(HandlerAction::Continue(ResponseContinue::default()));
        }

        // add context's ResponseHeaders, apply values as headers
        let mut header_mutation: HeaderMap = http::HeaderMap::new();
        // Report what the handler saw, so tests can check the conversion.
        header_mutation.insert("seen-status", resp.status().as_u16().into());
        if let Some(content_type) = resp.headers().get(http::header::CONTENT_TYPE) {
            header_mutation.insert("seen-content-type", content_type.clone());
        }
        if let Some(resp_headers) = context.get::<MockRespHeaders>() {
            for (key, value) in resp_headers.0.iter() {
                header_mutation.insert(
                    HeaderName::from_bytes(key.as_str().as_bytes())?,
                    value.parse()?,
                );
            }
        }

        if resp.status() == http::StatusCode::OK {
            info!("Fetching response body");
            let body = resp.body().collect().await?;
            header_mutation.insert(
                HeaderName::from_static("body-length"),
                body.len().to_string().parse()?,
            );
        }

        Ok(HandlerAction::Continue(ResponseContinue {
            header_mutation: header_mutation.into(),
            ..Default::default()
        }))
    }
}

// helpers

/// `HeaderMap` from static name/value pairs.
fn headers<const N: usize>(pairs: [(&'static str, &'static str); N]) -> HeaderMap {
    pairs
        .into_iter()
        .map(|(name, value)| {
            (
                HeaderName::from_static(name),
                HeaderValue::from_static(value),
            )
        })
        .collect()
}

/// Reads a body with `collect()` or `stream()` and returns its length.
async fn read_len(body: &LazyBody, mode: &str) -> anyhow::Result<usize> {
    if mode == "collect" {
        return Ok(body.collect().await?.len());
    }
    let mut stream = pin!(body.stream()?);
    let mut len = 0;
    while let Some(frame) = stream.next().await {
        if let Ok(data) = frame?.into_data() {
            len += data.len();
        }
    }
    Ok(len)
}
