//! Conversion between Envoy's ext_proc messages and this crate's types.

use std::{collections::HashMap, time::Duration};

use bytes::Bytes;
use envoy_types::pb::{
    envoy::{
        config::core::v3::{
            HeaderValue, HeaderValueOption, Metadata, header_value_option::HeaderAppendAction,
        },
        extensions::filters::http::ext_proc::v3::{
            ProcessingMode as EnvoyProcessingMode,
            processing_mode::{BodySendMode, HeaderSendMode},
        },
        service::ext_proc::v3::{
            BodyMutation, BodyResponse, CommonResponse, HeaderMutation as EnvoyHeaderMutation,
            HeadersResponse, HttpBody, HttpHeaders, ImmediateResponse, ProcessingRequest,
            ProcessingResponse, TrailersResponse, body_mutation, common_response::ResponseStatus,
            processing_request, processing_response,
        },
        r#type::v3::HttpStatus,
    },
    google::protobuf::Struct,
};
use http::{StatusCode, uri::PathAndQuery};
use tonic::Status;

use crate::{
    body::BodyMode,
    handler::{self, EnvoyAttributes, EnvoyMetadata, HeaderMutation},
};

pub mod json;

/// Which half of the exchange a message belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Request,
    Response,
}

/// An Envoy message, converted from `ProcessingRequest`.
// One per Envoy message, moved straight into an `Event`; boxing `Headers` would cost an allocation each.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Message {
    Headers(Direction, Headers),
    Body(Direction, BodyChunk),
    /// Not parsed: the bridge only uses trailers as the end of a body.
    Trailers(Direction),
}

/// Request or response head, without the body.
#[derive(Debug)]
pub enum Head {
    Request(http::request::Parts),
    Response(http::response::Parts),
}

/// A headers message with the attributes and metadata sent along with it.
#[derive(Debug)]
pub struct Headers {
    pub head: Head,
    pub end_of_stream: bool,
    pub attributes: EnvoyAttributes,
    pub metadata: EnvoyMetadata,
}

/// A body message; in BUFFERED mode it holds the whole body.
#[derive(Debug)]
pub struct BodyChunk {
    pub data: Bytes,
    pub end_of_stream: bool,
}

/// Header modes of Envoy's `processing_mode`; each `mode_override` repeats them, as it replaces the whole mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessingMode {
    /// Envoy ignores this in a `mode_override`; with `Skip`, the first message is the response headers.
    pub request_header_mode: HeaderMode,
    pub response_header_mode: HeaderMode,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HeaderMode {
    #[default]
    Send,
    Skip,
}

/// A reply to Envoy, converted into `ProcessingResponse`.
// `pub(crate)`, not `pub`: `From<Reply> for ProcessingResponse` would make `pub` reachable.
#[allow(clippy::redundant_pub_crate)]
#[derive(Debug)]
pub(crate) enum Reply {
    /// Headers reply when the body is not read.
    HeadersNoBody {
        direction: Direction,
        mutation: Mutation,
        processing_mode: ProcessingMode,
    },
    /// Headers reply replacing the body; Envoy discards the original without sending it.
    HeadersReplaceBody {
        direction: Direction,
        mutation: Mutation,
        body: Bytes,
        processing_mode: ProcessingMode,
    },
    /// Headers reply asking Envoy to send the body in `mode`; trailers too, as they can end a STREAMED body.
    HeadersWithBody {
        direction: Direction,
        mode: BodyMode,
        processing_mode: ProcessingMode,
    },
    /// Reply to a body chunk that is not the last.
    ContinueBody(Direction),
    /// Reply to the last body chunk; a body override also sets Content-Length.
    LastBody {
        direction: Direction,
        mutation: Mutation,
        body_override: Option<Bytes>,
    },
    /// Trailers reply, leaving them unchanged; carries the handler's metadata when trailers ended the body.
    Trailers {
        direction: Direction,
        dynamic_metadata: serde_json::Map<String, serde_json::Value>,
    },
    ImmediateResponse(handler::ImmediateResponse),
    /// Asks Envoy to extend the timer for the current message.
    MessageTimeout(Duration),
}

/// Header changes for a headers or body reply: user overrides plus pseudo-headers.
#[derive(Debug, Default, Clone)]
pub struct Mutation {
    pub overrides: HeaderMutation,
    pub pseudo: Option<PseudoHeader>,
    pub clear_route_cache: bool,
    pub dynamic_metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub enum PseudoHeader {
    Path(PathAndQuery),
    Status(StatusCode),
}

/// Invalid headers, or no request at all, fail with `InvalidArgument`.
impl TryFrom<ProcessingRequest> for Message {
    type Error = Status;

    fn try_from(message: ProcessingRequest) -> Result<Self, Status> {
        use processing_request::Request;
        let ProcessingRequest {
            request,
            attributes,
            metadata_context,
            ..
        } = message;
        let headers = |mut head: Head, end_of_stream| {
            let attributes = from_attributes(attributes);
            if let Some(version) = http_version(&attributes) {
                match &mut head {
                    Head::Request(parts) => parts.version = version,
                    Head::Response(parts) => parts.version = version,
                }
            }
            Headers {
                head,
                end_of_stream,
                attributes,
                metadata: from_metadata(metadata_context),
            }
        };
        Ok(match request {
            Some(Request::RequestHeaders(h)) => {
                let end = h.end_of_stream;
                Self::Headers(
                    Direction::Request,
                    headers(Head::Request(request_parts(h)?), end),
                )
            }
            Some(Request::ResponseHeaders(h)) => {
                let end = h.end_of_stream;
                Self::Headers(
                    Direction::Response,
                    headers(Head::Response(response_parts(h)?), end),
                )
            }
            Some(Request::RequestBody(b)) => Self::Body(Direction::Request, body_chunk(b)),
            Some(Request::ResponseBody(b)) => Self::Body(Direction::Response, body_chunk(b)),
            Some(Request::RequestTrailers(_)) => Self::Trailers(Direction::Request),
            Some(Request::ResponseTrailers(_)) => Self::Trailers(Direction::Response),
            None => return Err(Status::invalid_argument("ProcessingRequest has no request")),
        })
    }
}

impl From<Reply> for ProcessingResponse {
    fn from(reply: Reply) -> Self {
        match reply {
            Reply::HeadersNoBody {
                direction,
                mutation,
                processing_mode,
            } => headers_no_body(direction, mutation, processing_mode),
            Reply::HeadersReplaceBody {
                direction,
                mutation,
                body,
                processing_mode,
            } => headers_replace_body(direction, mutation, body, processing_mode),
            Reply::HeadersWithBody {
                direction,
                mode,
                processing_mode,
            } => headers_with_body(direction, mode, processing_mode),
            Reply::ContinueBody(direction) => {
                body_reply(direction, continue_status(), Mutation::default())
            }
            Reply::LastBody {
                direction,
                mutation,
                body_override,
            } => last_body(direction, mutation, body_override),
            Reply::Trailers {
                direction,
                dynamic_metadata,
            } => trailers(direction, dynamic_metadata),
            Reply::ImmediateResponse(resp) => immediate_response(resp),
            Reply::MessageTimeout(timeout) => override_message_timeout(timeout),
        }
    }
}

impl HeaderMode {
    const fn into_envoy(self) -> HeaderSendMode {
        match self {
            Self::Send => HeaderSendMode::Send,
            Self::Skip => HeaderSendMode::Skip,
        }
    }
}

impl Mutation {
    /// Whether any header or pseudo-header changes; metadata does not count.
    #[must_use]
    pub fn changes_headers(&self) -> bool {
        !self.overrides.is_empty() || self.pseudo.is_some()
    }

    /// Request changes; a new `:path` also makes Envoy re-pick the route.
    #[must_use]
    pub fn request(
        overrides: HeaderMutation,
        path: Option<PathAndQuery>,
        clear_route_cache: bool,
        dynamic_metadata: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        let clear_route_cache = clear_route_cache || path.is_some();
        Self {
            overrides,
            pseudo: path.map(PseudoHeader::Path),
            clear_route_cache,
            dynamic_metadata,
        }
    }

    #[must_use]
    pub fn response(
        overrides: HeaderMutation,
        status: Option<StatusCode>,
        dynamic_metadata: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        Self {
            overrides,
            pseudo: status.map(PseudoHeader::Status),
            clear_route_cache: false,
            dynamic_metadata,
        }
    }
}

/// `version` is set later from the `request.protocol` attribute, if Envoy sends it.
fn request_parts(headers: HttpHeaders) -> Result<http::request::Parts, Status> {
    let mut method = None;
    let mut scheme = None;
    let mut authority = None;
    let mut path = None;
    let mut header_map = http::HeaderMap::new();

    // Envoy lower-cases keys and puts values in `raw_value`.
    for header in headers.headers.into_iter().flat_map(|map| map.headers) {
        match header.key.as_str() {
            ":method" => method = Some(header.raw_value),
            ":scheme" => scheme = Some(header.raw_value),
            ":authority" => authority = Some(header.raw_value),
            ":path" => path = Some(header.raw_value),
            key if key.starts_with(':') => {}
            key => append_header(&mut header_map, key, header.raw_value)?,
        }
    }

    let path = path.as_deref().unwrap_or(b"/");
    let uri = authority
        .as_deref()
        .map_or_else(
            || http::Uri::builder().path_and_query(path).build(),
            |authority| {
                http::Uri::builder()
                    .scheme(scheme.as_deref().unwrap_or(b"http"))
                    .authority(authority)
                    .path_and_query(path)
                    .build()
            },
        )
        .map_err(|e| Status::invalid_argument(format!("Invalid URI: {e}")))?;

    let mut req = http::Request::builder()
        .method(
            http::Method::from_bytes(
                method
                    .as_deref()
                    .ok_or_else(|| Status::invalid_argument("Missing :method"))?,
            )
            .map_err(|e| Status::invalid_argument(format!("Invalid method: {e}")))?,
        )
        .uri(uri)
        .body(())
        .map_err(|e| Status::internal(format!("Failed to build request: {e}")))?;

    *req.headers_mut() = header_map;
    Ok(req.into_parts().0)
}

/// `version` is set later from the `request.protocol` attribute, if Envoy sends it.
fn response_parts(headers: HttpHeaders) -> Result<http::response::Parts, Status> {
    let mut status = None;
    let mut header_map = http::HeaderMap::new();

    // Envoy lower-cases keys and puts values in `raw_value`.
    for header in headers.headers.into_iter().flat_map(|map| map.headers) {
        match header.key.as_str() {
            ":status" => status = Some(header.raw_value),
            key if key.starts_with(':') => {}
            key => append_header(&mut header_map, key, header.raw_value)?,
        }
    }

    let status = StatusCode::from_bytes(
        status
            .as_deref()
            .ok_or_else(|| Status::invalid_argument("Missing :status"))?,
    )
    .map_err(|e| Status::invalid_argument(format!("Invalid :status: {e}")))?;

    let mut resp = http::Response::builder()
        .status(status)
        .body(())
        .map_err(|e| Status::internal(format!("Failed to build response: {e}")))?;

    *resp.headers_mut() = header_map;
    Ok(resp.into_parts().0)
}

fn headers_no_body(
    direction: Direction,
    mutation: Mutation,
    modes: ProcessingMode,
) -> ProcessingResponse {
    let common = continue_with(&mutation);
    headers_reply(direction, common, BodySendMode::None, modes, mutation)
}

fn headers_replace_body(
    direction: Direction,
    mut mutation: Mutation,
    body: Bytes,
    modes: ProcessingMode,
) -> ProcessingResponse {
    let common = CommonResponse {
        status: ResponseStatus::ContinueAndReplace as i32,
        ..last_body_reply(&mut mutation, Some(body))
    };
    headers_reply(direction, common, BodySendMode::None, modes, mutation)
}

fn headers_with_body(
    direction: Direction,
    mode: BodyMode,
    modes: ProcessingMode,
) -> ProcessingResponse {
    let mode = match mode {
        BodyMode::Streamed => BodySendMode::Streamed,
        BodyMode::Buffered => BodySendMode::Buffered,
    };
    headers_reply(
        direction,
        continue_status(),
        mode,
        modes,
        Mutation::default(),
    )
}

fn last_body(
    direction: Direction,
    mut mutation: Mutation,
    body_override: Option<Bytes>,
) -> ProcessingResponse {
    body_reply(
        direction,
        last_body_reply(&mut mutation, body_override),
        mutation,
    )
}

fn trailers(
    direction: Direction,
    dynamic_metadata: serde_json::Map<String, serde_json::Value>,
) -> ProcessingResponse {
    let trailers = TrailersResponse::default();
    ProcessingResponse {
        response: Some(match direction {
            Direction::Request => processing_response::Response::RequestTrailers(trailers),
            Direction::Response => processing_response::Response::ResponseTrailers(trailers),
        }),
        dynamic_metadata: to_metadata(dynamic_metadata),
        ..Default::default()
    }
}

fn immediate_response(resp: handler::ImmediateResponse) -> ProcessingResponse {
    ProcessingResponse {
        response: Some(processing_response::Response::ImmediateResponse(
            ImmediateResponse {
                status: Some(HttpStatus {
                    code: i32::from(resp.status.as_u16()),
                }),
                headers: to_header_mutation(&resp.header_mutation, None),
                body: Vec::from(resp.body),
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

fn override_message_timeout(timeout: Duration) -> ProcessingResponse {
    ProcessingResponse {
        override_message_timeout: Some(envoy_types::pb::google::protobuf::Duration {
            seconds: i64::try_from(timeout.as_secs()).unwrap_or(i64::MAX),
            nanos: i32::try_from(timeout.subsec_nanos()).unwrap_or(0),
        }),
        ..Default::default()
    }
}

// helpers

fn headers_reply(
    direction: Direction,
    common: CommonResponse,
    body_mode: BodySendMode,
    modes: ProcessingMode,
    mutation: Mutation,
) -> ProcessingResponse {
    let headers = HeadersResponse {
        response: Some(common),
    };
    ProcessingResponse {
        response: Some(match direction {
            Direction::Request => processing_response::Response::RequestHeaders(headers),
            Direction::Response => processing_response::Response::ResponseHeaders(headers),
        }),
        mode_override: Some(processing_mode(direction, body_mode, modes)),
        dynamic_metadata: to_metadata(mutation.dynamic_metadata),
        ..Default::default()
    }
}

fn body_reply(
    direction: Direction,
    common: CommonResponse,
    mutation: Mutation,
) -> ProcessingResponse {
    let body = BodyResponse {
        response: Some(common),
    };
    ProcessingResponse {
        response: Some(match direction {
            Direction::Request => processing_response::Response::RequestBody(body),
            Direction::Response => processing_response::Response::ResponseBody(body),
        }),
        dynamic_metadata: to_metadata(mutation.dynamic_metadata),
        ..Default::default()
    }
}

fn continue_status() -> CommonResponse {
    CommonResponse {
        status: ResponseStatus::Continue as i32,
        ..Default::default()
    }
}

/// `None` when empty, so no metadata is sent.
fn to_metadata(map: serde_json::Map<String, serde_json::Value>) -> Option<Struct> {
    (!map.is_empty()).then(|| json::to_struct(map))
}

fn last_body_reply(mutation: &mut Mutation, body_override: Option<Bytes>) -> CommonResponse {
    let body_mutation = body_override.map(|body| {
        mutation
            .overrides
            .set
            .insert(http::header::CONTENT_LENGTH, body.len().into());
        BodyMutation {
            mutation: Some(body_mutation::Mutation::Body(Vec::from(body))),
        }
    });
    CommonResponse {
        body_mutation,
        ..continue_with(mutation)
    }
}

fn continue_with(mutation: &Mutation) -> CommonResponse {
    CommonResponse {
        header_mutation: to_header_mutation(&mutation.overrides, mutation.pseudo.as_ref()),
        status: ResponseStatus::Continue as i32,
        clear_route_cache: mutation.clear_route_cache,
        ..Default::default()
    }
}

/// Header values are sent as `raw_value` only, so conversion cannot fail.
fn to_header_mutation(
    overrides: &HeaderMutation,
    pseudo: Option<&PseudoHeader>,
) -> Option<EnvoyHeaderMutation> {
    let pseudo = pseudo.into_iter().map(|header| {
        let (name, value) = match header {
            PseudoHeader::Path(path) => (":path", path.as_str().as_bytes()),
            PseudoHeader::Status(status) => (":status", status.as_str().as_bytes()),
        };
        header_option(name, value, HeaderAppendAction::OverwriteIfExistsOrAdd)
    });
    // A repeated name in `set` overwrites once, then appends the remaining values.
    let set = overrides.set.keys().flat_map(|name| {
        overrides
            .set
            .get_all(name)
            .iter()
            .enumerate()
            .map(move |(i, value)| {
                let action = if i == 0 {
                    HeaderAppendAction::OverwriteIfExistsOrAdd
                } else {
                    HeaderAppendAction::AppendIfExistsOrAdd
                };
                header_option(name.as_str(), value.as_bytes(), action)
            })
    });
    let append = overrides.append.iter().map(|(name, value)| {
        header_option(
            name.as_str(),
            value.as_bytes(),
            HeaderAppendAction::AppendIfExistsOrAdd,
        )
    });
    let set_headers: Vec<_> = pseudo.chain(set).chain(append).collect();
    (!set_headers.is_empty() || !overrides.remove.is_empty()).then(|| EnvoyHeaderMutation {
        set_headers,
        remove_headers: overrides.remove.iter().map(ToString::to_string).collect(),
    })
}

#[allow(deprecated)] // ext_proc honours only `append`
fn header_option(name: &str, value: &[u8], action: HeaderAppendAction) -> HeaderValueOption {
    HeaderValueOption {
        header: Some(HeaderValue {
            key: name.to_string(),
            raw_value: value.to_vec(),
            ..Default::default()
        }),
        // ext_proc reads the deprecated `append` (default false), not `append_action`.
        append: Some(envoy_types::pb::google::protobuf::BoolValue {
            value: action == HeaderAppendAction::AppendIfExistsOrAdd,
        }),
        append_action: action as i32,
        ..Default::default()
    }
}

/// Header modes from `modes`; trailers are sent only with a body, which they can end.
fn processing_mode(
    direction: Direction,
    body_mode: BodySendMode,
    modes: ProcessingMode,
) -> EnvoyProcessingMode {
    let trailer_mode = if body_mode == BodySendMode::None {
        HeaderSendMode::Skip
    } else {
        HeaderSendMode::Send
    };
    let mut mode = EnvoyProcessingMode {
        request_header_mode: modes.request_header_mode.into_envoy() as i32,
        request_body_mode: BodySendMode::None as i32,
        request_trailer_mode: HeaderSendMode::Skip as i32,
        response_header_mode: modes.response_header_mode.into_envoy() as i32,
        response_body_mode: BodySendMode::None as i32,
        response_trailer_mode: HeaderSendMode::Skip as i32,
    };
    match direction {
        Direction::Request => {
            mode.request_body_mode = body_mode as i32;
            mode.request_trailer_mode = trailer_mode as i32;
        }
        Direction::Response => {
            mode.response_body_mode = body_mode as i32;
            mode.response_trailer_mode = trailer_mode as i32;
        }
    }
    mode
}

fn body_chunk(body: HttpBody) -> BodyChunk {
    BodyChunk {
        data: Bytes::from(body.body),
        end_of_stream: body.end_of_stream,
    }
}

/// Attributes are grouped by filter name; handlers look them up by attribute name only.
fn from_attributes(attributes: HashMap<String, Struct>) -> EnvoyAttributes {
    EnvoyAttributes(
        attributes
            .into_values()
            .flat_map(|st| st.fields)
            .map(|(key, value)| (key, json::from_value(value)))
            .collect(),
    )
}

/// From the `request.protocol` attribute, which Envoy sends only if listed in `request_attributes` / `response_attributes`.
fn http_version(attributes: &EnvoyAttributes) -> Option<http::Version> {
    match attributes.0.get("request.protocol")?.as_str()? {
        "HTTP/1.0" => Some(http::Version::HTTP_10),
        "HTTP/1.1" => Some(http::Version::HTTP_11),
        "HTTP/2" => Some(http::Version::HTTP_2),
        "HTTP/3" => Some(http::Version::HTTP_3),
        _ => None,
    }
}

fn from_metadata(metadata: Option<Metadata>) -> EnvoyMetadata {
    EnvoyMetadata(
        metadata
            .into_iter()
            .flat_map(|m| m.filter_metadata)
            .map(|(ns, st)| (ns, serde_json::Value::Object(json::from_struct(st))))
            .collect(),
    )
}

fn append_header(
    header_map: &mut http::HeaderMap,
    key: &str,
    value: Vec<u8>,
) -> Result<(), Status> {
    let name = http::HeaderName::from_bytes(key.as_bytes())
        .map_err(|e| Status::invalid_argument(format!("Invalid header name: {e}")))?;
    let value = http::HeaderValue::from_maybe_shared(Bytes::from(value))
        .map_err(|e| Status::invalid_argument(format!("Invalid header value: {e}")))?;
    header_map.append(name, value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use envoy_types::pb::{
        envoy::config::core::v3::{HeaderMap, HeaderValue},
        google::protobuf::{Value, value::Kind},
    };
    use http::Method;

    use super::*;

    #[test]
    fn request_parts_reads_raw_values() {
        let headers = envoy_headers(
            &[
                (":method", "POST"),
                (":scheme", "https"),
                (":authority", "example.com"),
                (":path", "/a/b?c=1"),
                ("x-dup", "1"),
                ("x-dup", "2"),
                ("content-type", "application/json"),
            ],
            true,
        );
        let pointer = headers
            .headers
            .as_ref()
            .unwrap()
            .headers
            .last()
            .unwrap()
            .raw_value
            .as_ptr();
        let req = request_parts(headers).unwrap();
        assert_eq!(req.headers["content-type"].as_bytes().as_ptr(), pointer);
        let expected_headers = http_headers(&[
            ("x-dup", "1"),
            ("x-dup", "2"),
            ("content-type", "application/json"),
        ]);
        assert_eq!(
            (req.method, req.uri, req.headers),
            (
                Method::POST,
                "https://example.com/a/b?c=1".parse().unwrap(),
                expected_headers
            )
        );
    }

    #[test]
    fn request_parts_without_authority_keeps_path() {
        let headers = envoy_headers(&[(":method", "GET"), (":path", "/x?y=1")], true);
        let req = request_parts(headers).unwrap();
        assert_eq!(
            (req.method, req.uri, req.headers),
            (
                Method::GET,
                "/x?y=1".parse().unwrap(),
                http::HeaderMap::new()
            )
        );
    }

    #[test]
    fn request_parts_rejects_missing_or_invalid_method() {
        for pairs in [
            &[(":path", "/")][..],
            &[(":method", "BAD METHOD"), (":path", "/")][..],
        ] {
            assert!(request_parts(envoy_headers(pairs, true)).is_err());
        }
    }

    #[test]
    fn headers_no_body_skips_trailers() {
        for direction in [Direction::Request, Direction::Response] {
            assert_eq!(
                headers_no_body(direction, Mutation::default(), ProcessingMode::default()),
                expected_headers(
                    direction,
                    CommonResponse::default(),
                    expected_mode(direction, BodySendMode::None, HeaderSendMode::Skip)
                )
            );
        }
    }

    #[test]
    fn response_parts_reads_raw_values() {
        let headers = envoy_headers(
            &[
                (":status", "404"),
                ("x-dup", "1"),
                ("x-dup", "2"),
                ("content-type", "text/plain"),
            ],
            true,
        );
        let pointer = headers
            .headers
            .as_ref()
            .unwrap()
            .headers
            .last()
            .unwrap()
            .raw_value
            .as_ptr();
        let resp = response_parts(headers).unwrap();
        assert_eq!(resp.headers["content-type"].as_bytes().as_ptr(), pointer);
        let expected_headers = http_headers(&[
            ("x-dup", "1"),
            ("x-dup", "2"),
            ("content-type", "text/plain"),
        ]);
        assert_eq!(
            (resp.status, resp.headers),
            (StatusCode::NOT_FOUND, expected_headers)
        );
    }

    #[test]
    fn response_parts_rejects_missing_or_invalid_status() {
        for pairs in [
            &[("content-type", "text/plain")][..],
            &[(":status", "abc")][..],
        ] {
            assert!(response_parts(envoy_headers(pairs, true)).is_err());
        }
    }

    #[test]
    fn headers_with_body_sets_body_mode() {
        for direction in [Direction::Request, Direction::Response] {
            for (mode, expected) in [
                (BodyMode::Streamed, BodySendMode::Streamed),
                (BodyMode::Buffered, BodySendMode::Buffered),
            ] {
                assert_eq!(
                    ProcessingResponse::from(Reply::HeadersWithBody {
                        direction,
                        mode,
                        processing_mode: ProcessingMode::default(),
                    }),
                    expected_headers(
                        direction,
                        CommonResponse::default(),
                        expected_mode(direction, expected, HeaderSendMode::Send)
                    )
                );
            }
        }
    }

    #[test]
    fn version_comes_from_the_protocol_attribute() {
        for (protocol, expected) in [
            (None, http::Version::default()),
            (Some("HTTP/2"), http::Version::HTTP_2),
            (Some("HTTP/3"), http::Version::HTTP_3),
            (Some("unknown"), http::Version::default()),
        ] {
            let attributes = protocol
                .map(|protocol| {
                    let fields = [(
                        "request.protocol".to_string(),
                        Value {
                            kind: Some(Kind::StringValue(protocol.into())),
                        },
                    )];
                    let attributes = Struct {
                        fields: fields.into(),
                    };
                    ("envoy.filters.http.ext_proc".to_string(), attributes)
                })
                .into_iter()
                .collect();
            let message = ProcessingRequest {
                request: Some(processing_request::Request::RequestHeaders(envoy_headers(
                    &[(":method", "GET"), (":path", "/")],
                    true,
                ))),
                attributes,
                ..Default::default()
            };
            let Ok(Message::Headers(
                _,
                Headers {
                    head: Head::Request(parts),
                    ..
                },
            )) = Message::try_from(message)
            else {
                panic!("expected request headers")
            };
            assert_eq!(parts.version, expected, "{protocol:?}");
        }
    }

    #[test]
    fn empty_request_is_rejected() {
        let status = Message::try_from(ProcessingRequest::default()).unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn replacing_body_preserves_mutations_and_metadata() {
        for headers in [false, true] {
            let metadata = serde_json::Map::from_iter([("test".into(), "kept".into())]);
            let mut mutation = Mutation {
                dynamic_metadata: metadata.clone(),
                clear_route_cache: true,
                ..Default::default()
            };
            mutation
                .overrides
                .set
                .insert(http::header::CONTENT_LENGTH, "999".parse().unwrap());
            mutation
                .overrides
                .set
                .insert("x-test", "kept".parse().unwrap());
            let body = Bytes::from(b"new".to_vec());
            let body_pointer = body.as_ptr();
            let reply = if headers {
                Reply::HeadersReplaceBody {
                    direction: Direction::Request,
                    mutation,
                    body,
                    processing_mode: ProcessingMode::default(),
                }
            } else {
                Reply::LastBody {
                    direction: Direction::Request,
                    mutation,
                    body_override: Some(body),
                }
            };
            let mut response = ProcessingResponse::from(reply);
            let expected_mutation = EnvoyHeaderMutation {
                set_headers: [("content-length", "3"), ("x-test", "kept")]
                    .into_iter()
                    .map(|(name, value)| {
                        #[allow(deprecated)]
                        HeaderValueOption {
                            header: Some(HeaderValue {
                                key: name.into(),
                                raw_value: value.as_bytes().to_vec(),
                                ..Default::default()
                            }),
                            append: Some(envoy_types::pb::google::protobuf::BoolValue {
                                value: false,
                            }),
                            append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
                            ..Default::default()
                        }
                    })
                    .collect(),
                ..Default::default()
            };
            let common = CommonResponse {
                status: if headers {
                    ResponseStatus::ContinueAndReplace
                } else {
                    ResponseStatus::Continue
                } as i32,
                header_mutation: Some(expected_mutation),
                body_mutation: Some(BodyMutation {
                    mutation: Some(body_mutation::Mutation::Body(b"new".to_vec())),
                }),
                clear_route_cache: true,
                ..Default::default()
            };
            let mut expected = if headers {
                expected_headers(
                    Direction::Request,
                    common,
                    expected_mode(Direction::Request, BodySendMode::None, HeaderSendMode::Skip),
                )
            } else {
                ProcessingResponse {
                    response: Some(processing_response::Response::RequestBody(BodyResponse {
                        response: Some(common),
                    })),
                    ..Default::default()
                }
            };
            expected.dynamic_metadata = Some(Struct {
                fields: [(
                    "test".into(),
                    Value {
                        kind: Some(Kind::StringValue("kept".into())),
                    },
                )]
                .into(),
            });
            // Sort different header names while retaining the order of duplicate values.
            let common = match response.response.as_mut().unwrap() {
                processing_response::Response::RequestHeaders(h) => h.response.as_mut().unwrap(),
                processing_response::Response::RequestBody(b) => b.response.as_mut().unwrap(),
                other => panic!("unexpected response: {other:?}"),
            };
            let Some(body_mutation::Mutation::Body(body)) =
                common.body_mutation.as_ref().unwrap().mutation.as_ref()
            else {
                panic!("missing replacement body")
            };
            assert_eq!(body.as_ptr(), body_pointer);
            common
                .header_mutation
                .as_mut()
                .unwrap()
                .set_headers
                .sort_by(|a, b| {
                    a.header
                        .as_ref()
                        .unwrap()
                        .key
                        .cmp(&b.header.as_ref().unwrap().key)
                });
            assert_eq!(response, expected);
        }
    }

    // helpers

    fn envoy_headers(pairs: &[(&str, &str)], end_of_stream: bool) -> HttpHeaders {
        HttpHeaders {
            headers: Some(HeaderMap {
                headers: pairs
                    .iter()
                    .map(|(k, v)| HeaderValue {
                        key: (*k).to_string(),
                        raw_value: v.as_bytes().to_vec(),
                        ..Default::default()
                    })
                    .collect(),
            }),
            end_of_stream,
            ..Default::default()
        }
    }

    fn http_headers(pairs: &[(&str, &str)]) -> http::HeaderMap {
        pairs
            .iter()
            .map(|(name, value)| {
                (
                    name.parse::<http::HeaderName>().unwrap(),
                    value.parse().unwrap(),
                )
            })
            .collect()
    }

    fn expected_mode(
        direction: Direction,
        body: BodySendMode,
        trailers: HeaderSendMode,
    ) -> EnvoyProcessingMode {
        let mut mode = EnvoyProcessingMode {
            request_header_mode: HeaderSendMode::Send as i32,
            response_header_mode: HeaderSendMode::Send as i32,
            request_body_mode: BodySendMode::None as i32,
            response_body_mode: BodySendMode::None as i32,
            request_trailer_mode: HeaderSendMode::Skip as i32,
            response_trailer_mode: HeaderSendMode::Skip as i32,
        };
        match direction {
            Direction::Request => {
                mode.request_body_mode = body as i32;
                mode.request_trailer_mode = trailers as i32;
            }
            Direction::Response => {
                mode.response_body_mode = body as i32;
                mode.response_trailer_mode = trailers as i32;
            }
        }
        mode
    }

    fn expected_headers(
        direction: Direction,
        common: CommonResponse,
        mode: EnvoyProcessingMode,
    ) -> ProcessingResponse {
        let headers = HeadersResponse {
            response: Some(common),
        };
        ProcessingResponse {
            response: Some(match direction {
                Direction::Request => processing_response::Response::RequestHeaders(headers),
                Direction::Response => processing_response::Response::ResponseHeaders(headers),
            }),
            mode_override: Some(mode),
            ..Default::default()
        }
    }
}
