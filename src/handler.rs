//! Handler trait and actions.

use super::body::LazyBody;

/// Header changes, mapped onto Envoy's `HeaderMutation`.
#[derive(Debug, Default, Clone)]
pub struct HeaderMutation {
    /// Replaces existing values, or adds the header.
    pub set: http::HeaderMap,
    /// Adds values alongside existing ones.
    pub append: http::HeaderMap,
    /// Removes these headers.
    pub remove: Vec<http::HeaderName>,
}

/// Reply sent to the client instead of continuing.
#[derive(Debug)]
pub struct ImmediateResponse {
    pub status: http::StatusCode,
    pub header_mutation: HeaderMutation,
    /// Empty for none.
    pub body: bytes::Bytes,
}

/// What a handler returns: reply to the client, or continue with changes `C`.
// Clippy measures `C` as zero-sized here; every real `C` is about as large as `ImmediateResponse`.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum HandlerAction<C> {
    ImmediateResponse(ImmediateResponse),
    Continue(C),
}

/// Changes for a continued request; `RequestContinue::default()` changes nothing.
#[derive(Debug, Default)]
pub struct RequestContinue {
    pub header_mutation: HeaderMutation,
    /// Replaces `:path`; Envoy then re-picks the route.
    pub path_override: Option<http::uri::PathAndQuery>,
    /// Makes Envoy re-pick the route, e.g. after changing a header it matches on.
    pub clear_route_cache: bool,
    /// Emitted as Envoy dynamic metadata; keys are namespaces, allowed by `receiving_namespaces`.
    pub dynamic_metadata: serde_json::Map<String, serde_json::Value>,
    /// Replacement request body; not after `stream()`. Content-Length is set to match.
    pub body_override: Option<bytes::Bytes>,
}

/// Changes for a continued response; `ResponseContinue::default()` changes nothing.
#[derive(Debug, Default)]
pub struct ResponseContinue {
    pub header_mutation: HeaderMutation,
    /// Replaces the upstream's status code.
    pub status_override: Option<http::StatusCode>,
    /// Emitted as Envoy dynamic metadata; keys are namespaces, allowed by `receiving_namespaces`.
    pub dynamic_metadata: serde_json::Map<String, serde_json::Value>,
    /// Replacement response body; not after `stream()`. Content-Length is set to match.
    pub body_override: Option<bytes::Bytes>,
}

/// Envoy attributes listed in `request_attributes` / `response_attributes`, by attribute name.
/// Found in `req.extensions()` / `resp.extensions()`.
#[derive(Debug, Clone, Default)]
pub struct EnvoyAttributes(pub serde_json::Map<String, serde_json::Value>);

/// Dynamic metadata Envoy forwards (`metadata_options.forwarding_namespaces`), by namespace.
/// Found in `req.extensions()` / `resp.extensions()`.
#[derive(Debug, Clone, Default)]
pub struct EnvoyMetadata(pub serde_json::Map<String, serde_json::Value>);

/// Called once for the request and once for the response; both calls share one `context`.
/// Both methods default to pass-through without reading the body; implement only the methods you need.
#[async_trait::async_trait]
pub trait Handler: Send + Sync + 'static {
    /// Handle a request; values put in `context` reach the response handler.
    /// The default continues unchanged without reading the body.
    async fn handle_request(
        &self,
        _req: &http::Request<LazyBody>,
        _context: &mut http::Extensions,
    ) -> anyhow::Result<HandlerAction<RequestContinue>> {
        Ok(HandlerAction::Continue(RequestContinue::default()))
    }

    /// Handle a response with read-only access to the request context.
    /// The default continues unchanged without reading the body.
    async fn handle_response(
        &self,
        _resp: &http::Response<LazyBody>,
        _context: &http::Extensions,
    ) -> anyhow::Result<HandlerAction<ResponseContinue>> {
        Ok(HandlerAction::Continue(ResponseContinue::default()))
    }
}

// helpers

impl HeaderMutation {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.set.is_empty() && self.append.is_empty() && self.remove.is_empty()
    }
}

impl ImmediateResponse {
    /// Reply with `status`, no header changes and no body.
    #[must_use]
    pub fn new(status: http::StatusCode) -> Self {
        Self {
            status,
            header_mutation: HeaderMutation::default(),
            body: bytes::Bytes::new(),
        }
    }
}

impl From<http::HeaderMap> for HeaderMutation {
    fn from(set: http::HeaderMap) -> Self {
        Self {
            set,
            ..Self::default()
        }
    }
}
