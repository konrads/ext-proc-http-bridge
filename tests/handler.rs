use bytes::Bytes;
use ext_proc_http_bridge::{
    Handler, LazyBody,
    handler::{HandlerAction, RequestContinue},
};

struct Echo;

#[tokio::test]
async fn handler_can_read_a_body_without_envoy() -> anyhow::Result<()> {
    let request = http::Request::new(LazyBody::from_bytes("payload"));
    let mut context = http::Extensions::new();
    let HandlerAction::Continue(action) = Echo.handle_request(&request, &mut context).await? else {
        panic!("expected continuation")
    };
    assert_eq!(action.body_override, Some(Bytes::from_static(b"payload")));
    Ok(())
}

#[tokio::test]
async fn default_handlers_pass_through_without_reading_bodies() -> anyhow::Result<()> {
    let handler = PassThrough;
    let request = http::Request::new(LazyBody::from_bytes("request"));
    let response = http::Response::new(LazyBody::from_bytes("response"));
    let mut context = http::Extensions::new();
    context.insert(42_u32);

    let HandlerAction::Continue(action) = handler.handle_request(&request, &mut context).await?
    else {
        panic!("expected continuation")
    };
    assert!(action.header_mutation.is_empty());
    assert!(action.path_override.is_none());
    assert!(!action.clear_route_cache);
    assert!(action.dynamic_metadata.is_empty());
    assert!(action.body_override.is_none());

    let HandlerAction::Continue(action) = handler.handle_response(&response, &context).await?
    else {
        panic!("expected continuation")
    };
    assert!(action.header_mutation.is_empty());
    assert!(action.status_override.is_none());
    assert!(action.dynamic_metadata.is_empty());
    assert!(action.body_override.is_none());
    assert_eq!(context.len(), 1);
    assert_eq!(context.get::<u32>(), Some(&42));
    assert!(request.body().stream().is_ok());
    assert!(response.body().stream().is_ok());
    Ok(())
}

// helpers

struct PassThrough;

impl Handler for PassThrough {}

#[async_trait::async_trait]
impl Handler for Echo {
    async fn handle_request(
        &self,
        req: &http::Request<LazyBody>,
        _context: &mut http::Extensions,
    ) -> anyhow::Result<HandlerAction<RequestContinue>> {
        Ok(HandlerAction::Continue(RequestContinue {
            body_override: Some(req.body().collect().await?),
            ..Default::default()
        }))
    }
}
