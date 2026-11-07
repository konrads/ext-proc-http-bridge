#![allow(dead_code)]

use std::{convert::Infallible, io, net::TcpListener, time::Duration};

use actix_web::{App, HttpResponse, HttpServer, Responder, dev::Server, web};
use futures::StreamExt;

use super::Payload;

pub async fn start_node(listener: TcpListener) -> io::Result<Server> {
    Ok(
        HttpServer::new(|| App::new().default_service(web::to(handle_req)))
            .listen(listener)?
            .run(),
    )
}

// helpers

/// Echoes a non-empty request body; `/missing` gets 404, `/resp-stream` streams 128 KiB, `/mutate` echoes headers, `/no-body/*` has an empty body; otherwise a fixed payload.
#[allow(clippy::future_not_send)] // actix handlers run on a single-threaded runtime
async fn handle_req(req: actix_web::HttpRequest, body: web::Bytes) -> impl Responder {
    if req.path().ends_with("/missing") {
        HttpResponse::NotFound().finish()
    } else if req.path().ends_with("/resp-stream") {
        // 8 x 16 KiB, sent with delays so Envoy forwards several chunks.
        let chunks = futures::stream::iter(0..8).then(|_| async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<_, Infallible>(web::Bytes::from(vec![b'x'; 16 * 1024]))
        });
        HttpResponse::Ok().streaming(Box::pin(chunks))
    } else if req.path().contains("/no-body/") {
        HttpResponse::Ok().finish()
    } else if req.path().ends_with("/empty-header") {
        let value = req
            .headers()
            .get("x-client")
            .map_or("<absent>", |v| v.to_str().unwrap_or("<invalid>"))
            .to_string();
        HttpResponse::Ok()
            .insert_header(("echo-x-client", format!("[{value}]")))
            .finish()
    } else if req.path().ends_with("/mutate") {
        // Echoes the request headers the mutation test changes.
        let mut resp = HttpResponse::Ok();
        resp.insert_header(("x-upstream", "1"));
        for name in ["x-client", "x-multi", "x-remove-me"] {
            let values: Vec<_> = req
                .headers()
                .get_all(name)
                .filter_map(|v| v.to_str().ok())
                .collect();
            if !values.is_empty() {
                resp.insert_header((format!("echo-{name}"), values.join(",")));
            }
        }
        resp.content_type("application/json").body("{}")
    } else if body.is_empty() {
        HttpResponse::Ok().json(Payload {
            method: "resp".to_string(),
            value: 0,
        })
    } else {
        HttpResponse::Ok().body(body)
    }
}
