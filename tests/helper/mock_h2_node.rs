//! h2c upstream: echoes request trailers as `echo-trailer-*` headers, then sends a body and trailers.

use std::convert::Infallible;

use bytes::Bytes;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};

pub async fn serve(listener: tokio::net::TcpListener) -> anyhow::Result<()> {
    loop {
        let (tcp, _) = listener.accept().await?;
        tokio::spawn(async move {
            let service = service_fn(|req: http::Request<Incoming>| async {
                if req.uri().path() == "/h2/early" {
                    // Answers before reading the request body.
                    let frames = futures::stream::iter([
                        Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"early"))),
                        Ok(Frame::trailers(http::HeaderMap::new())),
                    ]);
                    return Ok::<_, hyper::Error>(http::Response::new(StreamBody::new(frames)));
                }
                let request_trailers = req.into_body().collect().await?.trailers().cloned();
                let mut trailers = http::HeaderMap::new();
                trailers.insert("x-trailer", http::HeaderValue::from_static("1"));
                trailers.insert("x-remove-trailer", http::HeaderValue::from_static("1"));
                let frames = futures::stream::iter([
                    Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"abc"))),
                    Ok(Frame::trailers(trailers)),
                ]);
                let mut resp = http::Response::new(StreamBody::new(frames));
                for (name, value) in request_trailers.iter().flatten() {
                    let echo =
                        http::HeaderName::from_bytes(format!("echo-trailer-{name}").as_bytes())
                            .expect("valid name");
                    resp.headers_mut().append(echo, value.clone());
                }
                Ok::<_, hyper::Error>(resp)
            });
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(tcp), service)
                .await
                .ok();
        });
    }
}
