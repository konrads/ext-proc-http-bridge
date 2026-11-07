use std::{
    convert::Infallible,
    env, fs, io, mem,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer;
use ext_proc_http_bridge::{
    ExtProc, ExtProcConfig,
    ext_proc::{HeaderMode, ProcessingMode},
};
use futures::{Stream, StreamExt};
use http::{HeaderMap, HeaderName, HeaderValue};
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::sync::oneshot;
use tonic::transport::{Server, server::TcpIncoming};
use tracing_subscriber::EnvFilter;

use self::helper::mock_handler::{MockHandler, RESP_STREAM_STATS, RespStreamStats};

mod helper;

#[derive(Debug, PartialEq, Eq)]
struct Response {
    status: u16,
    headers: HeaderMap,
    body: Bytes,
    trailers: Option<HeaderMap>,
}

/// Kills Envoy even when an assertion panics, so no process outlives the test.
struct KillOnDrop(Child);

/// Removes the generated Envoy config when the test ends.
struct RemoveOnDrop(PathBuf);

#[tokio::test]
async fn end_to_end() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("warn,ext_proc_http_bridge=debug")),
        )
        .with_test_writer()
        .try_init()
        .ok();

    // Our servers keep their listeners; only Envoy's port can be taken in between.
    let node_listener = TcpListener::bind("127.0.0.1:0")?;
    let h2_node_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let ext_proc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let skip_resp_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let [
        envoy_port,
        node_port,
        h2_node_port,
        ext_proc_port,
        skip_resp_port,
    ] = [
        free_port()?,
        node_listener.local_addr()?.port(),
        h2_node_listener.local_addr()?.port(),
        ext_proc_listener.local_addr()?.port(),
        skip_resp_listener.local_addr()?.port(),
    ];
    let config = fs::read_to_string("tests/envoy.yaml")?
        .replace("port_value: 10000", &format!("port_value: {envoy_port}"))
        .replace("port_value: 8080", &format!("port_value: {node_port}"))
        .replace("port_value: 8081", &format!("port_value: {h2_node_port}"))
        .replace("port_value: 50051", &format!("port_value: {ext_proc_port}"))
        .replace(
            "port_value: 50052",
            &format!("port_value: {skip_resp_port}"),
        );
    let config_path = env::temp_dir().join(format!("envoy-{envoy_port}.yaml"));
    fs::write(&config_path, config)?;
    let config_path = RemoveOnDrop(config_path);

    let mock_node = helper::mock_node::start_node(node_listener).await?;
    let mock_h2_node = tokio::spawn(helper::mock_h2_node::serve(h2_node_listener));

    let ext_proc = ExtProc::new(
        Arc::new(MockHandler),
        ExtProcConfig {
            override_message_timeout: Some(Duration::from_millis(600)),
            ..ExtProcConfig::default()
        },
    );
    let ext_proc_server = ExternalProcessorServer::new(ext_proc);
    let ext_proc_task = tokio::spawn(async move {
        Server::builder()
            .add_service(ext_proc_server)
            .serve_with_incoming(TcpIncoming::from(ext_proc_listener))
            .await
    });
    // Serves the route whose processing_mode skips response headers.
    let skip_resp = ExternalProcessorServer::new(ExtProc::new(
        Arc::new(MockHandler),
        ExtProcConfig {
            processing_mode: ProcessingMode {
                response_header_mode: HeaderMode::Skip,
                ..ProcessingMode::default()
            },
            ..ExtProcConfig::default()
        },
    ));
    let skip_resp_task = tokio::spawn(async move {
        Server::builder()
            .add_service(skip_resp)
            .serve_with_incoming(TcpIncoming::from(skip_resp_listener))
            .await
    });

    let _envoy = KillOnDrop(
        Command::new("envoy")
            .arg("-c")
            .arg(&config_path.0)
            .arg("--disable-hot-restart")
            .spawn()?,
    );

    let test = async {
        wait_for_port(envoy_port).await?;
        let base = format!("http://localhost:{envoy_port}");

        let client = reqwest::Client::new();

        for (path, expected) in [
            // ImmediateResponse: 401 returned directly by ext-proc, no upstream call
            (
                "/666/rest",
                Response::new(
                    401,
                    &[
                        ("invalid-token", "666"),
                        ("content-length", "13"),
                        ("content-type", "text/plain"),
                    ],
                    "invalid token",
                ),
            ),
            // Continue (no body read): request forwarded, response handler adds headers via ctx
            (
                "/123/rest",
                Response::json(200).with_headers(&[
                    ("seen-status", "200"),
                    ("seen-content-type", "application/json"),
                    ("body-length", "27"),
                    ("http-method", "GET"),
                ]),
            ),
            // Response body replaced after collect()
            (
                "/123/rest/resp-replace",
                Response::new(
                    200,
                    &[
                        ("content-type", "application/json"),
                        ("content-length", "13"),
                    ],
                    "resp-replaced",
                ),
            ),
            // Response handler exits early with its own status
            (
                "/123/rest/resp-exit",
                Response::new(418, &[("x-resp-exit", "1"), ("content-length", "0")], ""),
            ),
            // Response handler slower than message_timeout succeeds thanks to override_message_timeout
            ("/123/rest/slow-resp", Response::json(200)),
            // Response handler sees the upstream's real status
            (
                "/123/rest/missing",
                Response::new(
                    404,
                    &[
                        ("content-length", "0"),
                        ("seen-status", "404"),
                        ("http-method", "GET"),
                    ],
                    "",
                ),
            ),
            // Response handler requests the stream and returns at once; the body passes through
            ("/123/rest/unread", Response::json(200)),
            // Attributes reach both handlers; metadata set in the request phase comes back in the response phase
            (
                "/123/rest/meta",
                Response::json(200).with_headers(&[
                    ("seen-protocol", "HTTP/1.1"),
                    ("seen-response-code", "200.0"),
                    ("seen-user", "alice"),
                ]),
            ),
            // Path rewritten; Envoy re-picks the route, so the h2 upstream answers
            (
                "/123/rest/reroute",
                Response::new(
                    200,
                    &[
                        ("seen-status", "200"),
                        ("body-length", "3"),
                        ("transfer-encoding", "chunked"),
                    ],
                    "abc",
                ),
            ),
            // Header the route matches on: re-routed only with clear_route_cache
            (
                "/123/rest/header-route",
                Response::new(
                    200,
                    &[
                        ("seen-status", "200"),
                        ("body-length", "3"),
                        ("transfer-encoding", "chunked"),
                    ],
                    "abc",
                ),
            ),
            (
                "/123/rest/header-route-no-clear",
                Response::json(200).with_headers(&[
                    ("seen-status", "200"),
                    ("seen-content-type", "application/json"),
                    ("body-length", "27"),
                ]),
            ),
            // Upstream status replaced by the response handler
            ("/123/rest/status", Response::json(203)),
            (
                "/123/rest/resp-replace-unread",
                Response::new(
                    200,
                    &[
                        ("content-type", "application/json"),
                        ("transfer-encoding", "chunked"),
                    ],
                    "resp-replaced-unread",
                ),
            ),
            // Bodyless request and response, read by both handlers with collect() or stream()
            (
                "/123/rest/no-body/collect",
                Response::new(
                    200,
                    &[
                        ("content-length", "0"),
                        ("req-body-length", "0"),
                        ("resp-body-length", "0"),
                    ],
                    "",
                ),
            ),
            (
                "/123/rest/no-body/stream",
                Response::new(
                    200,
                    &[
                        ("content-length", "0"),
                        ("req-body-length", "0"),
                        ("resp-body-length", "0"),
                    ],
                    "",
                ),
            ),
        ] {
            let response = client.get(format!("{base}{path}")).send().await?;
            assert_eq!(Response::from_reqwest(response).await?, expected, "{path}");
        }

        // Upstream answers before the streamed request body is complete
        let chunks = (0..10)
            .map(|_| Frame::data(Bytes::from_static(b"abc")))
            .collect();
        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            h2_post(envoy_port, "/h2/early", chunks),
        )
        .await??;
        assert_eq!(
            Response::from_http(resp).await?,
            Response::new(
                200,
                &[("seen-status", "200"), ("body-length", "5")],
                "early"
            )
        );

        // Request and response headers set, appended and removed
        let resp = client
            .get(format!("{base}/123/rest/mutate"))
            .header("x-client", "orig")
            .header("x-multi", "orig")
            .header("x-remove-me", "1")
            .send()
            .await?;
        assert_eq!(
            Response::from_reqwest(resp).await?,
            Response::new(
                200,
                &[
                    ("content-length", "2"),
                    ("echo-x-client", "new"),
                    ("echo-x-multi", "orig,a,b"),
                    ("content-type", "text/x-overridden"),
                    ("set-cookie", "a=1"),
                    ("set-cookie", "b=2")
                ],
                "{}"
            )
        );

        // Streamed response body: the handler sees all of it in several chunks, and so does the client
        let resp = h2_post(envoy_port, "/123/resp-stream", vec![]).await?;
        assert_eq!(
            Response::from_http(resp).await?,
            Response::new(
                200,
                &[("content-type", "application/octet-stream")],
                vec![b'x'; 8 * 16 * 1024]
            )
        );

        let stats = RESP_STREAM_STATS
            .lock()
            .unwrap()
            .take()
            .expect("handler ran");
        assert!(
            stats.chunks > 1,
            "expected several chunks, got {}",
            stats.chunks
        );
        assert_eq!(
            stats,
            RespStreamStats {
                bytes: 8 * 16 * 1024,
                chunks: stats.chunks,
            }
        );

        // Streamed response body followed by trailers must not hang
        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            client.get(format!("{base}/h2/trailers")).send(),
        )
        .await??;
        assert_eq!(
            Response::from_reqwest(resp).await?,
            Response::new(200, &[("transfer-encoding", "chunked")], "abc")
        );

        let stats = RESP_STREAM_STATS
            .lock()
            .unwrap()
            .take()
            .expect("handler ran");
        assert!(stats.chunks > 0);
        assert_eq!(
            stats,
            RespStreamStats {
                bytes: 3,
                chunks: stats.chunks,
            }
        );

        // Request trailers pass through unchanged, whether the body is streamed, collected or not read
        for mode in ["stream", "collect", "none"] {
            let mut trailers = http::HeaderMap::new();
            trailers.insert("x-trailer", "1".parse()?);
            trailers.insert("x-remove-trailer", "1".parse()?);
            let frames = vec![
                Frame::data(Bytes::from_static(b"abc")),
                Frame::trailers(trailers),
            ];
            let resp = tokio::time::timeout(
                Duration::from_secs(5),
                h2_post(envoy_port, &format!("/h2/req-trailers/{mode}"), frames),
            )
            .await??;
            assert_eq!(
                Response::from_http(resp).await?,
                Response::new(
                    200,
                    &[
                        ("echo-trailer-x-trailer", "1"),
                        ("echo-trailer-x-remove-trailer", "1"),
                        ("seen-status", "200"),
                        ("body-length", "3")
                    ],
                    "abc"
                )
                .with_trailers(&[("x-trailer", "1"), ("x-remove-trailer", "1")]),
                "mode {mode}"
            );
        }

        // A collected body that is only trailers cannot take header changes: Envoy sends no body message
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-only", "1".parse()?);
        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            h2_post(
                envoy_port,
                "/h2/trailers-only-collect",
                vec![Frame::trailers(trailers)],
            ),
        )
        .await??;
        let resp = Response::from_http(resp).await?;
        assert_eq!(resp.status, 500, "{resp:?}");

        // Response trailers pass through unchanged, whether the body is streamed, collected or not read
        for mode in ["stream", "collect", "none"] {
            let resp = tokio::time::timeout(
                Duration::from_secs(5),
                h2_post(envoy_port, &format!("/h2/resp-trailers/{mode}"), vec![]),
            )
            .await??;
            assert_eq!(
                Response::from_http(resp).await?,
                Response::new(200, &[], "abc")
                    .with_trailers(&[("x-trailer", "1"), ("x-remove-trailer", "1")]),
                "mode {mode}"
            );
        }

        // processing_mode skips request headers: only the response handler runs
        let resp = client.get(format!("{base}/skip-req/rest/x")).send().await?;
        let resp = Response::from_reqwest(resp).await?;
        assert_eq!(
            resp.headers.get("seen-status").map(|v| v.as_bytes()),
            Some(&b"200"[..])
        );
        assert!(!resp.headers.contains_key("http-method"), "{resp:?}");

        // processing_mode skips response headers, mirrored in ExtProcConfig: the handler does not see the response
        let resp = client
            .get(format!("{base}/skip-resp/rest/x"))
            .send()
            .await?;
        let resp = Response::from_reqwest(resp).await?;
        assert_eq!(resp.status, 200);
        assert!(!resp.headers.contains_key("seen-status"), "{resp:?}");

        // ImmediateResponse on POST
        let payload = serde_json::json!({"method": "m1", "value": 999u16});
        let resp = client
            .post(format!("{base}/666/rpc"))
            .json(&payload)
            .send()
            .await?;
        assert_eq!(
            Response::from_reqwest(resp).await?,
            Response::new(
                401,
                &[("invalid-token", "666"), ("content-length", "0")],
                ""
            )
        );

        // Continue with request body read: handler parses body and passes ctx to response handler
        let resp = client
            .post(format!("{base}/123/rpc"))
            .json(&payload)
            .send()
            .await?;
        assert_eq!(
            Response::from_reqwest(resp).await?,
            Response::new(
                200,
                &[
                    ("content-length", "27"),
                    ("seen-status", "200"),
                    ("rpc-value", "999"),
                    ("rpc-method", "m1"),
                    ("body-length", "27")
                ],
                r#"{"method":"m1","value":999}"#
            )
        );

        // Body replaced after collect(); upstream echoes what it received
        let resp = client
            .post(format!("{base}/123/replace"))
            .body("original")
            .send()
            .await?;
        assert_eq!(
            Response::from_reqwest(resp).await?,
            Response::echo("replaced-body")
        );

        // Body replaced on a chunked HTTP/1.1 upload
        let chunked = futures::stream::iter([
            Ok::<_, Infallible>(Bytes::from_static(b"orig")),
            Ok(Bytes::from_static(b"inal")),
        ]);
        let resp = client
            .post(format!("{base}/123/replace"))
            .body(reqwest::Body::wrap_stream(chunked))
            .send()
            .await?;
        assert_eq!(
            Response::from_reqwest(resp).await?,
            Response::echo("replaced-body")
        );

        // Handler requests the stream and returns at once; the body passes through
        let chunks = (0..4)
            .map(|_| Frame::data(Bytes::from_static(b"abc")))
            .collect();
        let resp = h2_post(envoy_port, "/123/stream-unread", chunks).await?;
        assert_eq!(
            Response::from_http(resp).await?,
            Response::echo("abcabcabcabc")
        );

        // Overrides after stream() fail as soon as the handler returns, not after the whole body
        let (release, blocked) = oneshot::channel::<()>();
        let chunks =
            futures::stream::iter([Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"abc")))])
                .chain(futures::stream::once(async move {
                    let _ = blocked.await;
                    Ok(Frame::data(Bytes::from_static(b"remaining")))
                }));
        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            h2_post_stream(envoy_port, "/123/stream-override", chunks),
        )
        .await??;
        assert_eq!(
            Response::from_http(resp).await?,
            Response::new(500, &[], "")
        );

        drop(release);

        // Handler slower than override_message_timeout: Envoy's timer fires
        let resp = client.post(format!("{base}/123/too-slow")).send().await?;
        assert_eq!(
            Response::from_reqwest(resp).await?,
            Response::new(504, &[("content-length", "0"), ("server", "envoy")], "")
        );

        // Handler slower than message_timeout succeeds thanks to override_message_timeout
        let resp = client.post(format!("{base}/123/slow")).send().await?;
        assert_eq!(
            Response::from_reqwest(resp).await?,
            Response::json(200).with_headers(&[
                ("seen-status", "200"),
                ("seen-content-type", "application/json"),
                ("body-length", "27")
            ])
        );

        // Bodies replaced without reading them
        let resp = client
            .post(format!("{base}/123/replace-unread"))
            .body("original")
            .send()
            .await?;
        assert_eq!(
            Response::from_reqwest(resp).await?,
            Response::echo("replaced-unread")
        );

        // Header set to an empty value is kept, not dropped
        let resp = client
            .get(format!("{base}/123/rest/empty-header"))
            .header("x-client", "orig")
            .send()
            .await?;
        assert_eq!(
            Response::from_reqwest(resp).await?,
            Response::echo("").with_headers(&[("echo-x-client", "[]")])
        );

        // Streamed request body arrives in several chunks
        let chunks = (0..8)
            .map(|_| Frame::data(Bytes::from(vec![b'x'; 16 * 1024])))
            .collect();
        let resp = h2_post(envoy_port, "/123/stream", chunks).await?;
        assert_eq!(
            Response::from_http(resp).await?.normalize_chunks(2)?,
            Response::echo(vec![b'x'; 8 * 16 * 1024])
                .with_headers(&[("stream-bytes", "131072"), ("stream-chunks", "<chunks>")])
        );

        // Streamed request body followed by trailers must not hang
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-trailer", "1".parse()?);
        let frames = vec![
            Frame::data(Bytes::from_static(b"abc")),
            Frame::trailers(trailers),
        ];
        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            h2_post(envoy_port, "/123/stream", frames),
        )
        .await??;
        assert_eq!(
            Response::from_http(resp).await?.normalize_chunks(1)?,
            Response::echo("abc")
                .with_headers(&[("stream-chunks", "<chunks>"), ("stream-bytes", "3")])
        );

        anyhow::Ok(())
    };

    let result = tokio::select! {
        _ = mock_node => Err(anyhow::anyhow!("mock-node exited unexpectedly")),
        _ = mock_h2_node => Err(anyhow::anyhow!("mock-h2-node exited unexpectedly")),
        _ = ext_proc_task => Err(anyhow::anyhow!("ext-proc exited unexpectedly")),
        _ = skip_resp_task => Err(anyhow::anyhow!("skip-resp ext-proc exited unexpectedly")),
        // Fails a hung request instead of blocking the run.
        res = tokio::time::timeout(Duration::from_secs(60), test) => {
            res.map_err(|_| anyhow::anyhow!("e2e test timed out"))?
        }
    };

    result
}

/// POSTs over h2c, sending each frame after a short delay.
async fn h2_post(
    port: u16,
    path: &str,
    frames: Vec<Frame<Bytes>>,
) -> anyhow::Result<http::Response<Incoming>> {
    let frames = futures::stream::iter(frames).then(|frame| async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok::<_, Infallible>(frame)
    });
    h2_post_stream(port, path, frames).await
}

/// POSTs over h2c using the supplied body stream.
async fn h2_post_stream(
    port: u16,
    path: &str,
    frames: impl Stream<Item = Result<Frame<Bytes>, Infallible>> + Send + 'static,
) -> anyhow::Result<http::Response<Incoming>> {
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tcp)).await?;
    tokio::spawn(conn);
    let req = http::Request::post(format!("http://localhost:{port}{path}"))
        .body(StreamBody::new(Box::pin(frames)))?;
    Ok(sender.send_request(req).await?)
}

// helpers

impl Response {
    fn new(status: u16, headers: &[(&'static str, &str)], body: impl Into<Bytes>) -> Self {
        let mut headers = header_map(headers);
        headers.insert("server", HeaderValue::from_static("envoy"));
        Self {
            status,
            headers,
            body: body.into(),
            trailers: None,
        }
    }

    fn json(status: u16) -> Self {
        Self::new(
            status,
            &[
                ("content-length", "27"),
                ("content-type", "application/json"),
            ],
            r#"{"method":"resp","value":0}"#,
        )
    }

    fn echo(body: impl Into<Bytes>) -> Self {
        let body = body.into();
        let len = body.len().to_string();
        Self::new(
            200,
            &[
                ("content-length", &len),
                ("seen-status", "200"),
                ("body-length", &len),
            ],
            body,
        )
    }

    fn with_headers(mut self, headers: &[(&'static str, &str)]) -> Self {
        self.headers.extend(header_map(headers));
        self
    }

    fn with_trailers(mut self, trailers: &[(&'static str, &str)]) -> Self {
        self.trailers = Some(header_map(trailers));
        self
    }

    fn normalize_chunks(mut self, minimum: usize) -> anyhow::Result<Self> {
        let chunks: usize = self.headers["stream-chunks"].to_str()?.parse()?;
        assert!(
            chunks >= minimum,
            "expected at least {minimum} chunks, got {chunks}"
        );
        self.headers
            .insert("stream-chunks", HeaderValue::from_static("<chunks>"));
        Ok(self)
    }

    async fn from_reqwest(mut response: reqwest::Response) -> anyhow::Result<Self> {
        let status = response.status().as_u16();
        let headers = normalize_headers(mem::take(response.headers_mut()));
        Ok(Self {
            status,
            headers,
            body: response.bytes().await?,
            trailers: None,
        })
    }

    async fn from_http(response: http::Response<Incoming>) -> anyhow::Result<Self> {
        let (parts, body) = response.into_parts();
        let collected = body.collect().await?;
        let trailers = collected.trailers().cloned();
        Ok(Self {
            status: parts.status.as_u16(),
            headers: normalize_headers(parts.headers),
            body: collected.to_bytes(),
            trailers,
        })
    }
}

fn header_map(headers: &[(&'static str, &str)]) -> HeaderMap {
    headers
        .iter()
        .map(|(name, value)| {
            (
                HeaderName::from_static(name),
                HeaderValue::from_str(value).unwrap(),
            )
        })
        .collect()
}

fn normalize_headers(mut headers: HeaderMap) -> HeaderMap {
    for name in ["date", "x-envoy-upstream-service-time"] {
        headers.remove(name);
    }
    headers
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        fs::remove_file(&self.0).ok();
    }
}

/// A port that was free a moment ago.
fn free_port() -> io::Result<u16> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

async fn wait_for_port(port: u16) -> anyhow::Result<()> {
    let addr = format!("127.0.0.1:{port}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("timeout waiting for port {port}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
