//! Mock EPP (Endpoint Picker) service for ext_proc integration tests.
//!
//! Implements the Envoy `ExternalProcessor` gRPC service to simulate
//! llm-d EPP behavior on a persistent bidirectional stream per request.

use std::pin::Pin;

use clap::Parser;
use praxis_proto::envoy::service::{
    common::v3::{HeaderValue, HeaderValueOption, HttpStatus, StatusCode, header_value_option},
    ext_proc::v3::{
        BodyMutation, BodyResponse, CommonResponse, HeaderMutation, HeadersResponse, ImmediateResponse,
        ProcessingRequest, ProcessingResponse, StreamedBodyResponse, body_mutation,
        external_processor_server::{ExternalProcessor, ExternalProcessorServer},
        processing_request::Request,
        processing_response::Response,
    },
};
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Status, Streaming, transport::Server};

/// Mock EPP service mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Stream body through unmodified, add routing headers.
    Passthrough,
    /// Return 403 ImmediateResponse if no `authorization` header.
    RejectUnauthorized,
    /// Wrap request body: prepend `{"wrapped":` / append `}`.
    RewriteBody,
}

impl std::str::FromStr for Mode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "passthrough" => Ok(Self::Passthrough),
            "reject-unauthorized" => Ok(Self::RejectUnauthorized),
            "rewrite-body" => Ok(Self::RewriteBody),
            other => Err(format!("unknown mode: {other}")),
        }
    }
}

/// Mock EPP service CLI arguments.
#[derive(Parser, Debug)]
#[command(name = "praxis-ext-proc-mock")]
struct Args {
    /// Port to listen on.
    #[arg(long, default_value = "50051")]
    port: u16,

    /// Processing mode.
    #[arg(long, default_value = "passthrough")]
    mode: String,
}

/// Mock EPP gRPC service.
struct MockEpp {
    /// Processing mode for this instance.
    mode: Mode,
}

type ResponseStream = Pin<Box<ReceiverStream<Result<ProcessingResponse, Status>>>>;

#[tonic::async_trait]
impl ExternalProcessor for MockEpp {
    type ProcessStream = ResponseStream;

    async fn process(
        &self,
        request: tonic::Request<Streaming<ProcessingRequest>>,
    ) -> Result<tonic::Response<Self::ProcessStream>, Status> {
        let mode = self.mode;
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(32);

        tokio::spawn(async move {
            let mut model_name: Option<String> = None;

            while let Some(msg) = inbound.next().await {
                let msg = match msg {
                    Ok(m) => m,
                    Err(e) => {
                        tracing::warn!("stream error: {e}");
                        break;
                    },
                };

                let req = match msg.request {
                    Some(r) => r,
                    None => continue,
                };

                let responses = handle_request(req, mode, &mut model_name);

                for resp in responses {
                    if tx.send(Ok(resp)).await.is_err() {
                        tracing::debug!("client disconnected");
                        return;
                    }
                }
            }
        });

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Process a single request message and return response(s).
fn handle_request(req: Request, mode: Mode, model_name: &mut Option<String>) -> Vec<ProcessingResponse> {
    match req {
        Request::RequestHeaders(hdrs) => handle_request_headers(hdrs, mode, model_name),
        Request::RequestBody(body) => handle_request_body(body, mode),
        Request::ResponseHeaders(hdrs) => handle_response_headers(hdrs, model_name),
        Request::ResponseBody(body) => handle_response_body(body),
        _ => vec![],
    }
}

/// Handle request headers phase.
fn handle_request_headers(
    hdrs: praxis_proto::envoy::service::ext_proc::v3::HttpHeaders,
    mode: Mode,
    model_name: &mut Option<String>,
) -> Vec<ProcessingResponse> {
    // Extract headers from the proto message.
    let headers = hdrs.headers.as_ref().map(|hm| &hm.headers[..]).unwrap_or_default();

    // Extract model name from x-model-name header if present.
    for h in headers {
        if h.key == "x-model-name" {
            *model_name = Some(h.value.clone());
        }
    }

    // In reject-unauthorized mode, check for authorization header.
    if mode == Mode::RejectUnauthorized {
        let has_auth = headers.iter().any(|h| h.key == "authorization");
        if !has_auth {
            return vec![ProcessingResponse {
                response: Some(Response::ImmediateResponse(ImmediateResponse {
                    status: Some(HttpStatus {
                        code: StatusCode::Forbidden.into(),
                    }),
                    body: "Unauthorized: missing authorization header".to_owned(),
                    ..Default::default()
                })),
                ..Default::default()
            }];
        }
    }

    // Build routing headers to add.
    let mut set_headers = vec![
        make_header("x-selected-endpoint", "10.0.0.1:8080"),
        make_header("x-epp-decision", "routed"),
        make_header("x-session-token", "mock-session-001"),
    ];

    // If we have a model name, add it.
    if let Some(name) = model_name.as_deref() {
        set_headers.push(make_header("x-epp-model-hint", name));
    }

    vec![ProcessingResponse {
        response: Some(Response::RequestHeaders(HeadersResponse {
            response: Some(CommonResponse {
                header_mutation: Some(HeaderMutation {
                    set_headers,
                    remove_headers: vec![],
                }),
                ..Default::default()
            }),
        })),
        ..Default::default()
    }]
}

/// Handle request body phase.
fn handle_request_body(
    body: praxis_proto::envoy::service::ext_proc::v3::HttpBody,
    mode: Mode,
) -> Vec<ProcessingResponse> {
    let eos = body.end_of_stream;

    let mutation = match mode {
        Mode::RewriteBody => {
            // Wrap the body.
            let original = String::from_utf8_lossy(&body.body);
            let wrapped = format!("{{\"wrapped\":{original}}}");
            Some(BodyMutation {
                mutation: Some(body_mutation::Mutation::StreamedResponse(StreamedBodyResponse {
                    body: wrapped.into_bytes(),
                    end_of_stream: eos,
                })),
            })
        },
        _ => Some(BodyMutation {
            mutation: Some(body_mutation::Mutation::StreamedResponse(StreamedBodyResponse {
                body: body.body,
                end_of_stream: eos,
            })),
        }),
    };

    vec![ProcessingResponse {
        response: Some(Response::RequestBody(BodyResponse {
            response: Some(CommonResponse {
                body_mutation: mutation,
                ..Default::default()
            }),
        })),
        ..Default::default()
    }]
}

/// Handle response headers phase.
fn handle_response_headers(
    _hdrs: praxis_proto::envoy::service::ext_proc::v3::HttpHeaders,
    model_name: &Option<String>,
) -> Vec<ProcessingResponse> {
    let mut set_headers = vec![make_header("x-epp-served-by", "mock-epp")];

    if let Some(name) = model_name.as_deref() {
        set_headers.push(make_header("x-epp-model", name));
    }

    vec![ProcessingResponse {
        response: Some(Response::ResponseHeaders(HeadersResponse {
            response: Some(CommonResponse {
                header_mutation: Some(HeaderMutation {
                    set_headers,
                    remove_headers: vec![],
                }),
                ..Default::default()
            }),
        })),
        ..Default::default()
    }]
}

/// Handle response body phase.
fn handle_response_body(body: praxis_proto::envoy::service::ext_proc::v3::HttpBody) -> Vec<ProcessingResponse> {
    vec![ProcessingResponse {
        response: Some(Response::ResponseBody(BodyResponse {
            response: Some(CommonResponse {
                body_mutation: Some(BodyMutation {
                    mutation: Some(body_mutation::Mutation::StreamedResponse(StreamedBodyResponse {
                        body: body.body,
                        end_of_stream: body.end_of_stream,
                    })),
                }),
                ..Default::default()
            }),
        })),
        ..Default::default()
    }]
}

/// Build a `HeaderValueOption` with the given key and value.
fn make_header(key: &str, value: &str) -> HeaderValueOption {
    HeaderValueOption {
        header: Some(HeaderValue {
            key: key.to_owned(),
            value: value.to_owned(),
            raw_value: vec![],
        }),
        append: None,
        append_action: header_value_option::HeaderAppendAction::OverwriteIfExistsOrAdd.into(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let mode: Mode = args.mode.parse().map_err(|e: String| e)?;

    let addr = format!("0.0.0.0:{}", args.port).parse()?;
    let service = MockEpp { mode };

    tracing::info!("mock EPP listening on {addr} in {mode:?} mode");

    Server::builder()
        .add_service(ExternalProcessorServer::new(service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    Ok(())
}

/// Wait for SIGTERM or SIGINT for graceful shutdown.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        ctrl_c.await.expect("failed to register Ctrl+C handler");
    }
    tracing::info!("shutdown signal received");
}
