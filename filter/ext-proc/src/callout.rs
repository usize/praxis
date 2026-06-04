// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! gRPC stream management for the `ext_proc` filter.
//!
//! Maintains a persistent bidirectional `Process` stream per HTTP
//! request. The background task owns the gRPC client and relays
//! [`ProcessingRequest`] / [`ProcessingResponse`] messages between
//! the filter hooks and the external processor.
//!
//! [`ProcessingRequest`]: praxis_proto::envoy::service::ext_proc::v3::ProcessingRequest
//! [`ProcessingResponse`]: praxis_proto::envoy::service::ext_proc::v3::ProcessingResponse

use std::time::Duration;

use bytes::Bytes;
use praxis_filter::{FilterAction, FilterError, HttpFilterContext};
use praxis_proto::envoy::service::ext_proc::v3::{
    ProcessingRequest, ProcessingResponse, external_processor_client::ExternalProcessorClient, processing_request,
    processing_response,
};
use tokio::sync::mpsc;
use tonic::transport::Channel;

use crate::{
    Phase,
    mutations::{
        apply_body_response, apply_headers_response, immediate_to_rejection, request_to_proto_headers,
        response_to_proto_headers,
    },
};

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

/// Channel capacity for the request/response mpsc pairs.
///
/// Sized to allow a handful of in-flight messages without
/// back-pressure while keeping memory bounded.
const CHANNEL_CAPACITY: usize = 16;

// -----------------------------------------------------------------------------
// CalloutError
// -----------------------------------------------------------------------------

/// Errors that can occur during a gRPC callout.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CalloutError {
    /// gRPC transport or protocol error.
    #[error("ext_proc gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    /// The per-message timeout expired.
    #[error("ext_proc message timeout")]
    Timeout,

    /// The server closed the stream without sending a response.
    #[error("ext_proc server closed stream without response")]
    EmptyStream,

    /// The internal channel was closed unexpectedly.
    #[error("ext_proc internal channel closed")]
    ChannelClosed,
}

// -----------------------------------------------------------------------------
// StreamHandle
// -----------------------------------------------------------------------------

/// Handle to a persistent gRPC `Process` stream for one HTTP request.
///
/// Holds the send/receive channels and the background task that owns
/// the actual gRPC stream. Aborting the task on drop ensures that
/// abandoned streams do not leak.
pub(crate) struct StreamHandle {
    /// Sends [`ProcessingRequest`] messages to the background task.
    sender: mpsc::Sender<ProcessingRequest>,

    /// Receives [`ProcessingResponse`] messages from the background task.
    receiver: tokio::sync::Mutex<mpsc::Receiver<ProcessingResponse>>,

    /// Background task driving the bidirectional gRPC stream.
    task: tokio::task::JoinHandle<()>,
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl std::fmt::Debug for StreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamHandle")
            .field("sender_closed", &self.sender.is_closed())
            .finish()
    }
}

// -----------------------------------------------------------------------------
// Stream lifecycle
// -----------------------------------------------------------------------------

/// Open a persistent `Process` stream to the external processor.
///
/// Spawns a background task that owns the gRPC client and relays
/// messages between the returned [`StreamHandle`] channels and the
/// bidirectional stream. The task terminates when either channel is
/// dropped or the gRPC stream ends.
pub(crate) fn open_stream(channel: Channel, target: String) -> StreamHandle {
    let (req_tx, req_rx) = mpsc::channel::<ProcessingRequest>(CHANNEL_CAPACITY);
    let (resp_tx, resp_rx) = mpsc::channel::<ProcessingResponse>(CHANNEL_CAPACITY);

    let task = tokio::spawn(async move {
        let result = run_stream(channel, req_rx, &resp_tx, &target).await;
        if let Err(e) = result {
            tracing::warn!(target = %target, error = %e, "ext_proc background stream ended");
        }
    });

    StreamHandle {
        sender: req_tx,
        receiver: tokio::sync::Mutex::new(resp_rx),
        task,
    }
}

/// Drive the bidirectional gRPC stream until completion.
///
/// Reads requests from the inbound channel, forwards them on the
/// gRPC stream, reads responses from the stream, and forwards them
/// on the outbound channel.
async fn run_stream(
    channel: Channel,
    req_rx: mpsc::Receiver<ProcessingRequest>,
    resp_tx: &mpsc::Sender<ProcessingResponse>,
    target: &str,
) -> Result<(), CalloutError> {
    let req_stream = tokio_stream::wrappers::ReceiverStream::new(req_rx);

    let mut client = ExternalProcessorClient::new(channel);
    let response = client.process(req_stream).await.map_err(CalloutError::Grpc)?;
    let mut streaming = response.into_inner();

    loop {
        match streaming.message().await {
            Ok(Some(msg)) => {
                if resp_tx.send(msg).await.is_err() {
                    tracing::debug!(target = %target, "ext_proc response channel closed");
                    break;
                }
            },
            Ok(None) => {
                tracing::debug!(target = %target, "ext_proc gRPC stream closed by server");
                break;
            },
            Err(e) => {
                tracing::warn!(target = %target, error = %e, "ext_proc gRPC stream error");
                return Err(CalloutError::Grpc(e));
            },
        }
    }

    Ok(())
}

// -----------------------------------------------------------------------------
// Send / receive helpers
// -----------------------------------------------------------------------------

/// Send a request and await the response asynchronously with a timeout.
///
/// Used by async filter hooks (`on_request`, `on_request_body`,
/// `on_response`). If the processor responds with
/// `override_message_timeout` and no `response` oneof, the deadline
/// is extended (clamped to `max_timeout`) and the next message is
/// read. Without a configured `max_timeout`, override requests are
/// ignored and the response is returned as-is.
pub(crate) async fn send_and_receive_async(
    handle: &StreamHandle,
    request: ProcessingRequest,
    timeout: Duration,
    max_timeout: Option<Duration>,
    target: &str,
) -> Result<ProcessingResponse, FilterError> {
    let result = tokio::time::timeout(timeout, async {
        handle
            .sender
            .send(request)
            .await
            .map_err(|_closed| CalloutError::ChannelClosed)?;

        let mut rx = handle.receiver.lock().await;
        let resp = rx.recv().await.ok_or(CalloutError::EmptyStream)?;
        let result = receive_with_override(&mut rx, resp, max_timeout, target).await;

        drop(rx);

        result
    })
    .await;

    match result {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(e)) => {
            tracing::warn!(target = %target, error = %e, "ext_proc callout failed");
            Err(e.into())
        },
        Err(_elapsed) => {
            tracing::warn!(target = %target, "ext_proc callout timed out");
            Err(CalloutError::Timeout.into())
        },
    }
}

/// Send a request and block on the response synchronously.
///
/// Uses [`tokio::task::block_in_place`] + [`tokio::runtime::Handle::block_on`]
/// to bridge into an async context from a sync filter hook. Safe on
/// Pingora's multi-threaded runtime.
///
/// Used by `on_response_body` which is a sync method.
pub(crate) fn send_and_receive_blocking(
    handle: &StreamHandle,
    request: ProcessingRequest,
    timeout: Duration,
    max_timeout: Option<Duration>,
    target: &str,
) -> Result<ProcessingResponse, FilterError> {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(send_and_receive_async(
            handle,
            request,
            timeout,
            max_timeout,
            target,
        ))
    })
}

/// Handle `override_message_timeout` from the processor.
///
/// When a response carries `override_message_timeout` but no
/// `response` oneof, the deadline is extended (clamped to
/// `max_timeout`) and the next message is read. Without a
/// configured `max_timeout`, override requests are ignored and
/// the response is returned as-is.
async fn receive_with_override(
    rx: &mut mpsc::Receiver<ProcessingResponse>,
    resp: ProcessingResponse,
    max_timeout: Option<Duration>,
    target: &str,
) -> Result<ProcessingResponse, CalloutError> {
    if resp.response.is_some() {
        return Ok(resp);
    }

    let Some(override_dur) = parse_timeout_override(&resp, max_timeout) else {
        // No response oneof and no usable override — treat as no-op.
        return Ok(resp);
    };

    tracing::debug!(
        target = %target,
        override_ms = override_dur.as_millis(),
        "ext_proc: processor requested timeout override"
    );

    tokio::time::timeout(override_dur, async { rx.recv().await.ok_or(CalloutError::EmptyStream) })
        .await
        .map_err(|_elapsed| CalloutError::Timeout)?
}

/// Extract and clamp the `override_message_timeout` from a response.
///
/// Returns `None` if the field is absent, the duration is zero, or
/// `max_timeout` is not configured (overrides require an upper bound).
fn parse_timeout_override(resp: &ProcessingResponse, max_timeout: Option<Duration>) -> Option<Duration> {
    let max = max_timeout?;
    let proto_dur = resp.override_message_timeout.as_ref()?;
    let secs = u64::try_from(proto_dur.seconds).unwrap_or(0);
    let nanos = u32::try_from(proto_dur.nanos).unwrap_or(0);
    let dur = Duration::new(secs, nanos);

    if dur.is_zero() {
        return None;
    }

    Some(dur.min(max))
}

// -----------------------------------------------------------------------------
// Header processing (using persistent stream)
// -----------------------------------------------------------------------------

/// Send request headers on the persistent stream and apply mutations.
///
/// Sends a `RequestHeaders` message, waits for one response within
/// `timeout`, and applies header mutations or returns a rejection.
pub(crate) async fn process_request_headers(
    handle: &StreamHandle,
    target: &str,
    timeout: Duration,
    max_timeout: Option<Duration>,
    ctx: &mut HttpFilterContext<'_>,
) -> Result<FilterAction, FilterError> {
    let headers = request_to_proto_headers(ctx);
    let request = ProcessingRequest {
        request: Some(processing_request::Request::RequestHeaders(headers)),
        ..Default::default()
    };

    let response = send_and_receive_async(handle, request, timeout, max_timeout, target).await?;
    dispatch_response(&response, ctx, Phase::Request)
}

/// Send response headers on the persistent stream and apply mutations.
///
/// Same pattern as [`process_request_headers`] but wraps
/// `ResponseHeaders` and operates during the response phase.
pub(crate) async fn process_response_headers(
    handle: &StreamHandle,
    target: &str,
    timeout: Duration,
    max_timeout: Option<Duration>,
    ctx: &mut HttpFilterContext<'_>,
) -> Result<FilterAction, FilterError> {
    let headers = response_to_proto_headers(ctx);
    let request = ProcessingRequest {
        request: Some(processing_request::Request::ResponseHeaders(headers)),
        ..Default::default()
    };

    let response = send_and_receive_async(handle, request, timeout, max_timeout, target).await?;
    dispatch_response(&response, ctx, Phase::Response)
}

// -----------------------------------------------------------------------------
// Body processing
// -----------------------------------------------------------------------------

/// Process a request body chunk on the persistent stream.
///
/// Sends an `HttpBody` message and waits for the `ext_proc` response.
/// Returns the filter action after applying body/header mutations.
#[allow(
    clippy::too_many_arguments,
    reason = "body phase requires handle, target, timeout, max_timeout, ctx, body, and eos"
)]
pub(crate) async fn process_request_body(
    handle: &StreamHandle,
    target: &str,
    timeout: Duration,
    max_timeout: Option<Duration>,
    ctx: &mut HttpFilterContext<'_>,
    body: &mut Option<Bytes>,
    end_of_stream: bool,
) -> Result<FilterAction, FilterError> {
    let request = crate::mutations::request_body_to_request(body, end_of_stream);
    let response = send_and_receive_async(handle, request, timeout, max_timeout, target).await?;
    dispatch_body_response(&response, ctx, body, Phase::Request)
}

/// Process a response body chunk on the persistent stream (blocking).
///
/// Uses [`send_and_receive_blocking`] because `on_response_body` is
/// a sync method in the [`HttpFilter`] trait.
///
/// [`HttpFilter`]: praxis_filter::HttpFilter
#[allow(
    clippy::too_many_arguments,
    reason = "body phase requires handle, target, timeout, max_timeout, ctx, body, and eos"
)]
pub(crate) fn process_response_body(
    handle: &StreamHandle,
    target: &str,
    timeout: Duration,
    max_timeout: Option<Duration>,
    ctx: &mut HttpFilterContext<'_>,
    body: &mut Option<Bytes>,
    end_of_stream: bool,
) -> Result<FilterAction, FilterError> {
    let request = crate::mutations::response_body_to_request(body, end_of_stream);
    let response = send_and_receive_blocking(handle, request, timeout, max_timeout, target)?;
    dispatch_body_response(&response, ctx, body, Phase::Response)
}

// -----------------------------------------------------------------------------
// Response dispatch
// -----------------------------------------------------------------------------

/// Route a [`ProcessingResponse`] variant to the correct header mutation handler.
///
/// Returns [`FilterAction::Continue`] for header mutations or
/// [`FilterAction::Reject`] for immediate responses. Unexpected
/// response types produce a [`FilterError`].
fn dispatch_response(
    response: &ProcessingResponse,
    ctx: &mut HttpFilterContext<'_>,
    phase: Phase,
) -> Result<FilterAction, FilterError> {
    let Some(resp) = &response.response else {
        return Ok(FilterAction::Continue);
    };

    match (resp, phase) {
        (processing_response::Response::RequestHeaders(hr), Phase::Request)
        | (processing_response::Response::ResponseHeaders(hr), Phase::Response) => {
            apply_headers_response(hr, ctx, phase);
            Ok(FilterAction::Continue)
        },
        (processing_response::Response::ImmediateResponse(imm), _) => Ok(immediate_to_rejection(imm)),
        (other, _) => {
            let variant = response_variant_name(other);
            Err(format!("ext_proc: unexpected response type '{variant}' during {phase} headers phase").into())
        },
    }
}

/// Route a [`ProcessingResponse`] during a body phase.
///
/// Handles `RequestBody` / `ResponseBody` variants via
/// [`apply_body_response`], `ImmediateResponse` via rejection,
/// and rejects unexpected variants.
fn dispatch_body_response(
    response: &ProcessingResponse,
    ctx: &mut HttpFilterContext<'_>,
    body: &mut Option<Bytes>,
    phase: Phase,
) -> Result<FilterAction, FilterError> {
    let Some(resp) = &response.response else {
        return Ok(FilterAction::Continue);
    };

    match resp {
        processing_response::Response::RequestBody(br) | processing_response::Response::ResponseBody(br) => {
            apply_body_response(br, ctx, body, phase)
        },
        processing_response::Response::ImmediateResponse(imm) => Ok(immediate_to_rejection(imm)),
        other => {
            let variant = response_variant_name(other);
            Err(format!("ext_proc: unexpected response type '{variant}' during {phase} body phase").into())
        },
    }
}

/// Returns a human-readable name for a [`processing_response::Response`] variant.
fn response_variant_name(resp: &processing_response::Response) -> &'static str {
    match resp {
        processing_response::Response::RequestHeaders(_) => "RequestHeaders",
        processing_response::Response::ResponseHeaders(_) => "ResponseHeaders",
        processing_response::Response::RequestBody(_) => "RequestBody",
        processing_response::Response::ResponseBody(_) => "ResponseBody",
        processing_response::Response::RequestTrailers(_) => "RequestTrailers",
        processing_response::Response::ResponseTrailers(_) => "ResponseTrailers",
        processing_response::Response::ImmediateResponse(_) => "ImmediateResponse",
    }
}
