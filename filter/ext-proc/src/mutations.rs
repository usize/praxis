// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Proto-to-Praxis conversions for `ext_proc` header and body mutations.
//!
//! Translates between the Envoy `ext_proc` protobuf types and
//! Praxis filter context operations: building [`HttpHeaders`] and
//! [`HttpBody`] from request/response state and applying
//! [`HeaderMutation`], [`BodyMutation`], and [`ImmediateResponse`]
//! results back to the context.
//!
//! [`HttpHeaders`]: praxis_proto::envoy::service::ext_proc::v3::HttpHeaders
//! [`HttpBody`]: praxis_proto::envoy::service::ext_proc::v3::HttpBody
//! [`HeaderMutation`]: praxis_proto::envoy::service::ext_proc::v3::HeaderMutation
//! [`BodyMutation`]: praxis_proto::envoy::service::ext_proc::v3::BodyMutation
//! [`ImmediateResponse`]: praxis_proto::envoy::service::ext_proc::v3::ImmediateResponse

use std::borrow::Cow;

use bytes::Bytes;
use praxis_filter::{FilterAction, FilterError, HttpFilterContext, Rejection};
use praxis_proto::envoy::service::{
    common::v3::{HeaderValue, HeaderValueOption},
    ext_proc::v3::{
        BodyResponse, HeaderMutation, HeadersResponse, HttpBody, HttpHeaders, ImmediateResponse, ProcessingRequest,
        body_mutation, processing_request,
    },
};

use crate::Phase;

// -----------------------------------------------------------------------------
// Request → Proto
// -----------------------------------------------------------------------------

/// Build [`HttpHeaders`] from the current request context.
///
/// Includes `:method`, `:path`, `:scheme`, and `:authority`
/// pseudo-headers followed by all request headers, matching
/// the Envoy `ext_proc` convention that external processors
/// expect.
pub(crate) fn request_to_proto_headers(ctx: &HttpFilterContext<'_>) -> HttpHeaders {
    let path = ctx
        .request
        .uri
        .path_and_query()
        .map_or_else(|| ctx.request.uri.path(), http::uri::PathAndQuery::as_str);
    let scheme = if ctx.downstream_tls { "https" } else { "http" };

    let mut headers = vec![
        proto_header(":method", ctx.request.method.as_str()),
        proto_header(":path", path),
        proto_header(":scheme", scheme),
    ];

    if let Some(authority) = ctx.request.headers.get(http::header::HOST) {
        headers.push(proto_header(":authority", authority.to_str().unwrap_or_default()));
    }

    for (name, value) in &ctx.request.headers {
        headers.push(proto_header(name.as_str(), value.to_str().unwrap_or_default()));
    }

    HttpHeaders {
        headers: Some(praxis_proto::envoy::service::ext_proc::v3::HeaderMap { headers }),
        end_of_stream: false,
    }
}

/// Build [`HttpHeaders`] from the upstream response context.
///
/// Includes a `:status` pseudo-header followed by all response
/// headers. Returns empty headers when `ctx.response_header` is
/// `None` (should not happen during the response phase).
pub(crate) fn response_to_proto_headers(ctx: &HttpFilterContext<'_>) -> HttpHeaders {
    let mut headers = Vec::new();

    if let Some(resp) = ctx.response_header.as_ref() {
        headers.push(HeaderValue {
            key: ":status".to_owned(),
            value: resp.status.as_u16().to_string(),
            raw_value: Vec::new(),
        });

        for (name, value) in &resp.headers {
            headers.push(HeaderValue {
                key: name.as_str().to_owned(),
                value: value.to_str().unwrap_or_default().to_owned(),
                raw_value: Vec::new(),
            });
        }
    }

    HttpHeaders {
        headers: Some(praxis_proto::envoy::service::ext_proc::v3::HeaderMap { headers }),
        end_of_stream: false,
    }
}

// -----------------------------------------------------------------------------
// Body → Proto
// -----------------------------------------------------------------------------

/// Build a [`ProcessingRequest`] wrapping a request body chunk.
///
/// Sends the body bytes (or empty if `None`) with the
/// `end_of_stream` flag.
pub(crate) fn request_body_to_request(body: &Option<Bytes>, end_of_stream: bool) -> ProcessingRequest {
    let http_body = HttpBody {
        body: body.as_ref().map_or_else(Vec::new, |b| b.to_vec()),
        end_of_stream,
    };
    ProcessingRequest {
        request: Some(processing_request::Request::RequestBody(http_body)),
        ..Default::default()
    }
}

/// Build a [`ProcessingRequest`] wrapping a response body chunk.
///
/// Sends the body bytes (or empty if `None`) with the
/// `end_of_stream` flag.
pub(crate) fn response_body_to_request(body: &Option<Bytes>, end_of_stream: bool) -> ProcessingRequest {
    let http_body = HttpBody {
        body: body.as_ref().map_or_else(Vec::new, |b| b.to_vec()),
        end_of_stream,
    };
    ProcessingRequest {
        request: Some(processing_request::Request::ResponseBody(http_body)),
        ..Default::default()
    }
}

// -----------------------------------------------------------------------------
// Proto → Praxis mutations
// -----------------------------------------------------------------------------

/// Apply a [`HeadersResponse`] to the filter context.
///
/// Delegates to request or response mutation based on the
/// current processing [`Phase`].
pub(crate) fn apply_headers_response(hr: &HeadersResponse, ctx: &mut HttpFilterContext<'_>, phase: Phase) {
    let Some(common) = &hr.response else {
        return;
    };
    let Some(mutation) = &common.header_mutation else {
        return;
    };

    match phase {
        Phase::Request => apply_request_header_mutation(mutation, ctx),
        Phase::Response => apply_response_header_mutation(mutation, ctx),
    }
}

/// Apply header mutations to the upstream request.
///
/// Maps each [`HeaderAppendAction`] variant to the appropriate
/// context queue:
///
/// - `AppendIfExistsOrAdd` (default) → [`extra_request_headers`]
/// - `OverwriteIfExistsOrAdd` → [`request_headers_to_set`]
/// - `OverwriteIfExists` → [`request_headers_to_set`] (only if present)
/// - `AddIfAbsent` → [`extra_request_headers`] (only if absent)
///
/// Pseudo-headers (`:` prefix) are skipped because Praxis sets
/// method, path, scheme, and authority through dedicated fields.
///
/// [`HeaderAppendAction`]: praxis_proto::envoy::service::common::v3::header_value_option::HeaderAppendAction
/// [`extra_request_headers`]: HttpFilterContext::extra_request_headers
/// [`request_headers_to_set`]: HttpFilterContext::request_headers_to_set
pub(crate) fn apply_request_header_mutation(mutation: &HeaderMutation, ctx: &mut HttpFilterContext<'_>) {
    remove_request_headers(&mutation.remove_headers, ctx);
    set_request_headers(&mutation.set_headers, ctx);
}

/// Queue request header removals, skipping pseudo-headers.
fn remove_request_headers(names: &[String], ctx: &mut HttpFilterContext<'_>) {
    for name in names {
        if is_pseudo_header(name) {
            continue;
        }
        if let Ok(header_name) = http::HeaderName::try_from(name.as_str()) {
            ctx.request_headers_to_remove.push(header_name);
        }
    }
}

/// Apply set-header mutations to the request context.
fn set_request_headers(headers: &[HeaderValueOption], ctx: &mut HttpFilterContext<'_>) {
    for hvo in headers {
        let Some(hv) = &hvo.header else { continue };
        if is_pseudo_header(&hv.key) {
            continue;
        }
        let Ok(name) = http::HeaderName::try_from(hv.key.as_str()) else {
            continue;
        };
        dispatch_request_header(hvo, hv, name, ctx);
    }
}

/// Route a single request header mutation to the correct context queue.
fn dispatch_request_header(
    hvo: &HeaderValueOption,
    hv: &HeaderValue,
    name: http::HeaderName,
    ctx: &mut HttpFilterContext<'_>,
) {
    use praxis_proto::envoy::service::common::v3::header_value_option::HeaderAppendAction;

    let value = header_value_string(hv);

    match resolve_append_action(hvo) {
        HeaderAppendAction::OverwriteIfExistsOrAdd => {
            if let Ok(v) = http::HeaderValue::try_from(&value) {
                ctx.request_headers_to_set.push((name, v));
            }
        },
        HeaderAppendAction::OverwriteIfExists => {
            if ctx.request.headers.contains_key(&name)
                && let Ok(v) = http::HeaderValue::try_from(&value)
            {
                ctx.request_headers_to_set.push((name, v));
            }
        },
        HeaderAppendAction::AddIfAbsent => {
            if !ctx.request.headers.contains_key(&name) {
                ctx.extra_request_headers.push((Cow::Owned(hv.key.clone()), value));
            }
        },
        HeaderAppendAction::AppendIfExistsOrAdd => {
            ctx.extra_request_headers.push((Cow::Owned(hv.key.clone()), value));
        },
    }
}

/// Apply header mutations to the upstream response.
///
/// Modifies [`HttpFilterContext::response_header`] directly and
/// sets [`HttpFilterContext::response_headers_modified`] when
/// any mutation is applied. Pseudo-headers are skipped.
pub(crate) fn apply_response_header_mutation(mutation: &HeaderMutation, ctx: &mut HttpFilterContext<'_>) {
    let Some(resp) = ctx.response_header.as_mut() else {
        return;
    };

    let sets = set_response_headers(&mutation.set_headers, resp);
    let removes = remove_response_headers(&mutation.remove_headers, resp);

    if sets || removes {
        ctx.response_headers_modified = true;
    }
}

/// Apply set-header mutations to a response, returning whether any were applied.
fn set_response_headers(headers: &[HeaderValueOption], resp: &mut praxis_filter::Response) -> bool {
    let mut modified = false;
    for hvo in headers {
        if let Some(hv) = &hvo.header {
            if is_pseudo_header(&hv.key) {
                continue;
            }
            let value = header_value_string(hv);
            if let (Ok(name), Ok(val)) = (http::HeaderName::try_from(&hv.key), http::HeaderValue::try_from(&value)) {
                if should_append(hvo) {
                    resp.headers.append(name, val);
                } else {
                    resp.headers.insert(name, val);
                }
                modified = true;
            }
        }
    }
    modified
}

/// Apply remove-header mutations to a response, returning whether any were applied.
fn remove_response_headers(names: &[String], resp: &mut praxis_filter::Response) -> bool {
    let mut modified = false;
    for name in names {
        if is_pseudo_header(name) {
            continue;
        }
        if let Ok(header_name) = http::HeaderName::try_from(name.as_str())
            && resp.headers.remove(&header_name).is_some()
        {
            modified = true;
        }
    }
    modified
}

/// Apply a [`BodyResponse`] to the filter context and body buffer.
///
/// Handles three mutation variants:
/// - [`body_mutation::Mutation::StreamedResponse`] — replaces body bytes with the streamed chunk (used in
///   `FULL_DUPLEX_STREAMED`).
/// - [`body_mutation::Mutation::Body`] — replaces body bytes (used in `STREAMED` mode).
/// - [`body_mutation::Mutation::ClearBody`] — clears the body chunk.
///
/// Also applies any header mutations from the [`CommonResponse`].
///
/// [`CommonResponse`]: praxis_proto::envoy::service::ext_proc::v3::CommonResponse
#[allow(
    clippy::unnecessary_wraps,
    reason = "Result matches dispatch_body_response call-site contract"
)]
pub(crate) fn apply_body_response(
    br: &BodyResponse,
    ctx: &mut HttpFilterContext<'_>,
    body: &mut Option<Bytes>,
    phase: Phase,
) -> Result<FilterAction, FilterError> {
    let Some(common) = &br.response else {
        return Ok(FilterAction::Continue);
    };

    if let Some(mutation) = &common.header_mutation {
        match phase {
            Phase::Request => apply_request_header_mutation(mutation, ctx),
            Phase::Response => apply_response_header_mutation(mutation, ctx),
        }
    }

    if let Some(body_mutation) = &common.body_mutation {
        apply_body_mutation(body_mutation, body);
    }

    Ok(FilterAction::Continue)
}

/// Apply a [`BodyMutation`] to the body buffer.
///
/// [`BodyMutation`]: praxis_proto::envoy::service::ext_proc::v3::BodyMutation
fn apply_body_mutation(bm: &praxis_proto::envoy::service::ext_proc::v3::BodyMutation, body: &mut Option<Bytes>) {
    let Some(mutation) = &bm.mutation else {
        return;
    };

    match mutation {
        body_mutation::Mutation::Body(new_body) => {
            *body = Some(Bytes::copy_from_slice(new_body));
        },
        body_mutation::Mutation::ClearBody(true) => {
            *body = None;
        },
        body_mutation::Mutation::ClearBody(false) => {},
        body_mutation::Mutation::StreamedResponse(streamed) => {
            if streamed.body.is_empty() {
                *body = None;
            } else {
                *body = Some(Bytes::copy_from_slice(&streamed.body));
            }
        },
    }
}

/// Convert an [`ImmediateResponse`] to a [`FilterAction::Reject`].
///
/// Maps the proto status code (defaulting to 200 when absent),
/// body, and response headers to a [`Rejection`].
pub(crate) fn immediate_to_rejection(imm: &ImmediateResponse) -> FilterAction {
    let status = imm.status.as_ref().map_or(200, |s| {
        let code = s.code;
        u16::try_from(code).unwrap_or(500)
    });

    let status = if (100..=599).contains(&status) { status } else { 500 };

    let mut rejection = Rejection::status(status);

    if !imm.body.is_empty() {
        rejection = rejection.with_body(Bytes::copy_from_slice(imm.body.as_bytes()));
    }

    if let Some(hm) = &imm.headers {
        for hvo in &hm.set_headers {
            if let Some(hv) = &hvo.header {
                let value = header_value_string(hv);
                rejection = rejection.with_header(hv.key.clone(), value);
            }
        }
    }

    FilterAction::Reject(rejection)
}

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

/// Extract the string value from a [`HeaderValue`].
///
/// Prefers `raw_value` (as UTF-8) over `value` when non-empty,
/// matching the Envoy convention where `raw_value` carries the
/// original bytes.
pub(crate) fn header_value_string(hv: &HeaderValue) -> String {
    if hv.raw_value.is_empty() {
        hv.value.clone()
    } else {
        String::from_utf8_lossy(&hv.raw_value).into_owned()
    }
}

/// Returns `true` if the header name is an HTTP/2 pseudo-header.
pub(crate) fn is_pseudo_header(name: &str) -> bool {
    name.starts_with(':')
}

/// Build a [`HeaderValue`] proto with the given key and value.
fn proto_header(key: &str, value: &str) -> HeaderValue {
    HeaderValue {
        key: key.to_owned(),
        value: value.to_owned(),
        raw_value: Vec::new(),
    }
}

/// Resolve the [`HeaderAppendAction`] for a [`HeaderValueOption`].
///
/// Uses `append_action` when set (non-zero). Falls back to the
/// deprecated `append` field, mapping `true` / default to
/// [`AppendIfExistsOrAdd`] and `false` to
/// [`OverwriteIfExistsOrAdd`], matching proto3 default semantics.
///
/// [`HeaderAppendAction`]: praxis_proto::envoy::service::common::v3::header_value_option::HeaderAppendAction
/// [`AppendIfExistsOrAdd`]: praxis_proto::envoy::service::common::v3::header_value_option::HeaderAppendAction::AppendIfExistsOrAdd
/// [`OverwriteIfExistsOrAdd`]: praxis_proto::envoy::service::common::v3::header_value_option::HeaderAppendAction::OverwriteIfExistsOrAdd
fn resolve_append_action(
    hvo: &HeaderValueOption,
) -> praxis_proto::envoy::service::common::v3::header_value_option::HeaderAppendAction {
    use praxis_proto::envoy::service::common::v3::header_value_option::HeaderAppendAction;

    if hvo.append_action != 0 {
        return HeaderAppendAction::try_from(hvo.append_action).unwrap_or(HeaderAppendAction::AppendIfExistsOrAdd);
    }

    // proto3 default for append_action is 0 (APPEND_IF_EXISTS_OR_ADD).
    // Fall back to deprecated `append`; default to true (append)
    // when neither field is explicitly set.
    if hvo.append.unwrap_or(true) {
        HeaderAppendAction::AppendIfExistsOrAdd
    } else {
        HeaderAppendAction::OverwriteIfExistsOrAdd
    }
}

/// Whether the [`HeaderValueOption`] indicates an append operation.
fn should_append(hvo: &HeaderValueOption) -> bool {
    use praxis_proto::envoy::service::common::v3::header_value_option::HeaderAppendAction;

    resolve_append_action(hvo) == HeaderAppendAction::AppendIfExistsOrAdd
}
