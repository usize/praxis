// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Praxis Contributors

//! Provider fallback filter for Praxis.
//!
//! Implements a terminal filter that sends the buffered request body
//! to an ordered list of provider targets via [`CalloutClient`],
//! trying each in sequence until one returns a 2xx response.
//!
//! This is an **experiment** — see `docs/experiments/` for design
//! rationale and known limitations.
//!
//! [`CalloutClient`]: praxis_core::callout::CalloutClient

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderName, HeaderValue};
use praxis_core::callout::{
    CalloutClient, CalloutConfig, CalloutRequest, CalloutResponse, CalloutResult, CircuitBreakerConfig, FailureMode,
};
use praxis_filter::{
    BodyAccess, BodyMode, FilterAction, FilterError, HttpFilter, HttpFilterContext, Rejection, parse_filter_config,
};
use serde::Deserialize;
use tracing::warn;

// -----------------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------------

/// Default maximum request body size (10 MiB).
const DEFAULT_MAX_BODY_BYTES: usize = 10_485_760; // 10 MiB

/// Default per-target request timeout (30 s).
const DEFAULT_TIMEOUT_MS: u64 = 30_000;

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

/// Top-level YAML configuration for the provider fallback filter.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderFallbackConfig {
    /// Ordered list of provider targets to try.
    targets: Vec<TargetConfig>,

    /// Maximum request body size to buffer.
    #[serde(default = "default_max_body_bytes")]
    max_body_bytes: usize,

    /// Shared circuit breaker settings applied to every target.
    #[serde(default)]
    circuit_breaker: Option<CircuitBreakerCfg>,
}

/// Configuration for a single provider target.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetConfig {
    /// Full URL including path (e.g. `http://host:port/v1/chat/completions`).
    url: String,

    /// Optional model name to inject into the JSON body.
    #[serde(default)]
    model_override: Option<String>,

    /// Request timeout in milliseconds.
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,

    /// Static headers to send with every callout (e.g. `Authorization`).
    #[serde(default)]
    headers: HashMap<String, String>,
}

/// Circuit breaker thresholds (shared across targets).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CircuitBreakerCfg {
    /// Consecutive failures before the breaker opens.
    consecutive_failures: u32,

    /// Milliseconds to wait before a half-open probe.
    recovery_window_ms: u64,
}

/// Default value for [`ProviderFallbackConfig::max_body_bytes`].
fn default_max_body_bytes() -> usize {
    DEFAULT_MAX_BODY_BYTES
}

/// Default value for [`TargetConfig::timeout_ms`].
fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

// -----------------------------------------------------------------------------
// Filter Types
// -----------------------------------------------------------------------------

/// A resolved provider target ready for use at request time.
struct ResolvedTarget {
    /// Per-target callout client (owns its own circuit breaker).
    client: CalloutClient,

    /// Target URL.
    url: String,

    /// Optional model name override.
    model_override: Option<String>,

    /// Pre-resolved static headers.
    headers: Vec<(HeaderName, HeaderValue)>,
}

/// Terminal filter that tries provider targets in order.
///
/// Runs in `on_request_body` after the full body is buffered.
/// Returns the first successful (2xx) provider response as a
/// [`FilterAction::Reject`] (terminal — no upstream routing).
struct ProviderFallbackFilter {
    /// Ordered provider targets.
    targets: Vec<ResolvedTarget>,

    /// Maximum request body bytes to buffer.
    max_body_bytes: usize,
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

impl ProviderFallbackFilter {
    /// Create from parsed YAML config.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] if the config is invalid or a
    /// [`CalloutClient`] cannot be built.
    fn from_config(config: &serde_yaml::Value) -> Result<Box<dyn HttpFilter>, FilterError> {
        let cfg: ProviderFallbackConfig = parse_filter_config("provider_fallback", config)?;

        if cfg.targets.is_empty() {
            return Err("provider_fallback: at least one target is required".into());
        }

        let cb_cfg = cfg.circuit_breaker;
        let targets = cfg
            .targets
            .into_iter()
            .map(|t| resolve_target(t, cb_cfg.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Box::new(Self {
            targets,
            max_body_bytes: cfg.max_body_bytes,
        }))
    }
}

// -----------------------------------------------------------------------------
// HttpFilter Implementation
// -----------------------------------------------------------------------------

#[async_trait]
impl HttpFilter for ProviderFallbackFilter {
    fn name(&self) -> &'static str {
        "provider_fallback"
    }

    async fn on_request(&self, _ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        Ok(FilterAction::Continue)
    }

    fn request_body_access(&self) -> BodyAccess {
        BodyAccess::ReadWrite
    }

    fn request_body_mode(&self) -> BodyMode {
        BodyMode::StreamBuffer {
            max_bytes: Some(self.max_body_bytes),
        }
    }

    fn needs_request_context(&self) -> bool {
        true
    }

    async fn on_request_body(
        &self,
        ctx: &mut HttpFilterContext<'_>,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
    ) -> Result<FilterAction, FilterError> {
        if !end_of_stream {
            return Ok(FilterAction::Continue);
        }

        let request_body = body.as_ref().map_or_else(Vec::new, |b| b.to_vec());
        let method = ctx.request.method.clone();
        let forward_headers = collect_forward_headers(ctx);

        try_targets(&self.targets, &method, &request_body, &forward_headers).await
    }
}

// -----------------------------------------------------------------------------
// Core Dispatch
// -----------------------------------------------------------------------------

/// Try each target in order, returning the first 2xx response.
async fn try_targets(
    targets: &[ResolvedTarget],
    method: &http::Method,
    body: &[u8],
    forward_headers: &[(HeaderName, HeaderValue)],
) -> Result<FilterAction, FilterError> {
    for target in targets {
        let callout_body = prepare_body(body, target.model_override.as_deref());
        let request = build_callout_request(method, &target.url, forward_headers, &target.headers, callout_body);

        match target.client.execute(request).await {
            CalloutResult::Success(response) => {
                return Ok(success_to_rejection(response));
            },
            CalloutResult::Failed | CalloutResult::Rejected(_) => {
                warn!(target = %target.url, "provider target unavailable, trying next");
            },
        }
    }

    Ok(FilterAction::Reject(
        Rejection::status(502)
            .with_header("content-type", "text/plain")
            .with_body("all provider targets exhausted"),
    ))
}

// -----------------------------------------------------------------------------
// Helpers — Target Resolution
// -----------------------------------------------------------------------------

/// Build a [`ResolvedTarget`] from parsed config.
fn resolve_target(target: TargetConfig, cb: Option<&CircuitBreakerCfg>) -> Result<ResolvedTarget, FilterError> {
    let callout_config = CalloutConfig {
        circuit_breaker: cb.map(|c| CircuitBreakerConfig {
            consecutive_failures: c.consecutive_failures,
            recovery_window_ms: c.recovery_window_ms,
        }),
        failure_mode: FailureMode::Open,
        max_depth: 1,
        pool_max_idle_per_host: 4,
        status_on_error: 502,
        timeout_ms: target.timeout_ms,
    };

    let client =
        CalloutClient::new(callout_config).map_err(|e| -> FilterError { format!("provider_fallback: {e}").into() })?;
    let headers = resolve_headers(&target.headers)?;

    Ok(ResolvedTarget {
        client,
        url: target.url,
        model_override: target.model_override,
        headers,
    })
}

/// Convert string header pairs to typed [`HeaderName`]/[`HeaderValue`].
fn resolve_headers(headers: &HashMap<String, String>) -> Result<Vec<(HeaderName, HeaderValue)>, FilterError> {
    headers
        .iter()
        .map(|(name, value)| {
            let hn = HeaderName::try_from(name.as_str()).map_err(|e| -> FilterError {
                format!("provider_fallback: invalid header name '{name}': {e}").into()
            })?;
            let hv = HeaderValue::try_from(value.as_str())
                .map_err(|e| -> FilterError { format!("provider_fallback: invalid header value: {e}").into() })?;
            Ok((hn, hv))
        })
        .collect()
}

// -----------------------------------------------------------------------------
// Helpers — Request Building
// -----------------------------------------------------------------------------

/// Extract forwarded headers from the original request context.
fn collect_forward_headers(ctx: &HttpFilterContext<'_>) -> Vec<(HeaderName, HeaderValue)> {
    let mut headers = Vec::new();

    for name in &[http::header::CONTENT_TYPE, http::header::ACCEPT] {
        if let Some(value) = ctx.request.headers.get(name) {
            headers.push((name.clone(), value.clone()));
        }
    }

    if let Some(id) = ctx.request.headers.get("x-request-id") {
        headers.push((HeaderName::from_static("x-request-id"), id.clone()));
    }

    headers
}

/// Apply model override to the request body, if configured.
fn prepare_body(body: &[u8], model_override: Option<&str>) -> Vec<u8> {
    let Some(model) = model_override else {
        return body.to_vec();
    };

    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(body) else {
        warn!("model_override set but body is not valid JSON; forwarding as-is");
        return body.to_vec();
    };

    if let Some(obj) = value.as_object_mut() {
        obj.insert("model".to_owned(), serde_json::Value::String(model.to_owned()));
    }

    serde_json::to_vec(&value).unwrap_or_else(|_| body.to_vec())
}

/// Build a [`CalloutRequest`] for a single target.
fn build_callout_request(
    method: &http::Method,
    url: &str,
    forward_headers: &[(HeaderName, HeaderValue)],
    target_headers: &[(HeaderName, HeaderValue)],
    body: Vec<u8>,
) -> CalloutRequest {
    let mut headers: Vec<(HeaderName, HeaderValue)> = forward_headers.to_vec();
    headers.extend(target_headers.iter().cloned());

    CalloutRequest {
        body: Some(body),
        depth: 0,
        headers,
        method: method.clone(),
        url: url.to_owned(),
    }
}

// -----------------------------------------------------------------------------
// Helpers — Response Mapping
// -----------------------------------------------------------------------------

/// Convert a successful [`CalloutResponse`] into a terminal rejection.
fn success_to_rejection(response: CalloutResponse) -> FilterAction {
    let mut rejection = Rejection::status(response.status);

    for (name, value) in &response.headers {
        if let Ok(v) = value.to_str() {
            rejection = rejection.with_header(name.as_str(), v);
        }
    }

    if !response.body.is_empty() {
        rejection = rejection.with_body(response.body);
    }

    FilterAction::Reject(rejection)
}

// -----------------------------------------------------------------------------
// Filter Registration
// -----------------------------------------------------------------------------

/// Register this crate's filters into a Praxis [`FilterRegistry`].
///
/// Called automatically by the Praxis build-time filter discovery
/// system. Can also be called manually for testing or custom
/// server builds.
///
/// # Panics
///
/// Panics if `provider_fallback` collides with an already-registered
/// filter (built-in or from another external crate).
///
/// [`FilterRegistry`]: praxis_filter::FilterRegistry
#[expect(clippy::panic, reason = "duplicate filter name is a fatal startup error")]
pub fn register_filters(registry: &mut praxis_filter::FilterRegistry) {
    registry
        .register(
            "provider_fallback",
            praxis_filter::FilterFactory::Http(std::sync::Arc::new(move |config: &serde_yaml::Value| {
                ProviderFallbackFilter::from_config(config)
            })),
        )
        .unwrap_or_else(|_| panic!("duplicate filter name: 'provider_fallback'"));
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    reason = "tests"
)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // from_config tests
    // -------------------------------------------------------------------------

    #[test]
    fn from_config_minimal() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            r#"
targets:
  - url: "http://localhost:11434/v1/chat/completions"
"#,
        )
        .unwrap();
        let filter = ProviderFallbackFilter::from_config(&yaml).unwrap();
        assert_eq!(filter.name(), "provider_fallback");
    }

    #[test]
    fn from_config_full() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            r#"
max_body_bytes: 2097152
circuit_breaker:
  consecutive_failures: 5
  recovery_window_ms: 60000
targets:
  - url: "http://localhost:11434/v1/chat/completions"
    timeout_ms: 10000
  - url: "https://openrouter.ai/api/v1/chat/completions"
    model_override: "deepseek/deepseek-chat"
    timeout_ms: 30000
    headers:
      Authorization: "Bearer test-key"
"#,
        )
        .unwrap();
        let filter = ProviderFallbackFilter::from_config(&yaml).unwrap();
        assert_eq!(filter.name(), "provider_fallback");
    }

    #[test]
    fn from_config_rejects_empty_targets() {
        let yaml: serde_yaml::Value = serde_yaml::from_str("targets: []").unwrap();
        let err = ProviderFallbackFilter::from_config(&yaml);
        assert!(err.is_err(), "empty targets should be rejected");
    }

    #[test]
    fn from_config_rejects_missing_targets() {
        let yaml: serde_yaml::Value = serde_yaml::from_str("max_body_bytes: 1024").unwrap();
        let err = ProviderFallbackFilter::from_config(&yaml);
        assert!(err.is_err(), "missing targets should be rejected");
    }

    #[test]
    fn from_config_rejects_unknown_fields() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            r#"
targets:
  - url: "http://localhost:11434/v1/chat/completions"
bogus_field: true
"#,
        )
        .unwrap();
        let err = ProviderFallbackFilter::from_config(&yaml);
        assert!(err.is_err(), "unknown fields should be rejected");
    }

    // -------------------------------------------------------------------------
    // Body mode declarations
    // -------------------------------------------------------------------------

    #[test]
    fn body_mode_is_stream_buffer() {
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            r#"
targets:
  - url: "http://localhost:11434/v1/chat/completions"
"#,
        )
        .unwrap();
        let filter = ProviderFallbackFilter::from_config(&yaml).unwrap();

        assert_eq!(filter.request_body_access(), BodyAccess::ReadWrite);
        assert!(
            matches!(
                filter.request_body_mode(),
                BodyMode::StreamBuffer {
                    max_bytes: Some(DEFAULT_MAX_BODY_BYTES)
                }
            ),
            "body mode should be StreamBuffer with default limit"
        );
        assert!(filter.needs_request_context());
    }

    // -------------------------------------------------------------------------
    // Model override
    // -------------------------------------------------------------------------

    #[test]
    fn prepare_body_without_override_clones() {
        let body = br#"{"model":"gpt-4","messages":[]}"#;
        let result = prepare_body(body, None);
        assert_eq!(result, body.to_vec());
    }

    #[test]
    fn prepare_body_with_override_rewrites_model() {
        let body = br#"{"model":"gpt-4","messages":[]}"#;
        let result = prepare_body(body, Some("llama3"));
        let parsed: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["model"], "llama3");
        // Other fields preserved
        assert!(parsed["messages"].is_array());
    }

    #[test]
    fn prepare_body_override_adds_model_when_absent() {
        let body = br#"{"messages":[]}"#;
        let result = prepare_body(body, Some("llama3"));
        let parsed: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(parsed["model"], "llama3");
    }

    #[test]
    fn prepare_body_override_with_invalid_json_passes_through() {
        let body = b"not json";
        let result = prepare_body(body, Some("llama3"));
        assert_eq!(result, body.to_vec(), "invalid JSON should pass through");
    }

    // -------------------------------------------------------------------------
    // Registration
    // -------------------------------------------------------------------------

    #[test]
    fn register_filters_adds_provider_fallback() {
        let mut registry = praxis_filter::FilterRegistry::with_builtins();
        register_filters(&mut registry);
        assert!(
            registry.available_filters().contains(&"provider_fallback"),
            "provider_fallback should be registered"
        );
    }

    #[test]
    fn registered_filter_creates_from_valid_config() {
        let mut registry = praxis_filter::FilterRegistry::with_builtins();
        register_filters(&mut registry);
        let yaml: serde_yaml::Value = serde_yaml::from_str(
            r#"
targets:
  - url: "http://localhost:11434/v1/chat/completions"
"#,
        )
        .unwrap();
        let result = registry.create("provider_fallback", &yaml);
        assert!(result.is_ok(), "should create from valid config");
    }
}
