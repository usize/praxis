# Experiment: Provider Fallback Filter

**Status:** Prototype
**Branch:** `experiment/fallback-filter`
**Crate:** `filter/provider-fallback/`
**Related issues:** #279, #281, #549 (full cross-provider failover), #213 (unified types)

## Motivation

Praxis has no provider fallback mechanism. The full cross-provider
failover design (#279, #281) requires unified AI types (#213) and
request/response translation between API formats. That work is
unstarted.

The simpler case is useful now: same API format, different endpoint,
different credentials, optional model name swap. Examples:

- Local Ollama primary, OpenRouter fallback
- Self-hosted vLLM primary, cloud provider fallback
- Multiple OpenAI-compatible providers for redundancy

## Design

The `provider_fallback` filter is a **terminal filter** (like
`static_response`). It owns upstream communication entirely via
`CalloutClient` and returns the response directly as a `Reject`
action. No router, load balancer, or credential injection needed.

The filter runs last in the chain so policy filters execute first:

```
auth -> rate_limit -> guardrails -> prompt_enrich -> provider_fallback
                                                          |
                                                    for target in targets:
                                                      callout(target) -> 2xx? -> return response
                                                      callout(target) -> fail? -> try next
                                                    all failed -> 502
```

Each target gets its own `CalloutClient` with its own circuit breaker.
After N consecutive failures, the circuit opens and the client returns
`Failed` immediately (no network request). After `recovery_window_ms`,
one probe request is allowed through (half-open state).

### Config

```yaml
- filter: provider_fallback
  targets:
    - url: "http://192.168.1.50:11434/v1/chat/completions"
      timeout_ms: 10000
    - url: "https://openrouter.ai/api/v1/chat/completions"
      model_override: "deepseek/deepseek-chat"
      timeout_ms: 30000
      headers:
        Authorization: "Bearer sk-or-..."
  max_body_bytes: 10485760  # 10 MiB
  circuit_breaker:
    consecutive_failures: 3
    recovery_window_ms: 30000
```

### Key implementation details

- **Body buffering:** Uses `StreamBuffer` mode to collect the full
  request body before making callouts. Required for retry (same body
  sent to multiple targets).
- **Model override:** Per-target `model_override` parses the JSON
  body, sets the `model` field, and re-serializes. Non-JSON bodies
  are forwarded as-is with a warning.
- **Header forwarding:** `content-type`, `accept`, and `x-request-id`
  are forwarded from the original request. Each target can add its
  own static headers (e.g. `Authorization`).
- **Circuit breaker:** Shared config applied to each target's
  `CalloutClient` independently. Uses `FailureMode::Open` so
  failures return `Failed` (not `Rejected`), enabling the filter
  to try the next target.
- **Registration:** Auto-discovered via `[package.metadata.praxis-filters]`
  in the crate's `Cargo.toml`. The server's `build.rs` generates
  the registration call.

## Known limitations

1. **Terminal filter** -- replaces the router/load_balancer/upstream
   path entirely. Cannot compose with normal upstream routing for
   these targets.

2. **Same API format only** -- no request/response translation
   between providers. OpenAI-to-Anthropic would require #213/#281.

3. **Non-streaming** -- the callout client buffers the full provider
   response before returning it. SSE/streaming responses are received
   in full, then sent to the client as a single body. Streaming
   passthrough would require response source replacement.

4. **All request bodies buffered** -- `StreamBuffer` mode buffers
   every request body even when the first target succeeds immediately.

5. **Sequential targets** -- tries targets in order. No parallel
   speculation.

6. **No Pingora upstream features** -- connection pooling, HTTP/2
   multiplexing, and Pingora's optimized forwarding path are not
   used. The `reqwest` client in `CalloutClient` handles connections
   independently.

7. **All non-2xx responses trigger fallback** -- the `CalloutClient`
   treats any non-2xx as a failure. A 400 (bad request) from the
   first provider will cause a fallback attempt to the second
   provider, which may also return 400. Ideally, only 5xx and
   connection errors should trigger fallback.

## What this validates

- External filter crate auto-discovery works end-to-end
- `CalloutClient` is usable for multi-target retry patterns
- Terminal filter pattern (body buffering + `Reject` with response)
  is viable for proxy-owned upstream communication

## Path to production

If this experiment proves useful, the production version would:

1. Use Pingora's upstream connection pool instead of `reqwest`
2. Distinguish retriable errors (5xx, timeout) from client errors (4xx)
3. Support streaming response passthrough
4. Integrate with the unified AI types (#213) for cross-format translation
5. Move into the main filter crate as a builtin
