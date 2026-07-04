---
issue: # TBD
discussion: # TBD
status: proposed
authors:
  - usize
graduation_criteria:
  - Streaming response ownership for http_callout
  - Body buffering strategy for replay across targets
stakeholders:
  - shaneutt
---

# Provider Fallback

## What?

A `provider_fallback` filter that tries an ordered list of
provider targets for the same request, returning the first
successful response. Each target bundles endpoint, model
name, credentials, and timeout into a single object — the
equivalent of the "Provider" primitive found in AI gateways.

An experimental prototype exists on the
`experiment/fallback-filter` branch. This proposal covers
the path from prototype to production, focusing on the two
structural limitations that bound what is possible today.

### Goals

- Same-format provider failover (Ollama primary,
  OpenRouter fallback, etc.) without router/load_balancer
- Per-target circuit breakers for passive health tracking
- Model name rewriting per target
- Clear path toward streaming response fallback

## Why?

### Motivation

Praxis has no provider fallback mechanism. The full
cross-provider failover design (#279, #281) requires
unified AI types (#213) and request/response translation
between API formats — work that is unstarted.

The simpler case is useful now: same API format, different
endpoint, different credentials, optional model name swap.
This covers local-first setups (Ollama primary, cloud
fallback), multi-provider redundancy, and cost tiering
(free tier primary, paid fallback).

The prototype on `experiment/fallback-filter` validates
the approach: an external filter crate using `CalloutClient`
(from the http_callout proposal, #358) to make requests to
an ordered target list. It works for non-streaming requests
today. Two structural limitations prevent it from reaching
parity with competing gateways.

### The Provider bundling problem

AI gateways universally bundle backend, credentials, and
model into a single object. LiteLLM's `model_list` rows,
AgentGateway's `AgentgatewayBackend` CRD, and even Envoy
AI Gateway (which splits across three CRDs for persona
separation) all tie these together because fallback
requires atomic substitution of all three.

Praxis splits these across orthogonal primitives: `router`
selects clusters, `credential_injection` attaches auth,
`model_rewrite` swaps model names. This separation of
concerns is clean for single-provider pipelines but creates
extreme complexity for fallback — a failed target requires
coordinated rollback of cluster selection, credential
injection, and model rewriting, none of which were designed
for retry.

The `provider_fallback` filter sidesteps this by owning
the full request lifecycle for its targets. Each target
is a self-contained provider definition (URL, headers,
model override, timeout) with its own `CalloutClient` and
circuit breaker.

### What we lack

**1. Streaming response ownership.** The prototype buffers
the full provider response via `CalloutClient` before
returning it to the client. This works but defeats
streaming — the client receives the complete response as a
single body, not as incremental SSE chunks. For streaming
to work, `CalloutClient` (or a new response-source
primitive) needs to own the downstream response stream:
connect to the provider, relay chunks as they arrive, and
only trigger fallback if the connection fails before
committing the first byte.

This is the same "response source replacement" capability
identified as a non-goal in the http_callout proposal
(#358) and described in discussion #87. It is the
necessary next step: a filter that can replace the upstream
response with the result of its own outbound request, with
chunk-level streaming.

**2. Body buffering for replay.** The prototype uses
`StreamBuffer` to buffer the full request body for retry
across targets. This is correct — without the buffered
body, fallback to a second target is impossible. But it
means every request through the filter is fully buffered
regardless of whether fallback occurs. The
`max_body_bytes` limit (default 10 MiB) bounds memory, but
large-context AI requests can exceed this.

This is not unique to Praxis. AgentGateway buffers
request bodies for replay via `peekbody.rs`/`buflist.rs`
(v1.3.0 added explicit traffic-policy request buffering,
~1 MiB default). Envoy AI Gateway's ext_proc uses
`BUFFERED` mode — the original 32 KiB default was
famously inadequate and was raised to 50 MiB in v0.2.0.
Request buffering is the universal cost of fallback.

### The streaming fallback boundary

Every byte-level proxy — Praxis, AgentGateway, Envoy AI
Gateway — shares the same structural constraint: once
the first response byte is committed downstream, fallback
is impossible. This is codified in the Kubernetes Gateway
API retry spec (GEP-1731: "Passing a request to the next
server is only possible if nothing has been sent to a
client yet") and in Envoy's `upstream_reset_after_response_started`
behavior.

LiteLLM is the only gateway that attempts mid-stream
fallback, and it does so by operating one layer up: it
terminates the failed stream, opens a fresh request to a
different model with the partial output as an assistant
prefill continuation, and re-emits a single logical
stream. This is application-layer stream re-synthesis, not
transparent byte-proxy failover, and it carries real costs:
re-billed tokens, persona contamination, dependence on the
fallback model supporting assistant prefill, and a trail of
open correctness bugs (#27967, #18229, #19077, #25492).

For Praxis, the achievable and correct target is:

1. **Pre-first-byte fallback** (current prototype) —
   buffer response headers, inspect status, fail over
   before committing any bytes downstream. This captures
   the vast majority of provider failures (connection
   errors, timeouts, 429, 5xx).

2. **Streaming relay after commit** — once a target
   succeeds and the first chunk is relayed, the filter
   is committed. Stream failures after this point
   propagate to the client.

3. **Mid-stream re-synthesis** (future, opt-in) —
   LiteLLM-style continuation prompting, gated behind an
   explicit config flag and a per-model
   `supports_assistant_prefill` capability. Only worth
   pursuing if operational data shows a material fraction
   of failures occur after first byte.

### User Stories

- As an AI gateway operator, I want to configure a local
  Ollama instance as primary with a cloud provider as
  fallback, without changing my client integration.
- As a platform engineer, I want per-target circuit
  breakers so that a failing provider is skipped
  immediately without adding latency.
- As an SRE, I want streaming responses from the
  provider to be relayed incrementally, not buffered in
  full before delivery.

## How?

### Core design insight

The pipeline's contract changes from "mutate shared state"
to "emit a plan." Each target's complete upstream request is
derived as a pure function of (original request + target
config). Nothing is shared between attempts, so nothing
needs rollback. The pipeline keeps its invariant: every
filter executes at most once, forward-only.

### Requirements

1. **Prepared targets** — the filter builds each target's
   complete upstream state (peer, headers, body with model
   rewritten) from the original request snapshot,
   independently per target. No shared mutable state
   between attempts.

2. **Pingora-native dispatch** — use Pingora's existing
   retry loop (`proxy_to_upstream` → `error_while_proxy` →
   retry check → `upstream_peer`). No standalone connector
   usage. Provider targets get connection pooling, HTTP/2,
   TLS, backpressure, and streaming for free.

3. **Response-header-based retry** — extend retry to cover
   retriable upstream statuses (5xx, 429) in addition to
   connection failures. Intercept in `response_filter`
   (before headers are committed downstream) and
   `error_while_proxy`.

4. **Streaming responses** — the winning target's response
   flows through Pingora's normal response pipeline.
   `response_body_filter` streams chunks to the client.
   Response-phase filters (token counting, access logging)
   see the response normally.

5. **No pipeline replay** — request-phase filters execute
   once. The fallback filter is the last provider-affecting
   step. Credential injection and model rewrite are NOT in
   the pipeline for fallback routes — the plan carries
   everything.

6. **Callout/provider separation** — `CalloutClient`
   (reqwest) stays unchanged for side-channel callouts
   where isolation is a security property. Provider targets
   use Pingora's upstream machinery. Different primitives,
   not a config toggle.

### Design

#### New types

New types live in `filter/src/fallback.rs` or a similar
module-level file.

```rust
/// A complete, pre-built fallback plan emitted by the filter.
struct FallbackPlan {
    targets: Vec<PreparedTarget>,
    current: usize,                  // index of next target to try
    retry_on: Vec<u16>,              // retriable status codes [502, 503, 429]
    ttfb_timeout: Duration,          // per-attempt time-to-first-byte
    total_deadline: Instant,         // absolute deadline across all attempts
    attempts: Vec<Attempt>,          // observability: what happened per target
}

/// A target with everything pre-resolved. Pure function of
/// (original request snapshot + target config).
struct PreparedTarget {
    upstream: Upstream,              // address, TLS, connection options
    extra_headers: Vec<(HeaderName, HeaderValue)>,  // Authorization, etc.
    body: Bytes,                     // with model field already rewritten
    model_name: Option<String>,      // for observability
}

/// Record of one attempt for logging/metrics.
struct Attempt {
    target_index: usize,
    status: Option<u16>,             // None if connection failed
    duration: Duration,
}
```

`FallbackPlan` is stored in `ctx.extensions` via
[`RequestExtensions::insert`] (see
`filter/src/extensions.rs`). The type-map keying ensures
no collisions with other filters.

#### Filter changes (`provider_fallback` filter)

The filter (`filter/provider-fallback/src/lib.rs`) changes
from terminal (returning `FilterAction::Reject` with a
buffered response) to plan-emitting (returning
`FilterAction::Continue` after depositing a `FallbackPlan`
in extensions).

In `on_request_body` (when `end_of_stream`):

1. Parse the buffered JSON body once.
2. For each configured target: serialize the body with that
   target's model name, resolve headers, build `Upstream`
   from the target URL.
3. Build a `FallbackPlan` with all `PreparedTarget`s.
4. Insert it into `ctx.extensions`.
5. Set `ctx.cluster` and `ctx.upstream` to the FIRST target
   (so the normal `upstream_peer` path works on the initial
   attempt).
6. Set `ctx.extra_request_headers` for the first target's
   credentials.
7. Return `FilterAction::Continue` — let Pingora handle the
   upstream connection and response lifecycle.

The filter is no longer terminal. It sets up the plan and
lets the normal upstream lifecycle proceed. If the first
target succeeds, the plan is never consulted again.

#### Protocol layer changes

Three existing hooks and one new override are modified in
`protocol/src/http/pingora/handler/with_body.rs` (and
the shared utilities in `handler/mod.rs`).

**`upstream_peer`** (`handler/upstream_peer.rs`) — check
for `FallbackPlan`, advance to current target:

```text
if plan exists AND current > 0:
    set ctx.upstream from plan.targets[current]
    clear ctx.upstream_for_retry (force peer rebuild)
    apply per-target extra_headers to session
proceed with existing upstream_peer::execute()
```

This works because `upstream_peer::execute` already handles
the `ctx.upstream` → `ctx.upstream_for_retry` → `HttpPeer`
conversion (see `handler/upstream_peer.rs:29-43`). Clearing
`upstream_for_retry` forces it to re-read from the new
`ctx.upstream`.

**`response_filter`** (`handler/response_filter.rs`) —
intercept retriable statuses before headers are committed
downstream:

```text
run normal response pipeline
if plan exists AND status matches retry_on
   AND plan.has_next() AND !past deadline:
    record attempt
    return Err (retriable)
```

This must happen before `session.write_response_header`
commits the response to the client. The existing
`response_filter::execute` runs pipeline filters and syncs
headers but does not commit — Pingora does that after the
hook returns `Ok(())`. Returning `Err` prevents the commit.

**`error_while_proxy`** (new override in `with_body.rs`) —
advance plan on retriable error:

```text
if plan exists AND plan.has_next():
    advance plan.current
    set retry(true)
    return error
fall through to existing handle_connect_failure
```

This is a new `ProxyHttp` hook override. The existing
handler does not implement `error_while_proxy`; it uses
`fail_to_connect` for connection failures. The
`error_while_proxy` hook covers errors that occur after a
connection is established (including when `response_filter`
returns `Err` to trigger a retry on a retriable status).

**`fail_to_connect`** (`with_body.rs:183-191`) — same plan
advancement for connection-level failures:

```text
if plan exists AND plan.has_next():
    record attempt (connection failed)
    advance plan.current
    set retry(true)
    return error
fall through to existing handle_connect_failure logic
```

The existing `handle_connect_failure`
(`handler/mod.rs:227-258`) only retries idempotent requests
and enforces `RETRY_BODY_LIMIT` (64 KiB). For fallback
targets, the plan overrides both constraints — the filter
explicitly opted in by creating a plan, and the per-target
body is pre-built (not replayed from Pingora's retry
buffer).

**`upstream_request_filter`** (`with_body.rs:193-210`) —
apply per-target headers and body:

```text
existing hop-by-hop stripping, path rewrite, etc.
if plan exists:
    apply plan.targets[current].extra_headers
    (body already set via pre_read_body mechanism)
```

#### Body handling

The request body varies per target (model rewrite). Pingora's
retry replays from its internal retry buffer, but that buffer
holds the ORIGINAL body. For fallback, we need the per-target
body.

Approach: the `pre_read_body` mechanism already exists for
`StreamBuffer` mode (see
`handler/request_filter/stream_buffer.rs:84-211`). The filter
stores the per-target body in the plan. On retry,
`request_body_filter` checks the plan and substitutes the
current target's body into the `pre_read_body` queue
(`ctx.pre_read_body: Option<VecDeque<Bytes>>`, see
`context.rs:140`). This needs investigation — Pingora's retry
buffer behavior with `StreamBuffer` pre-read bodies may need
careful handling.

**This is the riskiest part of the design and should be
prototyped first.** See open question 1.

### Implementation sequence

**PR 1: Types and filter** — `FallbackPlan`,
`PreparedTarget`, updated `provider_fallback` filter that
emits a plan instead of making callouts. Unit tests for
plan construction, model rewriting, header resolution.

**PR 2: Protocol layer — connection failure fallback** —
`upstream_peer`, `fail_to_connect` changes. Integration
test with two backends, first refusing connections.

**PR 3: Protocol layer — response-header retry** —
`response_filter`, `error_while_proxy` changes. Integration
test with first backend returning 503, second returning 200.

**PR 4: Per-target body replay** — `request_body_filter` /
`upstream_request_filter` changes for model-rewritten
bodies. Integration test verifying different model fields
reach different backends.

### Open questions

1. **Pingora retry buffer + StreamBuffer interaction** —
   when `StreamBuffer` pre-reads the body and Pingora
   retries, does Pingora replay from its own buffer or from
   `pre_read_body`? If from its own buffer, the per-target
   body won't reach the retry target. The existing
   `request_body_filter` forwards chunks from
   `ctx.pre_read_body` (see
   `handler/request_body_filter.rs:44-52`), but it's
   unclear whether Pingora re-invokes `request_body_filter`
   on retry or replays from its internal 64 KiB buffer
   (`RETRY_BODY_LIMIT`, `handler/mod.rs:59`). Needs source
   reading and testing.

2. **Per-target extra_headers in upstream_request_filter** —
   the filter sets `ctx.extra_request_headers` for the first
   target during `on_request_body`. On retry for subsequent
   targets, those headers are stale. The protocol layer
   needs to swap them from the plan in
   `upstream_request_filter`. This is a variant of the
   credential leakage concern — but contained to the
   protocol layer (one place, not distributed across
   filters).

3. **Retry budget vs. Pingora's max_retries** — Pingora's
   retry loop has its own `max_retries` (128 by default).
   The `FallbackPlan` has its own budget (number of
   targets). These need to be coordinated — the plan budget
   should be the effective limit. The existing
   `handle_connect_failure` uses `MAX_RETRIES` (3, see
   `handler/mod.rs:52`) and a retry counter
   (`ctx.retries`, `context.rs:194`). The fallback plan
   should take over the retry counter when present.

4. **Non-idempotency** — inference POSTs aren't idempotent.
   The existing retry logic skips non-idempotent requests
   (`handler/mod.rs:228`). For fallback, we override this
   (the filter explicitly opts in by creating a plan). But
   double-billing is a real edge (provider processed, then
   500'd). The proposal should acknowledge this and
   recommend `Idempotency-Key` passthrough for providers
   that support it.
