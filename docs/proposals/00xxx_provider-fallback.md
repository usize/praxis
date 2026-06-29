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

> **Note:** Submit What? and Why? first. Add How? in a
> follow-up PR after the proposal direction is accepted.
