---
issue: https://github.com/praxis-proxy/ai/issues/249
discussion: https://github.com/orgs/praxis-proxy/discussions/761
status: proposed
authors:
  - usize
graduation_criteria:
  - Provider fallback with streaming response 
  - Body buffering strategy for replay across targets
stakeholders:
  - shaneutt
  - leseb
  - franciscojavierarceo
---

# Provider Fallback

## What?

A `provider_fallback` filter that tries an ordered list of
provider targets for the same request, returning the first
successful response. Each target bundles endpoint, model
name, credentials, and timeout into a single object — the
equivalent of the "Provider" primitive found in AI gateways.

An experimental prototype exists on the
[`experiment/fallback-filter`][proto-branch] branch. This proposal covers
the path from prototype to production, focusing on the two
structural limitations that bound what is possible today.

### Goals

- Same-format provider failover (vLLM primary,
  OpenRouter fallback, etc.) without `router`/`load_balancer`
- Per-target circuit breakers for passive health tracking
- Model name rewriting per target
- Clear path toward streaming response fallback

### Non-Goals

- Mid-stream fallbacks - after bytes are committed downstream
  we must pin to the initial provider to avoid breaking tool
  calling, chain-of-thought etc...


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

The prototype on [`experiment/fallback-filter`][proto-branch] validates
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
model override, timeout) with its own circuit breaker.

### What we lack

**1. Streaming response ownership.** The prototype buffers
the full provider response via `CalloutClient` (reqwest)
before returning it to the client. This works but defeats
streaming — the client receives the complete response as a
single body, not as incremental SSE chunks. For streaming
to work, provider targets need to use Pingora's upstream
machinery instead.

`CalloutClient` must remain a separate primitive with its
own connection pool — its core use case is side-channel
callouts to third-party services (guardrails, auth
endpoints) where isolation from the provider connection
pool is a security property. Provider fallback targets
should use Pingora's native upstream dispatch, gaining
connection pooling, HTTP/2, TLS, backpressure, and
streaming for free.

This is the same "response source replacement" capability
identified as a non-goal in the http_callout proposal
(#358) and described in discussion #87.

**2. Body buffering for replay.** The prototype uses
`StreamBuffer` to buffer the full request body for retry
across targets. This is correct — without the buffered
body, fallback to a second target is impossible. But it
means every request through the filter is fully buffered
regardless of whether fallback occurs. The
`max_body_bytes` limit (default 10 MiB) bounds memory, but
large-context AI requests can exceed this.

This is not unique to Praxis. AgentGateway buffers
request bodies for replay via. Envoy AI Gateway's ext_proc uses
`BUFFERED` mode because the original 32 KiB default proved
inadequate and was raised to 50 MiB in v0.2.0.
Request buffering is the universal cost of fallback.

### The streaming fallback boundary

Every byte-level proxy shares the structural constraint that once
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
open correctness bugs ([#27967][ll-27967], [#18229][ll-18229],
[#19077][ll-19077], [#25492][ll-25492]).

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
  llama-server instance as primary with a cloud provider as
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

[proto-branch]: https://github.com/usize/praxis/tree/experiment/fallback-filter
[ll-27967]: https://github.com/BerriAI/litellm/issues/27967
[ll-18229]: https://github.com/BerriAI/litellm/issues/18229
[ll-19077]: https://github.com/BerriAI/litellm/issues/19077
[ll-25492]: https://github.com/BerriAI/litellm/issues/25492
