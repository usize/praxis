# Provider Fallback Filter — Demo

Praxis experiment: ordered provider failover using `CalloutClient`.
Branch: `experiment/fallback-filter`

## Config

Two targets: local Ollama primary, OpenRouter cloud fallback.
Each target bundles endpoint, model name, credentials, and timeout.

```yaml
listeners:
  - name: ai
    address: "127.0.0.1:8070"
    filter_chains:
      - inference

filter_chains:
  - name: inference
    filters:
      - filter: provider_fallback
        max_body_bytes: 1048576
        circuit_breaker:
          consecutive_failures: 3
          recovery_window_ms: 30000
        targets:
          - url: "http://127.0.0.1:11434/v1/chat/completions"
            model_override: "qwen2.5:3b"
            timeout_ms: 30000
          - url: "https://openrouter.ai/api/v1/chat/completions"
            model_override: "openrouter/auto"
            timeout_ms: 60000
            headers:
              Authorization: "Bearer $OPENROUTER_API_KEY"
```

## Test 1: Happy path (Ollama responds)

```
$ curl -s http://127.0.0.1:8070/v1/chat/completions \
    -H 'Content-Type: application/json' \
    -H 'X-Request-Id: demo-001' \
    -d '{"model":"any","messages":[{"role":"user","content":"What is 2+2? Answer in one word."}]}'
```

### Response

```json
{
  "id": "chatcmpl-538",
  "object": "chat.completion",
  "created": 1782705070,
  "model": "qwen2.5:3b",
  "system_fingerprint": "fp_ollama",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "Four"
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 41,
    "completion_tokens": 2,
    "total_tokens": 43
  }
}
```

### Proxy logs

```
DEBUG provider_fallback: dispatching request  request_id="demo-001" method=POST body_bytes=89 target_count=2
DEBUG provider_fallback: trying target        request_id="demo-001" target_index=0 target=http://127.0.0.1:11434/v1/chat/completions
 INFO provider_fallback: target responded successfully  request_id="demo-001" target_index=0 target=http://127.0.0.1:11434/v1/chat/completions status=200 response_bytes=288
```

Target 0 (Ollama) succeeded. Target 1 (OpenRouter) was never tried.

## Test 2: Fallback (primary down, OpenRouter responds)

Same config but target 0 points to a dead port (simulating Ollama offline).

```
$ curl -s http://127.0.0.1:8070/v1/chat/completions \
    -H 'Content-Type: application/json' \
    -H 'X-Request-Id: demo-002' \
    -d '{"model":"any","messages":[{"role":"user","content":"What is 2+2? Answer in one word."}]}'
```

### Response

```json
{
  "id": "gen-1782705100-s9CO68EQCTub4BakBFzz",
  "object": "chat.completion",
  "created": 1782705101,
  "model": "google/gemini-3.5-flash-20260519",
  "provider": "Google",
  "choices": [
    {
      "index": 0,
      "finish_reason": "stop",
      "message": {
        "role": "assistant",
        "content": "Four"
      }
    }
  ],
  "usage": {
    "prompt_tokens": 12,
    "completion_tokens": 109,
    "total_tokens": 121
  }
}
```

### Proxy logs

```
DEBUG provider_fallback: dispatching request  request_id="demo-002" method=POST body_bytes=89 target_count=2
DEBUG provider_fallback: trying target        request_id="demo-002" target_index=0 target=http://127.0.0.1:19999/v1/chat/completions
 WARN callout request failed                  reason="error sending request for url (http://127.0.0.1:19999/v1/chat/completions)"
 WARN provider_fallback: target failed        request_id="demo-002" target_index=0 target=http://127.0.0.1:19999/v1/chat/completions
DEBUG provider_fallback: trying target        request_id="demo-002" target_index=1 target=https://openrouter.ai/api/v1/chat/completions
 INFO provider_fallback: target responded successfully  request_id="demo-002" target_index=1 target=https://openrouter.ai/api/v1/chat/completions status=200 response_bytes=1610
```

Target 0 failed immediately (connection refused). Target 1 (OpenRouter auto-router) selected Gemini 3.5 Flash and returned "Four".

## How it works

```
client ──POST──► Praxis (:8070)
                   │
                   ├─ policy filters run first (auth, rate limit, etc.)
                   │
                   └─ provider_fallback (terminal filter)
                        │
                        ├─ buffer full request body (StreamBuffer mode)
                        │
                        ├─ for each target:
                        │    ├─ rewrite "model" field in JSON body
                        │    ├─ attach target headers (Authorization, etc.)
                        │    ├─ CalloutClient.execute() with circuit breaker
                        │    │
                        │    ├─ 2xx? → return response to client (done)
                        │    └─ fail? → log, try next target
                        │
                        └─ all failed → 502 "all provider targets exhausted"
```

## Key design points

- **Terminal filter**: owns the upstream request entirely via `CalloutClient`. No router, load_balancer, or credential_injection needed.
- **Provider bundling**: each target is a self-contained provider definition (URL + model + credentials + timeout + circuit breaker). This avoids the complexity of coordinating rollback across separate routing/credential/model primitives.
- **Model override**: the `"model"` field in the JSON body is rewritten per-target, so clients send a single request and each provider gets the correct model name.
- **Circuit breaker**: per-target, passive health tracking. After N consecutive failures, the circuit opens and the target is skipped immediately (no network request) until the recovery window expires.
- **Auto-discovered**: the crate uses `[package.metadata.praxis-filters]` for build-time registration. Adding the crate as a server dependency is sufficient to make the filter available in YAML config.

## Known limitations

1. **Non-streaming**: full provider response is buffered before returning to client. SSE/streaming responses are received in full, then sent as a single body.
2. **Same API format only**: no request/response translation between providers.
3. **All non-2xx triggers fallback**: the `CalloutClient` treats any non-2xx (including 400 Bad Request) as a failure. Ideally only 5xx and connection errors should trigger fallback.
4. **Request body always buffered**: `StreamBuffer` mode buffers every request body, even when the first target succeeds immediately.

## Branch

```
df71f3e proposal: provider fallback filter
4be5029 fix(filter): address lint and review findings
6af2c8e fix(filter): use openrouter/auto model in test harness
854b351 fix(filter): update test harness for qwen2.5:3b and OpenRouter
a7d4b9c feat(filter): add provider_fallback local test harness
badc1d2 feat(filter): add structured logging to provider_fallback
495ca92 docs: add provider fallback experiment writeup
36a0b6c feat(filter): add provider_fallback filter crate
```
