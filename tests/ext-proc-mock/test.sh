#!/usr/bin/env bash
# ext_proc full-duplex integration test
#
# Orchestrates podman/docker containers:
#   1. fortio echo backend (port 3000)
#   2. mock EPP gRPC service (port 50051)
#   3. praxis proxy with ext-proc (port 8080, admin 9901)
#
# Runs test cases against the live stack and prints a summary.
#
# Usage:
#   ./tests/ext-proc-mock/test.sh
#   CONTAINER_ENGINE=docker ./tests/ext-proc-mock/test.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CONTAINER_ENGINE="${CONTAINER_ENGINE:-$(command -v podman 2>/dev/null || command -v docker 2>/dev/null || true)}"
if [[ -z "${CONTAINER_ENGINE}" ]]; then
    echo "ERROR: no container engine found (podman or docker)" >&2
    exit 1
fi

NETWORK="ext-proc-test"
BACKEND_NAME="ext-proc-backend"
EPP_NAME="ext-proc-epp"
PRAXIS_NAME="ext-proc-praxis"
EPP_IMAGE="praxis-ext-proc-mock:test"
PRAXIS_IMAGE="praxis-ext-proc:test"

PASSED=0
FAILED=0
TESTS_RUN=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

cleanup() {
    echo ""
    echo "=== Cleanup ==="
    ${CONTAINER_ENGINE} rm -f "${PRAXIS_NAME}" "${EPP_NAME}" "${BACKEND_NAME}" 2>/dev/null || true
    ${CONTAINER_ENGINE} network rm "${NETWORK}" 2>/dev/null || true
}

trap cleanup EXIT

wait_for_healthy() {
    local name="$1"
    local url="$2"
    local max_attempts="${3:-30}"
    local attempt=0

    while [[ ${attempt} -lt ${max_attempts} ]]; do
        if ${CONTAINER_ENGINE} exec "${name}" wget -qO- "${url}" >/dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    echo "ERROR: ${name} did not become healthy after ${max_attempts}s" >&2
    ${CONTAINER_ENGINE} logs "${name}" 2>&1 | tail -20 >&2
    return 1
}

wait_for_port() {
    local name="$1"
    local port="$2"
    local max_attempts="${3:-30}"
    local attempt=0

    while [[ ${attempt} -lt ${max_attempts} ]]; do
        if ${CONTAINER_ENGINE} exec "${name}" sh -c "wget -qO- http://127.0.0.1:${port}/ 2>/dev/null || true" 2>&1 | grep -q .; then
            return 0
        fi
        # Also try just checking if the process is running
        if ${CONTAINER_ENGINE} inspect --format='{{.State.Running}}' "${name}" 2>/dev/null | grep -q true; then
            sleep 1
            attempt=$((attempt + 1))
            continue
        fi
        attempt=$((attempt + 1))
        sleep 1
    done

    return 0
}

run_test() {
    local name="$1"
    shift
    TESTS_RUN=$((TESTS_RUN + 1))
    echo ""
    echo "--- Test: ${name} ---"
    if "$@"; then
        echo "  PASS: ${name}"
        PASSED=$((PASSED + 1))
    else
        echo "  FAIL: ${name}"
        FAILED=$((FAILED + 1))
    fi
}

curl_proxy() {
    ${CONTAINER_ENGINE} exec "${PRAXIS_NAME}" wget -qO- --header="$@" 2>&1 || true
}

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

echo "=== Building containers ==="

# Podman requires --ignorefile to override .containerignore;
# Docker BuildKit auto-discovers <Containerfile>.dockerignore files.
IS_PODMAN=""
if echo "${CONTAINER_ENGINE}" | grep -q podman; then
    IS_PODMAN=1
fi

echo "Building mock EPP image..."
${CONTAINER_ENGINE} build -t "${EPP_IMAGE}" \
    ${IS_PODMAN:+--ignorefile "${SCRIPT_DIR}/Containerfile.dockerignore"} \
    -f "${SCRIPT_DIR}/Containerfile" "${REPO_ROOT}"

echo "Building praxis ext-proc image..."
${CONTAINER_ENGINE} build -t "${PRAXIS_IMAGE}" \
    ${IS_PODMAN:+--ignorefile "${REPO_ROOT}/Containerfile.ext-proc.dockerignore"} \
    -f "${REPO_ROOT}/Containerfile.ext-proc" "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# Network + Backend
# ---------------------------------------------------------------------------

echo ""
echo "=== Starting infrastructure ==="

${CONTAINER_ENGINE} network create "${NETWORK}" 2>/dev/null || true

# Start fortio echo backend on port 3000
echo "Starting backend (fortio echo)..."
${CONTAINER_ENGINE} run -d --name "${BACKEND_NAME}" \
    --network "${NETWORK}" \
    docker.io/fortio/fortio:latest \
    server -echo-server-default-params="status=200" -http-port=3000

# Wait for backend
sleep 2

# ---------------------------------------------------------------------------
# Start EPP in passthrough mode
# ---------------------------------------------------------------------------

echo "Starting mock EPP (passthrough mode)..."
${CONTAINER_ENGINE} run -d --name "${EPP_NAME}" \
    --network "${NETWORK}" \
    "${EPP_IMAGE}" --port 50051 --mode passthrough

sleep 2

# ---------------------------------------------------------------------------
# Praxis config (patched for container networking)
# ---------------------------------------------------------------------------

# Create a patched config that uses container hostnames instead of 127.0.0.1
PRAXIS_CONFIG=$(cat <<'YAML'
listeners:
  - name: ext-proc-gateway
    address: "0.0.0.0:8080"
    filter_chains:
      - ext-proc-pipeline

filter_chains:
  - name: ext-proc-pipeline
    filters:
      - filter: ext_proc
        target: "http://EPPHOST:50051"
        message_timeout_ms: 5000
        processing_mode:
          request_header_mode: send
          response_header_mode: send
          request_body_mode: full_duplex_streamed
          response_body_mode: full_duplex_streamed
      - filter: router
        routes:
          - path_prefix: "/"
            cluster: backend
      - filter: load_balancer
        clusters:
          - name: backend
            endpoints:
              - "BACKENDHOST:3000"

admin:
  address: "0.0.0.0:9901"

insecure_options:
  allow_public_admin: true
YAML
)

# Replace hostnames with container names
PRAXIS_CONFIG="${PRAXIS_CONFIG//EPPHOST/${EPP_NAME}}"
PRAXIS_CONFIG="${PRAXIS_CONFIG//BACKENDHOST/${BACKEND_NAME}}"

# Write config to a temp file and copy into container.
# Use `podman cp` instead of a volume mount so the config is
# available inside the VM on macOS (host tmp paths are not
# always shared with the Podman VM).
TMPCONFIG=$(mktemp)
echo "${PRAXIS_CONFIG}" > "${TMPCONFIG}"

echo "Starting praxis (ext-proc enabled)..."
${CONTAINER_ENGINE} create --name "${PRAXIS_NAME}" \
    --network "${NETWORK}" \
    "${PRAXIS_IMAGE}"
${CONTAINER_ENGINE} cp "${TMPCONFIG}" "${PRAXIS_NAME}:/etc/praxis/config.yaml"
${CONTAINER_ENGINE} start "${PRAXIS_NAME}"
rm -f "${TMPCONFIG}"

# Wait for praxis health check
echo "Waiting for praxis to become healthy..."
sleep 3
wait_for_healthy "${PRAXIS_NAME}" "http://127.0.0.1:9901/healthy" 30

echo "All services running."

# ---------------------------------------------------------------------------
# Test Cases (passthrough mode)
# ---------------------------------------------------------------------------

echo ""
echo "=== Running tests (passthrough mode) ==="

test_get_with_epp_headers() {
    local resp
    resp=$(${CONTAINER_ENGINE} exec "${PRAXIS_NAME}" \
        wget -qS -O /dev/null http://127.0.0.1:8080/ 2>&1) || true

    if echo "${resp}" | grep -qi "x-epp-served-by"; then
        return 0
    fi
    echo "  Expected x-epp-served-by header in response"
    echo "  Got: ${resp}"
    return 1
}

test_post_with_routing_headers() {
    local resp
    resp=$(${CONTAINER_ENGINE} exec "${PRAXIS_NAME}" \
        wget -qS -O /dev/null \
        --post-data='{"model":"llama3","prompt":"hello"}' \
        --header="Content-Type: application/json" \
        http://127.0.0.1:8080/ 2>&1) || true

    local pass=true
    if ! echo "${resp}" | grep -qi "x-epp-served-by"; then
        echo "  Missing x-epp-served-by header"
        pass=false
    fi
    if ${pass}; then
        return 0
    fi
    echo "  Response: ${resp}"
    return 1
}

test_model_name_header() {
    local resp
    resp=$(${CONTAINER_ENGINE} exec "${PRAXIS_NAME}" \
        wget -qS -O /dev/null \
        --header="x-model-name: gpt-4" \
        http://127.0.0.1:8080/ 2>&1) || true

    if echo "${resp}" | grep -qi "x-epp-model: gpt-4"; then
        return 0
    fi
    echo "  Expected x-epp-model: gpt-4 in response"
    echo "  Got: ${resp}"
    return 1
}

test_post_body_passthrough() {
    local body
    body=$(${CONTAINER_ENGINE} exec "${PRAXIS_NAME}" \
        wget -qO- \
        --post-data='{"model":"llama3","prompt":"hello"}' \
        --header="Content-Type: application/json" \
        http://127.0.0.1:8080/ 2>&1) || true

    # Fortio echo returns the request details; check body came through
    if echo "${body}" | grep -q "llama3"; then
        return 0
    fi
    echo "  Expected body to contain 'llama3'"
    echo "  Got: ${body}"
    return 1
}

run_test "GET returns x-epp-served-by header" test_get_with_epp_headers
run_test "POST returns EPP routing headers" test_post_with_routing_headers
run_test "x-model-name header propagated as x-epp-model" test_model_name_header
run_test "POST body passes through correctly" test_post_body_passthrough

# ---------------------------------------------------------------------------
# Restart EPP in reject-unauthorized mode
# ---------------------------------------------------------------------------

echo ""
echo "=== Restarting EPP in reject-unauthorized mode ==="

${CONTAINER_ENGINE} rm -f "${EPP_NAME}" 2>/dev/null || true
${CONTAINER_ENGINE} run -d --name "${EPP_NAME}" \
    --network "${NETWORK}" \
    "${EPP_IMAGE}" --port 50051 --mode reject-unauthorized

sleep 2

test_reject_no_auth() {
    local resp
    resp=$(${CONTAINER_ENGINE} exec "${PRAXIS_NAME}" \
        wget -qS -O /dev/null \
        --post-data='{"prompt":"hello"}' \
        --header="Content-Type: application/json" \
        http://127.0.0.1:8080/ 2>&1) || true

    if echo "${resp}" | grep -q "403"; then
        return 0
    fi
    echo "  Expected 403 response"
    echo "  Got: ${resp}"
    return 1
}

test_allow_with_auth() {
    local resp
    resp=$(${CONTAINER_ENGINE} exec "${PRAXIS_NAME}" \
        wget -qS -O /dev/null \
        --post-data='{"prompt":"hello"}' \
        --header="Content-Type: application/json" \
        --header="Authorization: Bearer test-token" \
        http://127.0.0.1:8080/ 2>&1) || true

    if echo "${resp}" | grep -q "200"; then
        return 0
    fi
    echo "  Expected 200 response"
    echo "  Got: ${resp}"
    return 1
}

run_test "POST without Authorization returns 403" test_reject_no_auth
run_test "POST with Authorization returns 200" test_allow_with_auth

# ---------------------------------------------------------------------------
# Restart EPP in rewrite-body mode
# ---------------------------------------------------------------------------

echo ""
echo "=== Restarting EPP in rewrite-body mode ==="

${CONTAINER_ENGINE} rm -f "${EPP_NAME}" 2>/dev/null || true
${CONTAINER_ENGINE} run -d --name "${EPP_NAME}" \
    --network "${NETWORK}" \
    "${EPP_IMAGE}" --port 50051 --mode rewrite-body

sleep 2

test_body_rewrite() {
    local body
    body=$(${CONTAINER_ENGINE} exec "${PRAXIS_NAME}" \
        wget -qO- \
        --post-data='{"prompt":"hello"}' \
        --header="Content-Type: application/json" \
        http://127.0.0.1:8080/ 2>&1) || true

    # The mock EPP wraps the body with {"wrapped":...}
    if echo "${body}" | grep -q '"wrapped"'; then
        return 0
    fi
    echo "  Expected body to contain '\"wrapped\"'"
    echo "  Got: ${body}"
    return 1
}

run_test "POST body is wrapped by rewrite-body mode" test_body_rewrite

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
echo "==========================================="
echo "  Results: ${PASSED}/${TESTS_RUN} passed, ${FAILED} failed"
echo "==========================================="

if [[ ${FAILED} -gt 0 ]]; then
    exit 1
fi
