# Quickstart

## Container

Build the image:

```console
make container
```

Run with a config file:

```console
podman run -p 8080:8080 \
  -v ./praxis.yaml:/etc/praxis/praxis.yaml:ro \
  praxis
```

## Build from source

```console
make release
```

Binary: `target/release/praxis`

## Minimal config

Create `praxis.yaml`:

```yaml
listeners:
  - name: web
    address: "0.0.0.0:8080"
    filter_chains:
      - routing

filter_chains:
  - name: routing
    filters:
      - filter: router
        routes:
          - path_prefix: "/"
            cluster: backend
      - filter: load_balancer
        clusters:
          - name: backend
            endpoints:
              - "127.0.0.1:3000"
```

## Run

```console
cargo run --release -- -c praxis.yaml
```

Debug logging:

```console
RUST_LOG=debug cargo run --release -- -c praxis.yaml
```

The config path can also be set via the `PRAXIS_CONFIG`
environment variable:

```console
PRAXIS_CONFIG=praxis.yaml cargo run --release
```

Resolution order: `-c` flag, then `PRAXIS_CONFIG` env var,
then `praxis.yaml` in the working directory, then the
built-in default.

## Zero-config startup

Running without a config file starts with a built-in
default that listens on `127.0.0.1:8080` and returns
`{"status": "ok", "server": "praxis"}` on `/`:

```console
cargo run --release
```

## Admin endpoints

When `admin_address` is configured, a separate listener
serves health checks:

| Path | Purpose |
|------|---------|
| `GET /healthy` | Liveness check |
| `GET /ready` | Readiness check |

These are served on the admin port, not on main listeners.
See [configuration.md](configuration.md) for details.

## Next steps

- **Configuration**: named filter chains, listener
  composition, conditions, and all built-in filter
  options. See [configuration.md](configuration.md).
- **Load balancing**: round-robin, least-connections,
  consistent-hash, weighted endpoints. See
  [configuration.md](configuration.md#load-balancing).
- **Routing**: `path_prefix` + optional `host` matching;
  longest prefix wins. See
  [configuration.md](configuration.md#router).
- **TLS**: termination, re-encryption, and local dev
  setup. See [tls.md](tls.md).
- **Filters**: headers, request IDs, access logs,
  timeouts, IP ACL, and more. See
  [filters.md](filters.md#built-in-filters).
- **Custom filters**: implement `HttpFilter` or
  `TcpFilter`, register with `register_filters!`. See
  [extensions.md](extensions.md).
- **Architecture**: filter pipeline design, body modes,
  protocol abstraction. See
  [architecture.md](architecture.md).
- **Examples**: working YAML configs for all features.
  See `examples/configs/`.
