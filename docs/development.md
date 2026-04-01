# Development

## Requirements

- Rust stable 1.94+ (2024 edition)
- Rust nightly (for rustfmt only)
- CMake 3.31+ (required by `libz-ng-sys`, a Pingora
  transitive dependency)

## Build

```console
make build
make release
make check
```

## Test

```console
make test
```

```console
make test-integration
```

## Lint

```console
make lint
make fmt
```

> **Note**: Clippy runs with `-D warnings` so any warning
> is a build failure.

Run `make help` for all available targets.

## Code Conventions

- `#![deny(unsafe_code)]` in all crates
- Errors via `thiserror`
- Logging via `tracing`

See [architecture.md](architecture.md) for workspace layout
and crate dependencies.

## Conformance Testing (Planned)

The following tools are planned for protocol correctness
validation but are not yet integrated:

- **h2spec**: HTTP/2 compliance testing
- **curl test suite**: HTTP/1.1, TLS, and proxy semantics
- **httpbin**: header forwarding, redirects, chunking

## Robustness and Fuzzing (Planned)

The following tools are planned for robustness testing
but are not yet integrated:

- **cargo-fuzz**: fuzz header parsing via LLVM libFuzzer
- **boofuzz**: malformed HTTP for parser crash testing
- **toxiproxy**: inject latency, drops, partial responses

## Adding a Built-in Filter

1. Create the filter module under
   `praxis-filter/src/builtins/<category>/`.
2. Implement `HttpFilter` (or `TcpFilter` for TCP-level
   filters). Add a `from_config` factory that deserializes
   a `serde_yaml::Value` into your config struct.
3. Register it in `praxis-filter/src/registry.rs`
   alongside the existing built-ins.
4. Add unit tests and doctests.
5. Add an example config in the appropriate category under
   `examples/configs/`.
6. Add an integration test in `tests/integration/`.

## Adding a Protocol

1. Implement the `Protocol` trait in a new module under
   `praxis-protocol/src/`.
2. Add a variant to `ProtocolKind` in
   `praxis-core/src/config/listener.rs`.
3. Wire it up in `praxis/src/main.rs` where the protocol
   is selected.

## Performance & Benchmarking

See [benchmarks.md](./benchmarks.md).
