# fault

`fault` is a focused network fault-injection engine for explicit TCP and UDP
proxies. It applies chainable transport faults and records what happened.
Payload inspection is limited to DNS faults on UDP proxies.

Read the single-page [fault manual](docs/index.html) for the complete mental
model, examples, and transport semantics. AI agents can use the compact
[agent reference](docs/agent-reference.md) or the repository-owned
[`fault-network-injection` skill](skills/fault-network-injection/SKILL.md).

The project deliberately contains no embedded agent, MCP server, eBPF loader,
HTTP mutation layer, traffic generator, or cloud deployment runtime.

## Capabilities

- Named TCP and UDP proxies with independent fault chains
- Latency, jitter, bandwidth, blackhole, and connection-reset faults
- DNS delay, timeout, truncation, refusal, SERVFAIL, NXDOMAIN, empty answers,
  and random A records
- Live changes on long-running connections
- One versioned Run model with ordered YAML or JSON phases
- Per-stream and per-exchange transport evidence with bounded streaming
- Coloured CLI dashboards and durable NDJSON journals
- Embeddable Rust and Python APIs

TCP supports latency, jitter, bandwidth, blackhole, and connection reset. UDP
supports latency, jitter, and directional blackhole at the datagram boundary;
DNS faults additionally manipulate DNS responses. Bandwidth and connection
reset are intentionally unavailable for UDP.

## CLI

Build the executable with current stable Rust:

```console
cargo build --release -p fault-cli
```

Run an indefinite final phase and send traffic through it:

```console
fault run examples/google-proxy.json
curl --connect-to www.google.com:443:127.0.0.1:18080 \
  https://www.google.com/
```

Run several timed phases:

```console
fault run examples/timed-run.yaml
```

CLI configuration accepts `.json`, `.yaml`, and `.yml`. Every document uses
the same versioned Run model: routes at the top, fault chains in phases. A
phase without `duration` remains active until stopped and must be last.

The command accepts `--journal FILE` for bounded, best-effort NDJSON transport evidence,
`--output text|json`, and `--color auto|always|never`.

Install the bundled network-injection skill for a coding agent:

```console
fault skill install --target codex
fault skill install --target claude
fault skill install --target opencode
```

Use `fault skill show` to print the bundled `SKILL.md`. Existing modified
skills are preserved unless installation is repeated with `--force`. The
installer asks whether the skill belongs to the current workspace or your home
directory; scripts can provide `--scope workspace` or `--scope home`.

## Python

The Python package requires Python 3.14 or newer. Build the local PyO3 package
and run the adaptive example with:

```console
uv sync --project fault-python --python 3.14 --reinstall-package faultlib
uv run --project fault-python --python 3.14 python examples/python_proxy.py
```

Python receives typed status, transport-record, and phase-transition objects.
Live record delivery is bounded and best effort: a slow Python consumer never
stalls the proxy, and `dropped_records` reports missed evidence.
Transport evidence never applies backpressure to the engine. A bounded stream
drops records when its consumer falls behind and reports the loss through
`dropped_records`.

See [`fault-python/README.md`](fault-python/README.md) for the API boundary and
development workflow.

## Rust

Rust applications embed the same engine used by the CLI:

```toml
[dependencies]
fault-engine = "1"
fault-model = "1"
```

See [`fault-engine/README.md`](fault-engine/README.md) for a complete minimal
example.

## Workspace

- [`fault-model`](fault-model/README.md): versioned contracts and schemas
- [`fault-engine`](fault-engine/README.md): asynchronous proxy and scheduler
- [`fault-cli`](fault-cli/README.md): command and presentation adapter
- [`faultlib`](fault-python/README.md): PyO3 bindings and Python ergonomics
- [`docs/schemas/run.schema.json`](docs/schemas/run.schema.json): canonical Run contract

## Development

```console
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --locked
```

The workspace follows current stable Rust and separately checks Rust 1.85 as
its minimum supported version.
