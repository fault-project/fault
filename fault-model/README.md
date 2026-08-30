# fault-model

`fault-model` contains the versioned data contracts for
[fault](https://fault-project.com), a focused TCP, UDP, and DNS fault-injection
proxy.

Use this crate when you need to construct, validate, serialize, or inspect a
fault run without starting a network proxy. It has no asynchronous runtime and
performs no network I/O.

## What it defines

- `Run`, `Proxy`, `Phase`, and ordered per-proxy fault chains
- latency, jitter, bandwidth, blackhole, connection-reset, and DNS faults
- TCP stream and UDP exchange observations
- live progress, completed results, and journal events
- schema-version checks and lightweight semantic validation
- `schemars` implementations for generating JSON Schema

The same contracts are consumed by `fault-engine`, the `fault` CLI, and the
Python package. Rust is the canonical source of these semantics.

## Install

```toml
[dependencies]
fault-model = "1"
serde_json = "1"
```

The minimum supported Rust version is 1.85.

## Read and validate a run

```rust
use fault_model::{Run, validate_schema_version};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = r#"
    {
      "schema_version": 1,
      "name": "slow database",
      "proxies": [{
        "name": "database",
        "protocol": "tcp",
        "listen": "127.0.0.1:15432",
        "upstream": "database.internal:5432"
      }],
      "phases": [{
        "name": "degraded",
        "proxies": [{
          "proxy": "database",
          "faults": [{
            "type": "latency",
            "flow": "both",
            "distribution": {
              "type": "normal",
              "mean_ms": 200.0,
              "stddev_ms": 20.0
            }
          }]
        }]
      }]
    }"#;

    let run: Run = serde_json::from_str(source)?;
    validate_schema_version(&run)?;
    run.validate()?;

    assert_eq!(run.proxies[0].name, "database");
    Ok(())
}
```

A phase with a duration advances automatically. A final phase without a
duration stays active until explicitly stopped. Fault chains are ordered and
flows are always described from the client's perspective:

- `to-upstream`: client to dependency
- `to-client`: dependency to client
- `both`: both directions

## Schemas and documentation

The generated [human reference](https://fault-project.com/reference.html)
lists every field, variant, and constraint. The corresponding
[JSON Schemas](https://github.com/fault-project/fault/tree/main/docs/schemas)
are committed with the project.

Repository maintainers regenerate both from the workspace root with:

```console
cargo run -p fault-model --example generate_schemas
```

## Related packages

- [`fault-engine`](https://crates.io/crates/fault-engine) runs the proxies.
- [`fault-cli`](https://crates.io/crates/fault-cli) provides the `fault`
  executable.
- [`faultlib`](https://pypi.org/project/faultlib/) provides the Python binding.

Licensed under the
[Apache License 2.0](https://github.com/fault-project/fault/blob/main/LICENSE).
