# fault-engine

`fault-engine` is the embeddable asynchronous engine behind
[fault](https://fault-project.com). It runs explicit TCP and UDP proxies,
applies ordered network fault chains, changes those chains through phases, and
records what happened to each TCP stream or UDP exchange.

Use it when fault injection must live inside a Rust application. If you want a
standalone executable, use the [`fault-cli`](https://crates.io/crates/fault-cli)
package instead.

## Supported faults

| Fault | TCP | UDP |
|---|---:|---:|
| Latency | yes | yes |
| Jitter | yes | yes |
| Bandwidth | yes | no |
| Blackhole | yes | yes |
| Connection reset | yes | no |
| DNS response faults | no | yes |

Bandwidth is limited independently for each TCP connection stream. DNS faults
operate on DNS carried by a UDP proxy. The engine does not inspect or mutate
HTTP, gRPC, or other application protocols.

## Install

```toml
[dependencies]
fault-engine = "1"
fault-model = "1"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The minimum supported Rust version is 1.85.

## Start an engine

The engine accepts the same versioned `Run` document as the CLI and Python
binding:

```rust,no_run
use fault_engine::FaultEngine;
use fault_model::{Run, validate_schema_version};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        "name": "degraded for ten seconds",
        "duration": "10s",
        "proxies": [{
          "proxy": "database",
          "faults": [{
            "type": "latency",
            "flow": "both",
            "distribution": {
              "type": "uniform",
              "min_ms": 150.0,
              "max_ms": 250.0
            }
          }]
        }]
      }]
    }"#;

    let run: Run = serde_json::from_str(source)?;
    validate_schema_version(&run)?;
    run.validate()?;

    let engine = FaultEngine::from_run(&run).start().await?;
    println!("bound endpoints: {:?}", engine.endpoints());

    let result = engine.run_phases(run).await?;
    println!("outcome: {:?}", result.outcome);

    engine.shutdown().await?;
    Ok(())
}
```

`RunningEngine` exposes bound endpoints, current fault chains, transport
status, snapshots, phase progress, runtime failures, and explicit shutdown.
`ControlSession` supports adaptive orchestration while preserving the same
Rust-owned phase lifecycle.

## Transport evidence

The engine maintains aggregate status and can stream completed
`TcpStreamRecord` and `UdpExchangeRecord` values. Record delivery is bounded
and best effort: a slow consumer never adds backpressure to proxied traffic.
When records are omitted, `TransportStatus::dropped_records` reports the loss.

This is instrumentation, not an assertion framework. The engine records which
network effects occurred; the embedding application decides whether its own
behavior was correct.

## Contracts and reference

All public run, fault, progress, result, and transport types come from
[`fault-model`](https://crates.io/crates/fault-model). See the generated
[field reference](https://fault-project.com/reference.html) for their exact
wire representation.

Licensed under the
[Apache License 2.0](https://github.com/fault-project/fault/blob/main/LICENSE).
