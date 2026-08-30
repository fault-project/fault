# fault agent reference

Use `fault` to place transport-level failures between an application and its
real dependencies. Point the application at a local fault proxy; fault keeps
the real upstream address.

For the human-facing guide, see the [single-page manual](index.html).

## Mental model

- A proxy has a unique name, a `tcp` or `udp` listener, an upstream, and an
  ordered fault chain.
- `to-upstream` affects client requests; `to-client` affects upstream
  responses; `both` affects both directions.
- TCP produces connection streams. UDP produces request/response exchanges.
- TCP fault state follows a stream, including long-lived connections.
- A run is an ordered list of phases. Each timed phase begins after the
  previous phase ends.
- The CLI has one entry point and one document shape: `fault run FILE`.
- A phase replaces the complete fault chain for every proxy it names. An empty
  `proxies` list restores every proxy to healthy operation.
- A phase with `duration` advances automatically. A phase without `duration`
  remains active until explicitly stopped and must be the final declarative
  phase.
- YAML and JSON deserialize into the same versioned Rust contracts.

| Final phase | Lifecycle |
| --- | --- |
| Has `duration` | Run exits when that duration elapses |
| Omits `duration` | Run remains active until stopped or interrupted |

## Fault vocabulary

| Fault | Transport | Use |
| --- | --- | --- |
| `latency` | TCP, UDP | Baseline delay sampled from a distribution |
| `jitter` | TCP, UDP | Probabilistic delay between two bounds |
| `blackhole` | TCP, UDP | Pending TCP traffic or dropped UDP datagrams |
| `bandwidth` | TCP | Bytes per second, independently per stream |
| `connection-reset` | TCP | Probabilistically reset active connections |
| `dns` | DNS over UDP | Delay, timeout, truncate, refuse, SERVFAIL, NXDOMAIN, empty answer, or random A |

Bandwidth and connection reset are intentionally rejected for UDP.

## Minimal proxy

```yaml
schema_version: 1
name: permanent database latency
proxies:
  - name: database
    protocol: tcp
    listen: 127.0.0.1:15432
    upstream: database:5432

phases:
  - name: degraded
    proxies:
      - proxy: database
        faults:
          - type: latency
            flow: to-client
            distribution:
              type: uniform
              min_ms: 150.0
              max_ms: 300.0
```

```console
fault run database.yaml --journal fault.ndjson
```

Configure the application to use `127.0.0.1:15432` instead of
`database:5432`. For example, override its existing `DATABASE_URL` environment
variable; do not add fault-specific behavior to application code.

## Realistic timed run

```yaml
schema_version: 1
name: checkout dependency incident

proxies:
  - name: database
    protocol: tcp
    listen: 127.0.0.1:15432
    upstream: database:5432
  - name: payments
    protocol: tcp
    listen: 127.0.0.1:18081
    upstream: payments:8080
  - name: dns
    protocol: udp
    listen: 127.0.0.1:15353
    upstream: 1.1.1.1:53

phases:
  - name: baseline
    duration: 15s
    proxies: []

  - name: compound degradation
    duration: 60s
    proxies:
      - proxy: database
        faults:
          - type: latency
            flow: to-client
            distribution:
              type: uniform
              min_ms: 200.0
              max_ms: 450.0
          - type: bandwidth
            flow: to-client
            bytes_per_second: 32768
      - proxy: payments
        faults:
          - type: connection-reset
            flow: to-client
            probability: 0.2
      - proxy: dns
        faults:
          - type: dns
            case: serv-fail
            delay_ms: 250

  - name: recovered
    duration: 30s
    proxies: []
```

```console
fault run checkout.yaml --journal checkout.ndjson
```

## Choosing realistic faults

| Situation | Fault chain |
| --- | --- |
| Slow or unavailable resolver | DNS `timeout`, delayed response, or `serv-fail` |
| Wrong or missing address | DNS `random-a`, `nx-domain`, or `empty-answer` |
| Growing downstream backlog | Client-bound latency then bandwidth |
| Broken backpressure | Very low client-bound bandwidth |
| Congested remote region | Latency, probabilistic jitter, then bandwidth |
| Asymmetric partition | Blackhole only `to-upstream` or `to-client` |
| Pod or load-balancer rotation | Probabilistic connection reset |
| Retry amplification | Jitter and connection reset in one timed phase |
| Coordinated dependency incident | One phase targeting several named proxies |
| Recovery behavior | Faulted phase followed by an empty phase |

Fault chains are evaluated in declaration order. Include every fault that must
remain active concurrently in the same phase.

## Output and embedding

- The interactive CLI dashboard is aggregate and human-oriented.
- `--output json` provides machine-readable command output.
- `--journal FILE` writes best-effort NDJSON transport evidence.
- Evidence delivery is bounded: slow consumers lose records rather than
  slowing network traffic. Check `dropped_records`.
- Rust owns the model, engine, scheduling, events, and errors.
- Python 3.14+ exposes those Rust semantics through PyO3 for broader async
  experiments; it is not on the traffic hot path.

## Canonical contracts

- [Run schema](schemas/run.schema.json)
- [Run progress schema](schemas/run-progress.schema.json)
- [Run result schema](schemas/run-result.schema.json)
- [Journal event schema](schemas/journal-event.schema.json)

When exact fields or numeric constraints matter, use these generated schemas
as the source of truth.
