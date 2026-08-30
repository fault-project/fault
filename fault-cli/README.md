# fault

`fault-cli` provides the `fault` executable: a focused TCP, UDP, and DNS
fault-injection proxy.

Put it between a client and a real dependency, describe changing network
conditions in YAML or JSON, then send normal application traffic through it.
fault applies the configured faults and records which streams or exchanges
were affected. It does not generate traffic or mutate application-level HTTP
responses.

## Install

Download a ready-to-run executable from
[GitHub Releases](https://github.com/fault-project/fault/releases), or
install from crates.io with current stable Rust:

```console
cargo install fault-cli
```

The installed executable is named `fault`.

## Run your first fault

Save this as `google-proxy.yaml`:

```yaml
schema_version: 1
name: slow Google connection
proxies:
  - name: google
    protocol: tcp
    listen: 127.0.0.1:18080
    upstream: www.google.com:443
phases:
  - name: slow responses
    proxies:
      - proxy: google
        faults:
          - type: latency
            flow: to-client
            distribution:
              type: uniform
              min_ms: 250.0
              max_ms: 250.0
```

Start the proxy:

```console
fault run google-proxy.yaml
```

In another terminal, route a request through it while preserving Google's TLS
server name and HTTP host:

```console
curl --connect-to www.google.com:443:127.0.0.1:18080 \
  https://www.google.com/
```

The final phase has no `duration`, so it remains active until interrupted.
Runs may contain several named proxies and ordered timed phases.

## Output and journals

Interactive terminals receive a compact coloured dashboard showing the active
phase, fault chains, and aggregate TCP and UDP activity.

```console
fault run run.yaml --journal run.ndjson
fault --output json run run.yaml
fault --color never run run.yaml
```

The optional NDJSON journal records run boundaries and completed TCP streams
or UDP exchanges. Evidence delivery is bounded and best effort so journal I/O
cannot stall proxied traffic; `dropped_records` exposes any omitted records.

## Install the agent skill

The executable bundles a concise network-injection skill for common coding
agents:

```console
fault skill install --target codex
fault skill install --target claude
fault skill install --target opencode
```

Interactive installation asks whether the skill belongs to the current
workspace or your home directory. Scripts can pass `--scope workspace` or
`--scope home`. A different existing skill is preserved unless `--force` is
given. `fault skill show` prints the exact bundled `SKILL.md`.

## Learn more

- [Guide and realistic examples](https://fault-project.com)
- [Complete field reference](https://fault-project.com/reference.html)
- [Compact agent reference](https://fault-project.com/agent-reference.md)
- [`fault-engine`](https://crates.io/crates/fault-engine) for Rust embedding
- [`faultlib`](https://pypi.org/project/faultlib/) for Python orchestration

Licensed under the
[Apache License 2.0](https://github.com/fault-project/fault/blob/main/LICENSE).
