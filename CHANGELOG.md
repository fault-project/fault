# Changelog

## Unreleased

### Added

- `TransportStatus.effects` reports `bandwidth_bytes_limited`,
  `blackhole_activations`, `connection_resets`, and `dns_interventions`
  alongside the latency and jitter counters. They are cumulative run-wide
  totals that use the same names and meaning as the per-record
  `FaultRecord`, so each equals the sum over all streams and exchanges. They
  update live when a fault takes effect, so `run-progress` on stdout proves
  every fault type fired without a journal. Each blackholed direction and each
  reset stream counts once, however many times the fault blocks it.

## 1.1.0 - 2026-10-06

### Added

- `fault run --watch` hot-reloads the Run document when the file changes. A
  reload removes the active faults and restarts the phase timeline from the
  first phase. Proxies keep their connections unless the `proxies` section
  changed, in which case they are rebound. Saves that leave the content
  unchanged are ignored.
- `--watch` follows editors that save by renaming and Kubernetes ConfigMap
  and Secret volume updates. Mount the volume as a directory, because
  Kubernetes never updates `subPath` mounts.
- `config-reloaded` and `config-reload-failed` stdout events carry the SHA-256
  of the configuration file.
- After an invalid file, the current run continues unchanged. When new proxies
  cannot be bound, the previous configuration is restarted from its first
  phase.
- The `run-started` event includes `config_sha256`.
- Journals continue across reloads, including when the proxies restart.
- `fault_model::Proxy` implements `PartialEq` and `Eq`.
- The bundled agent skill and the Kubernetes sidecar documentation describe
  `--watch`.

### Changed

- When a run completes or is interrupted, the CLI now removes all faults.
  Previously it briefly restored the first phase's faults before shutting
  down.
- The minimum supported Rust version is now 1.89, up from 1.85.
- The workspace uses Cargo's `rust-version`-aware resolver (`resolver = "3"`),
  so dependency updates no longer select versions that need a newer Rust.
- Updated `rand` to 0.10 and `rand_distr` to 0.6.
- Refreshed all other dependencies to their latest compatible releases,
  including tokio 1.53, clap 4.6, PyO3 0.29.3, and uuid 1.27.

## 1.0.0 - 2026-08-30

This release establishes the new, deliberately incompatible `fault` API as
the stable baseline. It is a focused network fault-injection engine rather
than a continuation of the pre-1.0 CLI and agent surface.

### Changed

- Rebuilt `fault` as a focused explicit TCP and UDP fault-injection engine.
- Split versioned contracts, runtime behavior, and CLI presentation into
  `fault-model`, `fault-engine`, and `fault-cli`.
- Replaced the former command surface with one `fault run` command and one
  versioned Run document containing routes and phases. Omitting the final
  phase duration keeps it active until stopped.
- Added bounded connection observation, NDJSON journals, structured outcomes,
  actionable failures, and a live run dashboard.
- Added an abi3 Python 3.14 package for typed engine observations and adaptive
  schedules backed by the same Rust phase lifecycle as declarative runs.

### Removed

- Embedded AI agent and MCP server
- eBPF and stealth interception
- HTTP/L7 mutation and packet-level faults
- gRPC plugins, probes, traffic generation, reports, and cloud injection

The rewrite intentionally provides no backward-compatibility layer.
