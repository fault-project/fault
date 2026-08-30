# Changelog

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
