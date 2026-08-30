---
name: fault-network-injection
description: Design and run realistic network fault-injection experiments with fault using TCP, UDP, DNS, phases, the CLI, or Python. Use when investigating behavior under degraded or unavailable network dependencies; do not use for application-level response mutation or traffic generation.
---

# Fault Network Injection

Turn a resilience question into the smallest network experiment that can answer
it. Start with the application behavior under investigation, not with a list of
available faults.

## Design the experiment

1. Identify the client, real dependency, transport, and application endpoint
   that will be redirected through fault.
2. State the observable behavior that would distinguish a useful result: for
   example bounded queues, reconnecting a pool, respecting a deadline, or
   recovering after DNS returns.
3. Select the smallest realistic fault chain. Use several faults or proxies
   only when their interaction is the subject of the experiment.
4. Add phases when conditions must change over time. Include a healthy baseline
   or recovery phase only when it helps answer the question.
5. Produce the routing change alongside the configuration. Prefer an existing
   remote-URL environment variable or deployment-level address override over
   changing application code.

Read [the agent reference](https://fault-project.com/agent-reference.md)
for fault semantics,
configuration patterns, CLI usage, and realistic failure mappings. When
exact fields or constraints matter, inspect the generated schemas linked from
that reference rather than guessing.

## Preserve fault semantics

- Describe flows from the client’s perspective: `to-upstream` is request
  traffic and `to-client` is response traffic.
- Treat bandwidth as independently limited per TCP connection stream.
- Treat TCP as streams and UDP as exchanges. Do not describe UDP as having a
  connection.
- Remember that fault chains are ordered.
- A phase replaces the complete chain for each proxy it names. Put every
  concurrently active fault in that phase.
- A phase with `duration` advances automatically. A phase without `duration`
  remains active until explicitly stopped and must be the final declarative
  phase.
- Once a phase starts it is immutable. Schedule mutations apply only to future
  phases; starting or stopping a phase uses the engine’s canonical lifecycle.
- Do not invent unsupported HTTP, gRPC, packet-level, eBPF, probe,
  expectation, or traffic-generation behavior.

## Choose the interface

- Use `fault run FILE` for the CLI. It accepts one canonical YAML or JSON Run
  document containing routes and phases. Do not ask the user to choose another
  execution mode.
- Use the Rust library when embedding fault in a Rust system.
- Use the Python binding when network phases participate in broader async
  orchestration, such as restarting a pod and then changing network
  conditions. Python consumes the Rust model and engine; do not recreate
  scheduling or domain semantics in Python.

Do not execute an experiment merely because the user asked for a design or
configuration. Before running against an environment, establish that the
target and routing change are in scope. Never silently redirect production
traffic.

## Report useful evidence

Keep the CLI dashboard aggregate. Use the NDJSON journal or programmatic event
stream when per-stream or per-exchange evidence matters. Evidence delivery is
bounded and best effort: report `dropped_records` when interpreting an
incomplete record stream. Distinguish what fault applied from whether the
application behaved correctly; fault records network effects, not application
assertions.
