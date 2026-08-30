# Agent guidance

## Architectural invariants

### Python is a binding, never a second domain API

This is an absolute constraint.

- All domain concepts, lifecycle rules, state machines, decisions, validation,
  events, and errors must originate in the Rust model or engine.
- The Python package may expose the canonical Rust API ergonomically, but it
  must remain a thin binding over those Rust semantics.
- Never invent Python-only controllers, event queues, phase behavior,
  transition rules, validation, or other domain abstractions.
- Never duplicate Rust semantics in Python, even as a convenience layer.
- If a desired Python experience appears to require new semantics, stop and
  ask before implementing it. Design and implement the capability in Rust
  first only after receiving explicit approval.

Do not cross this boundary without explicit user authorization.
