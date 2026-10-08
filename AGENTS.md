# Agent guidance

## Architectural invariants

### Python and TypeScript are bindings, never a second domain API

This is an absolute constraint. It applies equally to the Python package
(`fault-python`) and the TypeScript package (`fault-typescript`).

- All domain concepts, lifecycle rules, state machines, decisions, validation,
  defaults, events, and errors must originate in Rust: the model, the engine,
  or the `fault-binding` handle that both bindings share.
- The Python and TypeScript packages may expose the canonical Rust API
  ergonomically, but they must remain thin bindings over those Rust
  semantics: they convert JSON and map error kinds, nothing more.
- Never invent Python-only or TypeScript-only controllers, event queues,
  phase behavior, transition rules, validation, or other domain abstractions.
- Never duplicate Rust semantics in Python or TypeScript, even as a
  convenience layer. Behavior one binding needs belongs in `fault-binding`,
  so the other binding gets it too.
- If a desired Python or TypeScript experience appears to require new
  semantics, stop and ask before implementing it. Design and implement the
  capability in Rust first only after receiving explicit approval.

Do not cross this boundary without explicit user authorization.

### TypeScript types are generated

`fault-typescript/ts/types.ts` is generated from `docs/schemas`. Never edit it by
hand; regenerate the schemas (`cargo run -p fault-model --example
generate_schemas`) and then the types (`npm --prefix fault-typescript run
generate:types`).
