# fault-binding

The language-neutral engine handle shared by the
[fault](https://fault-project.com) language bindings (`fault-python` and
`fault-typescript`). It is not published to crates.io.

The handle owns everything a binding would otherwise have to reimplement:
the `configured → running → stopped` lifecycle, phase schedule ownership,
argument validation, defaults, and the periodic status events produced by
`next_event`. Every value crossing it is JSON matching the schemas in
[`docs/schemas`](../docs/schemas), and every error carries an `ErrorKind`
that a binding maps onto its native error types.

A binding must only convert strings and errors. If a binding needs new
behavior, add it here or to `fault-engine` first.
