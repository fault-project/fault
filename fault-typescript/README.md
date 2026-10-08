# faultlib for TypeScript and JavaScript

`faultlib` runs [fault](https://fault-project.com)'s TCP, UDP, and DNS
fault-injection engine inside Node.js, Bun, or Deno. It is a Node-API addon
built with [napi-rs](https://napi.rs) over the same Rust engine as the CLI and
the Python package.

The package is a thin binding. Lifecycle rules, phase scheduling, validation,
defaults, and events all originate in Rust (`fault-binding` and
`fault-engine`); the TypeScript layer only converts JSON and errors. Its types
are generated from the published [JSON Schemas](../docs/schemas), so runs,
faults, records, and results have exactly the fields documented in the
[reference](https://fault-project.com/reference.html).

## Requirements

- Node.js 24 or newer, Bun 1.2 or newer, or Deno 2 with `--allow-ffi`.
  Older Node.js versions fail at import with a clear error. Deno needs a
  local `node_modules` directory to load the addon: set
  `"nodeModulesDir": "auto"` in `deno.json`.
- Prebuilt addons for Linux x64 (glibc 2.17+), macOS arm64 and x64, and
  Windows x64. They install automatically as the optional
  `@faultlib/native-*` dependency for your platform.

```console
npm install faultlib
```

## Start a proxy

```ts
import { Engine, type Run } from "faultlib";

const run: Run = {
  schema_version: 1,
  name: "slow api",
  proxies: [
    {
      name: "api",
      protocol: "tcp",
      listen: "127.0.0.1:0",
      upstream: "api.internal:443",
    },
  ],
  phases: [{ name: "controlled from TypeScript", proxies: [] }],
};

await using engine = await Engine.start(run);
console.log(`proxy listening on ${engine.endpoints.tcp[0]}`);

await engine.setFaults("api", [
  {
    type: "latency",
    flow: "both",
    distribution: { type: "normal", mean_ms: 200, stddev_ms: 20 },
  },
]);
```

`await using` shuts the engine down when the scope exits. Call
`engine.shutdown()` instead to receive the final `TransportSummary`;
`engine.summary` keeps it afterwards.

Call `engine.run()` to execute the run's own phases and get a `RunResult`.

## Observe traffic

```ts
for await (const event of engine.events({ statusInterval: 2 })) {
  switch (event.type) {
    case "status":
      console.log(`active streams: ${event.status.tcp.active}`);
      break;
    case "tcp-stream":
      console.log(`${event.stream.stream_id}: ${event.stream.outcome}`);
      break;
    case "udp-exchange":
      console.log(`${event.exchange.exchange_id}: ${event.exchange.outcome}`);
      break;
  }
}
```

`events()` yields every completed transport record, plus a `status` event
when no record completes within `statusInterval` seconds. It ends when the
engine stops. `records()` and `progress()` iterate records and run phase
progress on their own; `nextEvent()`, `nextRecord()`, and `nextProgress()`
wait for a single value. Delivery is bounded and best effort: a slow consumer
never stalls traffic, and `status.dropped_records` counts what it missed.

## Change phases while traffic flows

```ts
{
  await using schedule = await engine.beginSchedule();
  const slow = await schedule.addPhase(
    "slow",
    [{ proxy: "api", faults: [{ type: "bandwidth", flow: "both", bytes_per_second: 64_000 }] }],
    { duration: "30s" },
  );
  await schedule.startPhase(slow);

  for await (const { phase, kind, reason } of schedule.transitions()) {
    console.log(phase.name, kind, reason);
    if (phase.id === slow.id && phase.state === "stopped") break;
  }
}
```

Only pending phases can be modified, moved, or deleted. Ending the schedule
restores the faults that were active when it began.

## Errors

Every error is a `FaultError` with a `code`:

| Class | `code` | Raised for |
|---|---|---|
| `InvalidInputError` | `INVALID_INPUT` | Invalid runs, faults, durations, phase ids, or options |
| `PhaseStateError` | `PHASE_STATE` | Phase operations on a phase in the wrong state |
| `FaultError` | `RUNTIME` | Lifecycle and engine failures |

## Development

The addon builds from the workspace with current stable Rust:

```console
cd fault-typescript
npm ci
npm run build          # release addon, loader, and dist/
npm run build:debug    # faster debug addon
npm test               # Node.js
npm run test:bun
npm run test:deno
npm run check:examples
```

After changing `fault-model`, regenerate the schemas and the TypeScript types:

```console
cargo run -p fault-model --example generate_schemas
npm --prefix fault-typescript run generate:types
```

`npm run check:types` fails when `ts/types.ts` is stale. See
[`examples/typescript_proxy.mts`](../examples/typescript_proxy.mts) for an adaptive
example.
