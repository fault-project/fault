/**
 * Run a small adaptive fault proxy from TypeScript.
 *
 * Build the local package, then run this example with Node.js 24 or newer,
 * which runs TypeScript directly, and send traffic through the proxy:
 *
 *     npm --prefix fault-typescript ci
 *     npm --prefix fault-typescript run build
 *     node examples/typescript_proxy.mts
 *     curl --connect-to www.google.com:443:127.0.0.1:18080 \
 *       https://www.google.com/
 *
 * In your own project, install `faultlib` and import from "faultlib".
 */

import {
  Engine,
  type Run,
  type Schedule,
  type TcpStreamRecord,
} from "../fault-typescript/dist/index.js";

const run: Run = {
  schema_version: 1,
  name: "adaptive Google connection",
  proxies: [
    {
      name: "google",
      protocol: "tcp",
      listen: "127.0.0.1:18080",
      upstream: "www.google.com:443",
    },
  ],
  phases: [{ name: "controlled from TypeScript", proxies: [] }],
};

async function observe(
  engine: Engine,
  latencyObserved: () => void,
  recent: TcpStreamRecord[],
): Promise<void> {
  for await (const event of engine.events()) {
    switch (event.type) {
      case "status": {
        const { tcp, effects } = event.status;
        console.log(
          `streams=${tcp.active} impacted=${tcp.impacted} ` +
            `average_latency=${effects.average_latency_ms.toFixed(1)}ms`,
        );
        break;
      }
      case "tcp-stream": {
        const { stream } = event;
        recent.push(stream);
        if (recent.length > 100) recent.shift();
        console.log(
          `stream=${stream.stream_id} outcome=${stream.outcome} ` +
            `sent=${stream.bytes_to_upstream}B received=${stream.bytes_to_client}B`,
        );
        if (stream.faults.latency.applications > 0) latencyObserved();
        break;
      }
      case "udp-exchange":
        break;
    }
  }
}

async function drive(schedule: Schedule, latencyObserved: Promise<void>) {
  const latency = await schedule.addPhase(
    "variable latency",
    [
      {
        proxy: "google",
        faults: [
          {
            type: "latency",
            flow: "both",
            distribution: { type: "uniform", min_ms: 100, max_ms: 250 },
          },
        ],
      },
    ],
    { duration: "30s" },
  );
  await schedule.startPhase(latency);

  await latencyObserved;

  const bandwidth = await schedule.addPhase(
    "constrained bandwidth",
    [
      {
        proxy: "google",
        faults: [{ type: "bandwidth", flow: "both", bytes_per_second: 64_000 }],
      },
    ],
    { duration: "20s" },
  );
  await schedule.startPhase(bandwidth);

  for await (const { phase, reason } of schedule.transitions()) {
    console.log(`phase=${phase.name} state=${phase.state} reason=${reason}`);
    if (phase.id === bandwidth.id && phase.state === "stopped") return;
  }
}

await using engine = await Engine.start(run);
console.log(`Proxy listening on ${engine.endpoints.tcp[0]}`);
console.log("Press Ctrl-C to stop.");

const recent: TcpStreamRecord[] = [];
const { promise: latencyObserved, resolve } = Promise.withResolvers<void>();
const observing = observe(engine, resolve, recent);

{
  await using schedule = await engine.beginSchedule();
  await drive(schedule, latencyObserved);
}

const summary = await engine.shutdown();
await observing;
console.log(
  `run completed: streams=${summary.status.tcp.opened} ` +
    `failed=${summary.status.tcp.failed} recent_records=${recent.length}`,
);
