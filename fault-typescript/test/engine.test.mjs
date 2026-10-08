// Runs under `node --test`, `bun test`, and `deno test` through node:test.
import assert from "node:assert/strict";
import { connect, createServer } from "node:net";
import { after, before, test } from "node:test";

import {
  Engine,
  FaultError,
  InvalidInputError,
  PhaseStateError,
} from "../dist/index.js";

let upstream;

before(async () => {
  upstream = createServer((socket) => socket.pipe(socket));
  await new Promise((resolve) => upstream.listen(0, "127.0.0.1", resolve));
});

after(() => new Promise((resolve) => upstream.close(resolve)));

const run = () => ({
  schema_version: 1,
  name: "node binding",
  proxies: [
    {
      name: "api",
      protocol: "tcp",
      listen: "127.0.0.1:0",
      upstream: `127.0.0.1:${upstream.address().port}`,
    },
  ],
  phases: [{ name: "steady", proxies: [] }],
});

const latency = [
  {
    proxy: "api",
    faults: [
      {
        type: "latency",
        flow: "both",
        distribution: { type: "uniform", min_ms: 1, max_ms: 2 },
      },
    ],
  },
];

function roundTrip(endpoint) {
  const [host, port] = [
    endpoint.slice(0, endpoint.lastIndexOf(":")),
    Number(endpoint.slice(endpoint.lastIndexOf(":") + 1)),
  ];
  return new Promise((resolve, reject) => {
    const socket = connect(port, host, () => socket.end("ping"));
    let data = "";
    socket.on("data", (chunk) => (data += chunk));
    socket.on("end", () => resolve(data));
    socket.on("error", reject);
  });
}

test("rejects invalid runs with InvalidInputError", () => {
  assert.throws(() => new Engine({ ...run(), schema_version: 99 }), InvalidInputError);
  assert.throws(() => new Engine(run(), { eventCapacity: 0 }), (error) => {
    assert.ok(error instanceof FaultError);
    assert.equal(error.code, "INVALID_INPUT");
    assert.equal(error.message, "event_capacity must be at least 1");
    return true;
  });
});

test("owns the lifecycle in Rust", async () => {
  const engine = new Engine(run());
  assert.equal(engine.alive, false);
  assert.throws(() => engine.endpoints, FaultError);

  const endpoints = await engine.start();
  assert.equal(engine.alive, true);
  assert.deepEqual(engine.endpoints, endpoints);
  await assert.rejects(engine.start(), /already running/);

  const summary = await engine.shutdown();
  assert.equal(engine.alive, false);
  assert.deepEqual(engine.summary, summary);
  assert.equal(await engine.nextEvent({ statusInterval: 0.05 }), null);
  await engine[Symbol.asyncDispose]();
});

test("streams records and periodic status", async () => {
  const engine = await Engine.start(run());
  try {
    const idle = await engine.nextEvent({ statusInterval: 0.05 });
    assert.equal(idle.type, "status");

    assert.equal(await roundTrip(engine.endpoints.tcp[0]), "ping");
    for await (const event of engine.events({ statusInterval: 0.05 })) {
      if (event.type === "status") continue;
      assert.equal(event.type, "tcp-stream");
      assert.equal(event.stream.proxy, "api");
      break;
    }

    await assert.rejects(
      engine.nextEvent({ statusInterval: 0 }),
      InvalidInputError,
    );
  } finally {
    await engine.shutdown();
  }
});

test("drives an adaptive schedule", async () => {
  const engine = await Engine.start(run());
  try {
    const schedule = await engine.beginSchedule();
    assert.equal(schedule.alive, true);

    await assert.rejects(
      schedule.addPhase("bad", [], { duration: "soon" }),
      InvalidInputError,
    );
    const phase = await schedule.addPhase("slow", latency, { duration: "1h" });
    assert.equal(phase.state, "pending");
    await assert.rejects(schedule.stopPhase(phase), PhaseStateError);
    await assert.rejects(schedule.movePhase(phase, -1), InvalidInputError);
    assert.equal((await schedule.movePhase(phase.id, 5)).name, "slow");

    const [running] = await schedule.startPhase(phase);
    assert.equal(running.state, "running");
    assert.equal((await engine.activeFaults())[0].faults[0].type, "latency");
    assert.equal((await schedule.nextTransition()).kind, "added");

    await schedule[Symbol.asyncDispose]();
    assert.equal(schedule.alive, false);
    assert.deepEqual((await engine.activeFaults())[0].faults, []);
    assert.equal(await schedule.end(), false);
  } finally {
    await engine.shutdown();
  }
});
