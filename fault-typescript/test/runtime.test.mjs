import assert from "node:assert/strict";
import { test } from "node:test";

import { FaultError } from "../dist/index.js";
import { assertSupportedRuntime } from "../dist/runtime.js";

test("rejects Node.js older than 24 with a clear error", () => {
  assert.throws(() => assertSupportedRuntime({ node: "23.11.1" }, {}), (error) => {
    assert.ok(error instanceof FaultError);
    assert.match(error.message, /requires Node\.js 24 or newer; this is Node\.js 23\.11\.1/);
    return true;
  });
  assert.doesNotThrow(() => assertSupportedRuntime({ node: "24.0.0" }, {}));
  assert.doesNotThrow(() => assertSupportedRuntime({ node: "26.1.0" }, {}));
});

test("leaves Bun and Deno to their own compatibility", () => {
  assert.doesNotThrow(() => assertSupportedRuntime({ node: "22.0.0", bun: "1.4.2" }, {}));
  assert.doesNotThrow(() => assertSupportedRuntime({ node: "20.0.0" }, { Deno: {} }));
});
