import { FaultError } from "./errors.js";

/** Oldest Node.js major version faultlib supports. */
export const MINIMUM_NODE_MAJOR = 24;

/**
 * Fail with a clear message on unsupported Node.js versions.
 *
 * The platform addon packages deliberately declare no `engines` field: npm
 * silently skips optional dependencies whose engines do not match, which
 * would surface as an opaque "Cannot find native binding" error instead.
 * Bun and Deno report a Node.js compatibility version, so they are not
 * checked here.
 *
 * @internal
 */
export function assertSupportedRuntime(
  versions: NodeJS.ProcessVersions = process.versions,
  global: object = globalThis,
): void {
  if (versions.bun !== undefined || "Deno" in global) return;
  const major = Number(versions.node.split(".")[0]);
  if (major < MINIMUM_NODE_MAJOR) {
    throw new FaultError(
      `faultlib requires Node.js ${MINIMUM_NODE_MAJOR} or newer; ` +
        `this is Node.js ${versions.node}`,
    );
  }
}

assertSupportedRuntime();
