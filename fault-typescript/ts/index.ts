/**
 * Programmatic access to the fault network fault-injection engine.
 *
 * This package is a thin binding: every lifecycle rule, validation, and
 * event originates in the Rust engine.
 *
 * @packageDocumentation
 */

export { Engine } from "./engine.js";
export type { EngineOptions, EventOptions } from "./engine.js";
export {
  FaultError,
  InvalidInputError,
  PhaseStateError,
} from "./errors.js";
export type { FaultErrorCode } from "./errors.js";
export { Schedule } from "./schedule.js";
export type { PhaseChanges, PhaseOptions, PhaseRef } from "./schedule.js";
export type * from "./types.js";
