/** Stable error codes produced by the Rust engine handle. */
export type FaultErrorCode = "INVALID_INPUT" | "PHASE_STATE" | "RUNTIME";

/** Base class of every error raised by the fault engine. */
export class FaultError extends Error {
  override name = "FaultError";

  constructor(
    message: string,
    readonly code: FaultErrorCode = "RUNTIME",
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

/** A malformed or out-of-range argument, such as an invalid run or duration. */
export class InvalidInputError extends FaultError {
  override name = "InvalidInputError";

  constructor(message: string, options?: ErrorOptions) {
    super(message, "INVALID_INPUT", options);
  }
}

/** A phase operation targeted a phase in the wrong lifecycle state. */
export class PhaseStateError extends FaultError {
  override name = "PhaseStateError";

  constructor(message: string, options?: ErrorOptions) {
    super(message, "PHASE_STATE", options);
  }
}

const coded = /^\[(INVALID_INPUT|PHASE_STATE|RUNTIME)\] /;

function translate(error: unknown): unknown {
  if (!(error instanceof Error)) return error;
  const match = coded.exec(error.message);
  if (match === null) return error;
  const message = error.message.slice(match[0].length);
  const options = { cause: error };
  switch (match[1] as FaultErrorCode) {
    case "INVALID_INPUT":
      return new InvalidInputError(message, options);
    case "PHASE_STATE":
      return new PhaseStateError(message, options);
    case "RUNTIME":
      return new FaultError(message, "RUNTIME", options);
  }
}

/** Run a native call, converting coded native errors into error classes. */
export function sync<T>(call: () => T): T {
  try {
    return call();
  } catch (error) {
    throw translate(error);
  }
}

/** Await a native call, converting coded native errors into error classes. */
export async function settle<T>(call: Promise<T>): Promise<T> {
  try {
    return await call;
  } catch (error) {
    throw translate(error);
  }
}
