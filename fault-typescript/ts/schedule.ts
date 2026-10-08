import type { NativeEngine } from "../native.js";
import { settle, sync } from "./errors.js";
import type {
  ControlledPhase,
  HumanDuration,
  PhaseTransition,
  ProxyFaults,
} from "./types.js";

/** A phase, or the identifier of a phase, in an adaptive schedule. */
export type PhaseRef = ControlledPhase | string;

export interface PhaseOptions {
  /** Run time such as `"30s"`; omit to run until the phase is stopped. */
  duration?: HumanDuration | null;
}

export interface PhaseChanges extends PhaseOptions {
  name: string;
  faults: ProxyFaults[];
}

const id = (phase: PhaseRef) => (typeof phase === "string" ? phase : phase.id);
const parse = <T>(json: string) => JSON.parse(json) as T;

/**
 * A transactional schedule of immutable phase transitions.
 *
 * Create one with `engine.beginSchedule()`. Ending it, explicitly or by
 * leaving an `await using` scope, restores the faults that were active when
 * it began.
 */
export class Schedule implements AsyncDisposable {
  readonly #native: NativeEngine;

  /** @internal Use `Engine.beginSchedule()`. */
  constructor(native: NativeEngine) {
    this.#native = native;
  }

  /** Whether a phase schedule is active on the engine. */
  get alive(): boolean {
    return sync(() => this.#native.scheduleActive());
  }

  /** Append a pending phase. */
  async addPhase(
    name: string,
    faults: ProxyFaults[],
    options: PhaseOptions = {},
  ): Promise<ControlledPhase> {
    return parse(
      await settle(
        this.#native.addPhase(name, options.duration, JSON.stringify(faults)),
      ),
    );
  }

  /** Replace a pending phase. */
  async modifyPhase(
    phase: PhaseRef,
    changes: PhaseChanges,
  ): Promise<ControlledPhase> {
    return parse(
      await settle(
        this.#native.modifyPhase(
          id(phase),
          changes.name,
          changes.duration,
          JSON.stringify(changes.faults),
        ),
      ),
    );
  }

  /** Delete a pending phase. */
  async deletePhase(phase: PhaseRef): Promise<ControlledPhase> {
    return parse(await settle(this.#native.deletePhase(id(phase))));
  }

  /** Start a pending phase, superseding the running one. Returns every changed phase. */
  async startPhase(phase: PhaseRef): Promise<ControlledPhase[]> {
    return parse(await settle(this.#native.startPhase(id(phase))));
  }

  /** Stop a running phase and start the next pending one. Returns every changed phase. */
  async stopPhase(phase: PhaseRef): Promise<ControlledPhase[]> {
    return parse(await settle(this.#native.stopPhase(id(phase))));
  }

  /** Move a pending phase to a zero-based position among pending phases. */
  async movePhase(phase: PhaseRef, position: number): Promise<ControlledPhase> {
    return parse(await settle(this.#native.movePhase(id(phase), position)));
  }

  /** Wait for the next phase transition; `null` once the schedule ends. */
  async nextTransition(): Promise<PhaseTransition | null> {
    const transition = await settle(this.#native.nextTransition());
    return transition === null ? null : parse(transition);
  }

  /** Iterate phase transitions until the schedule ends. */
  async *transitions(): AsyncGenerator<PhaseTransition, void, undefined> {
    for (let next; (next = await this.nextTransition()) !== null; ) yield next;
  }

  /** End the schedule. Returns whether a schedule was active. */
  async end(): Promise<boolean> {
    return settle(this.#native.endSchedule());
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.end();
  }
}
