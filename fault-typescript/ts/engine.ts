import { NativeEngine } from "../native.js";
import { settle, sync } from "./errors.js";
import { Schedule } from "./schedule.js";
import type {
  BoundEndpoints,
  EngineEvent,
  FaultSpec,
  ProxyFaults,
  Run,
  RunProgress,
  RunResult,
  TransportRecord,
  TransportStatus,
  TransportSummary,
} from "./types.js";

export interface EngineOptions {
  /** Bound of the completed transport record channel. Defaults to 1024. */
  eventCapacity?: number;
}

export interface EventOptions {
  /**
   * Seconds to wait for a transport record before returning a status event
   * instead. Defaults to 2.
   */
  statusInterval?: number;
}

const parse = <T>(json: string) => JSON.parse(json) as T;
const parseNullable = <T>(json: string | null) =>
  json === null ? null : parse<T>(json);

/**
 * A set of TCP and UDP fault-injection proxies.
 *
 * TCP proxies operate on connection streams; UDP proxies operate on
 * request/response exchanges and support DNS-specific faults. Runs, faults,
 * and results use the shapes of fault's published JSON Schemas.
 *
 * ```ts
 * await using engine = await Engine.start(run);
 * ```
 */
export class Engine implements AsyncDisposable {
  readonly #native: NativeEngine;

  /** Validate a run and prepare its proxies without binding any socket. */
  constructor(run: Run, options: EngineOptions = {}) {
    this.#native = sync(
      () => new NativeEngine(JSON.stringify(run), options.eventCapacity),
    );
  }

  /** Create an engine and start it. */
  static async start(run: Run, options?: EngineOptions): Promise<Engine> {
    const engine = new Engine(run, options);
    await engine.start();
    return engine;
  }

  /** Whether the engine is started and not yet shut down. */
  get alive(): boolean {
    return this.#native.alive();
  }

  /** The bound endpoints, available while the engine runs. */
  get endpoints(): BoundEndpoints {
    return parse(sync(() => this.#native.endpoints()));
  }

  /** The final transport summary, available after shutdown. */
  get summary(): TransportSummary | null {
    return parseNullable(this.#native.summary());
  }

  /** Bind every configured proxy and return the actual endpoints. */
  async start(): Promise<BoundEndpoints> {
    return parse(await settle(this.#native.start()));
  }

  /** Replace the active fault chain of one named proxy. */
  async setFaults(proxy: string, faults: FaultSpec[]): Promise<void> {
    await settle(this.#native.setFaults(proxy, JSON.stringify(faults)));
  }

  /** Execute the configured phases and return the complete result. */
  async run(): Promise<RunResult> {
    return parse(await settle(this.#native.run()));
  }

  /** Return the lightweight current transport counters. */
  async status(): Promise<TransportStatus> {
    return parse(await settle(this.#native.status()));
  }

  /** Return the current transport summary. */
  async snapshot(): Promise<TransportSummary> {
    return parse(await settle(this.#native.snapshot()));
  }

  /** Return the active fault chain of every proxy. */
  async activeFaults(): Promise<ProxyFaults[]> {
    return parse(await settle(this.#native.activeFaults()));
  }

  /** Wait for the next run phase transition; `null` once the engine stops. */
  async nextProgress(): Promise<RunProgress | null> {
    return parseNullable(await settle(this.#native.nextProgress()));
  }

  /** Wait for the next completed record; `null` once the engine stops. */
  async nextRecord(): Promise<TransportRecord | null> {
    return parseNullable(await settle(this.#native.nextRecord()));
  }

  /**
   * Wait for the next completed record, or a status event when none
   * completes within the status interval; `null` once the engine stops.
   */
  async nextEvent(options: EventOptions = {}): Promise<EngineEvent | null> {
    return parseNullable(
      await settle(this.#native.nextEvent(options.statusInterval)),
    );
  }

  /** Iterate run phase transitions until the engine stops. */
  async *progress(): AsyncGenerator<RunProgress, void, undefined> {
    for (let next; (next = await this.nextProgress()) !== null; ) yield next;
  }

  /** Iterate completed transport records until the engine stops. */
  async *records(): AsyncGenerator<TransportRecord, void, undefined> {
    for (let next; (next = await this.nextRecord()) !== null; ) yield next;
  }

  /** Iterate engine events until the engine stops. */
  async *events(
    options: EventOptions = {},
  ): AsyncGenerator<EngineEvent, void, undefined> {
    for (let next; (next = await this.nextEvent(options)) !== null; ) {
      yield next;
    }
  }

  /** Take transactional control of the engine's faults. */
  async beginSchedule(): Promise<Schedule> {
    await settle(this.#native.beginSchedule());
    return new Schedule(this.#native);
  }

  /** Stop every proxy and return the final transport summary. */
  async shutdown(): Promise<TransportSummary> {
    return parse(await settle(this.#native.shutdown()));
  }

  /** Shut down if running; safe to call in any state. */
  async [Symbol.asyncDispose](): Promise<void> {
    await settle(this.#native.close());
  }
}
