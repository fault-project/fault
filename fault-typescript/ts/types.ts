// Generated from docs/schemas by scripts/generate-types.mjs.
// Do not edit by hand; run `npm run generate:types` instead.

/**
 * One observation from a running engine: a completed transport record, or
 * a periodic status when no record completed within the status interval.
 */
export type EngineEvent =
  | {
      status: TransportStatus;
      type: "status";
    }
  | {
      stream: TcpStreamRecord;
      type: "tcp-stream";
    }
  | {
      exchange: UdpExchangeRecord;
      type: "udp-exchange";
    };
export type TransportFailureCategory =
  | "dns-failed"
  | "connection-refused"
  | "timed-out"
  | "network-unreachable"
  | "connection-reset"
  | "broken-pipe"
  | "other";
export type TransportFailureStage = "connect" | "transfer" | "exchange";
export type TcpStreamOutcome =
  "active" | "completed" | "upstream-connect-failed" | "transfer-failed" | "cancelled" | "fault-reset";
export type UdpExchangeOutcome = "active" | "completed" | "transfer-failed" | "cancelled" | "fault-dropped";
export type JournalEvent =
  | {
      faults: ProxyFaults[];
      name?: string | null;
      proxies: Proxy[];
      schema_version: 1;
      started_at: string;
      type: "run-started";
    }
  | {
      stream: TcpStreamRecord;
      type: "tcp-stream-completed";
    }
  | {
      exchange: UdpExchangeRecord;
      type: "udp-exchange-completed";
    }
  | {
      completed_at: string;
      status: TransportStatus;
      type: "run-completed";
    };
export type FaultSpec =
  | {
      distribution: DelayDistribution;
      flow: TrafficFlow;
      type: "latency";
    }
  | {
      flow: TrafficFlow;
      max_delay_ms: number;
      min_delay_ms: number;
      probability: number;
      type: "jitter";
    }
  | {
      bytes_per_second: number;
      flow: TrafficFlow;
      type: "bandwidth";
    }
  | {
      flow: TrafficFlow;
      type: "blackhole";
    }
  | {
      flow: TrafficFlow;
      probability: number;
      type: "connection-reset";
    }
  | {
      case: DnsCase;
      delay_ms?: number | null;
      type: "dns";
    };
export type DelayDistribution =
  | {
      mean_ms: number;
      stddev_ms: number;
      type: "normal";
    }
  | {
      scale_ms: number;
      shape: number;
      type: "pareto";
    }
  | {
      normal_mean_ms: number;
      normal_stddev_ms: number;
      pareto_scale_ms: number;
      pareto_shape: number;
      type: "pareto-normal";
    }
  | {
      max_ms: number;
      min_ms: number;
      type: "uniform";
    };
export type TrafficFlow = "to-upstream" | "to-client" | "both";
export type DnsCase =
  "delay" | "timeout" | "truncated" | "refused" | "serv-fail" | "nx-domain" | "empty-answer" | "random-a";
export type TransportProtocol = "tcp" | "udp";
/**
 * The lifecycle change carried by a phase transition.
 */
export type PhaseTransitionKind = "added" | "modified" | "deleted" | "started" | "stopped";
/**
 * A validated, human-readable duration such as `250ms`, `30s`, or `2m`.
 */
export type HumanDuration = string;
/**
 * Lifecycle state of an adaptively scheduled phase.
 */
export type PhaseState = "pending" | "running" | "stopped" | "deleted";
/**
 * Why a phase started or stopped.
 */
export type PhaseTransitionReason = "explicit" | "automatic" | "duration-elapsed" | "superseded";
export type RunOutcome =
  | {
      type: "success";
    }
  | {
      message: string;
      type: "failed";
    };
export type TransportRecord =
  | {
      stream: TcpStreamRecord;
      type: "tcp-stream";
    }
  | {
      exchange: UdpExchangeRecord;
      type: "udp-exchange";
    };

/**
 * Socket addresses actually bound by a started engine, in configuration
 * order.
 */
export interface BoundEndpoints {
  /**
   * Bound address of every TCP proxy.
   */
  tcp: string[];
  /**
   * Bound address of every UDP proxy.
   */
  udp: string[];
}
export interface TransportStatus {
  /**
   * Completed records omitted from the best-effort event stream.
   */
  dropped_records: number;
  effects: FaultStatus;
  last_failure?: TransportFailure | null;
  tcp: TcpStreamStatus;
  udp: UdpExchangeStatus;
}
export interface FaultStatus {
  average_jitter_ms: number;
  average_latency_ms: number;
  /**
   * Bytes paced by bandwidth faults, summed over every stream and
   * exchange.
   */
  bandwidth_bytes_limited?: number;
  /**
   * Traffic directions blackholed, summed over every stream and exchange.
   * Each direction of a stream or exchange counts at most once.
   */
  blackhole_activations?: number;
  /**
   * Streams reset by a connection-reset fault.
   */
  connection_resets?: number;
  /**
   * DNS queries altered by a DNS fault.
   */
  dns_interventions?: number;
  jitter_applications: number;
  latency_applications: number;
}
export interface TransportFailure {
  category: TransportFailureCategory;
  message: string;
  stage: TransportFailureStage;
}
export interface TcpStreamStatus {
  active: number;
  active_impacted: number;
  average_bytes_to_client: number;
  average_bytes_to_upstream: number;
  completed: number;
  failed: number;
  impacted: number;
  opened: number;
}
export interface UdpExchangeStatus {
  active: number;
  active_impacted: number;
  average_request_bytes: number;
  average_response_bytes: number;
  completed: number;
  failed: number;
  impacted: number;
  started: number;
}
export interface TcpStreamRecord {
  bytes_to_client: number;
  bytes_to_upstream: number;
  closed_at?: string | null;
  failure?: TransportFailure | null;
  faults: FaultRecord;
  opened_at: string;
  outcome: TcpStreamOutcome;
  peer: string;
  proxy: string;
  stream_id: string;
  upstream: string;
}
export interface FaultRecord {
  bandwidth_bytes_limited: number;
  blackhole_activations: number;
  connection_resets: number;
  dns_interventions: number;
  jitter: DelayRecord;
  latency: DelayRecord;
}
export interface DelayRecord {
  applications: number;
  total_delay_ms: number;
}
export interface UdpExchangeRecord {
  completed_at?: string | null;
  exchange_id: string;
  failure?: TransportFailure | null;
  faults: FaultRecord;
  outcome: UdpExchangeOutcome;
  peer: string;
  proxy: string;
  request_bytes: number;
  response_bytes: number;
  started_at: string;
  upstream: string;
}
/**
 * The complete fault chain active on one named proxy during a phase.
 */
export interface ProxyFaults {
  /**
   * Ordered chain of faults applied to traffic on this proxy.
   */
  faults: FaultSpec[];
  /**
   * Name of a proxy declared by the containing run.
   */
  proxy: string;
}
export interface Proxy {
  /**
   * Local socket address clients connect or send datagrams to.
   */
  listen: string;
  /**
   * Stable name used by phases to select this proxy.
   */
  name: string;
  protocol: TransportProtocol;
  /**
   * Remote socket address to which traffic is forwarded.
   */
  upstream: string;
}
/**
 * An immutable record of one phase lifecycle change.
 */
export interface PhaseTransition {
  kind: PhaseTransitionKind;
  phase: ControlledPhase;
  /**
   * Set for `started` and `stopped` transitions.
   */
  reason?: PhaseTransitionReason | null;
}
/**
 * One phase of an adaptive schedule, as observed at a point in time.
 */
export interface ControlledPhase {
  /**
   * Omitted when the phase runs until explicitly stopped.
   */
  duration?: HumanDuration | null;
  /**
   * Fault chains applied to named proxies while the phase runs.
   */
  faults: ProxyFaults[];
  /**
   * Identifier assigned by the engine when the phase is added.
   */
  id: string;
  /**
   * Human-readable phase name.
   */
  name: string;
  /**
   * Projected UTC start, known only when every earlier phase is timed.
   */
  planned_start_at?: string | null;
  /**
   * UTC instant at which the phase started running.
   */
  started_at?: string | null;
  state: PhaseState;
}
/**
 * A lightweight phase transition published while a run executes.
 */
export interface RunProgress {
  phase_count: number;
  /**
   * Planned duration, or absent for an indefinite final phase.
   */
  phase_duration_ms?: number | null;
  /**
   * One-based position of the active phase.
   */
  phase_index: number;
  /**
   * Name of the phase that has just started.
   */
  phase_name: string;
  /**
   * UTC instant at which the phase became active.
   */
  phase_started_at: string;
  /**
   * Fault chains active for this phase.
   */
  proxies: ProxyFaults[];
  /**
   * Name of the executing run.
   */
  run_name: string;
}
export interface RunResult {
  /**
   * UTC instant at which execution ended.
   */
  completed_at: string;
  outcome: RunOutcome;
  /**
   * Name of the completed run.
   */
  run_name: string;
  /**
   * Version of the serialized result format. The current value is `1`.
   */
  schema_version: 1;
  /**
   * UTC instant at which execution began.
   */
  started_at: string;
  transport: TransportSummary;
}
export interface TransportSummary {
  status: TransportStatus;
  tcp_streams: TcpStreamRecord[];
  udp_exchanges: UdpExchangeRecord[];
}
export interface Run {
  /**
   * Human-readable name used in progress, results, and journals.
   */
  name: string;
  /**
   * Ordered phases. Each begins when the previous phase ends.
   *
   * @minItems 1
   */
  phases: [Phase, ...Phase[]];
  /**
   * Network entry points available throughout the run.
   *
   * @minItems 1
   */
  proxies: [Proxy, ...Proxy[]];
  /**
   * Version of the serialized run format. The current value is `1`.
   */
  schema_version: 1;
}
/**
 * A period during which one ordered set of network conditions is active.
 */
export interface Phase {
  /**
   * Omit to keep this phase active until explicitly stopped.
   */
  duration?: HumanDuration | null;
  /**
   * Human-readable phase name exposed in progress events.
   */
  name: string;
  /**
   * Fault chains applied to named proxies during this phase.
   */
  proxies: ProxyFaults[];
}
