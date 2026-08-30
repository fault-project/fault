from typing import Literal, NotRequired, TypedDict

type TrafficFlow = Literal["to-upstream", "to-client", "both"]
type TransportProtocol = Literal["tcp", "udp"]


class NormalDistribution(TypedDict):
    type: Literal["normal"]
    mean_ms: float
    stddev_ms: float


class ParetoDistribution(TypedDict):
    type: Literal["pareto"]
    shape: float
    scale_ms: float


class ParetoNormalDistribution(TypedDict):
    type: Literal["pareto-normal"]
    pareto_shape: float
    pareto_scale_ms: float
    normal_mean_ms: float
    normal_stddev_ms: float


class UniformDistribution(TypedDict):
    type: Literal["uniform"]
    min_ms: float
    max_ms: float


type DelayDistribution = (
    NormalDistribution
    | ParetoDistribution
    | ParetoNormalDistribution
    | UniformDistribution
)


class LatencyFault(TypedDict):
    type: Literal["latency"]
    flow: TrafficFlow
    distribution: DelayDistribution


class JitterFault(TypedDict):
    type: Literal["jitter"]
    flow: TrafficFlow
    min_delay_ms: float
    max_delay_ms: float
    probability: float


class BandwidthFault(TypedDict):
    type: Literal["bandwidth"]
    flow: TrafficFlow
    bytes_per_second: int


class BlackholeFault(TypedDict):
    type: Literal["blackhole"]
    flow: TrafficFlow


class ConnectionResetFault(TypedDict):
    type: Literal["connection-reset"]
    flow: TrafficFlow
    probability: float


class DnsFault(TypedDict):
    type: Literal["dns"]
    case: Literal[
        "delay",
        "timeout",
        "truncated",
        "refused",
        "serv-fail",
        "nx-domain",
        "empty-answer",
        "random-a",
    ]
    delay_ms: NotRequired[int | None]


type FaultSpec = (
    LatencyFault
    | JitterFault
    | BandwidthFault
    | BlackholeFault
    | ConnectionResetFault
    | DnsFault
)
