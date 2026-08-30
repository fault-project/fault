from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Self
from uuid import UUID

from ._types import JsonObject


@dataclass(frozen=True, slots=True)
class Endpoints:
    tcp: tuple[str, ...]
    udp: tuple[str, ...]

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(tcp=tuple(value["tcp"]), udp=tuple(value["udp"]))


class TcpStreamOutcome(StrEnum):
    ACTIVE = "active"
    COMPLETED = "completed"
    UPSTREAM_CONNECT_FAILED = "upstream-connect-failed"
    TRANSFER_FAILED = "transfer-failed"
    CANCELLED = "cancelled"
    FAULT_RESET = "fault-reset"


class UdpExchangeOutcome(StrEnum):
    ACTIVE = "active"
    COMPLETED = "completed"
    TRANSFER_FAILED = "transfer-failed"
    CANCELLED = "cancelled"
    FAULT_DROPPED = "fault-dropped"


class TransportFailureStage(StrEnum):
    CONNECT = "connect"
    TRANSFER = "transfer"
    EXCHANGE = "exchange"


class TransportFailureCategory(StrEnum):
    DNS_FAILED = "dns-failed"
    CONNECTION_REFUSED = "connection-refused"
    TIMED_OUT = "timed-out"
    NETWORK_UNREACHABLE = "network-unreachable"
    CONNECTION_RESET = "connection-reset"
    BROKEN_PIPE = "broken-pipe"
    OTHER = "other"


@dataclass(frozen=True, slots=True)
class TransportFailure:
    stage: TransportFailureStage
    category: TransportFailureCategory
    message: str

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(
            stage=TransportFailureStage(value["stage"]),
            category=TransportFailureCategory(value["category"]),
            message=value["message"],
        )


@dataclass(frozen=True, slots=True)
class DelayRecord:
    applications: int
    total_delay_ms: float

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(**value)


@dataclass(frozen=True, slots=True)
class FaultRecord:
    latency: DelayRecord
    jitter: DelayRecord
    bandwidth_bytes_limited: int
    blackhole_activations: int
    connection_resets: int
    dns_interventions: int

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(
            latency=DelayRecord.from_json(value["latency"]),
            jitter=DelayRecord.from_json(value["jitter"]),
            bandwidth_bytes_limited=value["bandwidth_bytes_limited"],
            blackhole_activations=value["blackhole_activations"],
            connection_resets=value["connection_resets"],
            dns_interventions=value["dns_interventions"],
        )


def _failure(value: JsonObject | None) -> TransportFailure | None:
    return None if value is None else TransportFailure.from_json(value)


@dataclass(frozen=True, slots=True)
class TcpStreamRecord:
    stream_id: UUID
    proxy: str
    peer: str
    upstream: str
    opened_at: datetime
    closed_at: datetime | None
    bytes_to_upstream: int
    bytes_to_client: int
    faults: FaultRecord
    outcome: TcpStreamOutcome
    failure: TransportFailure | None

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        closed_at = value["closed_at"]
        return cls(
            stream_id=UUID(value["stream_id"]),
            proxy=value["proxy"],
            peer=value["peer"],
            upstream=value["upstream"],
            opened_at=datetime.fromisoformat(value["opened_at"]),
            closed_at=(
                None if closed_at is None else datetime.fromisoformat(closed_at)
            ),
            bytes_to_upstream=value["bytes_to_upstream"],
            bytes_to_client=value["bytes_to_client"],
            faults=FaultRecord.from_json(value["faults"]),
            outcome=TcpStreamOutcome(value["outcome"]),
            failure=_failure(value["failure"]),
        )


@dataclass(frozen=True, slots=True)
class UdpExchangeRecord:
    exchange_id: UUID
    proxy: str
    peer: str
    upstream: str
    started_at: datetime
    completed_at: datetime | None
    request_bytes: int
    response_bytes: int
    faults: FaultRecord
    outcome: UdpExchangeOutcome
    failure: TransportFailure | None

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        completed_at = value["completed_at"]
        return cls(
            exchange_id=UUID(value["exchange_id"]),
            proxy=value["proxy"],
            peer=value["peer"],
            upstream=value["upstream"],
            started_at=datetime.fromisoformat(value["started_at"]),
            completed_at=(
                None
                if completed_at is None
                else datetime.fromisoformat(completed_at)
            ),
            request_bytes=value["request_bytes"],
            response_bytes=value["response_bytes"],
            faults=FaultRecord.from_json(value["faults"]),
            outcome=UdpExchangeOutcome(value["outcome"]),
            failure=_failure(value["failure"]),
        )


type TransportRecord = TcpStreamRecord | UdpExchangeRecord


def transport_record_from_json(value: JsonObject) -> TransportRecord:
    match value["type"]:
        case "tcp-stream":
            return TcpStreamRecord.from_json(value["stream"])
        case "udp-exchange":
            return UdpExchangeRecord.from_json(value["exchange"])
        case record_type:
            raise ValueError(f"unknown transport record type: {record_type!r}")


@dataclass(frozen=True, slots=True)
class TcpStreamStatus:
    active: int
    opened: int
    active_impacted: int
    impacted: int
    completed: int
    failed: int
    average_bytes_to_upstream: int
    average_bytes_to_client: int

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(**value)


@dataclass(frozen=True, slots=True)
class UdpExchangeStatus:
    active: int
    started: int
    active_impacted: int
    impacted: int
    completed: int
    failed: int
    average_request_bytes: int
    average_response_bytes: int

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(**value)


@dataclass(frozen=True, slots=True)
class FaultStatus:
    latency_applications: int
    average_latency_ms: float
    jitter_applications: int
    average_jitter_ms: float

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(**value)


@dataclass(frozen=True, slots=True)
class TransportStatus:
    tcp: TcpStreamStatus
    udp: UdpExchangeStatus
    effects: FaultStatus
    dropped_records: int
    last_failure: TransportFailure | None

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(
            tcp=TcpStreamStatus.from_json(value["tcp"]),
            udp=UdpExchangeStatus.from_json(value["udp"]),
            effects=FaultStatus.from_json(value["effects"]),
            dropped_records=value["dropped_records"],
            last_failure=_failure(value["last_failure"]),
        )


@dataclass(frozen=True, slots=True)
class TransportSummary:
    status: TransportStatus
    tcp_streams: tuple[TcpStreamRecord, ...]
    udp_exchanges: tuple[UdpExchangeRecord, ...]

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(
            status=TransportStatus.from_json(value["status"]),
            tcp_streams=tuple(
                TcpStreamRecord.from_json(item) for item in value["tcp_streams"]
            ),
            udp_exchanges=tuple(
                UdpExchangeRecord.from_json(item)
                for item in value["udp_exchanges"]
            ),
        )
