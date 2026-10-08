from dataclasses import dataclass

from ._types import JsonObject
from .transport import TcpStreamRecord, TransportStatus, UdpExchangeRecord


@dataclass(frozen=True, slots=True)
class StatusEvent:
    status: TransportStatus


@dataclass(frozen=True, slots=True)
class TcpStreamEvent:
    stream: TcpStreamRecord


@dataclass(frozen=True, slots=True)
class UdpExchangeEvent:
    exchange: UdpExchangeRecord


type EngineEvent = StatusEvent | TcpStreamEvent | UdpExchangeEvent


def engine_event_from_json(value: JsonObject) -> EngineEvent:
    match value["type"]:
        case "status":
            return StatusEvent(TransportStatus.from_json(value["status"]))
        case "tcp-stream":
            return TcpStreamEvent(TcpStreamRecord.from_json(value["stream"]))
        case "udp-exchange":
            return UdpExchangeEvent(
                UdpExchangeRecord.from_json(value["exchange"])
            )
        case event_type:
            raise ValueError(f"unknown engine event type: {event_type!r}")
