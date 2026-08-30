from dataclasses import dataclass

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
