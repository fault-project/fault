from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Literal, NotRequired, Self, TypedDict

from ._types import JsonObject
from .config import FaultSpec, TransportProtocol
from .transport import TransportSummary


class Proxy(TypedDict):
    name: str
    protocol: TransportProtocol
    listen: str
    upstream: str


class ProxyFaultConfig(TypedDict):
    proxy: str
    faults: list[FaultSpec]


class PhaseConfig(TypedDict):
    name: str
    duration: NotRequired[str]
    proxies: list[ProxyFaultConfig]


class Run(TypedDict):
    schema_version: Literal[1]
    name: str
    proxies: list[Proxy]
    phases: list[PhaseConfig]


@dataclass(frozen=True, slots=True)
class ProxyFaults:
    proxy: str
    faults: tuple[FaultSpec, ...]

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(proxy=value["proxy"], faults=tuple(value["faults"]))


@dataclass(frozen=True, slots=True)
class RunProgress:
    run_name: str
    phase_name: str
    phase_index: int
    phase_count: int
    phase_duration_ms: int | None
    phase_started_at: datetime
    proxies: tuple[ProxyFaults, ...]

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(
            run_name=value["run_name"],
            phase_name=value["phase_name"],
            phase_index=value["phase_index"],
            phase_count=value["phase_count"],
            phase_duration_ms=value["phase_duration_ms"],
            phase_started_at=datetime.fromisoformat(value["phase_started_at"]),
            proxies=tuple(
                ProxyFaults.from_json(item) for item in value["proxies"]
            ),
        )


class RunOutcomeType(StrEnum):
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RunOutcome:
    kind: RunOutcomeType
    message: str | None = None

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(
            kind=RunOutcomeType(value["type"]),
            message=value.get("message"),
        )


@dataclass(frozen=True, slots=True)
class RunResult:
    schema_version: int
    run_name: str
    started_at: datetime
    completed_at: datetime
    outcome: RunOutcome
    transport: TransportSummary

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        return cls(
            schema_version=value["schema_version"],
            run_name=value["run_name"],
            started_at=datetime.fromisoformat(value["started_at"]),
            completed_at=datetime.fromisoformat(value["completed_at"]),
            outcome=RunOutcome.from_json(value["outcome"]),
            transport=TransportSummary.from_json(value["transport"]),
        )
