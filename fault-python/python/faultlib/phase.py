import json
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from types import TracebackType
from typing import TYPE_CHECKING, Self
from uuid import UUID

from ._types import JsonObject
from .config import FaultSpec

if TYPE_CHECKING:
    from .engine import Engine

type FaultsByProxy = dict[str, list[FaultSpec]]


class PhaseState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    STOPPED = "stopped"
    DELETED = "deleted"


class PhaseTransitionKind(StrEnum):
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    STARTED = "started"
    STOPPED = "stopped"


class PhaseTransitionReason(StrEnum):
    EXPLICIT = "explicit"
    AUTOMATIC = "automatic"
    DURATION_ELAPSED = "duration-elapsed"
    SUPERSEDED = "superseded"


@dataclass(frozen=True, slots=True)
class Phase:
    id: UUID
    name: str
    state: PhaseState
    faults: FaultsByProxy
    duration: str | None
    planned_start_at: datetime | None
    started_at: datetime | None

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        planned_start_at = value["planned_start_at"]
        started_at = value["started_at"]
        return cls(
            id=UUID(value["id"]),
            name=value["name"],
            state=PhaseState(value["state"]),
            faults={item["proxy"]: item["faults"] for item in value["faults"]},
            duration=value["duration"],
            planned_start_at=None
            if planned_start_at is None
            else datetime.fromisoformat(planned_start_at),
            started_at=None
            if started_at is None
            else datetime.fromisoformat(started_at),
        )


@dataclass(frozen=True, slots=True)
class PhaseTransition:
    phase: Phase
    kind: PhaseTransitionKind
    reason: PhaseTransitionReason | None

    @classmethod
    def from_json(cls, value: JsonObject) -> Self:
        reason = value["reason"]
        return cls(
            phase=Phase.from_json(value["phase"]),
            kind=PhaseTransitionKind(value["kind"]),
            reason=None if reason is None else PhaseTransitionReason(reason),
        )


class Schedule:
    """A transactional schedule of immutable phase transitions.

    Leaving the ``async with`` block closes the schedule and restores the
    faults that were active when it began.
    """

    def __init__(self, engine: Engine):
        self._native = engine._native

    async def __aenter__(self) -> Self:
        await self._native.begin_schedule()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        await self._native.end_schedule()

    def alive(self) -> bool:
        """Whether a phase schedule is active on the engine."""
        return self._native.schedule_active()

    async def next_transition(self) -> PhaseTransition | None:
        """Wait for the next phase lifecycle transition."""
        transition = await self._native.schedule_next_transition()
        return (
            None
            if transition is None
            else PhaseTransition.from_json(json.loads(transition))
        )

    async def add_phase(
        self, name: str, faults: FaultsByProxy, *, duration: str | None = None
    ) -> Phase:
        return _phase(
            await self._native.schedule_add_phase(
                name, duration, json.dumps(_proxy_faults(faults))
            )
        )

    async def modify_phase(
        self,
        phase: Phase,
        *,
        name: str,
        duration: str | None,
        faults: FaultsByProxy,
    ) -> Phase:
        return _phase(
            await self._native.schedule_modify_phase(
                str(phase.id), name, duration, json.dumps(_proxy_faults(faults))
            )
        )

    async def delete_phase(self, phase: Phase) -> Phase:
        return _phase(await self._native.schedule_delete_phase(str(phase.id)))

    async def start_phase(self, phase: Phase) -> tuple[Phase, ...]:
        return _phases(await self._native.schedule_start_phase(str(phase.id)))

    async def stop_phase(self, phase: Phase) -> tuple[Phase, ...]:
        return _phases(await self._native.schedule_stop_phase(str(phase.id)))

    async def move_phase(self, phase: Phase, position: int) -> Phase:
        return _phase(
            await self._native.schedule_move_phase(str(phase.id), position)
        )


def _phase(value: str) -> Phase:
    return Phase.from_json(json.loads(value))


def _phases(value: str) -> tuple[Phase, ...]:
    return tuple(Phase.from_json(item) for item in json.loads(value))


def _proxy_faults(faults: FaultsByProxy) -> list[JsonObject]:
    return [
        {"proxy": proxy, "faults": value} for proxy, value in faults.items()
    ]
