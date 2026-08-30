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
    """A transactional schedule of immutable phase transitions."""

    def __init__(self, engine: Engine):
        self._engine = engine
        self._active = False

    async def __aenter__(self) -> Self:
        await self._engine._native.begin_schedule()
        self._active = True
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        if self._active and self._engine.alive():
            try:
                await self._engine._native.end_schedule()
            finally:
                self._active = False

    def alive(self) -> bool:
        return self._active and self._engine.alive()

    async def next_transition(self) -> PhaseTransition | None:
        """Wait for the next phase lifecycle transition."""
        phase = await self._engine._native.schedule_next_transition()
        return (
            None
            if phase is None
            else PhaseTransition.from_json(json.loads(phase))
        )

    async def add_phase(
        self, name: str, faults: FaultsByProxy, *, duration: str | None = None
    ) -> Phase:
        value = await self._engine._native.schedule_add_phase(
            name, duration, json.dumps(_proxy_faults(faults))
        )
        phase = Phase.from_json(json.loads(value))
        return phase

    async def modify_phase(
        self,
        phase: Phase,
        *,
        name: str,
        duration: str | None,
        faults: FaultsByProxy,
    ) -> Phase:
        value = await self._engine._native.schedule_modify_phase(
            str(phase.id), name, duration, json.dumps(_proxy_faults(faults))
        )
        phase = Phase.from_json(json.loads(value))
        return phase

    async def delete_phase(self, phase: Phase) -> Phase:
        return (await self._transition("delete", phase))[0]

    async def start_phase(self, phase: Phase) -> tuple[Phase, ...]:
        return await self._transition("start", phase)

    async def stop_phase(self, phase: Phase) -> tuple[Phase, ...]:
        return await self._transition("stop", phase)

    async def move_phase(self, phase: Phase, position: int) -> Phase:
        self._ensure_active()
        if position < 0:
            raise ValueError("phase position cannot be negative")
        values = json.loads(
            await self._engine._native.schedule_move_phase(
                str(phase.id), position
            )
        )
        return Phase.from_json(values[0])

    async def _transition(
        self, operation: str, phase: Phase
    ) -> tuple[Phase, ...]:
        self._ensure_active()
        method = getattr(self._engine._native, f"schedule_{operation}_phase")
        values = json.loads(await method(str(phase.id)))
        phases = tuple(Phase.from_json(value) for value in values)
        return phases

    def _ensure_active(self) -> None:
        if not self._active:
            raise RuntimeError("phase schedule is not active")


def _proxy_faults(faults: FaultsByProxy) -> list[JsonObject]:
    return [
        {"proxy": proxy, "faults": value} for proxy, value in faults.items()
    ]
