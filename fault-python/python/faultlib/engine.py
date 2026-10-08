import json
from types import TracebackType
from typing import Self

from ._fault import Engine as _Engine
from .config import FaultSpec
from .events import EngineEvent, engine_event_from_json
from .phase import Schedule
from .run import ProxyFaults, Run, RunProgress, RunResult
from .transport import (
    Endpoints,
    TransportRecord,
    TransportStatus,
    TransportSummary,
    transport_record_from_json,
)


class Engine:
    """A running set of TCP and UDP fault-injection proxies.

    TCP proxies operate on connection streams; UDP proxies operate on
    request/response exchanges and support DNS-specific faults.
    Runs and faults use the same mapping shapes as fault's published JSON
    schemas. Runtime results are typed Python objects.
    """

    def __init__(self, run: Run, *, event_capacity: int | None = None):
        self._native = _Engine(json.dumps(run), event_capacity)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        await self.shutdown()

    @property
    def endpoints(self) -> Endpoints:
        """The bound endpoints, available while the engine runs."""
        return Endpoints.from_json(json.loads(self._native.endpoints()))

    @property
    def summary(self) -> TransportSummary | None:
        """The final transport summary, available after shutdown."""
        summary = self._native.summary()
        return (
            None
            if summary is None
            else TransportSummary.from_json(json.loads(summary))
        )

    def alive(self) -> bool:
        """Whether the engine is started and not yet shut down."""
        return self._native.alive()

    def schedule(self) -> Schedule:
        """Create a mutable schedule of immutable phase transitions."""
        return Schedule(self)

    async def start(self) -> Endpoints:
        """Bind every configured proxy and return its actual endpoints."""
        return Endpoints.from_json(json.loads(await self._native.start()))

    async def set_faults(self, proxy: str, faults: list[FaultSpec]) -> None:
        """Replace the active fault chain for one named proxy."""
        await self._native.set_faults(proxy, json.dumps(faults))

    async def run(self) -> RunResult:
        """Execute the configured phases and return the complete result."""
        return RunResult.from_json(json.loads(await self._native.run()))

    async def status(self) -> TransportStatus:
        """Return the lightweight current transport counters."""
        return TransportStatus.from_json(
            json.loads(await self._native.status())
        )

    async def snapshot(self) -> TransportSummary:
        """Return the current transport summary."""
        return TransportSummary.from_json(
            json.loads(await self._native.snapshot())
        )

    async def active_faults(self) -> tuple[ProxyFaults, ...]:
        """Return the active fault chain for every proxy."""
        return tuple(
            ProxyFaults.from_json(proxy)
            for proxy in json.loads(await self._native.active_faults())
        )

    async def next_progress(self) -> RunProgress | None:
        """Wait for the next run phase transition."""
        event = await self._native.next_progress()
        return (
            None if event is None else RunProgress.from_json(json.loads(event))
        )

    async def next_record(self) -> TransportRecord | None:
        """Wait for the next completed TCP stream or UDP exchange record."""
        event = await self._native.next_record()
        return (
            None
            if event is None
            else transport_record_from_json(json.loads(event))
        )

    async def next_event(
        self, *, status_interval: float | None = None
    ) -> EngineEvent | None:
        """Return the next transport record, or a periodic status event.

        A status event is returned when no record completes within
        ``status_interval`` seconds (two by default). Returns ``None`` once
        the engine stops.
        """
        event = await self._native.next_event(status_interval)
        return (
            None if event is None else engine_event_from_json(json.loads(event))
        )

    async def shutdown(self) -> TransportSummary:
        """Stop all proxies and return the final transport summary."""
        return TransportSummary.from_json(
            json.loads(await self._native.shutdown())
        )
