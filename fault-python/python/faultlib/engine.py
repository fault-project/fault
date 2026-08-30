import asyncio
import json
from types import TracebackType
from typing import Self

from ._fault import Engine as _Engine
from .config import FaultSpec
from .events import EngineEvent, StatusEvent, TcpStreamEvent, UdpExchangeEvent
from .phase import Schedule
from .run import ProxyFaults, Run, RunProgress, RunResult
from .transport import (
    Endpoints,
    TcpStreamRecord,
    TransportRecord,
    TransportStatus,
    TransportSummary,
    UdpExchangeRecord,
    transport_record_from_json,
)


class Engine:
    """A running set of TCP and UDP fault-injection proxies.

    TCP proxies operate on connection streams; UDP proxies operate on
    request/response exchanges and support DNS-specific faults.
    Runs and faults use the same mapping shapes as fault's published JSON
    schemas. Runtime results are typed Python objects.
    """

    def __init__(self, run: Run, *, event_capacity: int = 1024):
        self._native = _Engine(json.dumps(run), event_capacity)
        self._alive = False
        self._endpoints: Endpoints | None = None
        self._summary: TransportSummary | None = None

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
        """The bound endpoints, available after the engine starts."""
        if self._endpoints is None:
            raise RuntimeError("the engine has not been started")
        return self._endpoints

    @property
    def summary(self) -> TransportSummary | None:
        """The final transport summary, available after shutdown."""
        return self._summary

    def alive(self) -> bool:
        """Whether this wrapper currently owns a running engine."""
        return self._alive

    def schedule(self) -> Schedule:
        """Create a mutable schedule of immutable phase transitions."""
        return Schedule(self)

    async def start(self) -> Endpoints:
        """Bind every configured proxy and return its actual endpoints."""
        self._endpoints = Endpoints.from_json(
            json.loads(await self._native.start())
        )
        self._alive = True
        return self._endpoints

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
        self, *, status_interval: float = 2.0
    ) -> EngineEvent | None:
        """Return the next transport record, or a periodic status event."""
        if status_interval <= 0:
            raise ValueError("status_interval must be greater than zero")

        try:
            async with asyncio.timeout(status_interval):
                record = await self.next_record()
        except TimeoutError:
            return StatusEvent(await self.status())

        if record is None:
            self._alive = False
            return None
        match record:
            case TcpStreamRecord():
                return TcpStreamEvent(record)
            case UdpExchangeRecord():
                return UdpExchangeEvent(record)

    async def shutdown(self) -> TransportSummary:
        """Stop all proxies and return the final transport summary."""
        try:
            summary = TransportSummary.from_json(
                json.loads(await self._native.shutdown())
            )
            self._summary = summary
            return summary
        finally:
            self._alive = False
