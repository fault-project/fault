"""Run a small fault proxy from Python.

Build and install the local package into its uv environment with:

    uv sync --project fault-python --python 3.14 --reinstall-package faultlib

Then run this example and send traffic through the proxy:

    uv run --project fault-python --python 3.14 python examples/python_proxy.py
    curl --connect-to www.google.com:443:127.0.0.1:18080 \
      https://www.google.com/

Repeat the sync command after changing the Rust bindings.
"""

import asyncio
from collections import deque

from faultlib import (
    Engine,
    PhaseState,
    Run,
    StatusEvent,
    TcpStreamEvent,
    TcpStreamRecord,
)


CONFIG: Run = {
    "schema_version": 1,
    "name": "adaptive Google connection",
    "proxies": [
        {
            "name": "google",
            "protocol": "tcp",
            "listen": "127.0.0.1:18080",
            "upstream": "www.google.com:443",
        }
    ],
    "phases": [{"name": "controlled from Python", "proxies": []}],
}


async def observe_engine(
    engine: Engine,
    latency_observed: asyncio.Event,
    recent_streams: deque[TcpStreamRecord],
) -> None:
    while engine.alive():
        match event := await engine.next_event():
            case StatusEvent(status):
                print(
                    f"streams={status.tcp.active} "
                    f"impacted={status.tcp.impacted} "
                    f"average_latency="
                    f"{status.effects.average_latency_ms:.1f}ms"
                )
            case TcpStreamEvent(stream):
                recent_streams.append(stream)
                print(
                    f"stream={stream.stream_id} "
                    f"outcome={stream.outcome} "
                    f"sent={stream.bytes_to_upstream}B "
                    f"received={stream.bytes_to_client}B"
                )
                if stream.faults.latency.applications:
                    latency_observed.set()
            case None:
                return
            case _:
                raise RuntimeError(f"unknown engine event: {event!r}")


async def run_schedule(
    engine: Engine,
    latency_observed: asyncio.Event,
) -> None:
    async with engine.schedule() as schedule:
        latency = await schedule.add_phase(
            "variable latency",
            {
                "google": [
                    {
                        "type": "latency",
                        "flow": "both",
                        "distribution": {
                            "type": "uniform",
                            "min_ms": 100.0,
                            "max_ms": 250.0,
                        },
                    }
                ]
            },
            duration="30s",
        )
        await schedule.start_phase(latency)

        await latency_observed.wait()

        bandwidth = await schedule.add_phase(
            "constrained bandwidth",
            {
                "google": [
                    {
                        "type": "bandwidth",
                        "flow": "both",
                        "bytes_per_second": 64_000,
                    }
                ]
            },
            duration="20s",
        )
        await schedule.start_phase(bandwidth)

        while transition := await schedule.next_transition():
            phase = transition.phase
            print(
                f"phase={phase.name!r} state={phase.state} reason={transition.reason}"
            )
            if phase.id == bandwidth.id and phase.state is PhaseState.STOPPED:
                return


async def main() -> None:
    engine = Engine(CONFIG)
    latency_observed = asyncio.Event()
    recent_streams: deque[TcpStreamRecord] = deque(maxlen=100)

    async with engine:
        print(f"Proxy listening on {engine.endpoints.tcp[0]}")
        print("Press Ctrl-C to stop.")

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(
                observe_engine(
                    engine,
                    latency_observed,
                    recent_streams,
                )
            )
            tasks.create_task(run_schedule(engine, latency_observed))

    if summary := engine.summary:
        print(
            f"run completed: streams={summary.status.tcp.opened} "
            f"failed={summary.status.tcp.failed} "
            f"recent_records={len(recent_streams)}"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
