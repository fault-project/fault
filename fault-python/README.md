# faultlib

`faultlib` is the Python interface to the Rust
[fault](https://fault-project.com) network fault-injection engine. It runs TCP
and UDP proxies inside an `asyncio` application, applies ordered fault chains,
and exposes typed progress, status, TCP-stream, and UDP-exchange records.

Use it when network degradation is one part of a larger Python experiment—for
example, adding database latency while restarting a pod and observing service
recovery. For shell-driven experiments, use the
[`fault` CLI](https://crates.io/crates/fault-cli).

Python is a thin binding: validation, phase lifecycle, scheduling decisions,
events, and errors remain canonical Rust behavior. The package adds typed
Python mappings and dataclasses, not a second fault model.

## Requirements and installation

faultlib requires Python 3.14 or newer.

```console
python -m pip install faultlib
```

## Start a proxy

```python
import asyncio

from faultlib import Engine, Run


RUN: Run = {
    "schema_version": 1,
    "name": "slow database",
    "proxies": [
        {
            "name": "database",
            "protocol": "tcp",
            "listen": "127.0.0.1:15432",
            "upstream": "database.internal:5432",
        }
    ],
    "phases": [
        {
            "name": "degraded for thirty seconds",
            "duration": "30s",
            "proxies": [
                {
                    "proxy": "database",
                    "faults": [
                        {
                            "type": "latency",
                            "flow": "both",
                            "distribution": {
                                "type": "normal",
                                "mean_ms": 200.0,
                                "stddev_ms": 20.0,
                            },
                        }
                    ],
                }
            ],
        }
    ],
}


async def main() -> None:
    async with Engine(RUN) as engine:
        print(f"proxy listening on {engine.endpoints.tcp[0]}")
        result = await engine.run()

    print(f"run outcome: {result.outcome.kind}")


asyncio.run(main())
```

Point the application at `127.0.0.1:15432` for the duration of the experiment.
The proxy forwards the connection to `database.internal:5432`; no database
protocol support is required.

`Run` and its nested `TypedDict` types give type checkers the same shape as the
published schema. Runtime results and events are frozen dataclasses rather
than unstructured dictionaries.

## Observe traffic

`Engine.next_event()` returns a completed transport record when one is
available and otherwise publishes periodic status:

```python
from faultlib import StatusEvent, TcpStreamEvent, UdpExchangeEvent


async def observe(engine: Engine) -> None:
    while engine.alive():
        match event := await engine.next_event():
            case StatusEvent(status):
                print(
                    f"active={status.tcp.active} "
                    f"impacted={status.tcp.impacted}"
                )
            case TcpStreamEvent(stream):
                print(stream.stream_id, stream.outcome)
            case UdpExchangeEvent(exchange):
                print(exchange.exchange_id, exchange.outcome)
            case None:
                return
```

Record delivery is bounded and best effort. A slow Python consumer never
stalls the Rust proxy. Aggregate status remains complete and reports omitted
records through `dropped_records`.

## Adapt a running experiment

`engine.schedule()` exposes Rust-owned phase controls to Python. You can add,
modify, delete, start, or stop future phases while ordinary Python tasks
coordinate the surrounding system. A phase becomes immutable once it starts;
invalid mutations raise `PhaseStateError`.

The complete example in the repository demonstrates engine events, adaptive
scheduling, and bounded record retention:
[examples/python_proxy.py](https://github.com/fault-project/fault/blob/main/examples/python_proxy.py).

## Supported behavior

- TCP: latency, jitter, bandwidth, blackhole, connection reset
- UDP: latency, jitter, directional blackhole
- DNS over UDP: delay, timeout, truncation, refusal, SERVFAIL, NXDOMAIN,
  empty answers, and random A records

See the [guide](https://fault-project.com) for realistic failure scenarios and
the generated [field reference](https://fault-project.com/reference.html) for
the exact wire contract.

## Local development

From a checkout of the repository:

```console
uv sync --project fault-python --python 3.14 --reinstall-package faultlib
uv run --project fault-python --python 3.14 python examples/python_proxy.py
uv run --project fault-python --python 3.14 ruff check \
  fault-python/python examples
```

Repeat `uv sync --reinstall-package faultlib` after changing Rust binding code.

Licensed under the
[Apache License 2.0](https://github.com/fault-project/fault/blob/main/LICENSE).
