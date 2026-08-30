"""Run a DNS fault from Python.

Build the local package and start the proxy with:

    uv sync --project fault-python --python 3.14 --reinstall-package faultlib
    uv run --project fault-python --python 3.14 \
        python examples/python_dns_proxy.py

Then query it from another terminal:

    dig @127.0.0.1 -p 15353 example.com
"""

import asyncio

from faultlib import Engine, Run


CONFIG: Run = {
    "schema_version": 1,
    "name": "delayed DNS",
    "proxies": [
        {
            "name": "dns",
            "protocol": "udp",
            "listen": "127.0.0.1:15353",
            "upstream": "1.1.1.1:53",
        }
    ],
    "phases": [
        {
            "name": "slow resolver",
            "proxies": [
                {
                    "proxy": "dns",
                    "faults": [{"type": "dns", "case": "delay", "delay_ms": 500}],
                }
            ],
        }
    ],
}


async def main() -> None:
    async with Engine(CONFIG) as engine:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(engine.run())
            endpoint = engine.endpoints.udp[0]
            host, port = endpoint.rsplit(":", maxsplit=1)
            print(f"DNS proxy listening on udp://{endpoint}")
            print(f"Try: dig @{host} -p {port} example.com")
            print("Press Ctrl-C to stop.")
            await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
