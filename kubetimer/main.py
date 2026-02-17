"""KubeTimer Operator — Main entry point.

Registers Kopf handlers, starts APScheduler, and runs the operator loop.
Deployments with expired TTL annotations are deleted via event-driven
APScheduler DateTrigger jobs (not periodic polling).
"""

import asyncio
import kopf
import uvloop

from kubetimer import register_all_handlers
from kubetimer.utils.logs import setup_logging

setup_logging()

loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)

register_all_handlers()


def main():
    """
    Main entry point for KubeTimer operator.

    Uses the uvloop event loop created at module level for
    high-performance async I/O.
    """
    kopf.run(
        standalone=True,
        clusterwide=True,
        liveness_endpoint="http://0.0.0.0:8080/healthz",
        loop=loop,
    )


if __name__ == "__main__":
    main()
