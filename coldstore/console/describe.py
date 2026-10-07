"""Human readable component summaries."""

from __future__ import annotations

from coldstore.ns import control_line
from coldstore.store import describe_store

from .runtime import Runtime


def collect(rt: Runtime) -> dict[str, str]:
    """Return one description line per component."""

    return {
        "pipeline": control_line().describe(),
        "store": describe_store(rt.store),
        "rooms": rt.rooms.describe(),
        "compressors": rt.compressors.describe(),
        "supply": rt.supply.describe(),
        "temp": rt.temp.describe(),
        "judge": (
            f"judge policy=v{rt.judge.policy().version} probes={len(rt.judge.probes())} "
            f"mode={rt.judge.policy().jitter_mode} up={rt.judge.policy().up_shift}"
        ),
        "ammonia": rt.ammonia.describe(),
        "interlock": rt.interlock.describe(),
        "cold_fans": rt.cold_fans.describe(),
        "evap": rt.evap.describe(),
        "defrost": rt.defrost.describe(),
        "load": rt.load.describe(rt.rooms),
        "quota": rt.accumulator.describe(),
        "audit": rt.auditor.describe(),
        "schedule": rt.scheduler.describe(rt.rooms),
    }
