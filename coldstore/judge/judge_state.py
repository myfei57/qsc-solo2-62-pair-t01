"""Per probe windows and linkage state persisted in the shared store."""

from __future__ import annotations

import json

from coldstore.store.commands import load_commands
from coldstore.store.store import Store

from .report import ProbeState, Sample

PROBES_KEY = "judge:probes"


def samples_key(probe: str) -> str:
    return f"judge:samples:{probe}"


def state_key(probe: str) -> str:
    return f"judge:state:{probe}"


def _sample_from_dict(payload: dict[str, object]) -> Sample:
    return Sample(
        probe=str(payload.get("probe", "")),
        value=float(payload.get("value", 0.0)),
        at=int(payload.get("at", 0)),
        policy_version=int(payload.get("policy_version", 0)),
        suspect=bool(payload.get("suspect", False)),
    )


def register_probe(store: Store, probe: str) -> None:
    probes = probe_ids(store)
    if probe not in probes:
        probes.append(probe)
        store.put(PROBES_KEY, json.dumps(probes, ensure_ascii=False))


def probe_ids(store: Store) -> list[str]:
    raw, present = store.get(PROBES_KEY)
    if not present:
        return []
    try:
        parsed = json.loads(raw)
    except ValueError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


def load_window(store: Store, probe: str) -> list[Sample]:
    window: list[Sample] = []
    for item in load_commands(store, samples_key(probe)):
        try:
            payload = json.loads(item)
        except ValueError:
            continue
        if isinstance(payload, dict):
            window.append(_sample_from_dict(payload))
    window.sort(key=lambda sample: (sample.at, sample.policy_version))
    return window


def save_window(store: Store, probe: str, window: list[Sample]) -> None:
    encoded = [json.dumps(sample.as_dict(), ensure_ascii=False) for sample in window]
    store.put(samples_key(probe), json.dumps(encoded, ensure_ascii=False))


def initial_state(probe: str) -> ProbeState:
    return ProbeState(
        probe=probe,
        tier=0,
        measured_tier=0,
        up_streak=0,
        down_streak=0,
        suspect_streak=0,
        healthy_streak=0,
        faulted=False,
        latched=False,
        last_action_at={},
        last_at=0,
    )


def load_state(store: Store, probe: str) -> ProbeState:
    raw, present = store.get(state_key(probe))
    if not present:
        return initial_state(probe)
    try:
        payload = json.loads(raw)
    except ValueError:
        return initial_state(probe)
    if not isinstance(payload, dict):
        return initial_state(probe)
    action_map = payload.get("last_action_at", {})
    if not isinstance(action_map, dict):
        action_map = {}
    return ProbeState(
        probe=str(payload.get("probe", probe)),
        tier=int(payload.get("tier", 0)),
        measured_tier=int(payload.get("measured_tier", 0)),
        up_streak=int(payload.get("up_streak", payload.get("streak", 0))),
        down_streak=int(payload.get("down_streak", 0)),
        suspect_streak=int(payload.get("suspect_streak", 0)),
        healthy_streak=int(payload.get("healthy_streak", 0)),
        faulted=bool(payload.get("faulted", False)),
        latched=bool(payload.get("latched", False)),
        last_action_at={str(key): int(value) for key, value in action_map.items()},
        last_at=int(payload.get("last_at", 0)),
    )


def save_state(store: Store, probe: str, state: ProbeState) -> None:
    store.put(state_key(probe), json.dumps(state.as_dict(), ensure_ascii=False))
