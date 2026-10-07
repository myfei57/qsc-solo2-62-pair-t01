"""Versioned, append-only judge policies.

A policy is never edited in place: a parameter change appends a new immutable
version and bumps the "current" pointer. Every sample is pinned to the version
that was current when it arrived, so old readings can be re-judged against the
rules in force at their sample time (see :mod:`coldstore.judge.replay`).
"""

from __future__ import annotations

import json

from coldstore.store.store import Store

from .report import (
    JITTER_MODES,
    JITTER_SUSPECT,
    LATCH_MODES,
    SHIFT_CONFIRMED,
    SHIFT_MODES,
    Policy,
    Tier,
)

POLICY_KEY = "judge:policy"
CURRENT_KEY = "judge:policy-current"

MIN_WINDOW = 1.0
MAX_WINDOW = 3600.0
MIN_SAMPLES = 2
MAX_SAMPLES_CAP = 1024
MAX_TIERS = 16


def default_policy(version: int = 1) -> Policy:
    """The safe starting rule set: confirm before up shift, suspect jitter."""

    return Policy(
        version=version,
        window_seconds=30.0,
        max_samples=64,
        tiers=(
            Tier(level=1, rate=0.05, actions=("fan-boost",), confirm=3, cooldown=30.0),
            Tier(level=2, rate=0.20, actions=("comp-stage-up", "alarm-warn"), confirm=3, cooldown=60.0),
            Tier(
                level=3,
                rate=0.50,
                actions=("comp-stage-up", "valve-open", "alarm-trip"),
                confirm=2,
                cooldown=60.0,
            ),
        ),
        jitter_mode=JITTER_SUSPECT,
        jitter_z=3.5,
        fault_streak=5,
        up_shift=SHIFT_CONFIRMED,
        down_shift=SHIFT_CONFIRMED,
        max_down_steps=1,
        latch="release",
        rate_per_probe=2.0,
        burst_per_probe=4,
        max_batch=256,
    )


def validate_policy(policy: Policy) -> None:
    """Reject a policy that the engine could not judge deterministically."""

    if not MIN_WINDOW <= policy.window_seconds <= MAX_WINDOW:
        raise ValueError("window_seconds must stay inside the supported range")
    if not MIN_SAMPLES <= policy.max_samples <= MAX_SAMPLES_CAP:
        raise ValueError("max_samples must stay inside the supported range")
    if not policy.tiers:
        raise ValueError("at least one tier is required")
    if len(policy.tiers) > MAX_TIERS:
        raise ValueError("too many tiers configured")
    if policy.jitter_mode not in JITTER_MODES:
        raise ValueError("jitter_mode must be suspect, median or off")
    if policy.up_shift not in SHIFT_MODES or policy.down_shift not in SHIFT_MODES:
        raise ValueError("shift mode must be confirmed or immediate")
    if policy.latch not in LATCH_MODES:
        raise ValueError("latch must be hold, release or standby")
    if policy.jitter_z <= 0:
        raise ValueError("jitter_z must be positive")
    if policy.fault_streak <= 0:
        raise ValueError("fault_streak must be positive")
    if policy.max_down_steps <= 0:
        raise ValueError("max_down_steps must be positive")
    if policy.rate_per_probe <= 0:
        raise ValueError("rate_per_probe must be positive")
    if policy.burst_per_probe <= 0:
        raise ValueError("burst_per_probe must be positive")
    if policy.max_batch <= 0:
        raise ValueError("max_batch must be positive")

    levels: list[int] = []
    last_rate = -1.0
    for tier in policy.tiers:
        if tier.level <= 0:
            raise ValueError("tier levels must be positive")
        if tier.level in levels:
            raise ValueError("tier levels must be unique")
        levels.append(tier.level)
        if tier.rate <= 0:
            raise ValueError("tier rates must be positive")
        if tier.rate <= last_rate:
            raise ValueError("tier rates must be strictly increasing")
        last_rate = tier.rate
        if tier.confirm <= 0:
            raise ValueError("tier confirm counts must be positive")
        if tier.cooldown < 0:
            raise ValueError("tier cooldown must not be negative")
        if not tier.actions:
            raise ValueError("every tier must allow at least one action")


def tier_from_dict(payload: dict[str, object]) -> Tier:
    actions = payload.get("actions", ())
    if isinstance(actions, str):
        actions = (actions,)
    if not isinstance(actions, (list, tuple)):
        raise ValueError("tier actions must be a list")
    return Tier(
        level=int(payload["level"]),
        rate=float(payload["rate"]),
        actions=tuple(str(action) for action in actions),
        confirm=int(payload.get("confirm", 1)),
        cooldown=float(payload.get("cooldown", 0.0)),
    )


def policy_from_dict(payload: dict[str, object]) -> Policy:
    raw_tiers = payload.get("tiers", [])
    if not isinstance(raw_tiers, list):
        raise ValueError("tiers must be a list")
    tiers = tuple(tier_from_dict(dict(item)) for item in raw_tiers)
    return Policy(
        version=int(payload.get("version", 1)),
        window_seconds=float(payload["window_seconds"]),
        max_samples=int(payload.get("max_samples", 64)),
        tiers=tiers,
        jitter_mode=str(payload.get("jitter_mode", JITTER_SUSPECT)),
        jitter_z=float(payload.get("jitter_z", 3.5)),
        fault_streak=int(payload.get("fault_streak", 5)),
        up_shift=str(payload.get("up_shift", SHIFT_CONFIRMED)),
        down_shift=str(payload.get("down_shift", SHIFT_CONFIRMED)),
        max_down_steps=int(payload.get("max_down_steps", 1)),
        latch=str(payload.get("latch", "release")),
        rate_per_probe=float(payload.get("rate_per_probe", 2.0)),
        burst_per_probe=int(payload.get("burst_per_probe", 4)),
        max_batch=int(payload.get("max_batch", 256)),
    )


class PolicyBook:
    """Persists every policy version and the pointer to the current one."""

    def __init__(self, store: Store) -> None:
        self._store = store

    def _load_all(self) -> list[Policy]:
        raw, present = self._store.get(POLICY_KEY)
        if not present:
            return []
        try:
            parsed = json.loads(raw)
        except ValueError:
            return []
        if not isinstance(parsed, list):
            return []
        policies: list[Policy] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            try:
                policies.append(policy_from_dict(item))
            except (KeyError, TypeError, ValueError):
                continue
        policies.sort(key=lambda policy: policy.version)
        return policies

    def versions(self) -> list[int]:
        return [policy.version for policy in self._load_all()]

    def current_version(self) -> int:
        raw, present = self._store.get(CURRENT_KEY)
        if present:
            try:
                return int(raw)
            except ValueError:
                pass
        policies = self._load_all()
        return policies[-1].version if policies else 0

    def current(self) -> Policy:
        policies = self._load_all()
        if not policies:
            policy = default_policy()
            self.publish(policy)
            return policy
        wanted = self.current_version()
        for policy in reversed(policies):
            if policy.version == wanted:
                return policy
        return policies[-1]

    def get(self, version: int) -> Policy | None:
        for policy in self._load_all():
            if policy.version == version:
                return policy
        return None

    def publish(self, policy: Policy) -> Policy:
        """Append ``policy`` as a new immutable version and point at it."""

        validate_policy(policy)
        policies = self._load_all()
        existing = {candidate.version for candidate in policies}
        if policy.version in existing:
            raise ValueError("policy version already exists")
        policies.append(policy)
        policies.sort(key=lambda candidate: candidate.version)
        encoded = [candidate.as_dict() for candidate in policies]
        self._store.put(POLICY_KEY, json.dumps(encoded, ensure_ascii=False))
        self._store.put(CURRENT_KEY, str(policy.version))
        return policy

    def revise(self, changes: dict[str, object]) -> Policy:
        """Clone the current policy into a new version, applying ``changes``."""

        current = self.current()
        payload = current.as_dict()
        if "tiers" in changes:
            raw_tiers = changes["tiers"]
            if not isinstance(raw_tiers, list):
                raise ValueError("tiers must be a list")
            payload["tiers"] = [dict(tier) for tier in raw_tiers]
        for name in (
            "window_seconds",
            "max_samples",
            "jitter_mode",
            "jitter_z",
            "fault_streak",
            "up_shift",
            "down_shift",
            "max_down_steps",
            "latch",
            "rate_per_probe",
            "burst_per_probe",
            "max_batch",
        ):
            if name in changes:
                payload[name] = changes[name]
        policy = policy_from_dict(payload)
        object.__setattr__(policy, "version", current.version + 1)
        return self.publish(policy)
