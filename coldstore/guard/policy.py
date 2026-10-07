"""Versioned sliding window policy: window length, tiers, jitter and flood rules.

Every policy change is stored as a new immutable version. Live judgement always
uses the current version, while replay walks the version that was effective at
each sample's own timestamp, so old readings can be recomputed under the rules
that were in force back then.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

from coldstore.store.store import Store

POLICY_CURRENT_KEY = "guard:policy:current"
POLICY_KEY_PREFIX = "guard:policy:"

DEFAULT_POLICY_SPEC: dict[str, object] = {
    "window_seconds": 30,
    "tiers": [
        {"name": "watch", "limit": 0.5, "actions": ["audit"], "confirm": 2, "hold": 3},
        {"name": "alarm", "limit": 1.0, "actions": ["audit", "notify"], "confirm": 2, "hold": 3},
        {"name": "critical", "limit": 2.0, "actions": ["audit", "notify"], "confirm": 1, "hold": 5},
    ],
    "spike_rate": 3.0,
    "step_adopt": 5,
    "max_suspicious_ratio": 0.5,
    "rate_per_second": 2.0,
    "burst": 5,
    "global_rate": 20.0,
    "global_burst": 60,
    "keep_samples": 3600,
    "keep_verdicts": 500,
}


@dataclass(frozen=True)
class Tier:
    """One escalation tier: a rate limit plus what may fire while it holds."""

    name: str
    limit: float
    actions: tuple[str, ...]
    confirm: int
    hold: int

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "limit": self.limit,
            "actions": list(self.actions),
            "confirm": self.confirm,
            "hold": self.hold,
        }


@dataclass(frozen=True)
class Policy:
    """One immutable version of the guard configuration."""

    version: int
    since: int
    window_seconds: int
    tiers: tuple[Tier, ...]
    spike_rate: float
    step_adopt: int
    max_suspicious_ratio: float
    rate_per_second: float
    burst: int
    global_rate: float
    global_burst: int
    keep_samples: int
    keep_verdicts: int

    def tier_name(self, index: int) -> str:
        if index <= 0 or index > len(self.tiers):
            return "normal"
        return self.tiers[index - 1].name

    def tier_actions(self, index: int) -> tuple[str, ...]:
        if index <= 0 or index > len(self.tiers):
            return ()
        return self.tiers[index - 1].actions

    def spec(self) -> dict[str, object]:
        """The editable form, round trippable through :func:`normalize_spec`."""

        return {
            "window_seconds": self.window_seconds,
            "tiers": [tier.as_dict() for tier in self.tiers],
            "spike_rate": self.spike_rate,
            "step_adopt": self.step_adopt,
            "max_suspicious_ratio": self.max_suspicious_ratio,
            "rate_per_second": self.rate_per_second,
            "burst": self.burst,
            "global_rate": self.global_rate,
            "global_burst": self.global_burst,
            "keep_samples": self.keep_samples,
            "keep_verdicts": self.keep_verdicts,
        }

    def as_dict(self) -> dict[str, object]:
        return {"version": self.version, "since": self.since, "spec": self.spec()}


def _int_field(spec: dict[str, object], name: str, low: int, high: int) -> int:
    value = spec[name]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"policy field {name} must be an integer")
    number = int(value)
    if number != value or number < low or number > high:
        raise ValueError(f"policy field {name} must be an integer between {low} and {high}")
    return number


def _float_field(spec: dict[str, object], name: str, low: float, high: float) -> float:
    value = spec[name]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"policy field {name} must be a number")
    number = float(value)
    if number < low or number > high:
        raise ValueError(f"policy field {name} must stay between {low} and {high}")
    return number


def _normalize_tiers(raw: object) -> list[dict[str, object]]:
    if not isinstance(raw, list) or not 1 <= len(raw) <= 8:
        raise ValueError("policy tiers must be a list of 1 to 8 entries")
    tiers: list[dict[str, object]] = []
    names: set[str] = set()
    last_limit = 0.0
    for entry in raw:
        if not isinstance(entry, dict):
            raise ValueError("each policy tier must be an object")
        name = str(entry.get("name", "")).strip()
        if not name or len(name) > 32:
            raise ValueError("tier names must be 1 to 32 characters")
        if name in names:
            raise ValueError(f"duplicate tier name {name}")
        names.add(name)
        limit = entry.get("limit")
        if isinstance(limit, bool) or not isinstance(limit, (int, float)):
            raise ValueError(f"tier {name} limit must be a number")
        limit = float(limit)
        if limit <= last_limit:
            raise ValueError("tier limits must be positive and strictly increasing")
        last_limit = limit
        actions_raw = entry.get("actions", [])
        if not isinstance(actions_raw, list) or len(actions_raw) > 8:
            raise ValueError(f"tier {name} actions must be a list of at most 8 entries")
        actions: list[str] = []
        for action in actions_raw:
            action = str(action).strip()
            if not action or len(action) > 64:
                raise ValueError(f"tier {name} has an invalid action name")
            actions.append(action)
        confirm = entry.get("confirm", 1)
        if isinstance(confirm, bool) or not isinstance(confirm, int) or not 1 <= confirm <= 10:
            raise ValueError(f"tier {name} confirm must be an integer between 1 and 10")
        hold = entry.get("hold", 1)
        if isinstance(hold, bool) or not isinstance(hold, int) or not 1 <= hold <= 20:
            raise ValueError(f"tier {name} hold must be an integer between 1 and 20")
        tiers.append(
            {"name": name, "limit": limit, "actions": actions, "confirm": confirm, "hold": hold}
        )
    return tiers


def normalize_spec(raw: object) -> dict[str, object]:
    """Validate an operator supplied spec and fill defaults for gaps."""

    if not isinstance(raw, dict):
        raise ValueError("policy spec must be an object")
    unknown = sorted(set(raw) - set(DEFAULT_POLICY_SPEC))
    if unknown:
        raise ValueError(f"unknown policy fields: {', '.join(unknown)}")
    spec = dict(DEFAULT_POLICY_SPEC)
    spec.update(raw)
    return {
        "window_seconds": _int_field(spec, "window_seconds", 5, 3600),
        "tiers": _normalize_tiers(spec["tiers"]),
        "spike_rate": _float_field(spec, "spike_rate", 0.01, 50.0),
        "step_adopt": _int_field(spec, "step_adopt", 2, 20),
        "max_suspicious_ratio": _float_field(spec, "max_suspicious_ratio", 0.01, 1.0),
        "rate_per_second": _float_field(spec, "rate_per_second", 0.1, 100.0),
        "burst": _int_field(spec, "burst", 1, 1000),
        "global_rate": _float_field(spec, "global_rate", 0.1, 10000.0),
        "global_burst": _int_field(spec, "global_burst", 1, 100000),
        "keep_samples": _int_field(spec, "keep_samples", 10, 100000),
        "keep_verdicts": _int_field(spec, "keep_verdicts", 10, 10000),
    }


def policy_from_spec(version: int, since: int, spec: dict[str, object]) -> Policy:
    tiers = tuple(
        Tier(
            name=str(tier["name"]),
            limit=float(tier["limit"]),
            actions=tuple(str(action) for action in tier["actions"]),
            confirm=int(tier["confirm"]),
            hold=int(tier["hold"]),
        )
        for tier in spec["tiers"]
    )
    return Policy(
        version=version,
        since=since,
        window_seconds=int(spec["window_seconds"]),
        tiers=tiers,
        spike_rate=float(spec["spike_rate"]),
        step_adopt=int(spec["step_adopt"]),
        max_suspicious_ratio=float(spec["max_suspicious_ratio"]),
        rate_per_second=float(spec["rate_per_second"]),
        burst=int(spec["burst"]),
        global_rate=float(spec["global_rate"]),
        global_burst=int(spec["global_burst"]),
        keep_samples=int(spec["keep_samples"]),
        keep_verdicts=int(spec["keep_verdicts"]),
    )


class PolicyBook:
    """Immutable policy versions persisted inside the store."""

    def __init__(self, store: Store) -> None:
        self._store = store

    def current(self) -> Policy:
        raw, present = self._store.get(POLICY_CURRENT_KEY)
        if present:
            try:
                policy = self.version(int(raw))
            except (ValueError, KeyError):
                policy = None
            if policy is not None:
                return policy
        policy = policy_from_spec(1, 0, normalize_spec({}))
        self._persist(policy)
        return policy

    def set(self, spec: object, now: int | None = None) -> Policy:
        clean = normalize_spec(spec)
        current = self.current()
        since = int(now) if now is not None else int(time.time())
        policy = policy_from_spec(current.version + 1, since, clean)
        self._persist(policy)
        return policy

    def version(self, number: int) -> Policy:
        raw, present = self._store.get(f"{POLICY_KEY_PREFIX}{number}")
        if not present:
            raise ValueError(f"policy version {number} not found")
        try:
            payload = json.loads(raw)
            spec = payload["spec"]
            return policy_from_spec(int(payload["version"]), int(payload["since"]), spec)
        except (ValueError, KeyError, TypeError) as exc:
            raise ValueError(f"policy version {number} is corrupt") from exc

    def versions(self) -> list[Policy]:
        versions: list[Policy] = []
        for key in self._store.keys():
            if not key.startswith(POLICY_KEY_PREFIX) or key == POLICY_CURRENT_KEY:
                continue
            try:
                versions.append(self.version(int(key[len(POLICY_KEY_PREFIX) :])))
            except ValueError:
                continue
        return sorted(versions, key=lambda policy: policy.version)

    def effective(self, at: int) -> Policy:
        """The version that was in force at sample time ``at``."""

        versions = self.versions()
        if not versions:
            return self.current()
        chosen = versions[0]
        for policy in versions:
            if policy.since <= at:
                chosen = policy
        return chosen

    def _persist(self, policy: Policy) -> None:
        self._store.put(
            f"{POLICY_KEY_PREFIX}{policy.version}",
            json.dumps(policy.as_dict(), ensure_ascii=False, sort_keys=True),
        )
        self._store.put(POLICY_CURRENT_KEY, str(policy.version))
