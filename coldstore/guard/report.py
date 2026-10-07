"""Guard projections: samples, window verdicts, probe and replay states."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Sample:
    """One archived probe reading; suspicious points stay but never drive linkage."""

    value: float
    at: int
    suspicious: bool = False
    late: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "value": self.value,
            "at": self.at,
            "suspicious": self.suspicious,
            "late": self.late,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> "Sample":
        return cls(
            value=float(raw.get("value", 0.0)),
            at=int(raw.get("at", 0)),
            suspicious=bool(raw.get("suspicious", False)),
            late=bool(raw.get("late", False)),
        )


@dataclass
class TierState:
    """Mutable per probe judgement machine, persisted between requests."""

    current: int = 0
    up_streak: int = 0
    down_streak: int = 0
    pending: int = 0
    last_value: float | None = None
    last_at: int | None = None
    streak: int = 0
    high_water: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "current": self.current,
            "up_streak": self.up_streak,
            "down_streak": self.down_streak,
            "pending": self.pending,
            "last_value": self.last_value,
            "last_at": self.last_at,
            "streak": self.streak,
            "high_water": self.high_water,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> "TierState":
        state = cls()
        try:
            state.current = int(raw.get("current", 0))
            state.up_streak = int(raw.get("up_streak", 0))
            state.down_streak = int(raw.get("down_streak", 0))
            state.pending = int(raw.get("pending", 0))
            state.streak = int(raw.get("streak", 0))
            state.high_water = int(raw.get("high_water", 0))
            last_value = raw.get("last_value")
            last_at = raw.get("last_at")
            state.last_value = None if last_value is None else float(last_value)
            state.last_at = None if last_at is None else int(last_at)
        except (TypeError, ValueError):
            return cls()
        return state


@dataclass(frozen=True)
class WindowVerdict:
    """The judged outcome of one closed window."""

    id: str
    probe: str
    end: int
    start: int
    rate: float | None
    tier: int
    tier_name: str
    candidate: int
    reliable: bool
    accepted: int
    suspicious: int
    policy_version: int
    late: bool
    revision: bool
    actions: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "probe": self.probe,
            "end": self.end,
            "start": self.start,
            "rate": self.rate,
            "tier": self.tier,
            "tier_name": self.tier_name,
            "candidate": self.candidate,
            "reliable": self.reliable,
            "accepted": self.accepted,
            "suspicious": self.suspicious,
            "policy_version": self.policy_version,
            "late": self.late,
            "revision": self.revision,
            "actions": list(self.actions),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> "WindowVerdict":
        rate = raw.get("rate")
        return cls(
            id=str(raw.get("id", "")),
            probe=str(raw.get("probe", "")),
            end=int(raw.get("end", 0)),
            start=int(raw.get("start", 0)),
            rate=None if rate is None else float(rate),
            tier=int(raw.get("tier", 0)),
            tier_name=str(raw.get("tier_name", "normal")),
            candidate=int(raw.get("candidate", 0)),
            reliable=bool(raw.get("reliable", True)),
            accepted=int(raw.get("accepted", 0)),
            suspicious=int(raw.get("suspicious", 0)),
            policy_version=int(raw.get("policy_version", 0)),
            late=bool(raw.get("late", False)),
            revision=bool(raw.get("revision", False)),
            actions=tuple(str(action) for action in raw.get("actions", []) or []),
        )


@dataclass(frozen=True)
class IngestOutcome:
    """What one ingested sample did to the judgement line."""

    probe: str
    value: float
    at: int
    dropped: bool
    suspicious: bool
    verdict: WindowVerdict | None
    tier: int
    tier_name: str
    actions: tuple[str, ...]
    policy_version: int

    def as_dict(self) -> dict[str, object]:
        return {
            "probe": self.probe,
            "value": self.value,
            "at": self.at,
            "dropped": self.dropped,
            "suspicious": self.suspicious,
            "verdict": self.verdict.as_dict() if self.verdict else None,
            "tier": self.tier,
            "tier_name": self.tier_name,
            "actions": list(self.actions),
            "policy_version": self.policy_version,
        }


@dataclass(frozen=True)
class ProbeState:
    """Operator view of one probe: tier, streaks and flood counters."""

    probe: str
    tier: int
    tier_name: str
    up_streak: int
    down_streak: int
    samples: int
    accepted: int
    dropped: int
    throttle_events: int
    throttled: bool
    last_at: int
    last_verdict: dict[str, object] | None

    def as_dict(self) -> dict[str, object]:
        return {
            "probe": self.probe,
            "tier": self.tier,
            "tier_name": self.tier_name,
            "up_streak": self.up_streak,
            "down_streak": self.down_streak,
            "samples": self.samples,
            "accepted": self.accepted,
            "dropped": self.dropped,
            "throttle_events": self.throttle_events,
            "throttled": self.throttled,
            "last_at": self.last_at,
            "last_verdict": self.last_verdict,
        }


@dataclass(frozen=True)
class GuardState:
    """Everything the console, the audit trail and the snapshot share."""

    policy: dict[str, object]
    probes: tuple[ProbeState, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "policy": self.policy,
            "probes": [probe.as_dict() for probe in self.probes],
        }


@dataclass(frozen=True)
class ReplayReport:
    """Read only recomputation of archived samples under historic versions."""

    probe: str
    since: int | None
    until: int | None
    windows: int
    tier_changes: int
    versions: tuple[int, ...]
    digest: str
    truncated: bool
    verdicts: tuple[dict[str, object], ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "probe": self.probe,
            "since": self.since,
            "until": self.until,
            "windows": self.windows,
            "tier_changes": self.tier_changes,
            "versions": list(self.versions),
            "digest": self.digest,
            "truncated": self.truncated,
            "verdicts": list(self.verdicts),
        }
