"""Immutable projections for the sliding window judge.

These dataclasses are the single contract shared by the engine, the console
handlers, the snapshot/archive export and the tests. Every projection has an
``as_dict`` form so the console, the archive snapshot and the audit detail all
speak the same shape.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Jitter handling policies.
JITTER_SUSPECT = "suspect"  # mark the point suspect, keep it out of the linkage
JITTER_MEDIAN = "median"  # smooth it with the window median, keep it in play
JITTER_OFF = "off"  # never treat a point as jitter
JITTER_MODES = (JITTER_SUSPECT, JITTER_MEDIAN, JITTER_OFF)

# Escalation / de-escalation policies.
SHIFT_CONFIRMED = "confirmed"  # up shift only after confirm points, down at once
SHIFT_IMMEDIATE = "immediate"  # jump straight to the measured tier
SHIFT_MODES = (SHIFT_CONFIRMED, SHIFT_IMMEDIATE)

# Latched top tier handling.
LATCH_HOLD = "hold"  # stay at the top tier until released
LATCH_RELEASE = "release"  # drop back down once the window clears
LATCH_STANDBY = "standby"  # hold the tier but stop issuing repeat actions
LATCH_MODES = (LATCH_HOLD, LATCH_RELEASE, LATCH_STANDBY)


@dataclass(frozen=True)
class Tier:
    """One configured excursion band and the actions it may trigger."""

    level: int
    rate: float
    actions: tuple[str, ...]
    confirm: int = 1
    cooldown: float = 0.0

    def as_dict(self) -> dict[str, object]:
        return {
            "level": self.level,
            "rate": self.rate,
            "actions": list(self.actions),
            "confirm": self.confirm,
            "cooldown": self.cooldown,
        }


@dataclass(frozen=True)
class Policy:
    """The full, versioned rule set used to judge a window of readings."""

    version: int
    window_seconds: float
    max_samples: int
    tiers: tuple[Tier, ...]
    jitter_mode: str = JITTER_SUSPECT
    jitter_z: float = 3.5
    fault_streak: int = 5
    up_shift: str = SHIFT_CONFIRMED
    down_shift: str = SHIFT_CONFIRMED
    max_down_steps: int = 1
    latch: str = LATCH_RELEASE
    rate_per_probe: float = 2.0
    burst_per_probe: int = 4
    max_batch: int = 256

    def tier_for(self, magnitude: float) -> int:
        """Highest configured tier whose rate band the magnitude reaches."""

        level = 0
        for tier in self.tiers:
            if magnitude >= tier.rate and tier.level > level:
                level = tier.level
        return level

    def tier(self, level: int) -> Tier | None:
        for candidate in self.tiers:
            if candidate.level == level:
                return candidate
        return None

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "window_seconds": self.window_seconds,
            "max_samples": self.max_samples,
            "tiers": [tier.as_dict() for tier in self.tiers],
            "jitter_mode": self.jitter_mode,
            "jitter_z": self.jitter_z,
            "fault_streak": self.fault_streak,
            "up_shift": self.up_shift,
            "down_shift": self.down_shift,
            "max_down_steps": self.max_down_steps,
            "latch": self.latch,
            "rate_per_probe": self.rate_per_probe,
            "burst_per_probe": self.burst_per_probe,
            "max_batch": self.max_batch,
        }


@dataclass(frozen=True)
class Sample:
    """One probe observation, pinned to the policy version it arrived under."""

    probe: str
    value: float
    at: int
    policy_version: int
    suspect: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "probe": self.probe,
            "value": self.value,
            "at": self.at,
            "policy_version": self.policy_version,
            "suspect": self.suspect,
        }


@dataclass(frozen=True)
class WindowStats:
    """The smoothed view of one probe's window before tiering."""

    base: float
    rate: float
    magnitude: float
    median: float
    used: int
    suspects: int
    direction: str

    def as_dict(self) -> dict[str, object]:
        return {
            "base": self.base,
            "rate": self.rate,
            "magnitude": self.magnitude,
            "median": self.median,
            "used": self.used,
            "suspects": self.suspects,
            "direction": self.direction,
        }


@dataclass(frozen=True)
class Conclusion:
    """The linkage verdict for one probe at one sample time."""

    probe: str
    at: int
    policy_version: int
    tier: int
    measured_tier: int
    base: float
    rate: float
    magnitude: float
    suspects: int
    suspect_fault: bool
    actions: tuple[str, ...]
    held: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "probe": self.probe,
            "at": self.at,
            "policy_version": self.policy_version,
            "tier": self.tier,
            "measured_tier": self.measured_tier,
            "base": self.base,
            "rate": self.rate,
            "magnitude": self.magnitude,
            "suspects": self.suspects,
            "suspect_fault": self.suspect_fault,
            "actions": list(self.actions),
            "held": self.held,
        }


@dataclass(frozen=True)
class IngestResult:
    """Outcome of pushing a flood of samples through the judge."""

    accepted: int
    throttled: int
    duplicates: int
    conclusions: list[Conclusion] = field(default_factory=list)
    throttled_probes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "accepted": self.accepted,
            "throttled": self.throttled,
            "duplicates": self.duplicates,
            "conclusions": [item.as_dict() for item in self.conclusions],
            "throttled_probes": list(self.throttled_probes),
        }


@dataclass(frozen=True)
class ProbeState:
    """Persisted linkage state of one probe."""

    probe: str
    tier: int
    measured_tier: int
    up_streak: int
    down_streak: int
    suspect_streak: int
    healthy_streak: int
    faulted: bool
    latched: bool
    last_action_at: dict[str, int]
    last_at: int

    def as_dict(self) -> dict[str, object]:
        return {
            "probe": self.probe,
            "tier": self.tier,
            "measured_tier": self.measured_tier,
            "up_streak": self.up_streak,
            "down_streak": self.down_streak,
            "suspect_streak": self.suspect_streak,
            "healthy_streak": self.healthy_streak,
            "faulted": self.faulted,
            "latched": self.latched,
            "last_action_at": dict(self.last_action_at),
            "last_at": self.last_at,
        }
