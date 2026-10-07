"""Sliding window judgement over probe samples ordered by sampling time.

Replaces the old two point difference: every probe keeps a time sorted buffer,
each accepted sample closes one window, and the window's least squares rate is
judged against the configured tiers. Jitter is quarantined as suspicious instead
of being smoothed into the signal, floods are token bucketed, and tier changes
need confirmation going up but a hold going down, so a single twitch cannot
move the compressor bank.
"""

from __future__ import annotations

import bisect
import hashlib
import json
import time
import uuid
from dataclasses import replace
from typing import TYPE_CHECKING, Callable

from coldstore.store.store import Store
from coldstore.temp import validate_temp

from .policy import Policy, PolicyBook, Tier
from .report import (
    GuardState,
    IngestOutcome,
    ProbeState,
    ReplayReport,
    Sample,
    TierState,
    WindowVerdict,
)

if TYPE_CHECKING:  # pragma: no cover - imported only for type checkers
    from coldstore.audit import Auditor

SAMPLES_PREFIX = "guard:samples:"
VERDICTS_PREFIX = "guard:verdicts:"
TIER_PREFIX = "guard:tier:"
THROTTLE_PREFIX = "guard:throttle:"
PROBES_KEY = "guard:probes"
MAX_PROBES = 256
REPLAY_LIMIT = 200


def validate_probe(probe: str) -> None:
    """Reject probe names that would corrupt the key space."""

    if not probe or not probe.strip():
        raise ValueError("probe must be a non empty name")
    if len(probe) > 64:
        raise ValueError("probe name is too long")
    if any(char.isspace() for char in probe):
        raise ValueError("probe name must not contain whitespace")


class Bucket:
    """Token bucket refilled from the guard clock."""

    def __init__(self, rate: float, capacity: int, now: float) -> None:
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.updated = now

    def allow(self, now: float) -> bool:
        if now > self.updated:
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


def slope(points: list[tuple[int, float]]) -> float:
    """Least squares rate of the window, robust against single point noise."""

    count = len(points)
    mean_at = sum(at for at, _ in points) / count
    mean_value = sum(value for _, value in points) / count
    denom = sum((at - mean_at) ** 2 for at, _ in points)
    if denom <= 0:
        return 0.0
    return sum((at - mean_at) * (value - mean_value) for at, value in points) / denom


def candidate_tier(rate: float, tiers: tuple[Tier, ...]) -> int:
    """Highest tier whose limit the absolute window rate exceeds."""

    candidate = 0
    for index, tier in enumerate(tiers, start=1):
        if abs(rate) > tier.limit:
            candidate = index
    return candidate


def classify(value: float, at: int, state: TierState, policy: Policy) -> tuple[bool, bool]:
    """Mark a jumpy sample suspicious; returns (suspicious, adopted_new_baseline).

    Suspicious samples stay in the raw archive but never enter window
    statistics or linkage. A run of ``step_adopt`` jumps in one direction is
    accepted as a genuine new baseline so a real step cannot lock the probe out.
    """

    if state.last_at is None or state.last_value is None:
        state.last_value = value
        state.last_at = at
        state.streak = 0
        return False, False
    gap = max(at - state.last_at, 1)
    jump = abs(value - state.last_value) / gap
    if jump <= policy.spike_rate:
        state.last_value = value
        state.last_at = at
        state.streak = 0
        return False, False
    state.streak += 1
    if state.streak >= policy.step_adopt:
        state.last_value = value
        state.last_at = at
        state.streak = 0
        return False, True
    return True, False


def window_stat(
    samples: list[Sample], end: int, window_seconds: int
) -> tuple[float | None, int, int]:
    """Rate, accepted count and suspicious count inside (end - window, end]."""

    low = end - window_seconds
    ats = [sample.at for sample in samples]
    start = bisect.bisect_right(ats, low)
    points: list[tuple[int, float]] = []
    suspicious = 0
    for sample in samples[start:]:
        if sample.at > end:
            break
        if sample.suspicious:
            suspicious += 1
        else:
            points.append((sample.at, sample.value))
    if len(points) < 2:
        return None, len(points), suspicious
    return slope(points), len(points), suspicious


def step_tier(
    state: TierState, candidate: int, reliable: bool, tiers: tuple[Tier, ...]
) -> tuple[bool, tuple[str, ...]]:
    """Advance the tier machine; returns (changed, entry actions).

    Escalation needs ``confirm`` consecutive windows at the candidate tier,
    except a jump of two or more tiers which lands immediately. Recovery steps
    down one tier at a time after ``hold`` consecutive windows back inside.
    Unreliable windows freeze the machine in place.
    """

    if not reliable:
        return False, ()
    if candidate > state.current:
        state.down_streak = 0
        if candidate >= state.current + 2:
            state.current = candidate
            state.up_streak = 0
            state.pending = 0
            return True, tiers[candidate - 1].actions
        if state.pending != candidate:
            state.pending = candidate
            state.up_streak = 0
        state.up_streak += 1
        if state.up_streak >= tiers[candidate - 1].confirm:
            state.current = candidate
            state.up_streak = 0
            state.pending = 0
            return True, tiers[candidate - 1].actions
        return False, ()
    if candidate < state.current:
        state.up_streak = 0
        state.pending = 0
        state.down_streak += 1
        if state.down_streak >= tiers[state.current - 1].hold:
            state.current -= 1
            state.down_streak = 0
            return True, tiers[state.current - 1].actions if state.current >= 1 else ()
        return False, ()
    state.up_streak = 0
    state.down_streak = 0
    state.pending = 0
    return False, ()


def evaluate_stream(
    samples: list[Sample],
    policy_at: Callable[[int], Policy],
    classify_samples: bool,
) -> tuple[TierState, list[WindowVerdict], list[dict[str, object]]]:
    """Fold a sample stream in sampling order into verdicts and tier changes.

    Shared by replay and by the late sample rebuild so that the recomputed
    timeline matches what an in order arrival would have produced.
    """

    state = TierState()
    stream: list[Sample] = []
    verdicts: list[WindowVerdict] = []
    transitions: list[dict[str, object]] = []
    for sample in sorted(samples, key=lambda item: item.at):
        policy = policy_at(sample.at)
        suspicious = sample.suspicious
        adopted = False
        if classify_samples:
            suspicious, adopted = classify(sample.value, sample.at, state, policy)
        elif not suspicious:
            state.last_value = sample.value
            state.last_at = sample.at
            state.streak = 0
        bisect.insort(
            stream,
            Sample(sample.value, sample.at, suspicious, sample.late),
            key=lambda item: item.at,
        )
        if suspicious:
            continue
        rate, accepted, suspect_count = window_stat(stream, sample.at, policy.window_seconds)
        if rate is None:
            continue
        total = accepted + suspect_count
        reliable = total > 0 and suspect_count / total <= policy.max_suspicious_ratio
        candidate = candidate_tier(rate, policy.tiers)
        before = state.current
        changed, actions = step_tier(state, candidate, reliable, policy.tiers)
        verdicts.append(
            WindowVerdict(
                id="",
                probe="",
                end=sample.at,
                start=sample.at - policy.window_seconds,
                rate=rate,
                tier=state.current,
                tier_name=policy.tier_name(state.current),
                candidate=candidate,
                reliable=reliable,
                accepted=accepted,
                suspicious=suspect_count,
                policy_version=policy.version,
                late=sample.late,
                revision=False,
                actions=actions if changed else (),
            )
        )
        if changed:
            transitions.append(
                {
                    "at": sample.at,
                    "from": before,
                    "to": state.current,
                    "rate": rate,
                    "policy_version": policy.version,
                    "adopted": adopted,
                }
            )
    return state, verdicts, transitions


class Guard:
    """Sliding window judge shared by the console, the audit trail and snapshots."""

    def __init__(
        self,
        store: Store,
        auditor: "Auditor | None" = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._store = store
        self._auditor = auditor
        self._clock = clock
        self._book = PolicyBook(store)
        self._buckets: dict[str, Bucket] = {}
        self._global_bucket: Bucket | None = None
        self._global_params: tuple[float, int] | None = None
        self._throttled: set[str] = set()

    # policy -------------------------------------------------------------

    def policy(self) -> Policy:
        return self._book.current()

    def policies(self) -> list[Policy]:
        return self._book.versions()

    def set_policy(self, spec: object) -> Policy:
        policy = self._book.set(spec, now=int(self._clock()))
        self._audit(
            "guard.policy",
            {"version": policy.version, "since": policy.since, "spec": policy.spec()},
        )
        return policy

    # ingest -------------------------------------------------------------

    def ingest(self, probe: str, value: float, at: int | None = None) -> IngestOutcome:
        validate_probe(probe)
        validate_temp(value)
        policy = self._book.current()
        moment = int(at) if at is not None else int(self._clock())
        counters = self._counters(probe)
        if not self._allow(probe, policy):
            counters["dropped"] += 1
            if probe not in self._throttled:
                self._throttled.add(probe)
                counters["throttle_events"] += 1
                self._audit(
                    "guard.throttle",
                    {"probe": probe, "at": moment, "policy_version": policy.version},
                )
            self._save_counters(probe, counters)
            return IngestOutcome(
                probe=probe,
                value=value,
                at=moment,
                dropped=True,
                suspicious=False,
                verdict=None,
                tier=self._machine(probe).current,
                tier_name=policy.tier_name(self._machine(probe).current),
                actions=(),
                policy_version=policy.version,
            )
        self._throttled.discard(probe)
        counters["accepted"] += 1
        self._register_probe(probe)
        machine = self._machine(probe)
        suspicious, adopted = classify(value, moment, machine, policy)
        if adopted:
            self._audit(
                "guard.step",
                {"probe": probe, "at": moment, "value": value, "policy_version": policy.version},
            )
        late = moment < machine.high_water
        samples = self._samples(probe)
        bisect.insort(
            samples,
            Sample(value=value, at=moment, suspicious=suspicious, late=late),
            key=lambda sample: sample.at,
        )
        if len(samples) > policy.keep_samples:
            del samples[: len(samples) - policy.keep_samples]
        verdict: WindowVerdict | None = None
        actions: tuple[str, ...] = ()
        if suspicious:
            self._save_samples(probe, samples)
            self._save_machine(probe, machine)
            self._save_counters(probe, counters)
            return IngestOutcome(
                probe=probe,
                value=value,
                at=moment,
                dropped=False,
                suspicious=True,
                verdict=None,
                tier=machine.current,
                tier_name=policy.tier_name(machine.current),
                actions=(),
                policy_version=policy.version,
            )
        if late:
            verdict, actions = self._rebuild(probe, samples, machine, policy)
        else:
            rate, accepted, suspect_count = window_stat(samples, moment, policy.window_seconds)
            reliable = True
            candidate = machine.current
            if rate is not None:
                total = accepted + suspect_count
                reliable = total > 0 and suspect_count / total <= policy.max_suspicious_ratio
                candidate = candidate_tier(rate, policy.tiers)
                before = machine.current
                changed, actions = step_tier(machine, candidate, reliable, policy.tiers)
                if changed:
                    self._audit(
                        "guard.tier",
                        {
                            "probe": probe,
                            "from": before,
                            "to": machine.current,
                            "rate": round(rate, 6),
                            "at": moment,
                            "policy_version": policy.version,
                            "actions": list(actions),
                        },
                    )
            machine.high_water = max(machine.high_water, moment)
            verdict = WindowVerdict(
                id=uuid.uuid4().hex,
                probe=probe,
                end=moment,
                start=moment - policy.window_seconds,
                rate=rate,
                tier=machine.current,
                tier_name=policy.tier_name(machine.current),
                candidate=candidate,
                reliable=reliable,
                accepted=accepted,
                suspicious=suspect_count,
                policy_version=policy.version,
                late=False,
                revision=False,
                actions=actions,
            )
            if rate is not None:
                # Warmup windows make no judgement, so they stay out of the
                # persisted log and cannot diverge from a later replay.
                self._append_verdict(probe, verdict, policy)
        self._save_samples(probe, samples)
        self._save_machine(probe, machine)
        self._save_counters(probe, counters)
        return IngestOutcome(
            probe=probe,
            value=value,
            at=moment,
            dropped=False,
            suspicious=False,
            verdict=verdict,
            tier=machine.current,
            tier_name=policy.tier_name(machine.current),
            actions=actions,
            policy_version=policy.version,
        )

    # replay -------------------------------------------------------------

    def replay(self, probe: str, since: int | None = None, until: int | None = None) -> ReplayReport:
        """Recompute archived samples under the version effective at each timestamp."""

        validate_probe(probe)
        samples = [
            sample
            for sample in self._samples(probe)
            if (since is None or sample.at >= since) and (until is None or sample.at <= until)
        ]
        _state, verdicts, transitions = evaluate_stream(samples, self._book.effective, True)
        verdicts = [replace(verdict, probe=probe) for verdict in verdicts]
        digest = hashlib.sha1(
            json.dumps(
                [
                    [item.end, item.rate, item.tier, item.candidate, item.policy_version]
                    for item in verdicts
                ],
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:12]
        versions = tuple(sorted({item.policy_version for item in verdicts}))
        truncated = len(verdicts) > REPLAY_LIMIT
        shown = verdicts[:REPLAY_LIMIT]
        self._audit(
            "guard.replay",
            {
                "probe": probe,
                "windows": len(verdicts),
                "tier_changes": len(transitions),
                "versions": list(versions),
                "digest": digest,
            },
        )
        return ReplayReport(
            probe=probe,
            since=since,
            until=until,
            windows=len(verdicts),
            tier_changes=len(transitions),
            versions=versions,
            digest=digest,
            truncated=truncated,
            verdicts=tuple(item.as_dict() for item in shown),
        )

    # state --------------------------------------------------------------

    def verdicts(self, probe: str | None = None, limit: int = 50) -> list[dict[str, object]]:
        if limit <= 0 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        names = [probe] if probe else self._probes()
        merged: list[WindowVerdict] = []
        for name in names:
            merged.extend(self._verdicts(name))
        merged.sort(key=lambda item: (item.end, item.probe, item.id), reverse=True)
        return [item.as_dict() for item in merged[:limit]]

    def reset(self, probe: str | None = None) -> None:
        names = [probe] if probe else self._probes()
        for name in names:
            self._store.delete(f"{SAMPLES_PREFIX}{name}")
            self._store.delete(f"{VERDICTS_PREFIX}{name}")
            self._store.delete(f"{TIER_PREFIX}{name}")
            self._store.delete(f"{THROTTLE_PREFIX}{name}")
            self._buckets.pop(name, None)
            self._throttled.discard(name)
        if probe is None:
            self._store.delete(PROBES_KEY)
            self._global_bucket = None
        else:
            self._save_probes([name for name in self._probes() if name != probe])

    def state(self) -> GuardState:
        policy = self._book.current()
        probes = tuple(self._probe_state(name, policy) for name in self._probes())
        return GuardState(
            policy={
                "current": policy.version,
                "versions": [item.version for item in self._book.versions()],
                "spec": policy.spec(),
            },
            probes=probes,
        )

    def describe(self) -> str:
        policy = self._book.current()
        return (
            f"guard probes={len(self._probes())} policy=v{policy.version} "
            f"window={policy.window_seconds}s tiers={len(policy.tiers)}"
        )

    # internals ----------------------------------------------------------

    def _rebuild(
        self, probe: str, samples: list[Sample], machine: TierState, policy: Policy
    ) -> tuple[WindowVerdict | None, tuple[str, ...]]:
        """Recompute the probe timeline as if every sample arrived in order."""

        fresh, verdicts, transitions = evaluate_stream(samples, lambda _at: policy, False)
        actions: tuple[str, ...] = ()
        revision = fresh.current != machine.current
        if revision:
            actions = policy.tier_actions(fresh.current)
            self._audit(
                "guard.revision",
                {
                    "probe": probe,
                    "from": machine.current,
                    "to": fresh.current,
                    "transitions": len(transitions),
                    "policy_version": policy.version,
                },
            )
        machine.current = fresh.current
        machine.up_streak = fresh.up_streak
        machine.down_streak = fresh.down_streak
        machine.pending = fresh.pending
        # The accept baseline follows sampling time, so a late sample must not
        # drag it backwards; the rebuilt stream carries the newest accepted point.
        machine.last_value = fresh.last_value
        machine.last_at = fresh.last_at
        machine.streak = 0
        for sample in sorted(samples, key=lambda item: item.at, reverse=True):
            if not sample.suspicious:
                break
            machine.streak += 1
        if not verdicts:
            return None, actions
        last = verdicts[-1]
        verdict = replace(
            last,
            id=uuid.uuid4().hex,
            probe=probe,
            late=True,
            revision=revision,
            actions=actions,
        )
        self._append_verdict(probe, verdict, policy)
        return verdict, actions

    def _allow(self, probe: str, policy: Policy) -> bool:
        now = self._clock()
        params = (policy.global_rate, policy.global_burst)
        if self._global_bucket is None or self._global_params != params:
            self._global_bucket = Bucket(policy.global_rate, policy.global_burst, now)
            self._global_params = params
        bucket = self._buckets.get(probe)
        probe_params = (policy.rate_per_second, policy.burst)
        if bucket is None or (bucket.rate, bucket.capacity) != probe_params:
            bucket = Bucket(policy.rate_per_second, policy.burst, now)
            self._buckets[probe] = bucket
        return self._global_bucket.allow(now) and bucket.allow(now)

    def _audit(self, kind: str, detail: dict[str, object]) -> None:
        if self._auditor is not None:
            self._auditor.record(kind, json.dumps(detail, ensure_ascii=False, sort_keys=True))

    def _probe_state(self, probe: str, policy: Policy) -> ProbeState:
        machine = self._machine(probe)
        counters = self._counters(probe)
        verdicts = self._verdicts(probe)
        return ProbeState(
            probe=probe,
            tier=machine.current,
            tier_name=policy.tier_name(machine.current),
            up_streak=machine.up_streak,
            down_streak=machine.down_streak,
            samples=len(self._samples(probe)),
            accepted=counters["accepted"],
            dropped=counters["dropped"],
            throttle_events=counters["throttle_events"],
            throttled=probe in self._throttled,
            last_at=machine.high_water,
            last_verdict=verdicts[-1].as_dict() if verdicts else None,
        )

    def _probes(self) -> list[str]:
        raw, present = self._store.get(PROBES_KEY)
        if not present:
            return []
        try:
            parsed = json.loads(raw)
        except ValueError:
            return []
        if not isinstance(parsed, list):
            return []
        return [str(item) for item in parsed]

    def _save_probes(self, probes: list[str]) -> None:
        self._store.put(PROBES_KEY, json.dumps(sorted(probes), ensure_ascii=False))

    def _register_probe(self, probe: str) -> None:
        probes = self._probes()
        if probe in probes:
            return
        if len(probes) >= MAX_PROBES:
            raise ValueError("too many probes registered")
        probes.append(probe)
        self._save_probes(probes)

    def _samples(self, probe: str) -> list[Sample]:
        raw, present = self._store.get(f"{SAMPLES_PREFIX}{probe}")
        if not present:
            return []
        try:
            parsed = json.loads(raw)
        except ValueError:
            return []
        if not isinstance(parsed, list):
            return []
        samples: list[Sample] = []
        for item in parsed:
            if isinstance(item, dict):
                try:
                    samples.append(Sample.from_dict(item))
                except (TypeError, ValueError):
                    continue
        return samples

    def _save_samples(self, probe: str, samples: list[Sample]) -> None:
        self._store.put(
            f"{SAMPLES_PREFIX}{probe}",
            json.dumps([sample.as_dict() for sample in samples], ensure_ascii=False),
        )

    def _machine(self, probe: str) -> TierState:
        raw, present = self._store.get(f"{TIER_PREFIX}{probe}")
        if not present:
            return TierState()
        try:
            parsed = json.loads(raw)
        except ValueError:
            return TierState()
        if not isinstance(parsed, dict):
            return TierState()
        return TierState.from_dict(parsed)

    def _save_machine(self, probe: str, machine: TierState) -> None:
        self._store.put(f"{TIER_PREFIX}{probe}", json.dumps(machine.as_dict(), ensure_ascii=False))

    def _counters(self, probe: str) -> dict[str, int]:
        raw, present = self._store.get(f"{THROTTLE_PREFIX}{probe}")
        counters = {"accepted": 0, "dropped": 0, "throttle_events": 0}
        if not present:
            return counters
        try:
            parsed = json.loads(raw)
        except ValueError:
            return counters
        if not isinstance(parsed, dict):
            return counters
        for key in counters:
            try:
                counters[key] = int(parsed.get(key, 0))
            except (TypeError, ValueError):
                counters[key] = 0
        return counters

    def _save_counters(self, probe: str, counters: dict[str, int]) -> None:
        self._store.put(f"{THROTTLE_PREFIX}{probe}", json.dumps(counters, ensure_ascii=False))

    def _verdicts(self, probe: str) -> list[WindowVerdict]:
        raw, present = self._store.get(f"{VERDICTS_PREFIX}{probe}")
        if not present:
            return []
        try:
            parsed = json.loads(raw)
        except ValueError:
            return []
        if not isinstance(parsed, list):
            return []
        verdicts: list[WindowVerdict] = []
        for item in parsed:
            if isinstance(item, dict):
                try:
                    verdicts.append(WindowVerdict.from_dict(item))
                except (TypeError, ValueError):
                    continue
        return verdicts

    def _append_verdict(self, probe: str, verdict: WindowVerdict, policy: Policy) -> None:
        verdicts = self._verdicts(probe)
        verdicts.append(verdict)
        if len(verdicts) > policy.keep_verdicts:
            del verdicts[: len(verdicts) - policy.keep_verdicts]
        self._store.put(
            f"{VERDICTS_PREFIX}{probe}",
            json.dumps([item.as_dict() for item in verdicts], ensure_ascii=False),
        )
