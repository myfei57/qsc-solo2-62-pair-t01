"""The sliding window judge engine.

One call to :meth:`Judge.ingest` accepts a flood of readings from several
probes, rate limits them, sorts every accepted point by sample time and then
advances each probe's tier state machine in that global order. The state
machine encodes the two decisions fixed up front:

* up shifts are *confirmed* (a configurable run of excursion points) while a
  down shift happens at once but moves at most ``max_down_steps`` tiers;
* a jittery point is marked suspect and kept out of the linkage by default;
  median smoothing is available as a configured alternative.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

from coldstore.store.store import Store

from . import judge_state as persistence
from .policy import PolicyBook
from .report import (
    Conclusion,
    IngestResult,
    JITTER_SUSPECT,
    LATCH_HOLD,
    LATCH_STANDBY,
    Policy,
    ProbeState,
    Sample,
    SHIFT_IMMEDIATE,
    WindowStats,
)
from .throttle import RateLimiter
from .window import mark_jitter, prune, stats

AuditHook = Callable[[str, dict[str, object]], None]


class Judge:
    """Holds the policy book, persisted probe state and the flood limiter."""

    def __init__(self, store: Store, audit: AuditHook | None = None) -> None:
        self._store = store
        self._book = PolicyBook(store)
        self._audit = audit
        current = self._book.current()
        self._limiter = RateLimiter(current.rate_per_probe, current.burst_per_probe)

    # -- policy ---------------------------------------------------------

    @property
    def policies(self) -> PolicyBook:
        return self._book

    def policy(self) -> Policy:
        return self._book.current()

    # -- audit ----------------------------------------------------------

    def _record(self, kind: str, detail: dict[str, object]) -> None:
        if self._audit is not None:
            self._audit(kind, detail)

    # -- ingestion ------------------------------------------------------

    def ingest(self, readings: list[dict[str, object]], at: int | None = None) -> IngestResult:
        """Validate, throttle and time-order a batch, then judge every point."""

        policy = self.policy()
        if len(readings) > policy.max_batch:
            raise ValueError("batch exceeds the configured max_batch")
        self._limiter.reconfigure(policy.rate_per_probe, policy.burst_per_probe)

        accepted: list[Sample] = []
        throttled = 0
        duplicates = 0
        throttled_probes: set[str] = set()

        for item in readings:
            probe = str(item.get("probe", ""))
            if not probe:
                raise ValueError("every reading needs a probe id")
            value = float(item["value"])
            if item.get("at") is not None:
                sample_at = int(item["at"])
            elif at is not None:
                sample_at = int(at)
            else:
                raise ValueError("every reading needs a sample time")

            if not self._limiter.allow(probe):
                throttled += 1
                throttled_probes.add(probe)
                continue

            window = persistence.load_window(self._store, probe)
            if any(piece.at == sample_at for piece in window):
                duplicates += 1
                continue

            accepted.append(
                Sample(
                    probe=probe,
                    value=value,
                    at=sample_at,
                    policy_version=policy.version,
                )
            )

        # Concurrent probes are judged in one global sample-time order.
        accepted.sort(key=lambda sample: (sample.at, sample.probe, sample.policy_version))

        conclusions: list[Conclusion] = []
        for sample in accepted:
            conclusions.append(self._advance(sample))

        if throttled:
            self._record(
                "judge.throttled",
                {"count": throttled, "probes": sorted(throttled_probes)},
            )
        return IngestResult(
            accepted=len(accepted),
            throttled=throttled,
            duplicates=duplicates,
            conclusions=conclusions,
            throttled_probes=sorted(throttled_probes),
        )

    # -- per probe state machine ---------------------------------------

    def _advance(self, sample: Sample) -> Conclusion:
        policy = self._book.get(sample.policy_version) or self.policy()
        probe = sample.probe
        persistence.register_probe(self._store, probe)

        window = persistence.load_window(self._store, probe)
        state = persistence.load_state(self._store, probe)

        candidate = window + [sample]
        suspect = mark_jitter(candidate, policy)[-1]
        sample = replace(sample, suspect=suspect)
        window.append(sample)
        window = prune(window, policy, sample.at)
        persistence.save_window(self._store, probe, window)

        # Jitter is a density signal: a wild point now and then is noise, but
        # ``fault_streak`` suspects in the current window means a bad probe.
        suspects_in_window = sum(1 for piece in window if piece.suspect)

        if suspect and policy.jitter_mode == JITTER_SUSPECT:
            state = replace(
                state,
                suspect_streak=state.suspect_streak + 1,
                healthy_streak=0,
                last_at=sample.at,
            )
            if not state.faulted and suspects_in_window >= policy.fault_streak:
                state = replace(state, faulted=True)
                self._record(
                    "judge.suspect-fault",
                    {"probe": probe, "at": sample.at, "suspects": suspects_in_window},
                )
            conclusion = self._conclusion(probe, sample.at, policy, state, window, (), held=True)
            persistence.save_state(self._store, probe, state)
            self._record("judge.suspect", conclusion.as_dict())
            return conclusion

        if state.faulted:
            # Recovery needs a run of clean points that drains suspect density.
            state = replace(
                state,
                healthy_streak=state.healthy_streak + 1,
                suspect_streak=0,
                last_at=sample.at,
            )
            if suspects_in_window < policy.fault_streak:
                state = replace(state, faulted=False, healthy_streak=0)
                self._record("judge.probe-recovered", {"probe": probe, "at": sample.at})
            persistence.save_state(self._store, probe, state)
            return self._conclusion(probe, sample.at, policy, state, window, (), held=True)

        view = stats(window, policy)
        measured = policy.tier_for(view.magnitude)
        state, actions = self._transition(state, sample, policy, view, measured)
        state = replace(
            state,
            measured_tier=measured,
            suspect_streak=0,
            healthy_streak=0,
            last_at=sample.at,
        )
        persistence.save_state(self._store, probe, state)

        conclusion = self._conclusion(probe, sample.at, policy, state, window, actions, held=False)
        if actions:
            self._record("judge.action", conclusion.as_dict())
        elif state.tier > 0:
            self._record("judge.tier", conclusion.as_dict())
        return conclusion

    def _transition(
        self,
        state: ProbeState,
        sample: Sample,
        policy: Policy,
        view: WindowStats,
        measured: int,
    ) -> tuple[ProbeState, tuple[str, ...]]:
        current = state.tier

        # A hold latch keeps the probe pinned at the top tier until released.
        if state.latched and policy.latch == LATCH_HOLD:
            return self._settle(state, sample, policy, current, 0, 0)

        if measured > current:
            up_streak = state.up_streak + 1
            target = current + 1
            needed = policy.tier(target).confirm if policy.tier(target) else 1
            if policy.up_shift == SHIFT_IMMEDIATE:
                new_tier, up_streak = measured, 0
            elif up_streak >= needed:
                new_tier, up_streak = target, 0
            else:
                new_tier = current
            return self._settle(state, sample, policy, new_tier, up_streak, 0)

        if measured < current:
            down_streak = state.down_streak + 1
            if policy.down_shift == SHIFT_IMMEDIATE:
                new_tier = max(measured, current - policy.max_down_steps)
                down_streak = 0
            else:
                target = current - 1
                needed = policy.tier(target).confirm if policy.tier(target) else 1
                if down_streak >= needed:
                    new_tier, down_streak = max(measured, target), 0
                else:
                    new_tier = current
            return self._settle(state, sample, policy, new_tier, 0, down_streak)

        # Measured tier matches the current tier: hold it and refresh.
        return self._settle(state, sample, policy, current, 0, 0)

    def _settle(
        self,
        state: ProbeState,
        sample: Sample,
        policy: Policy,
        new_tier: int,
        up_streak: int,
        down_streak: int,
    ) -> tuple[ProbeState, tuple[str, ...]]:
        latched = state.latched
        if new_tier >= self._top_level(policy) and policy.latch == LATCH_HOLD:
            latched = True

        actions: tuple[str, ...] = ()
        if new_tier > state.tier:
            actions = self._allowed_actions(sample.at, policy, state, new_tier)
        elif new_tier == state.tier and new_tier > 0 and policy.latch != LATCH_STANDBY:
            actions = self._allowed_actions(sample.at, policy, state, new_tier)

        last_action_at = dict(state.last_action_at)
        for action in actions:
            last_action_at[action] = sample.at

        return (
            replace(
                state,
                tier=new_tier,
                up_streak=up_streak,
                down_streak=down_streak,
                latched=latched,
                last_action_at=last_action_at,
            ),
            actions,
        )

    def _allowed_actions(
        self,
        at: int,
        policy: Policy,
        state: ProbeState,
        tier_level: int,
    ) -> tuple[str, ...]:
        tier = policy.tier(tier_level)
        if tier is None:
            return ()
        allowed: list[str] = []
        for action in tier.actions:
            last = state.last_action_at.get(action, 0)
            if at - last >= tier.cooldown:
                allowed.append(action)
        return tuple(allowed)

    def _top_level(self, policy: Policy) -> int:
        return max((tier.level for tier in policy.tiers), default=0)

    def _conclusion(
        self,
        probe: str,
        at: int,
        policy: Policy,
        state: ProbeState,
        window: list[Sample],
        actions: tuple[str, ...],
        held: bool,
    ) -> Conclusion:
        view = stats(window, policy)
        return Conclusion(
            probe=probe,
            at=at,
            policy_version=policy.version,
            tier=state.tier,
            measured_tier=state.measured_tier,
            base=view.base,
            rate=view.rate,
            magnitude=view.magnitude,
            suspects=view.suspects,
            suspect_fault=state.faulted,
            actions=actions,
            held=held,
        )

    # -- queries --------------------------------------------------------

    def probes(self) -> list[str]:
        return persistence.probe_ids(self._store)

    def window_of(self, probe: str) -> list[Sample]:
        return persistence.load_window(self._store, probe)

    def state_of(self, probe: str) -> ProbeState:
        return persistence.load_state(self._store, probe)

    def release(self, probe: str) -> ProbeState:
        """Manually clear a latched / faulted probe linkage."""

        state = persistence.load_state(self._store, probe)
        state = replace(state, latched=False, faulted=False, suspect_streak=0, healthy_streak=0)
        persistence.save_state(self._store, probe, state)
        self._record("judge.released", {"probe": probe})
        return state

    def reset_probe(self, probe: str) -> None:
        self._store.delete(persistence.samples_key(probe))
        self._store.delete(persistence.state_key(probe))
