"""Re-judge stored readings against historical policy versions.

Every sample is pinned to the policy version current when it arrived, so a
replay reproduces the exact verdict operators saw at the time. Passing an
explicit ``version`` answers the what-if question "what would the current tier
have been under this other rule set" without touching live linkage state.
"""

from __future__ import annotations

from dataclasses import replace

from coldstore.store.store import Store

from . import judge_state as persistence
from .policy import PolicyBook
from .report import Conclusion, JITTER_SUSPECT, Policy, ProbeState, Sample
from .window import mark_jitter, prune, stats


def replay_probe(
    store: Store,
    probe: str,
    version: int | None = None,
) -> list[Conclusion]:
    """Rebuild every conclusion for one probe from its stored samples."""

    book = PolicyBook(store)
    samples = persistence.load_window(store, probe)
    if not samples:
        return []

    history: list[Sample] = []  # full arrival history, mirroring the live engine
    window: list[Sample] = []
    state = persistence.initial_state(probe)
    conclusions: list[Conclusion] = []

    for sample in samples:
        policy = _policy_for(book, sample, version)
        candidate = history + [sample]
        suspect = mark_jitter(candidate, policy)[-1]
        sample = replace(sample, suspect=suspect)
        history.append(sample)
        window = prune(window + [sample], policy, sample.at)

        suspects_in_window = sum(1 for piece in window if piece.suspect)

        if suspect and policy.jitter_mode == JITTER_SUSPECT:
            state = replace(
                state,
                suspect_streak=state.suspect_streak + 1,
                healthy_streak=0,
                faulted=state.faulted or suspects_in_window >= policy.fault_streak,
                last_at=sample.at,
            )
            conclusions.append(_conclusion(probe, sample.at, policy, state, window, (), True))
            continue

        view = stats(window, policy)
        measured = policy.tier_for(view.magnitude)
        if state.faulted:
            state = replace(
                state,
                healthy_streak=state.healthy_streak + 1,
                suspect_streak=0,
                faulted=suspects_in_window >= policy.fault_streak,
                measured_tier=measured,
                last_at=sample.at,
            )
            conclusions.append(_conclusion(probe, sample.at, policy, state, window, (), True))
            continue
        state, _actions = _step(state, sample, policy, measured)
        state = replace(
            state,
            measured_tier=measured,
            suspect_streak=0,
            healthy_streak=0,
            last_at=sample.at,
        )
        conclusions.append(_conclusion(probe, sample.at, policy, state, window, (), False))

    return conclusions


def _policy_for(book: PolicyBook, sample: Sample, override: int | None) -> Policy:
    if override is not None:
        chosen = book.get(override)
        if chosen is None:
            raise ValueError("unknown policy version")
        return chosen
    chosen = book.get(sample.policy_version)
    return chosen or book.current()


def _step(
    state: ProbeState,
    sample: Sample,
    policy: Policy,
    measured: int,
) -> tuple[ProbeState, tuple[str, ...]]:
    """Pure mirror of the engine's confirmed/immediate tier movement."""

    from .report import LATCH_HOLD, SHIFT_IMMEDIATE

    current = state.tier
    if state.latched and policy.latch == LATCH_HOLD:
        return replace(state, up_streak=0, down_streak=0), ()

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
        return replace(state, tier=new_tier, up_streak=up_streak, down_streak=0), ()

    if measured < current:
        down_streak = state.down_streak + 1
        if policy.down_shift == SHIFT_IMMEDIATE:
            new_tier, down_streak = max(measured, current - policy.max_down_steps), 0
        else:
            target = current - 1
            needed = policy.tier(target).confirm if policy.tier(target) else 1
            if down_streak >= needed:
                new_tier, down_streak = max(measured, target), 0
            else:
                new_tier = current
        return replace(state, tier=new_tier, up_streak=0, down_streak=down_streak), ()

    return replace(state, up_streak=0, down_streak=0), ()


def _conclusion(
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
