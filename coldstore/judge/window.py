"""Deterministic sliding window arithmetic.

The window is cut by *sample time*, not wall clock time, so replaying a batch
of late or historical readings produces exactly the same numbers the live
system would have produced at the time. Jitter detection uses a robust z score
built on the median and the median absolute deviation, which a single wild
probe spike cannot move the way it moves a mean and standard deviation.
"""

from __future__ import annotations

from .report import JITTER_MEDIAN, JITTER_OFF, JITTER_SUSPECT, Policy, Sample, WindowStats


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _mad(values: list[float], center: float) -> float:
    return median([abs(value - center) for value in values])


def robust_z(value: float, values: list[float]) -> float:
    """Median/MAD z score; 0 when the window is too flat to estimate spread."""

    center = median(values)
    spread = _mad(values, center)
    if spread <= 0.0:
        # No measurable spread: only an exact repeat is "normal".
        return 0.0 if value == center else float("inf")
    return 0.6745 * abs(value - center) / spread


def mark_jitter(window: list[Sample], policy: Policy) -> list[bool]:
    """One shared decision per point: is it a jitter outlier?"""

    values = [sample.value for sample in window]
    flags: list[bool] = []
    for sample in window:
        flagged = sample.suspect or (
            policy.jitter_mode != JITTER_OFF
            and len(values) >= 3
            and robust_z(sample.value, values) > policy.jitter_z
        )
        flags.append(flagged)
    return flags


def select(window: list[Sample], policy: Policy) -> tuple[list[Sample], list[Sample]]:
    """Split the window into samples used for linkage and suspect samples."""

    flags = mark_jitter(window, policy)
    clean = [sample for sample, flagged in zip(window, flags) if not flagged]
    suspect = [sample for sample, flagged in zip(window, flags) if flagged]

    if policy.jitter_mode == JITTER_MEDIAN:
        # Smoothing keeps every point in play; suspect points ride the median.
        return list(window), suspect
    if policy.jitter_mode == JITTER_SUSPECT:
        # Suspect points are stored and audited but never drive the linkage.
        return clean, suspect
    return list(window), []


def smooth(window: list[Sample], policy: Policy) -> list[float]:
    """Values fed to the rate math, median filled when smoothing jitter."""

    if policy.jitter_mode != JITTER_MEDIAN:
        return [sample.value for sample in window]
    center = median([sample.value for sample in window])
    flags = mark_jitter(window, policy)
    return [center if flagged else sample.value for sample, flagged in zip(window, flags)]


def stats(window: list[Sample], policy: Policy) -> WindowStats:
    """Compute the smoothed level and closing rate for one probe window."""

    used, suspect = select(window, policy)
    if not used:
        return WindowStats(
            base=window[-1].value if window else 0.0,
            rate=0.0,
            magnitude=0.0,
            median=0.0,
            used=0,
            suspects=len(suspect),
            direction="steady",
        )

    values = smooth(used, policy)
    center = median(values)
    first_value, last_value = values[0], values[-1]
    first_at, last_at = used[0].at, used[-1].at
    elapsed = last_at - first_at
    rate = (last_value - first_value) / elapsed if elapsed > 0 else 0.0
    magnitude = abs(rate)
    if rate > 0:
        direction = "rising"
    elif rate < 0:
        direction = "falling"
    else:
        direction = "steady"
    return WindowStats(
        base=last_value,
        rate=rate,
        magnitude=magnitude,
        median=center,
        used=len(used),
        suspects=len(suspect),
        direction=direction,
    )


def prune(window: list[Sample], policy: Policy, at: int) -> list[Sample]:
    """Keep only samples inside the time window and the sample cap."""

    cutoff = at - policy.window_seconds
    fresh = [sample for sample in window if sample.at >= cutoff]
    if len(fresh) > policy.max_samples:
        fresh = fresh[-policy.max_samples :]
    return fresh
