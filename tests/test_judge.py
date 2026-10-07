"""Behavioural tests for the versioned sliding window judge."""

from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace

from coldstore.judge import (
    Judge,
    PolicyBook,
    default_policy,
    median,
    replay_probe,
    robust_z,
    validate_policy,
)
from coldstore.judge.report import JITTER_MEDIAN, LATCH_HOLD, SHIFT_IMMEDIATE
from coldstore.judge.snapshot import archive, archives, content_hash, snapshot
from coldstore.store import Store


def lenient(judge: Judge) -> None:
    """Loosen flood control so an instant historical batch is fully admitted."""

    judge.policies.revise({"burst_per_probe": 512, "rate_per_probe": 10000.0})


class WindowMathCase(unittest.TestCase):
    def test_median_handles_even_and_odd(self) -> None:
        self.assertEqual(median([1, 3, 2]), 2)
        self.assertEqual(median([1, 2, 3, 4]), 2.5)
        self.assertEqual(median([]), 0.0)

    def test_robust_z_flags_a_single_wild_spike(self) -> None:
        values = [-18.0, -18.1, -17.9, -18.0]
        self.assertLess(robust_z(-18.0, values), 1.0)
        self.assertGreater(robust_z(60.0, values), 100.0)

    def test_policy_validation(self) -> None:
        validate_policy(default_policy())
        with self.assertRaises(ValueError):
            validate_policy(replace(default_policy(), window_seconds=0))
        with self.assertRaises(ValueError):
            validate_policy(replace(default_policy(), tiers=tuple(reversed(default_policy().tiers))))


class PolicyBookCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_seed_then_revise_appends_versions(self) -> None:
        judge = Judge(self.store)
        self.assertEqual(judge.policy().version, 1)
        judge.policies.revise({"window_seconds": 45})
        self.assertEqual(judge.policy().version, 2)
        self.assertEqual(judge.policy().window_seconds, 45)
        self.assertEqual(judge.policies.get(1).window_seconds, 30)
        self.assertEqual(judge.policies.versions(), [1, 2])

    def test_version_must_be_unique(self) -> None:
        book = PolicyBook(self.store)
        book.publish(default_policy(1))
        with self.assertRaises(ValueError):
            book.publish(default_policy(1))


class JudgeCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        self.judge = Judge(self.store)
        lenient(self.judge)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _ingest(self, probe: str, pairs: list[tuple[float, int]]):
        return self.judge.ingest(
            [{"probe": probe, "value": value, "at": at} for value, at in pairs]
        )

    def test_steady_window_holds_tier_zero(self) -> None:
        self._ingest("p", [(-18.0, t) for t in range(0, 60, 10)])
        self.assertEqual(self.judge.state_of("p").tier, 0)

    def test_upshift_needs_confirmation_then_actions_fire(self) -> None:
        # 1 deg per 10 s = 0.1 deg/s: clears tier 1 (0.05) but not tier 2.
        result = self._ingest(
            "p", [(-18.0 + i, i * 10) for i in range(6)]
        )
        tiers = [(c.at, c.tier) for c in result.conclusions]
        # first three excursion points only confirm; tier rises on the third.
        self.assertIn((30, 1), tiers)
        self.assertTrue(any(c.actions for c in result.conclusions))
        self.assertEqual(self.judge.state_of("p").tier, 1)

    def test_downshift_moves_one_level_per_tick(self) -> None:
        self.judge.policies.revise({"window_seconds": 90, "up_shift": SHIFT_IMMEDIATE})
        self._ingest("p", [(-18.0 + i * 2, i * 10) for i in range(6)])
        self.assertEqual(self.judge.state_of("p").tier, 2)
        result = self._ingest("p", [(20.0, t) for t in range(60, 130, 10)])
        tier_path = [c.tier for c in result.conclusions]
        # never drops more than one tier between adjacent ticks
        for earlier, later in zip(tier_path, tier_path[1:]):
            self.assertGreaterEqual(earlier - later, 0)
            self.assertLessEqual(earlier - later, 1)

    def test_immediate_upshift_jumps_to_measured_tier(self) -> None:
        self.judge.policies.revise({"up_shift": SHIFT_IMMEDIATE})
        result = self._ingest("p", [(-18.0 + i * 8, i * 10) for i in range(4)])
        self.assertEqual(result.conclusions[-1].tier, 3)

    def test_suspect_spike_is_excluded_from_linkage(self) -> None:
        sequence = [(-18.0, 0), (-18.0, 10), (-18.0, 20), (60.0, 30), (-18.0, 40)]
        result = self._ingest("p", sequence)
        spike = next(c for c in result.conclusions if c.at == 30)
        self.assertTrue(spike.held)
        self.assertEqual(spike.tier, 0)
        self.assertEqual(spike.suspects, 1)

    def test_median_mode_smooths_spike_away(self) -> None:
        self.judge.policies.revise({"jitter_mode": JITTER_MEDIAN})
        sequence = [(-18.0, 0), (-18.0, 10), (-18.0, 20), (60.0, 30), (-18.0, 40)]
        result = self._ingest("p", sequence)
        spike = next(c for c in result.conclusions if c.at == 30)
        self.assertFalse(spike.held)
        self.assertEqual(spike.magnitude, 0.0)

    def test_suspect_fault_freezes_then_recovers(self) -> None:
        self.judge.policies.revise(
            {"fault_streak": 3, "jitter_z": 2.0, "window_seconds": 120}
        )
        # solid baseline with intermittent wild spikes: each stays an outlier
        sequence = [
            (-18.0, 0), (-18.0, 10), (99.0, 20),
            (-18.0, 30), (99.0, 40), (-18.0, 50),
            (99.0, 60),
        ]
        self._ingest("p", sequence)
        self.assertTrue(self.judge.state_of("p").faulted)
        # healthy points age the spikes out of the 120 s window, then recover
        result = self._ingest("p", [(-18.0, t) for t in range(70, 160, 10)])
        self.assertFalse(result.conclusions[-1].suspect_fault)
        self.assertFalse(self.judge.state_of("p").faulted)

    def test_hold_latch_pins_top_tier_until_released(self) -> None:
        self.judge.policies.revise(
            {"up_shift": SHIFT_IMMEDIATE, "down_shift": SHIFT_IMMEDIATE, "latch": LATCH_HOLD}
        )
        self._ingest("p", [(-18.0 + i * 8, i * 10) for i in range(4)])
        self.assertTrue(self.judge.state_of("p").latched)
        result = self._ingest("p", [(30.0, t) for t in range(40, 100, 10)])
        self.assertTrue(all(c.tier == 3 for c in result.conclusions))
        self.judge.release("p")
        self.assertFalse(self.judge.state_of("p").latched)

    def test_concurrent_probes_sorted_by_sample_time(self) -> None:
        batch = [
            {"probe": "a", "value": -18.0, "at": 30},
            {"probe": "b", "value": -18.0, "at": 10},
            {"probe": "a", "value": -18.0, "at": 10},
            {"probe": "a", "value": -18.0, "at": 20},
        ]
        result = self.judge.ingest(batch)
        self.assertEqual(result.accepted, 4)
        self.assertEqual([s.at for s in self.judge.window_of("a")], [10, 20, 30])

    def test_duplicate_sample_time_is_rejected(self) -> None:
        self._ingest("p", [(-18.0, 10)])
        result = self._ingest("p", [(-17.0, 10)])
        self.assertEqual(result.duplicates, 1)
        self.assertEqual(result.accepted, 0)

    def test_batch_over_cap_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            self.judge.ingest(
                [{"probe": "p", "value": 1.0, "at": i} for i in range(1000)]
            )


class ThrottleCase(unittest.TestCase):
    def test_token_bucket_collapses_a_flood(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = Store.open(f"{tmp}/s.json")
            judge = Judge(store)  # default burst 4
            result = judge.ingest(
                [{"probe": "f", "value": -18.0, "at": i} for i in range(20)]
            )
            self.assertEqual(result.accepted, 4)
            self.assertEqual(result.throttled, 16)
            self.assertEqual(result.throttled_probes, ["f"])


class ReplayCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        self.judge = Judge(self.store)
        lenient(self.judge)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_conclusions_carry_the_pinned_policy_version(self) -> None:
        before = self.judge.policy().version
        self.judge.ingest([{"probe": "p", "value": -18.0, "at": 0}])
        self.judge.policies.revise({"window_seconds": 60})
        after = self.judge.policy().version
        self.judge.ingest([{"probe": "p", "value": -17.0, "at": 10}])
        replayed = replay_probe(self.store, "p")
        self.assertEqual([c.policy_version for c in replayed], [before, after])

    def test_override_version_runs_the_what_if(self) -> None:
        self.judge.policies.revise({"window_seconds": 90})
        self.judge.ingest(
            [{"probe": "p", "value": -18.0 + i, "at": i * 10} for i in range(4)]
        )
        under_v1 = replay_probe(self.store, "p", version=1)
        under_v2 = replay_probe(self.store, "p", version=2)
        self.assertTrue(all(c.policy_version == 1 for c in under_v1))
        self.assertTrue(all(c.policy_version == 2 for c in under_v2))


class SnapshotCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        self.judge = Judge(self.store)
        lenient(self.judge)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_archive_and_live_snapshot_share_one_hash(self) -> None:
        self.judge.ingest([{"probe": "p", "value": -18.0, "at": t} for t in range(0, 40, 10)])
        record = archive(self.store, self.judge, note="shift")
        live = snapshot(self.judge)
        self.assertEqual(record["content_hash"], live["content_hash"])
        self.assertEqual(record["content_hash"], content_hash(record["snapshot"]))
        self.assertEqual(len(archives(self.store)), 1)
        payload = json.loads(json.dumps(record["snapshot"]))
        self.assertEqual(payload["schema"], "judge-snapshot/v1")


if __name__ == "__main__":
    unittest.main()
