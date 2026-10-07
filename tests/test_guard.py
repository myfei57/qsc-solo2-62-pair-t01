"""Behavioural tests for the sliding window guard."""

from __future__ import annotations

import json
import tempfile
import unittest

from coldstore.audit import Auditor
from coldstore.comp import Bank
from coldstore.guard import Guard, execute_actions
from coldstore.store import Store


class GuardCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        self.now = [0.0]
        self.auditor = Auditor(self.store)
        self.guard = Guard(self.store, self.auditor, clock=lambda: self.now[0])

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def feed(self, probe: str, values: list[float], start: int = 0):
        outcomes = []
        for index, value in enumerate(values):
            self.now[0] += 1.0
            outcomes.append(self.guard.ingest(probe, value, at=start + index))
        return outcomes

    def audit_kinds(self) -> list[str]:
        return [entry.kind for entry in self.auditor.entries()]

    def test_escalation_needs_two_confirming_windows(self) -> None:
        outcomes = self.feed("p1", [0.0, 0.8, 1.6, 2.4])
        self.assertEqual([outcome.tier for outcome in outcomes], [0, 0, 1, 1])
        self.assertAlmostEqual(outcomes[1].verdict.rate, 0.8)
        self.assertEqual(outcomes[2].actions, ("audit",))
        self.assertIn("guard.tier", self.audit_kinds())

    def test_jump_of_two_tiers_lands_immediately(self) -> None:
        outcomes = self.feed("p1", [0.0, 2.5])
        self.assertEqual(outcomes[-1].tier, 3)
        self.assertEqual(outcomes[-1].actions, ("audit", "notify"))

    def test_recovery_steps_down_after_hold_windows(self) -> None:
        outcomes = self.feed("p1", [0.0, 0.8, 1.6, 1.6, 1.6, 1.6, 1.6])
        self.assertEqual([outcome.tier for outcome in outcomes], [0, 0, 1, 1, 1, 1, 0])

    def test_spike_is_quarantined_not_smoothed(self) -> None:
        outcomes = self.feed("p1", [0.0, 0.8, 1.6, 2.4, 50.0, 4.0])
        self.assertTrue(outcomes[4].suspicious)
        self.assertIsNone(outcomes[4].verdict)
        self.assertFalse(outcomes[5].suspicious)
        self.assertAlmostEqual(outcomes[5].verdict.rate, 0.8, places=6)
        self.assertEqual(outcomes[5].tier, 1)

    def test_sustained_jump_is_adopted_as_new_baseline(self) -> None:
        outcomes = self.feed("p1", [0.0, 10.0, 20.0, 30.0, 40.0, 50.0])
        self.assertTrue(all(outcome.suspicious for outcome in outcomes[1:5]))
        self.assertFalse(outcomes[5].suspicious)
        self.assertIn("guard.step", self.audit_kinds())

    def test_unreliable_window_freezes_the_tier_machine(self) -> None:
        outcomes = self.feed("p1", [0.0, 0.8, 50.0, -50.0, 50.0, -50.0, 4.8])
        last = outcomes[-1]
        self.assertIsNotNone(last.verdict)
        self.assertFalse(last.verdict.reliable)
        self.assertEqual(last.tier, 0)

    def test_late_sample_rebuilds_the_timeline(self) -> None:
        self.feed("p1", [0.0, 0.1, 0.2], start=10)
        outcome = self.guard.ingest("p1", -2.0, at=9)
        self.assertIsNotNone(outcome.verdict)
        self.assertTrue(outcome.verdict.late)
        self.assertTrue(outcome.verdict.revision)
        self.assertEqual(outcome.tier, 2)
        self.assertEqual(outcome.actions, ("audit", "notify"))
        self.assertIn("guard.revision", self.audit_kinds())

    def test_flood_is_token_bucketed_and_audited(self) -> None:
        self.guard.set_policy({"burst": 2, "rate_per_second": 0.1})
        outcomes = self.feed("p1", [0.0, 0.1, 0.2, 0.3, 0.4])
        self.assertEqual([outcome.dropped for outcome in outcomes], [False, False, True, True, True])
        state = self.guard.state().as_dict()
        probe = state["probes"][0]
        self.assertEqual(probe["accepted"], 2)
        self.assertEqual(probe["dropped"], 3)
        self.assertEqual(probe["throttle_events"], 1)
        self.assertEqual(self.audit_kinds().count("guard.throttle"), 1)

    def test_replay_uses_the_version_effective_at_sample_time(self) -> None:
        self.feed("p1", [0.0, 0.4, 0.8, 1.2, 1.6, 2.0], start=100)
        self.now[0] = 200.0
        self.guard.set_policy(
            {"tiers": [{"name": "watch", "limit": 0.2, "actions": ["audit"], "confirm": 2, "hold": 3}]}
        )
        self.feed("p1", [2.3, 2.6, 2.9, 3.2, 3.5], start=206)
        report = self.guard.replay("p1", since=100, until=210)
        self.assertEqual(report.versions, (1, 2))
        self.assertEqual(report.windows, 9)
        self.assertEqual(report.tier_changes, 1)
        versions = [verdict["policy_version"] for verdict in report.verdicts]
        self.assertEqual(versions, [1, 1, 1, 1, 1, 2, 2, 2, 2])
        again = self.guard.replay("p1", since=100, until=210)
        self.assertEqual(report.digest, again.digest)

    def test_live_verdicts_match_replay_under_constant_policy(self) -> None:
        self.feed("p1", [0.0, 0.8, 1.6, 2.4, 3.2, 4.0])
        live = [
            (item["end"], round(item["rate"], 6), item["tier"], item["candidate"])
            for item in self.guard.verdicts("p1")
        ][::-1]
        report = self.guard.replay("p1", since=0, until=5)
        replayed = [
            (item["end"], round(item["rate"], 6), item["tier"], item["candidate"])
            for item in report.verdicts
        ]
        self.assertEqual(live, replayed)

    def test_policy_versions_are_immutable(self) -> None:
        first = self.guard.policy()
        self.guard.set_policy({"window_seconds": 60})
        second = self.guard.policy()
        self.assertEqual((first.version, second.version), (1, 2))
        self.assertEqual(self.guard.policies()[0].window_seconds, first.window_seconds)
        self.assertEqual(second.window_seconds, 60)
        with self.assertRaises(ValueError):
            self.guard.set_policy({"window_seconds": 1})
        with self.assertRaises(ValueError):
            self.guard.set_policy({"tiers": [{"name": "x", "limit": 1.0}, {"name": "y", "limit": 0.5}]})


class LinkageCase(unittest.TestCase):
    def test_execute_actions_against_the_bank(self) -> None:
        bank = Bank()
        bank.add_compressor("c1", 2)
        results = execute_actions(
            bank, ["comp.stage_up:c1", "comp.stage_down:c1", "comp.stop:c1", "notify", "bogus"]
        )
        self.assertEqual(results[0], {"action": "comp.stage_up:c1", "ok": True, "stage": 1})
        self.assertEqual(results[1]["stage"], 0)
        self.assertTrue(results[2]["ok"])
        self.assertEqual(results[3], {"action": "notify", "ok": True, "delivered": False})
        self.assertFalse(results[4]["ok"])
        missing = execute_actions(bank, ["comp.stage_up:nope"])
        self.assertFalse(missing[0]["ok"])


if __name__ == "__main__":
    unittest.main()
