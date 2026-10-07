"""Execute the linkage actions a tier allows against the compressor bank.

Actions are configured per tier as strings. Device actions carry an explicit
target, for example ``comp.stage_up:c1``; anything else is recorded as a
notification so the audit trail still shows what the tier concluded.
"""

from __future__ import annotations

from coldstore.comp import Bank

DEVICE_ACTIONS = ("comp.stage_up", "comp.stage_down", "comp.start", "comp.stop")


def execute_actions(bank: Bank, actions: tuple[str, ...] | list[str]) -> list[dict[str, object]]:
    """Run each action, returning one result row per action."""

    results: list[dict[str, object]] = []
    for action in actions:
        name, _, target = action.partition(":")
        name = name.strip()
        target = target.strip()
        try:
            if name == "comp.stage_up" and target:
                results.append({"action": action, "ok": True, "stage": bank.stage_up(target)})
            elif name == "comp.stage_down" and target:
                results.append({"action": action, "ok": True, "stage": bank.stage_down(target)})
            elif name == "comp.start" and target:
                bank.start(target)
                results.append({"action": action, "ok": True})
            elif name == "comp.stop" and target:
                bank.stop(target)
                results.append({"action": action, "ok": True})
            elif name in ("audit", "notify"):
                results.append({"action": action, "ok": True, "delivered": False})
            else:
                results.append({"action": action, "ok": False, "error": "unknown action"})
        except ValueError as exc:
            results.append({"action": action, "ok": False, "error": str(exc)})
    return results
