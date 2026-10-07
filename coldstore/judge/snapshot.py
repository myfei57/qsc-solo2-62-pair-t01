"""Canonical judge snapshot shared by the console and the archive.

There is exactly one serialization here, so a point in time archived to disk
has the same shape and field meanings as the live ``/judge`` view. A content
hash lets operators prove that an archived snapshot has not drifted.
"""

from __future__ import annotations

import hashlib
import json

from coldstore.store.store import Store

from .engine import Judge


def canonical_payload(judge: Judge) -> dict[str, object]:
    """Build the versioned, deterministic judge document."""

    book = judge.policies
    current = judge.policy()
    probes: list[dict[str, object]] = []
    for probe in judge.probes():
        window = judge.window_of(probe)
        state = judge.state_of(probe)
        probes.append(
            {
                "probe": probe,
                "state": state.as_dict(),
                "window": [sample.as_dict() for sample in window],
                "samples": len(window),
            }
        )
    return {
        "schema": "judge-snapshot/v1",
        "current_version": current.version,
        "versions": book.versions(),
        "policy": current.as_dict(),
        "probes": probes,
    }


def content_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def snapshot(judge: Judge) -> dict[str, object]:
    payload = canonical_payload(judge)
    return {"snapshot": payload, "content_hash": content_hash(payload)}


ARCHIVE_KEY = "judge:archive"


def archive(store: Store, judge: Judge, note: str = "") -> dict[str, object]:
    """Persist one immutable snapshot copy and return it with its hash."""

    from coldstore.store.commands import append_command, load_commands

    payload = canonical_payload(judge)
    record = {"note": note, "content_hash": content_hash(payload), "snapshot": payload}
    append_command(store, ARCHIVE_KEY, json.dumps(record, ensure_ascii=False))
    return record


def archives(store: Store) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for item in _load_raw(store):
        try:
            records.append(json.loads(item))
        except ValueError:
            continue
    return records


def _load_raw(store: Store) -> list[str]:
    from coldstore.store.commands import load_commands

    return load_commands(store, ARCHIVE_KEY)
