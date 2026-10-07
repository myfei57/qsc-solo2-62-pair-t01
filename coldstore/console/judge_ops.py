"""Console endpoints for the sliding window judge."""

from __future__ import annotations

import json

from coldstore.judge import archive, archives, replay_probe, snapshot
from coldstore.judge.policy import policy_from_dict, validate_policy

from .http import Request, RequestError, Response, json_response
from .runtime import Runtime


def judge_snapshot(rt: Runtime, request: Request) -> Response:
    return json_response(snapshot(rt.judge))


def judge_ingest(rt: Runtime, request: Request) -> Response:
    raw = request.payload.get("readings", [])
    if not isinstance(raw, list):
        raise RequestError(400, "readings must be a list")
    at = request.int_field("at", 0) or None
    readings: list[dict[str, object]] = []
    for item in raw:
        if not isinstance(item, dict):
            raise RequestError(400, "every reading must be an object")
        readings.append(item)
    result = rt.judge.ingest(readings, at=at)
    return json_response(result.as_dict())


def judge_probe(rt: Runtime, request: Request) -> Response:
    probe = request.str_field("probe")
    window = rt.judge.window_of(probe)
    state = rt.judge.state_of(probe)
    return json_response(
        {
            "probe": probe,
            "state": state.as_dict(),
            "window": [sample.as_dict() for sample in window],
            "samples": len(window),
        }
    )


def judge_probes(rt: Runtime, request: Request) -> Response:
    return json_response({"probes": rt.judge.probes()})


def judge_policy(rt: Runtime, request: Request) -> Response:
    book = rt.judge.policies
    version = request.int_field("version", 0)
    policy = book.current() if version <= 0 else book.get(version)
    if policy is None:
        raise RequestError(404, "unknown policy version")
    return json_response(
        {"policy": policy.as_dict(), "versions": book.versions(), "current": book.current_version()}
    )


def judge_policy_revise(rt: Runtime, request: Request) -> Response:
    changes = dict(request.payload)
    changes.pop("version", None)
    try:
        policy = rt.judge.policies.revise(changes)
    except (TypeError, KeyError) as exc:
        raise RequestError(400, f"invalid policy: {exc}") from exc
    rt.auditor.record(
        "judge.policy", json.dumps({"version": policy.version}, ensure_ascii=False)
    )
    return json_response({"policy": policy.as_dict(), "current": policy.version})


def judge_policy_replace(rt: Runtime, request: Request) -> Response:
    raw = request.payload.get("policy")
    if not isinstance(raw, dict):
        raise RequestError(400, "policy object is required")
    try:
        candidate = policy_from_dict(raw)
        validate_policy(candidate)
    except (TypeError, KeyError, ValueError) as exc:
        raise RequestError(400, f"invalid policy: {exc}") from exc
    current = rt.judge.policies.current()
    object.__setattr__(candidate, "version", current.version + 1)
    published = rt.judge.policies.publish(candidate)
    rt.auditor.record(
        "judge.policy", json.dumps({"version": published.version}, ensure_ascii=False)
    )
    return json_response({"policy": published.as_dict(), "current": published.version})


def judge_replay(rt: Runtime, request: Request) -> Response:
    probe = request.str_field("probe")
    version = request.int_field("version", 0) or None
    conclusions = replay_probe(rt.store, probe, version=version)
    return json_response(
        {"probe": probe, "version": version, "conclusions": [c.as_dict() for c in conclusions]}
    )


def judge_release(rt: Runtime, request: Request) -> Response:
    probe = request.str_field("probe")
    state = rt.judge.release(probe)
    return json_response({"probe": probe, "state": state.as_dict()})


def judge_archive_create(rt: Runtime, request: Request) -> Response:
    note = request.str_field("note", "")
    record = archive(rt.store, rt.judge, note=note)
    rt.auditor.record(
        "judge.archive",
        json.dumps({"content_hash": record["content_hash"], "note": note}, ensure_ascii=False),
    )
    return json_response(record)


def judge_archive_list(rt: Runtime, request: Request) -> Response:
    records = archives(rt.store)
    return json_response({"archives": records, "count": len(records)})
