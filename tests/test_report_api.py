"""批次入库与日报发布：原子性、幂等、快照不可变与并发行为。"""

from __future__ import annotations

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.db import EventConflictError, PublishConflictError, ReportStore
from app.main import app
from app.report_models import CrackEventIn

SAMPLES = Path(__file__).resolve().parent.parent / "samples"
DAY = "2026-11-01"


def _event(event_id, car_id="CAR-001", level="L2", day=DAY) -> dict:
    return {
        "event_id": event_id,
        "survey_day": day,
        "car_id": car_id,
        "defect_level": level,
    }


@pytest.fixture()
def store(tmp_path):
    s = ReportStore(tmp_path / "cracks.db")
    yield s
    s.close()


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("CRACK_DB_PATH", str(tmp_path / "cracks.db"))
    with TestClient(app) as c:
        yield c


# ------------------------------------------------------------------ 入库语义


def test_batch_insert_is_atomic_and_reports_outcomes(store):
    events = [
        CrackEventIn.model_validate(_event("E1", level="L2")),
        CrackEventIn.model_validate(_event("E2", level="L3")),
        CrackEventIn.model_validate(_event("E3", car_id="CAR-002", level="L1")),
    ]
    result = store.ingest(events)
    assert result.received_event_count == 3
    assert result.inserted_event_count == 3
    assert result.duplicate_event_count == 0
    assert result.inserted_event_ids == ["E1", "E2", "E3"]

    batch = store.get_batch(result.batch_id)
    assert batch is not None
    assert [e.outcome for e in batch.events] == ["inserted", "inserted", "inserted"]


def test_same_event_id_same_content_across_batches_counted_once(store):
    first = store.ingest([CrackEventIn.model_validate(_event("E1"))])
    second = store.ingest(
        [
            CrackEventIn.model_validate(_event("E1")),
            CrackEventIn.model_validate(_event("E2", level="L3")),
        ]
    )
    assert second.inserted_event_ids == ["E2"]
    assert second.duplicate_event_ids == ["E1"]
    assert second.inserted_event_count == 1
    assert second.duplicate_event_count == 1

    detail = store.get_batch(second.batch_id)
    assert [e.outcome for e in detail.events] == ["duplicate", "inserted"]

    # 重投完全相同的批次也不重复计数
    replay = store.ingest(
        [
            CrackEventIn.model_validate(_event("E1")),
            CrackEventIn.model_validate(_event("E2", level="L3")),
        ]
    )
    assert replay.inserted_event_count == 0
    assert replay.duplicate_event_count == 2
    assert first.batch_id != replay.batch_id


def test_same_event_id_different_content_rejects_entire_batch(store):
    store.ingest([CrackEventIn.model_validate(_event("E1", car_id="CAR-001"))])
    bad = [
        CrackEventIn.model_validate(_event("E1", car_id="CAR-999")),
        CrackEventIn.model_validate(_event("E2")),
    ]
    with pytest.raises(EventConflictError) as excinfo:
        store.ingest(bad)
    assert excinfo.value.conflicts[0]["event_id"] == "E1"
    assert excinfo.value.conflicts[0]["scope"] == "existing_event"

    # 不留部分记录：没有新批次，也没有 E2
    rows = store._conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
    assert rows == 1
    assert store._conn.execute(
        "SELECT COUNT(*) FROM events WHERE event_id = 'E2'"
    ).fetchone()[0] == 0


def test_within_batch_conflict_also_rejects_entire_batch(store):
    with pytest.raises(EventConflictError) as excinfo:
        store.ingest(
            [
                CrackEventIn.model_validate(_event("E1", level="L1")),
                CrackEventIn.model_validate(_event("E2")),
                CrackEventIn.model_validate(_event("E1", level="L3")),
            ]
        )
    assert excinfo.value.conflicts[0]["scope"] == "within_batch"
    assert store._conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0] == 0
    assert store._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
    assert store._conn.execute("SELECT COUNT(*) FROM batch_events").fetchone()[0] == 0


def test_http_batch_conflict_returns_409_without_partial_records(client):
    ok = client.post(
        "/api/v1/events/batches",
        json={"events": [_event("E1", car_id="CAR-001")]},
    )
    assert ok.status_code == 200

    bad = client.post(
        "/api/v1/events/batches",
        json={"events": [_event("E1", car_id="CAR-999"), _event("E2")]},
    )
    assert bad.status_code == 409
    body = bad.json()
    assert body["detail"]["conflicts"][0]["event_id"] == "E1"

    detail = client.get("/api/v1/events/batches/2")
    assert detail.status_code == 404
    # 同号同内容重投恰好一次入库
    replay = client.post("/api/v1/events/batches", json={"events": [_event("E1", car_id="CAR-001")]})
    assert replay.status_code == 200
    assert replay.json()["inserted_event_count"] == 0


def test_http_rejects_invalid_batch_payload(client):
    resp = client.post(
        "/api/v1/events/batches",
        json={"events": [{**_event("E1"), "defect_level": ""}]},
    )
    assert resp.status_code == 422
    resp = client.post("/api/v1/events/batches", json={"events": []})
    assert resp.status_code == 422
    resp = client.post(
        "/api/v1/events/batches",
        json={"events": [{**_event("E1"), "survey_day": "2026-13-01"}]},
    )
    assert resp.status_code == 422
    resp = client.post(
        "/api/v1/events/batches",
        json={"events": [{**_event("E1"), "extra": "x"}]},
    )
    assert resp.status_code == 422


# ------------------------------------------------------------------ 日报发布


def test_report_counts_by_car_and_level_and_lists_event_ids(store):
    store.ingest(
        [
            CrackEventIn.model_validate(_event("E1", "CAR-001", "L2")),
            CrackEventIn.model_validate(_event("E2", "CAR-001", "L2")),
            CrackEventIn.model_validate(_event("E3", "CAR-001", "L3")),
            CrackEventIn.model_validate(_event("E4", "CAR-002", "L1")),
            CrackEventIn.model_validate(_event("E5", "CAR-002", "L1", day="2026-11-02")),
        ]
    )
    report, replayed = store.publish("pub-1", date(2026, 11, 1))
    assert not replayed
    assert report.version == 1
    assert report.total_events == 4
    assert report.survey_day == date(2026, 11, 1)
    assert [(c.car_id, c.defect_level, c.count) for c in report.counts] == [
        ("CAR-001", "L2", 2),
        ("CAR-001", "L3", 1),
        ("CAR-002", "L1", 1),
    ]
    assert report.event_ids == ["E1", "E2", "E3", "E4"]
    assert report.batch_ids


def test_same_publish_request_returns_original_version(store):
    store.ingest([CrackEventIn.model_validate(_event("E1"))])
    original, replayed = store.publish("pub-1", date(2026, 11, 1))
    assert not replayed

    # 同一请求号之后又来新批次，重试仍返回原版
    store.ingest([CrackEventIn.model_validate(_event("E2")), CrackEventIn.model_validate(_event("E3"))])
    again, replayed = store.publish("pub-1", date(2026, 11, 1))
    assert replayed
    assert again.version == original.version
    assert again.event_ids == original.event_ids
    assert again.total_events == original.total_events
    assert again.published_at == original.published_at


def test_new_request_id_includes_later_batches_and_old_version_is_immutable(store):
    store.ingest([CrackEventIn.model_validate(_event("E1", "CAR-001", "L2"))])
    v1, _ = store.publish("pub-1", date(2026, 11, 1))
    assert v1.event_ids == ["E1"]

    store.ingest(
        [
            CrackEventIn.model_validate(_event("E2", "CAR-001", "L2")),
            CrackEventIn.model_validate(_event("E3", "CAR-002", "L3")),
        ]
    )
    v2, replayed = store.publish("pub-2", date(2026, 11, 1))
    assert not replayed
    assert v2.version == 2
    assert v2.event_ids == ["E1", "E2", "E3"]
    assert v2.total_events == 3
    assert v1.total_events == 1
    assert v1.event_ids == ["E1"]

    assert store.get_report(date(2026, 11, 1), 1) == v1
    assert store.get_report(date(2026, 11, 1), 2) == v2
    assert store.list_reports(date(2026, 11, 1)) == [v1, v2]


def test_versions_are_per_survey_day(store):
    store.ingest(
        [
            CrackEventIn.model_validate(_event("E1", day="2026-11-01")),
            CrackEventIn.model_validate(_event("E2", day="2026-11-02")),
        ]
    )
    r1, _ = store.publish("pub-a", date(2026, 11, 1))
    r2, _ = store.publish("pub-b", date(2026, 11, 2))
    assert (r1.version, r2.version) == (1, 1)
    assert r1.event_ids == ["E1"]
    assert r2.event_ids == ["E2"]


def test_publish_request_id_cannot_move_to_another_day(store):
    store.publish("pub-1", date(2026, 11, 1))
    with pytest.raises(PublishConflictError):
        store.publish("pub-1", date(2026, 11, 2))


def test_publish_failure_leaves_no_queryable_report(store, monkeypatch):
    store.ingest([CrackEventIn.model_validate(_event("E1"))])
    original = store._insert_report_row

    def boom(report):
        original(report)
        raise RuntimeError("模拟发布中途失败")

    monkeypatch.setattr(store, "_insert_report_row", boom)
    with pytest.raises(RuntimeError):
        store.publish("pub-x", date(2026, 11, 1))

    monkeypatch.setattr(store, "_insert_report_row", original)
    assert store.list_reports(date(2026, 11, 1)) == []
    # 失败请求号不被占用，重试可正常发布
    report, replayed = store.publish("pub-x", date(2026, 11, 1))
    assert report.version == 1
    assert not replayed


def test_successful_publish_after_failed_publish_does_not_skip_version(store, monkeypatch):
    store.ingest([CrackEventIn.model_validate(_event("E1"))])

    def boom(report):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(store, "_insert_report_row", boom)
    with pytest.raises(sqlite3.OperationalError):
        store.publish("pub-fail", date(2026, 11, 1))
    monkeypatch.undo()

    report, _ = store.publish("pub-ok", date(2026, 11, 1))
    assert report.version == 1


def test_concurrent_publishes_get_distinct_versions_and_consistent_snapshots(store):
    for i in range(50):
        store.ingest([CrackEventIn.model_validate(_event(f"E{i}"))])

    barrier = threading.Barrier(8)

    def publish(i: int):
        barrier.wait()
        return store.publish(f"pub-{i}", date(2026, 11, 1))

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = [f.result() for f in [pool.submit(publish, i) for i in range(8)]]

    versions = {r.version for r, _ in results}
    assert versions == set(range(1, 9))
    expected_ids = sorted(f"E{i}" for i in range(50))
    for report, _ in results:
        assert report.total_events == 50
        assert report.event_ids == expected_ids


def test_publish_never_sees_uncommitted_batch(store):
    """发布事务与入库事务串行：失败/未提交的补传不会进入快照。"""
    store.ingest([CrackEventIn.model_validate(_event("E1"))])

    # 含同号异内容的批会被整体拒绝，发布只能看到已提交的 E1
    try:
        store.ingest(
            [
                CrackEventIn.model_validate(_event("E1", car_id="CAR-X")),
                CrackEventIn.model_validate(_event("E2")),
            ]
        )
    except EventConflictError:
        pass

    report, _ = store.publish("pub-1", date(2026, 11, 1))
    assert report.event_ids == ["E1"]


# ------------------------------------------------------------------ HTTP 端到端


def test_http_publish_flow_with_samples(client):
    ingest_sample = json.loads((SAMPLES / "backfill_batch.json").read_text(encoding="utf-8"))
    publish_sample = json.loads((SAMPLES / "publish_retry.json").read_text(encoding="utf-8"))
    republish_sample = json.loads(
        (SAMPLES / "republish_after_backfill.json").read_text(encoding="utf-8")
    )

    r = client.post("/api/v1/events/batches", json=ingest_sample)
    assert r.status_code == 200
    assert r.json()["inserted_event_count"] == 3

    r = client.post("/api/v1/reports/publish", json=publish_sample)
    assert r.status_code == 200
    v1 = r.json()
    assert v1["version"] == 1
    assert v1["total_events"] == 3
    assert v1["event_ids"] == ["EV-20261101-0001", "EV-20261101-0002", "EV-20261101-0003"]

    r = client.post("/api/v1/reports/publish", json=publish_sample)
    assert r.status_code == 200
    assert r.headers.get("X-Idempotent-Replay") == "true"
    assert r.json() == v1

    # 补传后重发：新请求号 -> 新版本，旧版仍可查
    later = {"events": [
        {"event_id": "EV-20261101-0004", "survey_day": "2026-11-01",
         "car_id": "CAR-002", "defect_level": "L3"},
    ]}
    assert client.post("/api/v1/events/batches", json=later).status_code == 200
    r = client.post("/api/v1/reports/publish", json=republish_sample)
    assert r.status_code == 200
    v2 = r.json()
    assert v2["version"] == 2
    assert v2["total_events"] == 4

    assert client.get("/api/v1/reports/2026-11-01/1").json() == v1
    assert client.get("/api/v1/reports/2026-11-01/2").json() == v2
    assert client.get("/api/v1/reports/2026-11-01").json() == [v1, v2]
    assert client.get("/api/v1/reports/2026-11-03/1").status_code == 404


def test_openapi_exposes_ingest_and_republish_examples(client):
    docs = client.get("/openapi.json").json()
    assert "/api/v1/events/batches" in docs["paths"]
    assert "/api/v1/reports/publish" in docs["paths"]
    assert "/api/v1/reports/{survey_day}" in docs["paths"]
    examples = docs["paths"]["/api/v1/events/batches"]["post"]["requestBody"]["content"][
        "application/json"
    ]["examples"]
    assert "reingest_same_batch" in examples
    assert "reingest_conflict" in examples
    publish_examples = docs["paths"]["/api/v1/reports/publish"]["post"]["requestBody"][
        "content"
    ]["application/json"]["examples"]
    assert "retry_same_request" in publish_examples
    assert "republish_after_backfill" in publish_examples


def test_identifier_normalization_makes_int_and_str_same_event(store):
    first = store.ingest(
        [CrackEventIn.model_validate(
            {"event_id": 1001, "survey_day": DAY, "car_id": 7, "defect_level": "L1"}
        )]
    )
    assert first.inserted_event_ids == ["1001"]
    second = store.ingest(
        [CrackEventIn.model_validate(
            {"event_id": "1001", "survey_day": DAY, "car_id": "7", "defect_level": "L1"}
        )]
    )
    assert second.inserted_event_count == 0
    assert second.duplicate_event_ids == ["1001"]
    batch = store.get_batch(first.batch_id)
    assert batch.events[0].car_id == "7"


def test_alias_fields_are_accepted_and_normalized(client):
    resp = client.post(
        "/api/v1/events/batches",
        json={"events": [{
            "event_no": "EV-ALIAS",
            "inspection_day": "2026-11-05",
            "car_number": 88,
            "level": "L3",
        }]},
    )
    assert resp.status_code == 200
    assert resp.json()["inserted_event_ids"] == ["EV-ALIAS"]


def test_http_publish_request_id_cannot_move_day(client):
    first = client.post(
        "/api/v1/reports/publish",
        json={"publish_request_id": "pub-fixed", "survey_day": "2026-11-01"},
    )
    assert first.status_code == 200
    conflict = client.post(
        "/api/v1/reports/publish",
        json={"publish_request_id": "pub-fixed", "survey_day": "2026-11-02"},
    )
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["conflicts"][0]["requested_survey_day"] == "2026-11-02"
    # 原巡测日重试仍返回原版
    retry = client.post(
        "/api/v1/reports/publish",
        json={"publish_request_id": "pub-fixed", "survey_day": "2026-11-01"},
    )
    assert retry.status_code == 200
    assert retry.headers.get("X-Idempotent-Replay") == "true"


def test_concurrent_backfill_and_publish_only_exposes_committed_batches(store):
    """补传与发布真并发时，每个快照的 total_events 必须是 5 的整数倍。"""
    stop = threading.Event()
    errors: list[BaseException] = []

    def writer():
        batch_no = 0
        try:
            while not stop.is_set() and batch_no < 60:
                batch_no += 1
                events = [
                    CrackEventIn.model_validate(
                        _event(f"B{batch_no:03d}-{j}", day="2026-11-09")
                    )
                    for j in range(5)
                ]
                store.ingest(events)
        except BaseException as exc:  # pragma: no cover - 失败时由断言报告
            errors.append(exc)

    def reader(snapshot: list):
        try:
            for idx in range(30):
                report, _ = store.publish(f"pub-conc-{idx}", date(2026, 11, 9))
                snapshot.append(report)
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    snapshots: list = []
    w = threading.Thread(target=writer)
    r = threading.Thread(target=reader, args=(snapshots,))
    w.start()
    r.start()
    r.join()
    stop.set()
    w.join()

    assert not errors
    assert snapshots
    for report in snapshots:
        assert report.total_events % 5 == 0
        # 快照内的事件必须等量来自完整批次
        assert len(report.batch_ids) * 5 >= report.total_events
        assert report.total_events == len(report.event_ids)
        *_, last_batch = report.batch_ids
        batch = store.get_batch(last_batch)
        assert batch is not None


def test_concurrent_same_request_id_yields_one_version(store):
    store.ingest([CrackEventIn.model_validate(_event("E1"))])
    barrier = threading.Barrier(8)

    def publish_same():
        barrier.wait()
        return store.publish("pub-same", date(2026, 11, 1))

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = [f.result() for f in [pool.submit(publish_same) for _ in range(8)]]

    assert {r.version for r, _ in results} == {1}
    assert sum(1 for _, replayed in results if not replayed) == 1
    assert store.list_reports(date(2026, 11, 1)) == [results[0][0]]


def test_store_survives_reopen(tmp_path):
    db = tmp_path / "persist.db"
    first = ReportStore(db)
    first.ingest([CrackEventIn.model_validate(_event("E1"))])
    v1, _ = first.publish("pub-1", date(2026, 11, 1))
    first.close()

    second = ReportStore(db)
    try:
        assert second.get_report(date(2026, 11, 1), 1) == v1
        batch = second.get_batch(v1.batch_ids[0])
        assert batch is not None
        assert batch.events[0].event_id == "E1"
        v2, replayed = second.publish("pub-2", date(2026, 11, 1))
        assert not replayed
        assert v2.version == 2
    finally:
        second.close()
