"""SQLite 持久化：整批原子入库、跨批事件幂等、日报版本快照。

关键不变量：

1. 一个补传批次 = 一个 SQLite 事务。批内任一条「同号异内容」冲突，
   整个事务回滚，批记录、事件记录、批明细都不会留下。
2. 全局事件号在 ``events`` 表中唯一。跨批同号同内容只保留首次入库的那一条，
   后面的批次把该事件标记为 duplicate；跨批同号异内容直接拒绝后到批次。
3. 日报发布在 ``BEGIN IMMEDIATE`` 事务中完成：读取的只能是已提交批次，
   快照、版本号与报告行一起提交；任何异常都整体回滚，查不到半成品。
4. ``publish_request_id`` 有唯一约束，重试同一请求号返回原始快照。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import date, datetime, timezone
from pathlib import Path

from .report_models import (
    BatchDetailResponse,
    BatchEventRecord,
    BatchIngestResponse,
    CarLevelCount,
    CrackEventIn,
    DailyReportVersion,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at          TEXT NOT NULL,
    received_event_count INTEGER NOT NULL,
    inserted_event_count INTEGER NOT NULL,
    duplicate_event_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,
    survey_day   TEXT NOT NULL,
    car_id       TEXT NOT NULL,
    defect_level TEXT NOT NULL,
    batch_id     INTEGER NOT NULL REFERENCES batches(batch_id),
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_survey_day ON events(survey_day);

CREATE TABLE IF NOT EXISTS batch_events (
    batch_id  INTEGER NOT NULL REFERENCES batches(batch_id),
    seq       INTEGER NOT NULL,
    event_id  TEXT NOT NULL,
    outcome   TEXT NOT NULL CHECK (outcome IN ('inserted', 'duplicate')),
    PRIMARY KEY (batch_id, seq)
);

CREATE TABLE IF NOT EXISTS reports (
    report_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    publish_request_id TEXT NOT NULL UNIQUE,
    survey_day         TEXT NOT NULL,
    version            INTEGER NOT NULL,
    published_at       TEXT NOT NULL,
    total_events       INTEGER NOT NULL,
    counts_json        TEXT NOT NULL,
    event_ids_json     TEXT NOT NULL,
    batch_ids_json     TEXT NOT NULL,
    UNIQUE (survey_day, version)
);
"""


class EventConflictError(Exception):
    """批次中存在同号异内容事件，整批被拒绝。"""

    def __init__(self, conflicts: list[dict[str, object]]):
        self.conflicts = conflicts
        super().__init__("批次包含同号异内容事件，整批未入库")


class PublishConflictError(Exception):
    """发布请求号已被另一巡测日占用。"""

    def __init__(self, publish_request_id: str, existing_day: str, requested_day: str):
        self.publish_request_id = publish_request_id
        self.existing_day = existing_day
        self.requested_day = requested_day
        super().__init__("发布请求号已被其他巡测日的日报占用")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_report(row: sqlite3.Row) -> DailyReportVersion:
    return DailyReportVersion.model_validate(
        {
            "publish_request_id": row["publish_request_id"],
            "survey_day": row["survey_day"],
            "version": row["version"],
            "published_at": row["published_at"],
            "total_events": row["total_events"],
            "counts": json.loads(row["counts_json"]),
            "event_ids": json.loads(row["event_ids_json"]),
            "batch_ids": json.loads(row["batch_ids_json"]),
        }
    )


class ReportStore:
    """单进程内共享一个 SQLite 连接，所有写操作经 ``RLock`` 串行化。

    单连接 + 显式 ``BEGIN IMMEDIATE`` 保证：并发 HTTP 请求下，发布事务看到的
    永远是完整提交的批次；进程崩溃时 SQLite 日志保证提交原子性。
    """

    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None,  # 显式事务控制
            timeout=30.0,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(_SCHEMA)

    # ------------------------------------------------------------------ 入库

    def ingest(self, events: list[CrackEventIn]) -> BatchIngestResponse:
        """整批入库；同号同内容跨批只计一次，同号异内容拒绝整批。"""
        received = len(events)

        # 先做纯内存的批内判重，事务之前就能发现批内同号异内容。
        first_in_batch: dict[str, tuple[dict[str, str], int]] = {}
        conflicts: list[dict[str, object]] = []
        for seq, event in enumerate(events):
            content = event.content()
            prior = first_in_batch.get(event.event_id)
            if prior is not None and prior[0] != content:
                conflicts.append(
                    {
                        "event_id": event.event_id,
                        "reason": "content_mismatch_within_batch",
                        "scope": "within_batch",
                        "existing": prior[0],
                        "incoming": content,
                        "existing_seq": prior[1],
                        "incoming_seq": seq,
                    }
                )
            elif prior is None:
                first_in_batch[event.event_id] = (content, seq)
        if conflicts:
            raise EventConflictError(conflicts)

        inserted_ids: list[str] = []
        duplicate_ids: list[str] = []
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # 与已提交事件逐号比对（跨批同号同内容 -> duplicate，异内容 -> 冲突）
                for event_id, (content, _seq) in first_in_batch.items():
                    row = self._conn.execute(
                        "SELECT survey_day, car_id, defect_level, batch_id "
                        "FROM events WHERE event_id = ?",
                        (event_id,),
                    ).fetchone()
                    if row is None:
                        continue
                    existing = {
                        "survey_day": row["survey_day"],
                        "car_id": row["car_id"],
                        "defect_level": row["defect_level"],
                    }
                    if existing != content:
                        conflicts.append(
                            {
                                "event_id": event_id,
                                "reason": "content_mismatch_with_existing",
                                "scope": "existing_event",
                                "existing": existing,
                                "incoming": content,
                                "existing_batch_id": row["batch_id"],
                            }
                        )
                if conflicts:
                    # 整批拒绝：回滚后不存在任何本批记录。
                    self._conn.execute("ROLLBACK")
                    raise EventConflictError(conflicts)

                now = _now().isoformat()
                self._conn.execute(
                    "INSERT INTO batches "
                    "(ingested_at, received_event_count, inserted_event_count, duplicate_event_count) "
                    "VALUES (?, ?, 0, 0)",
                    (now, received),
                )
                batch_id = int(self._conn.execute("SELECT last_insert_rowid()").fetchone()[0])

                for seq, event in enumerate(events):
                    row = self._conn.execute(
                        "SELECT 1 FROM events WHERE event_id = ?", (event.event_id,)
                    ).fetchone()
                    if row is None:
                        self._conn.execute(
                            "INSERT INTO events "
                            "(event_id, survey_day, car_id, defect_level, batch_id, created_at) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                event.event_id,
                                event.survey_day.isoformat(),
                                event.car_id,
                                event.defect_level,
                                batch_id,
                                now,
                            ),
                        )
                        outcome = "inserted"
                        inserted_ids.append(event.event_id)
                    else:
                        outcome = "duplicate"
                        duplicate_ids.append(event.event_id)
                    self._conn.execute(
                        "INSERT INTO batch_events (batch_id, seq, event_id, outcome) "
                        "VALUES (?, ?, ?, ?)",
                        (batch_id, seq, event.event_id, outcome),
                    )

                self._conn.execute(
                    "UPDATE batches SET inserted_event_count = ?, duplicate_event_count = ? "
                    "WHERE batch_id = ?",
                    (len(inserted_ids), len(duplicate_ids), batch_id),
                )
                self._conn.execute("COMMIT")
            except EventConflictError:
                raise
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

        return BatchIngestResponse(
            batch_id=batch_id,
            received_event_count=received,
            inserted_event_count=len(inserted_ids),
            duplicate_event_count=len(duplicate_ids),
            inserted_event_ids=inserted_ids,
            duplicate_event_ids=duplicate_ids,
        )

    def get_batch(self, batch_id: int) -> BatchDetailResponse | None:
        with self._lock:
            batch = self._conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                return None
            rows = self._conn.execute(
                "SELECT be.seq, be.event_id, be.outcome, e.survey_day, e.car_id, e.defect_level "
                "FROM batch_events be "
                "JOIN events e ON e.event_id = be.event_id "
                "WHERE be.batch_id = ? ORDER BY be.seq",
                (batch_id,),
            ).fetchall()
        return BatchDetailResponse(
            batch_id=batch_id,
            ingested_at=batch["ingested_at"],
            received_event_count=batch["received_event_count"],
            inserted_event_count=batch["inserted_event_count"],
            duplicate_event_count=batch["duplicate_event_count"],
            events=[
                BatchEventRecord(
                    event_id=r["event_id"],
                    survey_day=r["survey_day"],
                    car_id=r["car_id"],
                    defect_level=r["defect_level"],
                    outcome=r["outcome"],
                )
                for r in rows
            ],
        )

    # ------------------------------------------------------------------ 发布

    def publish(
        self, publish_request_id: str, survey_day: date
    ) -> tuple[DailyReportVersion, bool]:
        """发布一版日报，返回 ``(快照, 是否为同一请求号的重试)``。

        全过程在一个写事务内：读取的批次要么完整可见、要么完全不可见。
        """
        day_text = survey_day.isoformat()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._conn.execute(
                    "SELECT * FROM reports WHERE publish_request_id = ?",
                    (publish_request_id,),
                ).fetchone()
                if existing is not None:
                    if existing["survey_day"] != day_text:
                        raise PublishConflictError(
                            publish_request_id, existing["survey_day"], day_text
                        )
                    self._conn.execute("COMMIT")
                    return _row_to_report(existing), True

                event_rows = self._conn.execute(
                    "SELECT event_id, car_id, defect_level, batch_id FROM events "
                    "WHERE survey_day = ? ORDER BY event_id",
                    (day_text,),
                ).fetchall()

                event_ids = [r["event_id"] for r in event_rows]
                batch_ids = sorted({int(r["batch_id"]) for r in event_rows})

                counter: dict[tuple[str, str], int] = {}
                for r in event_rows:
                    key = (r["car_id"], r["defect_level"])
                    counter[key] = counter.get(key, 0) + 1
                counts = [
                    CarLevelCount(car_id=car, defect_level=lvl, count=n)
                    for (car, lvl), n in sorted(counter.items())
                ]

                next_version_row = self._conn.execute(
                    "SELECT COALESCE(MAX(version), 0) + 1 FROM reports WHERE survey_day = ?",
                    (day_text,),
                ).fetchone()
                version = int(next_version_row[0])
                published_at = _now().isoformat()

                report = DailyReportVersion(
                    publish_request_id=publish_request_id,
                    survey_day=survey_day,
                    version=version,
                    published_at=published_at,
                    total_events=len(event_ids),
                    counts=counts,
                    event_ids=event_ids,
                    batch_ids=batch_ids,
                )
                self._insert_report_row(report)
                self._conn.execute("COMMIT")
                return report, False
            except BaseException:
                # 包含业务冲突与任何底层异常：都能保证不留下半成品报告。
                try:
                    self._conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                raise

    def _insert_report_row(self, report: DailyReportVersion) -> None:
        """写入报告快照行；独立方法便于故障注入测试回滚语义。"""
        self._conn.execute(
            "INSERT INTO reports (publish_request_id, survey_day, version, published_at, "
            "total_events, counts_json, event_ids_json, batch_ids_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                report.publish_request_id,
                report.survey_day.isoformat(),
                report.version,
                report.published_at.isoformat(),
                report.total_events,
                json.dumps(
                    [c.model_dump(mode="json") for c in report.counts],
                    ensure_ascii=False,
                ),
                json.dumps(report.event_ids, ensure_ascii=False),
                json.dumps(report.batch_ids),
            ),
        )

    def list_reports(self, survey_day: date) -> list[DailyReportVersion]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM reports WHERE survey_day = ? ORDER BY version",
                (survey_day.isoformat(),),
            ).fetchall()
        return [_row_to_report(r) for r in rows]

    def get_report(self, survey_day: date, version: int) -> DailyReportVersion | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM reports WHERE survey_day = ? AND version = ?",
                (survey_day.isoformat(), version),
            ).fetchone()
        return None if row is None else _row_to_report(row)
