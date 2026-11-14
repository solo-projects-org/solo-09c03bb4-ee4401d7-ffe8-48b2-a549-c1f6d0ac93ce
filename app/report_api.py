"""批次接收、批次复核与按巡测日发布日报的 HTTP 接口。"""

from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field

from .db import EventConflictError, PublishConflictError, ReportStore
from .report_models import (
    BatchDetailResponse,
    BatchIngestRequest,
    BatchIngestResponse,
    CarLevelCount,
    DailyReportVersion,
    PublishReportRequest,
)


class ConflictDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str
    conflicts: list[dict[str, object]] = Field(default_factory=list)


router = APIRouter(prefix="/api/v1", tags=["日报"])


def get_store(request: Request) -> ReportStore:
    """返回应用级 ReportStore；测试可通过 dependency_overrides 替换。"""
    store = getattr(request.app.state, "store", None)
    if store is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="存储尚未初始化",
        )
    return store


StoreDep = Annotated[ReportStore, Depends(get_store)]


@router.post(
    "/events/batches",
    response_model=BatchIngestResponse,
    status_code=status.HTTP_200_OK,
    summary="整批补传裂纹事件（幂等入库）",
    description=(
        "一个请求即一个批次，整批在单个事务内入库。\n\n"
        "- 跨批同号同内容：只计一次，本批记为 duplicate；\n"
        "- 跨批或批内同号异内容：拒绝整批（409），且不留下本批任何记录；\n"
        "- 重投完全相同的批次：inserted=0，结果仍可复核。\n\n"
        "同一批次重投会生成新的 batch_id，但事件不会重复计数。"
    ),
    responses={
        409: {
            "model": ConflictDetail,
            "description": "批次内存在同号异内容事件，整批未入库",
        }
    },
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "ingest": {
                            "summary": "首次补传 3 条事件",
                            "value": {
                                "events": [
                                    {
                                        "event_id": "EV-20261101-0001",
                                        "survey_day": "2026-11-01",
                                        "car_id": "CAR-001",
                                        "defect_level": "L2",
                                    },
                                    {
                                        "event_id": "EV-20261101-0002",
                                        "survey_day": "2026-11-01",
                                        "car_id": "CAR-001",
                                        "defect_level": "L3",
                                    },
                                    {
                                        "event_id": "EV-20261101-0003",
                                        "survey_day": "2026-11-01",
                                        "car_id": "CAR-002",
                                        "defect_level": "L1",
                                    },
                                ]
                            },
                        },
                        "reingest_same_batch": {
                            "summary": "重投：事件号与内容完全相同，只计一次",
                            "value": {
                                "events": [
                                    {
                                        "event_id": "EV-20261101-0001",
                                        "survey_day": "2026-11-01",
                                        "car_id": "CAR-001",
                                        "defect_level": "L2",
                                    },
                                    {
                                        "event_id": "EV-20261101-0002",
                                        "survey_day": "2026-11-01",
                                        "car_id": "CAR-001",
                                        "defect_level": "L3",
                                    },
                                    {
                                        "event_id": "EV-20261101-0003",
                                        "survey_day": "2026-11-01",
                                        "car_id": "CAR-002",
                                        "defect_level": "L1",
                                    },
                                ]
                            },
                        },
                        "reingest_conflict": {
                            "summary": "同号异内容：整批 409 且不留下任何记录",
                            "value": {
                                "events": [
                                    {
                                        "event_id": "EV-20261101-0001",
                                        "survey_day": "2026-11-01",
                                        "car_id": "CAR-001",
                                        "defect_level": "L2",
                                    },
                                    {
                                        "event_id": "EV-20261101-0004",
                                        "survey_day": "2026-11-01",
                                        "car_id": "CAR-003",
                                        "defect_level": "L1",
                                    },
                                    {
                                        "event_id": "EV-20261101-0001",
                                        "survey_day": "2026-11-01",
                                        "car_id": "CAR-009",
                                        "defect_level": "L1",
                                    },
                                ]
                            },
                        },
                    }
                }
            }
        }
    },
)
def ingest_batch(payload: BatchIngestRequest, store: StoreDep) -> BatchIngestResponse:
    try:
        return store.ingest(payload.events)
    except EventConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": str(exc), "conflicts": exc.conflicts},
        ) from exc


@router.get(
    "/events/batches/{batch_id}",
    response_model=BatchDetailResponse,
    summary="按批次号复核入库明细",
    description="返回该批次的接收条数、实际入库/重复条数，以及逐条事件结论。",
)
def get_batch(batch_id: int, store: StoreDep) -> BatchDetailResponse:
    batch = store.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="批次不存在")
    return batch


@router.post(
    "/reports/publish",
    response_model=DailyReportVersion,
    status_code=status.HTTP_200_OK,
    summary="按巡测日发布一版可复核的裂纹日报",
    description=(
        "在单个写事务中基于**已完整提交的批次**生成不可变快照。\n\n"
        "- `publish_request_id` 是幂等键：同号重试返回原版（响应头 "
        "`X-Idempotent-Replay: true`），不会生成新版本；\n"
        "- 新请求号生成新版本，纳入发布时刻已提交的全部事件；\n"
        "- 旧版本始终可查且内容不变；\n"
        "- 发布过程任何异常都整体回滚，不会留下可查询的半成品。\n\n"
        "响应中的 `batch_ids` 指出本版事件来自哪些批次，便于复核批次完整性。"
    ),
    responses={
        409: {
            "model": ConflictDetail,
            "description": "该发布请求号已被另一巡测日使用",
        }
    },
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "first_publish": {
                            "summary": "首次发布 2026-11-01 日报",
                            "value": {
                                "publish_request_id": "pub-20261101-001",
                                "survey_day": "2026-11-01",
                            },
                        },
                        "retry_same_request": {
                            "summary": "重试同一请求号：返回原版，不生成新版本",
                            "value": {
                                "publish_request_id": "pub-20261101-001",
                                "survey_day": "2026-11-01",
                            },
                        },
                        "republish_after_backfill": {
                            "summary": "补传后重发：新请求号，纳入后到批次",
                            "value": {
                                "publish_request_id": "pub-20261101-002",
                                "survey_day": "2026-11-01",
                            },
                        },
                    }
                }
            }
        }
    },
)
def publish_report(
    payload: PublishReportRequest, store: StoreDep, response: Response
) -> DailyReportVersion:
    try:
        report, replayed = store.publish(payload.publish_request_id, payload.survey_day)
    except PublishConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": str(exc),
                "conflicts": [
                    {
                        "publish_request_id": exc.publish_request_id,
                        "existing_survey_day": exc.existing_day,
                        "requested_survey_day": exc.requested_day,
                    }
                ],
            },
        ) from exc
    if replayed:
        response.headers["X-Idempotent-Replay"] = "true"
    return report


@router.get(
    "/reports/{survey_day}",
    response_model=list[DailyReportVersion],
    summary="列出某巡测日的全部日报版本（旧版不变）",
)
def list_reports(survey_day: date, store: StoreDep) -> list[DailyReportVersion]:
    return store.list_reports(survey_day)


@router.get(
    "/reports/{survey_day}/{version}",
    response_model=DailyReportVersion,
    summary="查询某巡测日的指定版本日报",
)
def get_report(survey_day: date, version: int, store: StoreDep) -> DailyReportVersion:
    report = store.get_report(survey_day, version)
    if report is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="日报版本不存在")
    return report
