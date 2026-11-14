"""裂纹事件补传（批次接收）与按巡测日发布日报的数据模型。

命名约定（中文需求 -> 字段名）：

- 全局事件号 -> ``event_id``（接受字符串或整数，统一规范化为字符串）
- 巡测日     -> ``survey_day``（ISO 日期 ``YYYY-MM-DD``）
- 车号       -> ``car_id``（接受字符串或整数，统一规范化为字符串）
- 缺陷等级   -> ``defect_level``（非空字符串，如 L1/L2/L3）

为便于对接不同采集端，少数输入字段接受常见别名（例如 ``event_no`` /
``inspection_day``）；响应始终使用规范字段名。
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

# 单批事件数上限：既防止误传超大请求，也保证单事务可快速完成。
MAX_BATCH_EVENTS = 10_000
# 标识类字段的规范字符串长度上限。
MAX_ID_LEN = 256
MAX_LEVEL_LEN = 64


def _identifier(value: Any) -> str:
    """把事件号/车号规范化为非空字符串；拒绝布尔、空串与空白串。"""
    if isinstance(value, bool):
        raise ValueError("标识不能是布尔值")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("标识不能为空字符串")
        if len(text) > MAX_ID_LEN:
            raise ValueError(f"标识长度不能超过 {MAX_ID_LEN} 个字符")
        return text
    raise ValueError("标识必须是字符串或整数")


class CrackEventIn(BaseModel):
    """一条待入库的裂纹事件。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    event_id: Annotated[
        str | int,
        Field(
            validation_alias=AliasChoices("event_id", "event_no", "global_event_id"),
            description="全局事件号；字符串或整数，按字符串规范化后判重",
        ),
    ]
    survey_day: Annotated[
        date,
        Field(
            validation_alias=AliasChoices("survey_day", "inspection_day"),
            description="巡测日（YYYY-MM-DD）",
        ),
    ]
    car_id: Annotated[
        str | int,
        Field(
            validation_alias=AliasChoices("car_id", "car_number", "vehicle_id"),
            description="车号；字符串或整数，按字符串规范化",
        ),
    ]
    defect_level: Annotated[
        str,
        Field(
            validation_alias=AliasChoices("defect_level", "level"),
            min_length=1,
            max_length=MAX_LEVEL_LEN,
            description="缺陷等级，如 L1/L2/L3",
        ),
    ]

    @field_validator("event_id", "car_id", mode="before")
    @classmethod
    def _normalize_identifier(cls, v: Any) -> str:
        return _identifier(v)

    @field_validator("defect_level", mode="before")
    @classmethod
    def _normalize_level(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise ValueError("缺陷等级必须是字符串")
        text = v.strip()
        if not text:
            raise ValueError("缺陷等级不能为空")
        return text

    def content(self) -> dict[str, str]:
        """参与「同号同内容」比较的规范化内容。"""
        return {
            "survey_day": self.survey_day.isoformat(),
            "car_id": self.car_id,
            "defect_level": self.defect_level,
        }


class BatchIngestRequest(BaseModel):
    """一次整批补传：一个请求即一个批次，整批原子入库。"""

    model_config = ConfigDict(extra="forbid")

    events: Annotated[
        list[CrackEventIn],
        Field(min_length=1, max_length=MAX_BATCH_EVENTS, description="本批次的事件列表"),
    ]


class BatchIngestResponse(BaseModel):
    """批次入库结果（重投时 inserted=0，duplicate 为重复条数）。"""

    model_config = ConfigDict(extra="forbid")

    batch_id: int
    received_event_count: int
    inserted_event_count: int
    duplicate_event_count: int
    inserted_event_ids: list[str]
    duplicate_event_ids: list[str]


class BatchEventRecord(BaseModel):
    """批次内的单条事件及其入库结论。"""

    model_config = ConfigDict(extra="forbid")

    event_id: str
    survey_day: date
    car_id: str
    defect_level: str
    outcome: Literal["inserted", "duplicate"]


class BatchDetailResponse(BaseModel):
    """按批次号复核入库明细。"""

    model_config = ConfigDict(extra="forbid")

    batch_id: int
    ingested_at: datetime
    received_event_count: int
    inserted_event_count: int
    duplicate_event_count: int
    events: list[BatchEventRecord]


class PublishReportRequest(BaseModel):
    """按巡测日发布一版日报。

    ``publish_request_id`` 是幂等键：重试同一请求号返回原版快照；
    新请求号生成一个纳入“发布时已完整提交批次”的新版本。
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    publish_request_id: Annotated[
        str,
        Field(
            validation_alias=AliasChoices("publish_request_id", "request_id", "publish_id"),
            min_length=1,
            max_length=128,
            description="发布请求号；同一请求号重试返回原版",
        ),
    ]
    survey_day: Annotated[
        date,
        Field(
            validation_alias=AliasChoices("survey_day", "inspection_day"),
            description="巡测日（YYYY-MM-DD）",
        ),
    ]

    @field_validator("publish_request_id", mode="before")
    @classmethod
    def _normalize_request_id(cls, v: Any) -> str:
        if not isinstance(v, str):
            raise ValueError("发布请求号必须是字符串")
        text = v.strip()
        if not text:
            raise ValueError("发布请求号不能为空")
        return text


class CarLevelCount(BaseModel):
    """某车号 × 某缺陷等级的事件计数。"""

    model_config = ConfigDict(extra="forbid")

    car_id: str
    defect_level: str
    count: int


class DailyReportVersion(BaseModel):
    """一版不可变的日报快照。"""

    model_config = ConfigDict(extra="forbid")

    publish_request_id: str
    survey_day: date
    version: Annotated[int, Field(ge=1, description="同一巡测日内的版本号，从 1 递增")]
    published_at: datetime
    total_events: int
    counts: list[CarLevelCount]
    event_ids: Annotated[list[str], Field(description="本版纳入的全局事件号（升序）")]
    batch_ids: Annotated[
        list[int],
        Field(description="本版纳入事件首次入库的批次号（升序），可据此复核批次完整性"),
    ]
