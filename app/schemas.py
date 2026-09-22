"""请求 / 响应的数据模型。"""

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class Measurement(BaseModel):
    metric: str = Field(min_length=1, description="指标名，如 temperature")
    value: float
    unit: Optional[str] = None
    ts: Optional[datetime] = None


class PackageIn(BaseModel):
    """仪器上报的一个批量测量包。"""

    device_id: str = Field(min_length=1)
    seq: int = Field(ge=1, description="包序号，按设备单调递增、连续")
    idempotency_key: Optional[str] = Field(
        default=None, description="幂等键；也可用 Idempotency-Key 请求头传入"
    )
    sent_at: Optional[datetime] = None
    measurements: List[Measurement] = Field(min_length=1)


class IngestResponse(BaseModel):
    status: Literal["committed", "buffered", "duplicate"]
    device_id: str
    seq: int
    committed_upto: int
    committed_seqs: List[int] = Field(
        default_factory=list, description="本次请求触发提交的全部序号（含排水）"
    )
    duplicate_reason: Optional[str] = None


class DeviceState(BaseModel):
    device_id: str
    committed_upto: int
    next_expected: int
    buffered_count: int
    committed_count: int
    duplicate_count: int


class GapsResponse(BaseModel):
    device_id: str
    committed_upto: int
    next_expected: int
    buffered_seqs: List[int]
    gaps: List[int] = Field(description="缓冲区最大序号之前缺失的序号")


class CommittedResponse(BaseModel):
    device_id: str
    committed_upto: int
    count: int
    seqs: List[int]


class DuplicateRecord(BaseModel):
    id: int
    device_id: str
    seq: Optional[int]
    idempotency_key: Optional[str]
    reason: str
    received_at: str


class DuplicatesResponse(BaseModel):
    device_id: str
    count: int
    records: List[DuplicateRecord]


class RollupWindow(BaseModel):
    """单个半开窗口 [start, end) 的聚合结果；空窗口 count=0 且统计值为 null。"""

    start: datetime
    end: datetime
    count: int
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None


class RollupResponse(BaseModel):
    """按事件时间（sent_at）的窗口聚合结果。"""

    model_config = {"populate_by_name": True}

    device_id: str
    metric: str
    from_: datetime = Field(alias="from", description="聚合范围起点（含）")
    to: datetime = Field(description="聚合范围终点（不含）")
    window_seconds: float
    allowed_lateness: float
    watermark: Optional[datetime] = Field(
        default=None,
        description="设备已提交数据的最大 sent_at 减 allowed_lateness；无数据时为 null",
    )
    late_count: int = Field(
        description="范围内 sent_at 早于水位的迟到样本数（不改写已关闭窗口）"
    )
    windows: List[RollupWindow] = Field(description="按 start 升序的全部窗口")


# ---------------------------------------------------------------------------
# 阈值告警
# ---------------------------------------------------------------------------

class RuleBase(BaseModel):
    """规则的可配置字段（创建与全量更新共用）。"""

    upper_limit: Optional[float] = Field(
        default=None, description="上限；value > upper_limit 视为越界，null 表示不限"
    )
    lower_limit: Optional[float] = Field(
        default=None, description="下限；value < lower_limit 视为越界，null 表示不限"
    )
    trigger_count: int = Field(
        default=1, ge=1, description="连续越界多少次后触发告警"
    )
    recovery_count: int = Field(
        default=1, ge=1, description="告警后连续不越界多少次后恢复"
    )
    enabled: bool = Field(default=True, description="是否参与评估")

    @model_validator(mode="after")
    def _check_limits(self):
        if self.upper_limit is None and self.lower_limit is None:
            raise ValueError("upper_limit 与 lower_limit 至少需要提供一个")
        if (
            self.upper_limit is not None
            and self.lower_limit is not None
            and self.lower_limit > self.upper_limit
        ):
            raise ValueError("lower_limit 不能大于 upper_limit")
        return self


class RuleCreate(RuleBase):
    device_id: str = Field(min_length=1)
    metric: str = Field(min_length=1, description="指标名，如 temperature")


class RuleUpdate(RuleBase):
    """PUT 全量更新；device_id 与 metric 不可修改。"""


class RuleOut(BaseModel):
    id: int
    device_id: str
    metric: str
    upper_limit: Optional[float]
    lower_limit: Optional[float]
    trigger_count: int
    recovery_count: int
    enabled: bool
    created_at: str
    updated_at: str


class RuleListResponse(BaseModel):
    count: int
    rules: List[RuleOut]


class AlertEventOut(BaseModel):
    """一次触发或恢复事件；id 为按提交顺序单调递增的事件序号。"""

    id: int
    rule_id: Optional[int] = Field(
        description="规则删除后为 null，事件本身仍可查询"
    )
    device_id: str
    metric: str
    seq: int = Field(description="触发 / 恢复时处理的包序号")
    event_type: Literal["triggered", "recovered"]
    value: float = Field(description="触发或恢复的那个测量值")
    event_time: Optional[str] = Field(
        description="触发 / 恢复时间：优先取测量 ts，其次包 sent_at，再无则提交时刻"
    )
    created_at: str = Field(description="事件落库时间")


class AlertEventsResponse(BaseModel):
    count: int
    limit: int
    offset: int
    events: List[AlertEventOut]


class CurrentAlertOut(BaseModel):
    """当前仍处于告警中的规则状态（规则状态为 triggered 且规则启用）。"""

    rule_id: int
    device_id: str
    metric: str
    upper_limit: Optional[float]
    lower_limit: Optional[float]
    trigger_count: int
    recovery_count: int
    triggered_seq: int = Field(description="触发告警的包序号")
    triggered_event_id: int = Field(description="触发事件的事件序号")
    triggered_at: Optional[str] = Field(description="触发时间")
    last_value: Optional[float]
    last_seq: Optional[int]
    recovery_streak: int = Field(description="当前连续不越界计数（未达恢复次数）")


class CurrentAlertsResponse(BaseModel):
    count: int
    alerts: List[CurrentAlertOut]
