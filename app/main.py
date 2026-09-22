"""FastAPI 路由与应用入口。"""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, Header, HTTPException, Query

from . import db
from .alerts import (
    AlertRuleConflict,
    AlertRuleError,
    create_rule,
    delete_rule,
    get_rule,
    update_rule,
)
from .logic import IdempotencyConflict, ingest_package
from .rollup import RollupError, compute_rollup
from .schemas import (
    AlertEventsResponse,
    AlertEventOut,
    CurrentAlertOut,
    CurrentAlertsResponse,
    CommittedResponse,
    DeviceState,
    DuplicatesResponse,
    DuplicateRecord,
    GapsResponse,
    IngestResponse,
    PackageIn,
    RollupResponse,
    RuleCreate,
    RuleListResponse,
    RuleOut,
    RuleUpdate,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()  # 容器/进程启动时自动建库建表
    yield


app = FastAPI(
    title="Lab Instrument Ingest API",
    version="1.0.0",
    description="实验室仪器批量测量包接收服务：幂等去重、乱序缓冲、按序提交。",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/api/v1/packages", response_model=IngestResponse, status_code=200)
def post_package(
    pkg: PackageIn,
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    key = pkg.idempotency_key or idempotency_key
    if not key:
        raise HTTPException(
            status_code=422,
            detail="缺少幂等键：请在请求体 idempotency_key 或 Idempotency-Key 请求头中提供",
        )
    with db.ingest_lock, db.get_conn() as conn:
        try:
            return ingest_package(conn, pkg, key)
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/v1/devices", response_model=List[DeviceState])
def list_devices():
    with db.get_conn() as conn:
        rows = conn.execute(
            """
            SELECT d.device_id,
                   d.committed_upto,
                   (SELECT COUNT(*) FROM buffer    b WHERE b.device_id = d.device_id) AS buffered_count,
                   (SELECT COUNT(*) FROM committed c WHERE c.device_id = d.device_id) AS committed_count,
                   (SELECT COUNT(*) FROM duplicates p WHERE p.device_id = d.device_id) AS duplicate_count
            FROM devices d ORDER BY d.device_id
            """
        ).fetchall()
    return [
        DeviceState(
            device_id=r["device_id"],
            committed_upto=r["committed_upto"],
            next_expected=r["committed_upto"] + 1,
            buffered_count=r["buffered_count"],
            committed_count=r["committed_count"],
            duplicate_count=r["duplicate_count"],
        )
        for r in rows
    ]


@app.get("/api/v1/devices/{device_id}/gaps", response_model=GapsResponse)
def get_gaps(device_id: str):
    with db.get_conn() as conn:
        dev = conn.execute(
            "SELECT committed_upto FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if dev is None:
            raise HTTPException(status_code=404, detail=f"未知设备: {device_id}")
        committed_upto = dev["committed_upto"]
        buffered = [
            r["seq"]
            for r in conn.execute(
                "SELECT seq FROM buffer WHERE device_id = ? ORDER BY seq", (device_id,)
            ).fetchall()
        ]
    gaps: List[int] = []
    if buffered:
        have = set(buffered)
        gaps = [s for s in range(committed_upto + 1, max(buffered) + 1) if s not in have]
    return GapsResponse(
        device_id=device_id,
        committed_upto=committed_upto,
        next_expected=committed_upto + 1,
        buffered_seqs=buffered,
        gaps=gaps,
    )


@app.get("/api/v1/devices/{device_id}/committed", response_model=CommittedResponse)
def get_committed(
    device_id: str,
    from_seq: Optional[int] = Query(default=None, ge=1),
    to_seq: Optional[int] = Query(default=None, ge=1),
    limit: int = Query(default=1000, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
):
    with db.get_conn() as conn:
        dev = conn.execute(
            "SELECT committed_upto FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if dev is None:
            raise HTTPException(status_code=404, detail=f"未知设备: {device_id}")
        where, params = "device_id = ?", [device_id]
        if from_seq is not None:
            where += " AND seq >= ?"
            params.append(from_seq)
        if to_seq is not None:
            where += " AND seq <= ?"
            params.append(to_seq)
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM committed WHERE {where}", params
        ).fetchone()["n"]
        seqs = [
            r["seq"]
            for r in conn.execute(
                f"SELECT seq FROM committed WHERE {where} ORDER BY seq LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
        ]
    return CommittedResponse(
        device_id=device_id,
        committed_upto=dev["committed_upto"],
        count=total,
        seqs=seqs,
    )


@app.get("/api/v1/devices/{device_id}/duplicates", response_model=DuplicatesResponse)
def get_duplicates(
    device_id: str,
    reason: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    with db.get_conn() as conn:
        dev = conn.execute(
            "SELECT 1 FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if dev is None:
            raise HTTPException(status_code=404, detail=f"未知设备: {device_id}")
        where, params = "device_id = ?", [device_id]
        if reason:
            where += " AND reason = ?"
            params.append(reason)
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM duplicates WHERE {where}", params
        ).fetchone()["n"]
        rows = conn.execute(
            f"SELECT * FROM duplicates WHERE {where} ORDER BY id LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
    return DuplicatesResponse(
        device_id=device_id,
        count=total,
        records=[DuplicateRecord(**dict(r)) for r in rows],
    )


@app.get("/api/v1/devices/{device_id}/metrics/rollup", response_model=RollupResponse)
def get_metrics_rollup(
    device_id: str,
    metric: str = Query(min_length=1, description="指标名，如 temperature"),
    from_: datetime = Query(alias="from", description="聚合范围起点（含），ISO 8601"),
    to: datetime = Query(description="聚合范围终点（不含），ISO 8601"),
    window_seconds: float = Query(gt=0, description="窗口宽度（秒）"),
    allowed_lateness: float = Query(
        default=0.0, ge=0, description="允许的迟到时长（秒），用于计算水位"
    ),
):
    with db.get_conn() as conn:
        dev = conn.execute(
            "SELECT 1 FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if dev is None:
            raise HTTPException(status_code=404, detail=f"未知设备: {device_id}")
        try:
            return compute_rollup(
                conn, device_id, metric, from_, to, window_seconds, allowed_lateness
            )
        except RollupError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# 阈值告警规则
# ---------------------------------------------------------------------------

@app.post("/api/v1/rules", response_model=RuleOut, status_code=201)
def post_rule(rule_in: RuleCreate):
    with db.ingest_lock, db.get_conn() as conn:
        try:
            rule = create_rule(
                conn,
                rule_in.device_id,
                rule_in.metric,
                rule_in.upper_limit,
                rule_in.lower_limit,
                rule_in.trigger_count,
                rule_in.recovery_count,
                rule_in.enabled,
            )
        except AlertRuleConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except AlertRuleError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return RuleOut(**conn.execute(
            "SELECT * FROM alert_rules WHERE id = ?", (rule.id,)
        ).fetchone())


@app.get("/api/v1/rules", response_model=RuleListResponse)
def list_rules(
    device_id: Optional[str] = Query(default=None, description="按设备过滤"),
    metric: Optional[str] = Query(default=None, description="按指标过滤"),
):
    where, params = [], []
    if device_id:
        where.append("device_id = ?")
        params.append(device_id)
    if metric:
        where.append("metric = ?")
        params.append(metric)
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    with db.get_conn() as conn:
        rows = conn.execute(
            f"SELECT * FROM alert_rules {clause} ORDER BY id", params
        ).fetchall()
    return RuleListResponse(count=len(rows), rules=[RuleOut(**dict(r)) for r in rows])


def _get_rule_or_404(conn, rule_id: int):
    rule = get_rule(conn, rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail=f"规则不存在: {rule_id}")
    return rule


@app.get("/api/v1/rules/{rule_id}", response_model=RuleOut)
def get_rule_detail(rule_id: int):
    with db.get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM alert_rules WHERE id = ?", (rule_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"规则不存在: {rule_id}")
        return RuleOut(**dict(row))


@app.put("/api/v1/rules/{rule_id}", response_model=RuleOut)
def put_rule(rule_id: int, rule_in: RuleUpdate):
    with db.ingest_lock, db.get_conn() as conn:
        _get_rule_or_404(conn, rule_id)
        try:
            update_rule(
                conn,
                rule_id,
                rule_in.upper_limit,
                rule_in.lower_limit,
                rule_in.trigger_count,
                rule_in.recovery_count,
                rule_in.enabled,
            )
        except AlertRuleError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        row = conn.execute(
            "SELECT * FROM alert_rules WHERE id = ?", (rule_id,)
        ).fetchone()
        return RuleOut(**dict(row))


@app.delete("/api/v1/rules/{rule_id}", status_code=204)
def remove_rule(rule_id: int):
    with db.ingest_lock, db.get_conn() as conn:
        if not delete_rule(conn, rule_id):
            raise HTTPException(status_code=404, detail=f"规则不存在: {rule_id}")
    return None


# ---------------------------------------------------------------------------
# 当前告警与告警事件
# ---------------------------------------------------------------------------

@app.get("/api/v1/alerts", response_model=CurrentAlertsResponse)
def get_current_alerts(
    device_id: Optional[str] = Query(default=None, description="按设备过滤"),
    metric: Optional[str] = Query(default=None, description="按指标过滤"),
):
    """当前仍处于告警状态的规则。triggered_* 取自最近一次触发事件。"""
    where, params = ["s.state = 'triggered'", "r.enabled = 1"], []
    if device_id:
        where.append("r.device_id = ?")
        params.append(device_id)
    if metric:
        where.append("r.metric = ?")
        params.append(metric)
    clause = " AND ".join(where)
    sql = f"""
        SELECT r.id               AS rule_id,
               r.device_id        AS device_id,
               r.metric           AS metric,
               r.upper_limit      AS upper_limit,
               r.lower_limit      AS lower_limit,
               r.trigger_count    AS trigger_count,
               r.recovery_count   AS recovery_count,
               t.seq              AS triggered_seq,
               t.id               AS triggered_event_id,
               t.event_time       AS triggered_at,
               s.last_value       AS last_value,
               s.last_seq         AS last_seq,
               s.recovery_streak  AS recovery_streak
          FROM alert_rule_states s
          JOIN alert_rules r ON r.id = s.rule_id
          JOIN alert_events t
            ON t.id = (
                SELECT MAX(e.id) FROM alert_events e
                 WHERE e.rule_id = r.id AND e.event_type = 'triggered'
            )
         WHERE {clause}
         ORDER BY t.id
    """
    with db.get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return CurrentAlertsResponse(
        count=len(rows), alerts=[CurrentAlertOut(**dict(r)) for r in rows]
    )


@app.get("/api/v1/devices/{device_id}/alerts/events", response_model=AlertEventsResponse)
def get_device_alert_events(
    device_id: str,
    metric: Optional[str] = Query(default=None, description="按指标过滤"),
    event_type: Optional[str] = Query(
        default=None, description="按事件类型过滤：triggered / recovered"
    ),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """按设备（可选指标）分页查询告警事件，按事件序号升序返回。"""
    with db.get_conn() as conn:
        dev = conn.execute(
            "SELECT 1 FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if dev is None:
            raise HTTPException(status_code=404, detail=f"未知设备: {device_id}")
        if event_type not in (None, "triggered", "recovered"):
            raise HTTPException(
                status_code=422,
                detail="event_type 仅支持 triggered 或 recovered",
            )
        where, params = ["device_id = ?"], [device_id]
        if metric:
            where.append("metric = ?")
            params.append(metric)
        if event_type:
            where.append("event_type = ?")
            params.append(event_type)
        clause = " AND ".join(where)
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM alert_events WHERE {clause}", params
        ).fetchone()["n"]
        rows = conn.execute(
            f"SELECT * FROM alert_events WHERE {clause} ORDER BY id LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
    return AlertEventsResponse(
        count=total,
        limit=limit,
        offset=offset,
        events=[AlertEventOut(**dict(r)) for r in rows],
    )
