"""SQLite 连接与建表。

数据库文件路径由环境变量 DB_PATH 决定，默认 ./lab.db。
应用启动时调用 init_db() 自动创建数据库与表结构。
"""

import os
import sqlite3
import threading
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "lab.db"))

# 摄入操作使用进程级锁串行化，保证"缓冲-排水-提交"在同一事务中原子完成。
ingest_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id      TEXT PRIMARY KEY,
    committed_upto INTEGER NOT NULL,          -- 已连续提交到的最大序号
    created_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key           TEXT PRIMARY KEY,
    device_id     TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    payload_hash  TEXT NOT NULL,              -- 请求体规范化后的 sha256，用于检测同键不同载荷
    response_json TEXT NOT NULL,              -- 首次处理结果，重放时原样返回
    created_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS buffer (
    device_id    TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    received_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (device_id, seq)
);

CREATE TABLE IF NOT EXISTS committed (
    device_id    TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    committed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (device_id, seq)
);

CREATE TABLE IF NOT EXISTS duplicates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id       TEXT NOT NULL,
    seq             INTEGER,
    idempotency_key TEXT,
    reason          TEXT NOT NULL,            -- idempotent_replay / already_committed / already_buffered
    received_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_duplicates_device ON duplicates (device_id, id);

-- 持久化阈值告警规则：按 (设备, 指标) 唯一，上下限至少配置一个
CREATE TABLE IF NOT EXISTS alert_rules (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id       TEXT NOT NULL,
    metric          TEXT NOT NULL,
    upper_limit     REAL,                     -- 上限；value > upper_limit 视为越界，null 表示不限
    lower_limit     REAL,                     -- 下限；value < lower_limit 视为越界，null 表示不限
    trigger_count   INTEGER NOT NULL,         -- 连续越界多少次触发告警
    recovery_count  INTEGER NOT NULL,         -- 告警后连续不越界多少次恢复
    enabled         INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (device_id, metric),
    CHECK (upper_limit IS NOT NULL OR lower_limit IS NOT NULL),
    CHECK (trigger_count >= 1),
    CHECK (recovery_count >= 1),
    CHECK (lower_limit IS NULL OR upper_limit IS NULL OR lower_limit <= upper_limit)
);

-- 规则的迟滞状态机状态：与规则一对一
CREATE TABLE IF NOT EXISTS alert_rule_states (
    rule_id          INTEGER PRIMARY KEY REFERENCES alert_rules (id) ON DELETE CASCADE,
    state            TEXT NOT NULL DEFAULT 'normal',  -- normal / triggered
    breach_streak    INTEGER NOT NULL DEFAULT 0,      -- 连续越界计数
    recovery_streak  INTEGER NOT NULL DEFAULT 0,      -- 告警后连续不越界计数
    last_value       REAL,
    last_seq         INTEGER,
    last_evaluated_at TEXT
);

-- 告警 / 恢复事件，按提交顺序单调写入（id 即提交先后序号）
CREATE TABLE IF NOT EXISTS alert_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id         INTEGER REFERENCES alert_rules (id) ON DELETE SET NULL,
                    -- 规则删除后置 null，事件本身保留可查
    device_id       TEXT NOT NULL,
    metric          TEXT NOT NULL,
    seq             INTEGER NOT NULL,         -- 触发 / 恢复时所处理的包序号
    event_type      TEXT NOT NULL,            -- triggered / recovered
    value           REAL NOT NULL,            -- 触发或恢复的那个测量值
    event_time      TEXT,                     -- 测量值 ts；无 ts 时取包 sent_at；再无则提交时刻
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_alert_events_device_metric
    ON alert_events (device_id, metric, id);
CREATE INDEX IF NOT EXISTS idx_alert_events_rule ON alert_events (rule_id, id);
"""


def init_db() -> None:
    """创建数据库文件与全部表（幂等）。"""
    directory = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(directory, exist_ok=True)
    with get_conn() as conn:
        conn.executescript(SCHEMA)


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
