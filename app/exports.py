"""只读归档导出：对已提交测量包建立不可变快照，按游标分页输出 NDJSON。

设计要点：

- 导出任务创建时把请求的 seq 范围内**已提交（committed）**的包复制到
  export_items 快照表。此后无论缓冲区如何排水、committed 如何增长，
  同一任务用游标续传得到的页面字节与 manifest 摘要都保持稳定；
- 每个包序列化为一行规范化 JSON（与摄入时的规范化规则一致：
  sort_keys、ensure_ascii=False），页面即 NDJSON：
  行之间以 ``\\n`` 分隔且以 ``\\n`` 结尾；
- manifest 的 content_sha256 是**全部页面字节按序拼接**后的 SHA-256，
  客户端逐页下载并拼接响应体即可在本地复算校验；
- 导出全程只写 exports / export_items 两张导出专用表，绝不修改
  devices / committed / buffer / duplicates / alert_* 中的任何数据，
  也不驱动告警评估。
"""

import base64
import hashlib
import json
import sqlite3
from typing import List, Optional, Tuple

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000

_NDJSON_SEPARATOR = "\n"


class ExportError(ValueError):
    """导出范围非法（设备不存在已提交数据、seq 越界等）。"""


class CursorError(ValueError):
    """游标格式非法、归属其他任务或 offset 越界。"""


# ---------------------------------------------------------------------------
# 游标：不透明 base64url 字符串，载荷为 {"e": 任务id, "o": 页内起始位置}
# ---------------------------------------------------------------------------


def encode_cursor(export_id: int, offset: int) -> str:
    raw = json.dumps(
        {"e": export_id, "o": offset}, separators=(",", ":"), sort_keys=True
    )
    return base64.urlsafe_b64encode(raw.encode("utf-8")).rstrip(b"=").decode("ascii")


def decode_cursor(token: str) -> Tuple[int, int]:
    """解析游标，非法时抛 CursorError。返回 (export_id, offset)。"""
    try:
        padded = token + "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        obj = json.loads(raw)
        export_id = obj["e"]
        offset = obj["o"]
    except Exception as exc:
        raise CursorError("游标格式非法") from exc
    # bool 是 int 的子类，显式排除；只接受非负整数
    if (
        not isinstance(export_id, int)
        or isinstance(export_id, bool)
        or not isinstance(offset, int)
        or isinstance(offset, bool)
        or export_id < 1
        or offset < 0
    ):
        raise CursorError("游标格式非法")
    return export_id, offset


# ---------------------------------------------------------------------------
# 规范化
# ---------------------------------------------------------------------------


def canonical_line(payload_json: str) -> str:
    """把已存储的规范化 payload 重新确定性序列化为一行 NDJSON 文本（不含换行）。

    committed.payload_json 在摄入时已按 sort_keys + ensure_ascii=False
    规范化存储；这里反序列化后以完全相同的设置再序列化，作为防御性兜底，
    保证导出内容只取决于数据本身。
    """
    return json.dumps(
        json.loads(payload_json), sort_keys=True, ensure_ascii=False
    )


# ---------------------------------------------------------------------------
# 任务管理
# ---------------------------------------------------------------------------


def get_export(conn: sqlite3.Connection, export_id: int) -> Optional[sqlite3.Row]:
    """读取任务 manifest；不存在返回 None。"""
    return conn.execute(
        """
        SELECT id AS export_id, device_id, from_seq, to_seq, page_size,
               package_count, first_seq, last_seq,
               content_sha256, content_bytes, created_at
          FROM exports WHERE id = ?
        """,
        (export_id,),
    ).fetchone()


def create_export(
    conn: sqlite3.Connection,
    device_id: str,
    from_seq: int,
    to_seq: int,
    page_size: int,
) -> sqlite3.Row:
    """创建导出任务并固化快照。

    调用方需保证 from_seq/to_seq/page_size 已通过模型层校验。
    设备不存在抛 KeyError；范围超出已提交区间抛 ExportError。
    """
    if from_seq < 1 or to_seq < from_seq:
        raise ExportError(
            f"导出范围非法：from_seq={from_seq}, to_seq={to_seq}（需满足 1 <= "
            f"from_seq <= to_seq）"
        )
    dev = conn.execute(
        "SELECT 1 FROM devices WHERE device_id = ?", (device_id,)
    ).fetchone()
    if dev is None:
        raise KeyError(device_id)

    # committed 按排水规则连续写入、从不删除，已提交序号构成连续区间
    bounds = conn.execute(
        "SELECT MIN(seq) AS lo, MAX(seq) AS hi FROM committed WHERE device_id = ?",
        (device_id,),
    ).fetchone()
    lo, hi = bounds["lo"], bounds["hi"]
    if lo is None:
        raise ExportError(f"设备 {device_id} 尚无已提交数据，无法导出")
    if from_seq < lo or to_seq > hi:
        raise ExportError(
            f"导出序号范围 [{from_seq}, {to_seq}] 越界："
            f"设备 {device_id} 当前已提交范围为 [{lo}, {hi}]"
        )

    rows = conn.execute(
        """
        SELECT seq, payload_json
          FROM committed
         WHERE device_id = ? AND seq BETWEEN ? AND ?
         ORDER BY seq
        """,
        (device_id, from_seq, to_seq),
    ).fetchall()

    hasher = hashlib.sha256()
    content_bytes = 0
    items: List[Tuple[int, int, str]] = []
    for position, row in enumerate(rows):
        line = canonical_line(row["payload_json"]) + _NDJSON_SEPARATOR
        encoded = line.encode("utf-8")
        hasher.update(encoded)
        content_bytes += len(encoded)
        items.append((position, row["seq"], line[:-1]))

    package_count = len(items)
    expected_count = to_seq - from_seq + 1
    if package_count != expected_count:
        # 防御：理论上 committed 连续不会触发，触发说明数据被外部破坏
        raise ExportError(
            f"导出范围不完整：期望 {expected_count} 个包，实际已提交 {package_count} 个"
        )

    cur = conn.execute(
        """
        INSERT INTO exports
            (device_id, from_seq, to_seq, page_size, package_count,
             first_seq, last_seq, content_sha256, content_bytes)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            device_id,
            from_seq,
            to_seq,
            page_size,
            package_count,
            items[0][1],
            items[-1][1],
            hasher.hexdigest(),
            content_bytes,
        ),
    )
    export_id = cur.lastrowid
    conn.executemany(
        "INSERT INTO export_items (export_id, position, seq, payload_json)"
        " VALUES (?,?,?,?)",
        [
            (export_id, position, seq, payload_json)
            for position, seq, payload_json in items
        ],
    )
    return get_export(conn, export_id)


def fetch_page(
    conn: sqlite3.Connection,
    export_id: int,
    cursor: Optional[str],
    limit: Optional[int],
) -> Tuple[sqlite3.Row, bytes, int, int, Optional[str]]:
    """取一页 NDJSON。

    返回 (任务行, 页面字节, 本页起始 position, 本页包数, 下一页游标)。
    任务不存在抛 KeyError；游标非法 / 归属不符 / 越界抛 CursorError。
    cursor 为 None 时从首页（offset=0）开始；limit 为 None 时使用任务
    创建时固化的 page_size。
    """
    task = get_export(conn, export_id)
    if task is None:
        raise KeyError(export_id)

    if cursor is None:
        offset = 0
    else:
        cursor_export_id, offset = decode_cursor(cursor)
        if cursor_export_id != export_id:
            raise CursorError("游标不属于该导出任务")
        if offset >= task["package_count"]:
            raise CursorError(
                f"游标越界：offset={offset}，"
                f"该导出共 {task['package_count']} 个包（合法 offset 区间 [0, "
                f"{task['package_count'] - 1}]）"
            )

    page_limit = limit if limit is not None else task["page_size"]
    rows = conn.execute(
        """
        SELECT payload_json
          FROM export_items
         WHERE export_id = ?
         ORDER BY position
         LIMIT ? OFFSET ?
        """,
        (export_id, page_limit, offset),
    ).fetchall()

    body = "".join(
        row["payload_json"] + _NDJSON_SEPARATOR for row in rows
    ).encode("utf-8")
    page_count = len(rows)
    next_offset = offset + page_count
    next_cursor = (
        encode_cursor(export_id, next_offset)
        if next_offset < task["package_count"]
        else None
    )
    return task, body, offset, page_count, next_cursor


def verify_export(
    conn: sqlite3.Connection, export_id: int, submitted_sha256: str
) -> Tuple[sqlite3.Row, bool, str]:
    """比对客户端提交的内容摘要与 manifest 摘要。任务不存在抛 KeyError。

    返回 (任务行, 是否一致, 归一化后的提交摘要)。
    """
    task = get_export(conn, export_id)
    if task is None:
        raise KeyError(export_id)
    submitted = submitted_sha256.strip().lower()
    return task, submitted == task["content_sha256"], submitted
