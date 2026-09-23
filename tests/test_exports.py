"""只读归档导出的测试：

- 创建任务返回 manifest（包数、首尾序号、规范化内容 SHA-256）；
- NDJSON 游标分页、续传内容稳定（换页大小也稳定）、越界 / 任务不存在明确报错；
- verify 接口校验完整性；
- 导出是快照：导出后继续摄入（提交、缓冲、重复、告警）不改变导出内容；
- 导出不改动 committed / buffer / duplicates / alert_* 任何数据。
"""

import hashlib
import json

import pytest
from fastapi.testclient import TestClient

from app import exports
from app.main import app


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


def pkg(device, seq, key, value=1.0, sent_at=None, unit="C"):
    body = {
        "device_id": device,
        "seq": seq,
        "idempotency_key": key,
        "measurements": [{"metric": "temperature", "value": value, "unit": unit}],
    }
    if sent_at is not None:
        body["sent_at"] = sent_at
    return body


def ingest_n(client, device, n, start=1, **over):
    for i in range(start, start + n):
        params = {"value": 20.0 + i}
        params.update(over)
        r = client.post(
            "/api/v1/packages",
            json=pkg(device, i, f"{device}-{i}", **params),
        )
        assert r.status_code == 200, r.text


def create_export(client, device, from_seq, to_seq, page_size=100):
    r = client.post(
        "/api/v1/exports",
        json={
            "device_id": device,
            "from_seq": from_seq,
            "to_seq": to_seq,
            "page_size": page_size,
        },
    )
    assert r.status_code == 201, r.text
    return r.json()


def walk_pages(client, export_id, first_cursor, limit=None):
    """从首页游标开始走完整个导出，返回 (拼接字节, 每页响应列表)。"""
    blob = b""
    pages = []
    cursor = first_cursor
    while True:
        params = {"cursor": cursor}
        if limit is not None:
            params["limit"] = limit
        r = client.get(f"/api/v1/exports/{export_id}/pages", params=params)
        assert r.status_code == 200, r.text
        pages.append(r)
        blob += r.content
        cursor = r.headers.get("x-next-cursor")
        if cursor is None:
            assert r.headers["x-has-more"] == "false"
            break
        assert r.headers["x-has-more"] == "true"
    return blob, pages


def parse_ndjson(body: bytes):
    assert body.endswith(b"\n")
    return [line for line in body.decode("utf-8").rstrip("\n").split("\n") if line]


# ---------------------------------------------------------------------------
# manifest 与分页
# ---------------------------------------------------------------------------


def test_create_export_manifest(client):
    ingest_n(client, "dev-m", 5)
    resp = create_export(client, "dev-m", 2, 4, page_size=2)
    man = resp["manifest"]
    assert man["device_id"] == "dev-m"
    assert man["from_seq"] == 2 and man["to_seq"] == 4
    assert man["package_count"] == 3
    assert man["first_seq"] == 2 and man["last_seq"] == 4
    assert man["page_size"] == 2
    assert len(man["content_sha256"]) == 64
    assert man["content_bytes"] > 0
    # 首页游标指向 offset 0
    export_id, offset = exports.decode_cursor(resp["first_cursor"])
    assert (export_id, offset) == (man["export_id"], 0)
    # manifest 可独立获取
    r = client.get(f"/api/v1/exports/{man['export_id']}/manifest")
    assert r.status_code == 200
    assert r.json() == man


def test_pages_are_ndjson_in_seq_order(client):
    ingest_n(client, "dev-p", 5, sent_at="2026-09-23T08:00:00Z")
    resp = create_export(client, "dev-p", 1, 5, page_size=2)
    eid = resp["manifest"]["export_id"]

    blob, pages = walk_pages(client, eid, resp["first_cursor"])
    # 5 个包 / 每页 2 个 -> 3 页（2,2,1）
    assert [int(p.headers["x-package-count"]) for p in pages] == [2, 2, 1]
    assert [int(p.headers["x-offset"]) for p in pages] == [0, 2, 4]
    assert all(p.headers["content-type"].startswith("application/x-ndjson") for p in pages)
    lines = parse_ndjson(blob)
    seqs = [json.loads(line)["seq"] for line in lines]
    assert seqs == [1, 2, 3, 4, 5]
    # 每页自身也是以换行结尾的合法 NDJSON 片段
    for p in pages:
        assert all(json.loads(line) for line in parse_ndjson(p.content))


def test_content_sha256_matches_concatenated_pages(client):
    ingest_n(client, "dev-h", 6)
    resp = create_export(client, "dev-h", 1, 6, page_size=2)
    man = resp["manifest"]

    # 用任务页大小走完
    blob, _ = walk_pages(client, man["export_id"], resp["first_cursor"])
    assert hashlib.sha256(blob).hexdigest() == man["content_sha256"]
    assert len(blob) == man["content_bytes"]

    # 用不同 limit 重新分页：页边界不同，但完整内容字节必须一致
    blob2, _ = walk_pages(client, man["export_id"], resp["first_cursor"], limit=5)
    assert blob2 == blob

    # 省略 cursor 也从首页开始
    r = client.get(f"/api/v1/exports/{man['export_id']}/pages")
    assert r.status_code == 200
    assert int(r.headers["x-offset"]) == 0


def test_resume_with_same_cursor_is_stable(client):
    ingest_n(client, "dev-s", 7)
    resp = create_export(client, "dev-s", 1, 7, page_size=3)
    eid = resp["manifest"]["export_id"]

    r1 = client.get(f"/api/v1/exports/{eid}/pages", params={"cursor": resp["first_cursor"]})
    r2 = client.get(f"/api/v1/exports/{eid}/pages", params={"cursor": resp["first_cursor"]})
    assert r1.content == r2.content  # 同一游标重复取，字节稳定
    mid = r1.headers["x-next-cursor"]
    r3 = client.get(f"/api/v1/exports/{eid}/pages", params={"cursor": mid})
    r4 = client.get(f"/api/v1/exports/{eid}/pages", params={"cursor": mid})
    assert r3.content == r4.content

    # 任意中断后续传都能拼出同一摘要
    blob = r1.content + r3.content
    cursor = r3.headers.get("x-next-cursor")
    while cursor is not None:
        r = client.get(f"/api/v1/exports/{eid}/pages", params={"cursor": cursor})
        blob += r.content
        cursor = r.headers.get("x-next-cursor")
    assert hashlib.sha256(blob).hexdigest() == resp["manifest"]["content_sha256"]


def test_snapshot_independent_of_later_ingest(client):
    ingest_n(client, "dev-x", 3)
    resp = create_export(client, "dev-x", 1, 3, page_size=2)
    man = resp["manifest"]
    eid = man["export_id"]
    blob_before, _ = walk_pages(client, eid, resp["first_cursor"])

    # 导出之后：正常提交新包
    ingest_n(client, "dev-x", 2, start=4)
    # 乱序包进缓冲区
    client.post(
        "/api/v1/packages", json=pkg("dev-x", 7, "dev-x-7", value=99.0)
    )
    # 重复重放
    client.post(
        "/api/v1/packages", json=pkg("dev-x", 1, "dev-x-1", value=21.0)
    )
    blob_after, pages = walk_pages(client, eid, resp["first_cursor"])
    assert blob_after == blob_before
    assert sum(int(p.headers["x-package-count"]) for p in pages) == 3
    # manifest 一字不改
    assert client.get(f"/api/v1/exports/{eid}/manifest").json() == man
    # 之后新建的导出可以覆盖更大的范围
    resp2 = create_export(client, "dev-x", 1, 5, page_size=10)
    assert resp2["manifest"]["package_count"] == 5
    assert resp2["manifest"]["export_id"] != eid


# ---------------------------------------------------------------------------
# 错误：范围越界、任务不存在、游标问题
# ---------------------------------------------------------------------------


def test_create_export_errors(client):
    ingest_n(client, "dev-e", 5)

    # 未知设备
    r = client.post(
        "/api/v1/exports",
        json={"device_id": "nope", "from_seq": 1, "to_seq": 1},
    )
    assert r.status_code == 404

    # 超出已提交范围：上界越界
    r = client.post(
        "/api/v1/exports",
        json={"device_id": "dev-e", "from_seq": 1, "to_seq": 6},
    )
    assert r.status_code == 422
    # 下界越界（首包自举为 seq=1，已提交范围 [1,5]）
    r = client.post(
        "/api/v1/exports",
        json={"device_id": "dev-e", "from_seq": 0, "to_seq": 5},
    )
    assert r.status_code == 422
    r = client.post(
        "/api/v1/exports",
        json={"device_id": "dev-e", "from_seq": 3, "to_seq": 2},
    )
    assert r.status_code == 422

    # 乱序自举设备：首包 seq=10，已提交范围就是 [10,10]
    client.post("/api/v1/packages", json=pkg("dev-boot", 10, "dev-boot-10"))
    r = client.post(
        "/api/v1/exports",
        json={"device_id": "dev-boot", "from_seq": 1, "to_seq": 10},
    )
    assert r.status_code == 422
    r = client.post(
        "/api/v1/exports",
        json={"device_id": "dev-boot", "from_seq": 10, "to_seq": 10},
    )
    assert r.status_code == 201


def test_task_not_found_errors(client):
    assert client.get("/api/v1/exports/9999/manifest").status_code == 404
    assert client.get("/api/v1/exports/9999/pages").status_code == 404
    r = client.post(
        "/api/v1/exports/9999/verify", json={"content_sha256": "a" * 64}
    )
    assert r.status_code == 404


def test_cursor_errors(client):
    ingest_n(client, "dev-c", 3)
    a = create_export(client, "dev-c", 1, 3, page_size=1)["manifest"]["export_id"]
    b = create_export(client, "dev-c", 1, 2, page_size=1)["manifest"]["export_id"]

    # 非法游标字符串
    r = client.get(f"/api/v1/exports/{a}/pages", params={"cursor": "@@@not-a-cursor"})
    assert r.status_code == 422
    # 游标属于另一个任务
    other_cursor = client.get(
        f"/api/v1/exports/{b}/pages"
    ).headers.get("x-next-cursor")
    r = client.get(f"/api/v1/exports/{a}/pages", params={"cursor": other_cursor})
    assert r.status_code == 422

    # offset == package_count：末页之后再续传 -> 明确越界错误
    end_cursor = exports.encode_cursor(a, 3)
    r = client.get(f"/api/v1/exports/{a}/pages", params={"cursor": end_cursor})
    assert r.status_code == 422
    assert "越界" in r.json()["detail"]
    # 负 offset 属于格式非法
    r = client.get(
        f"/api/v1/exports/{a}/pages",
        params={"cursor": exports.encode_cursor(a, -1)},
    )
    assert r.status_code == 422

    # limit 越界
    r = client.get(f"/api/v1/exports/{a}/pages", params={"limit": 0})
    assert r.status_code == 422
    r = client.get(f"/api/v1/exports/{a}/pages", params={"limit": 1001})
    assert r.status_code == 422

    # 任务不存在时，即使带游标也是 404
    r = client.get(
        "/api/v1/exports/9999/pages", params={"cursor": end_cursor}
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# manifest 校验接口
# ---------------------------------------------------------------------------


def test_verify_export_digest(client):
    ingest_n(client, "dev-v", 4)
    resp = create_export(client, "dev-v", 1, 4, page_size=3)
    man = resp["manifest"]
    blob, _ = walk_pages(client, man["export_id"], resp["first_cursor"])
    digest = hashlib.sha256(blob).hexdigest()

    r = client.post(
        f"/api/v1/exports/{man['export_id']}/verify",
        json={"content_sha256": digest},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["valid"] is True
    assert body["expected_sha256"] == man["content_sha256"]
    assert body["submitted_sha256"] == digest
    assert body["package_count"] == 4 and body["content_bytes"] == man["content_bytes"]

    # 大写 + 两侧空白也能匹配（服务端归一化）
    r = client.post(
        f"/api/v1/exports/{man['export_id']}/verify",
        json={"content_sha256": "  " + digest.upper() + " "},
    )
    assert r.json()["valid"] is True

    # 错误摘要 -> 200 但 valid=false
    wrong = "f" * 64
    r = client.post(
        f"/api/v1/exports/{man['export_id']}/verify",
        json={"content_sha256": wrong},
    )
    assert r.status_code == 200 and r.json()["valid"] is False

    # 摘要格式非法 -> 422
    r = client.post(
        f"/api/v1/exports/{man['export_id']}/verify",
        json={"content_sha256": "abc"},
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# 只读：不改变提交、缓冲、重复、告警数据
# ---------------------------------------------------------------------------


def test_export_is_read_only(client):
    # 构造：已提交 1..4，7 在缓冲区；有触发的告警规则；有重复记录
    client.post(
        "/api/v1/rules",
        json={
            "device_id": "dev-ro",
            "metric": "temperature",
            "upper_limit": 30.0,
            "trigger_count": 1,
            "recovery_count": 1,
        },
    )
    ingest_n(client, "dev-ro", 4, value=40.0)  # 全部越界，seq=1 即触发
    client.post("/api/v1/packages", json=pkg("dev-ro", 7, "dev-ro-7", value=21.0))
    client.post("/api/v1/packages", json=pkg("dev-ro", 1, "dev-ro-1", value=41.0))

    def snapshot():
        return {
            "devices": client.get("/api/v1/devices").json(),
            "gaps": client.get("/api/v1/devices/dev-ro/gaps").json(),
            "committed": client.get(
                "/api/v1/devices/dev-ro/committed"
            ).json(),
            "duplicates": client.get(
                "/api/v1/devices/dev-ro/duplicates"
            ).json(),
            "rules": client.get("/api/v1/rules").json(),
            "alerts": client.get("/api/v1/alerts").json(),
            "events": client.get(
                "/api/v1/devices/dev-ro/alerts/events"
            ).json(),
        }

    before = snapshot()
    resp = create_export(client, "dev-ro", 1, 4, page_size=2)
    walk_pages(client, resp["manifest"]["export_id"], resp["first_cursor"])
    # 同一游标重复取，模拟续传 / 重试
    client.get(
        f"/api/v1/exports/{resp['manifest']['export_id']}/pages",
        params={"cursor": resp["first_cursor"]},
    )
    client.post(
        f"/api/v1/exports/{resp['manifest']['export_id']}/verify",
        json={"content_sha256": resp["manifest"]["content_sha256"]},
    )
    assert snapshot() == before


def test_ndjson_is_canonical_and_unicode_preserved(client):
    # 非 ASCII 原样输出（ensure_ascii=False），规范化内容稳定
    r = client.post(
        "/api/v1/packages",
        json=pkg("dev-uni", 1, "dev-uni-1", value=21.5, unit="摄氏度℃"),
    )
    assert r.status_code == 200
    resp = create_export(client, "dev-uni", 1, 1)
    body = client.get(
        f"/api/v1/exports/{resp['manifest']['export_id']}/pages"
    ).content.decode("utf-8")
    assert "摄氏度℃" in body  # 未被转义成 \uXXXX
    line = parse_ndjson(body.encode("utf-8"))[0]
    obj = json.loads(line)
    assert obj["device_id"] == "dev-uni" and obj["seq"] == 1
    # 规范化：键排序 + 紧凑分隔符，重新序列化结果不变
    assert json.dumps(obj, sort_keys=True, ensure_ascii=False) == line
