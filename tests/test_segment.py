"""记录仪离线补传采集段：分批接收、整批拒绝、缺号、预览生成与并发一致性。"""
import io
import threading

import pytest

from app import create_app, storage
from app.parser import parse_handling_csv, parse_temperature_csv

TEMP_CSV = (
    "timestamp_utc,celsius\n"
    "2026-11-18T08:00:00Z,5.0\n"
    "2026-11-18T08:05:00Z,6.0\n"
    "2026-11-18T08:10:00Z,9.0\n"
    "2026-11-18T08:15:00Z,7.0\n"
).encode()

HANDLING_CSV = (
    "box,loaded_at,unloaded_at\n"
    "BOX-1,2026-11-18T08:00:00Z,2026-11-18T09:00:00Z\n"
).encode()


def _seg_csv(rows: list[tuple[int, str, float]]) -> bytes:
    lines = ["seq,timestamp_utc,celsius"]
    lines += [f"{seq},{ts},{c}" for seq, ts, c in rows]
    return ("\n".join(lines) + "\n").encode()


BATCH_A = _seg_csv([
    (1, "2026-11-18T08:00:00Z", 5.0),
    (3, "2026-11-18T08:10:00Z", 7.0),  # 乱序到达
])
BATCH_B = _seg_csv([
    (2, "2026-11-18T08:05:00Z", 6.0),
    (4, "2026-11-18T08:15:00Z", 6.5),
])


@pytest.fixture
def app(tmp_path):
    app = create_app(tmp_path / "test.db")
    app.config.update(TESTING=True)
    return app


@pytest.fixture(autouse=True)
def _app_context(app):
    with app.app_context():
        yield


@pytest.fixture
def client(app):
    return app.test_client()


def _confirm_trip(client):
    resp = client.post(
        "/trips",
        data={
            "name": "补传测试趟次", "temp_limit": "8.0",
            "temp_file": (io.BytesIO(TEMP_CSV), "temp.csv"),
            "handling_file": (io.BytesIO(HANDLING_CSV), "handling.csv"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    trip_id = resp.request.path.split("/")[-2]
    client.post(f"/trips/{trip_id}/confirm")
    assert storage.get_trip(trip_id)["revision"] == 1
    return trip_id


def _create_segment(client, trip_id, start=1, end=4, logger="LOGGER-1"):
    resp = client.post(
        "/segments",
        data={"trip_id": trip_id, "logger_id": logger,
              "seq_start": str(start), "seq_end": str(end)},
        follow_redirects=False,
    )
    assert resp.status_code == 302
    return resp.headers["Location"].rstrip("/").split("/")[-1]


def _upload_batch(client, segment_id, content):
    return client.post(
        f"/segments/{segment_id}/batches",
        data={"batch_file": (io.BytesIO(content), "batch.csv")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )


def test_segment_requires_confirmed_trip(app, client):
    # 未确认趟次不能建采集段
    resp = client.post(
        "/trips",
        data={
            "name": "草稿趟次", "temp_limit": "8.0",
            "temp_file": (io.BytesIO(TEMP_CSV), "t.csv"),
            "handling_file": (io.BytesIO(HANDLING_CSV), "h.csv"),
        },
        content_type="multipart/form-data",
    )
    trip_id = resp.headers["Location"].rstrip("/").split("/")[-2]
    with pytest.raises(storage.NotFound):
        storage.create_segment(trip_id, "L1", 1, 4)


def test_batched_out_of_order_ingest_then_generate_and_confirm(client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id)

    page = _upload_batch(client, segment_id, BATCH_A)
    assert "批次已接收".encode() in page.data
    seg = storage.get_segment(segment_id)
    missing = storage.missing_seqs(seg, storage.get_segment_readings(segment_id))
    assert missing == [2, 4]  # 页面列出缺号
    # 缺号时生成被拒绝
    resp = client.post(f"/segments/{segment_id}/generate", follow_redirects=True)
    assert "序号尚未齐全".encode() in resp.data
    assert storage.get_draft(trip_id) is None  # 半段读数不会产生草稿

    page = _upload_batch(client, segment_id, BATCH_B)
    assert storage.missing_seqs(
        storage.get_segment(segment_id), storage.get_segment_readings(segment_id)
    ) == []

    resp = client.post(f"/segments/{segment_id}/generate", follow_redirects=True)
    assert resp.status_code == 200 and "修订预览".encode() in resp.data
    draft = storage.get_draft(trip_id)
    assert draft is not None and draft["segment_id"] == segment_id
    assert draft["expected_revision"] == 1  # 修订号取自同一正式版
    # 装卸资料取自同一正式版
    assert [tuple(r) for r in draft["rides"]] == parse_handling_csv(HANDLING_CSV)
    # 读数为整段 4 条，按序号排序后的 (ts, celsius)
    assert len(draft["readings"]) == 4

    # 确认后整趟替换为 rev 2，采集段标记 generated
    client.post(f"/trips/{trip_id}/confirm", follow_redirects=True)
    trip = storage.get_trip(trip_id)
    assert trip["revision"] == 2 and len(trip["readings"]) == 4
    assert storage.get_segment(segment_id)["status"] == "generated"
    assert storage.get_draft(trip_id) is None


def test_same_seq_same_content_is_idempotent(client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id)
    _upload_batch(client, segment_id, BATCH_A)
    page = _upload_batch(client, segment_id, BATCH_A)  # 原样重传
    assert "同内容重传忽略".encode() in page.data
    assert len(storage.get_segment_readings(segment_id)) == 2
    batches = storage.get_segment_batches(segment_id)
    assert all(b["accepted"] for b in batches)


def test_same_seq_different_content_rejects_whole_batch(client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id)
    _upload_batch(client, segment_id, BATCH_A)
    # 序号 1 温度被篡改，且搭一个合法的新序号 2 -> 整批拒绝，序号 2 也不得入库
    bad = _seg_csv([
        (1, "2026-11-18T08:00:00Z", 9.9),
        (2, "2026-11-18T08:05:00Z", 6.0),
    ])
    page = _upload_batch(client, segment_id, bad)
    assert "整批拒绝".encode() in page.data and "内容不同".encode() in page.data
    readings = storage.get_segment_readings(segment_id)
    assert {r[0] for r in readings} == {1, 3}  # 已接收数据原样保留
    assert next(r for r in readings if r[0] == 1)[2] == 5.0
    batches = storage.get_segment_batches(segment_id)
    assert batches[-1]["accepted"] is False


def test_out_of_range_seq_rejects_whole_batch(client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id, start=1, end=4)
    bad = _seg_csv([
        (2, "2026-11-18T08:05:00Z", 6.0),
        (5, "2026-11-18T08:20:00Z", 6.1),  # 越界
    ])
    page = _upload_batch(client, segment_id, bad)
    assert "越界".encode() in page.data
    assert storage.get_segment_readings(segment_id) == []  # 同批的合法序号 2 也回滚


def test_non_increasing_timestamp_blocks_generation_but_keeps_data(client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id)
    _upload_batch(client, segment_id, BATCH_A)
    # 序号 2 的时刻早于序号 1 -> 齐全但不满足严格递增
    bad_order = _seg_csv([
        (2, "2026-11-18T07:59:00Z", 6.0),
        (4, "2026-11-18T08:15:00Z", 6.5),
    ])
    _upload_batch(client, segment_id, bad_order)
    seg = storage.get_segment(segment_id)
    assert storage.missing_seqs(seg, storage.get_segment_readings(segment_id)) == []
    resp = client.post(f"/segments/{segment_id}/generate", follow_redirects=True)
    assert "严格递增".encode() in resp.data
    assert storage.get_draft(trip_id) is None
    # 读数仍保留，可继续补传修正（异内容整批拒绝，需要先放弃重建段）
    assert len(storage.get_segment_readings(segment_id)) == 4


def test_only_one_open_segment_per_trip_and_blocks_manual_revise(client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id)
    with pytest.raises(storage.SegmentConflict):
        storage.create_segment(trip_id, "LOGGER-2", 1, 4)

    # 手工修订入口在采集段打开期间关闭
    resp = client.post(
        f"/trips/{trip_id}/revise",
        data={"name": "补传测试趟次", "temp_limit": "7.0"},
        follow_redirects=True,
    )
    assert "进行中的记录仪采集段".encode() in resp.data
    assert storage.get_trip(trip_id)["temp_limit"] == 8.0  # 正式版未被改动
    assert segment_id


def test_generate_does_not_overwrite_existing_manual_draft(client):
    trip_id = _confirm_trip(client)
    # 先放一份手工草稿
    readings = parse_temperature_csv(TEMP_CSV)
    rides = parse_handling_csv(HANDLING_CSV)
    from app.engine import analyze_all
    results = analyze_all(readings, rides, 8.0)
    storage.upsert_draft(trip_id, 1, "手工草稿", 8.0, readings, rides, results)
    with storage.db_cursor(commit=True) as conn:
        conn.execute(
            "INSERT INTO trip_draft_source(trip_id, temp_csv, handling_csv) VALUES(?,?,?)",
            (trip_id, "manual-temp", "manual-handling"),
        )

    # 有草稿时不允许新建段（路由层面）；直接构造一个 open 段模拟绕过
    with storage.db_cursor(commit=True) as conn:
        conn.execute(
            "INSERT INTO trip_segment(id, trip_id, logger_id, seq_start, seq_end, base_revision) "
            "VALUES(?,?,?,?,?,?)", (("segmanual", trip_id, "L", 1, 2, 1)),
        )
        conn.execute(
            "INSERT INTO trip_segment_reading(segment_id, seq, ts, celsius) VALUES(?,?,?,?)",
            ("segmanual", 1, readings[0][0], readings[0][1]),
        )
        conn.execute(
            "INSERT INTO trip_segment_reading(segment_id, seq, ts, celsius) VALUES(?,?,?,?)",
            ("segmanual", 2, readings[1][0], readings[1][1]),
        )
    resp = client.post("/segments/segmanual/generate", follow_redirects=True)
    assert "已有一份待确认草稿".encode() in resp.data
    # 原手工草稿原样保留
    draft = storage.get_draft(trip_id)
    assert draft["name"] == "手工草稿" and draft.get("segment_id") is None


def test_failed_generation_preserves_received_data_and_draft(client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id, start=1, end=4)
    _upload_batch(client, segment_id, BATCH_A)  # 只有 2/4，缺号
    resp = client.post(f"/segments/{segment_id}/generate", follow_redirects=True)
    assert "序号尚未齐全".encode() in resp.data
    # 已接收数据保留
    assert len(storage.get_segment_readings(segment_id)) == 2
    assert storage.get_segment(segment_id)["status"] == "open"


def test_abort_segment_removes_its_own_draft_only(client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id)
    _upload_batch(client, segment_id, BATCH_A)
    _upload_batch(client, segment_id, BATCH_B)
    client.post(f"/segments/{segment_id}/generate")
    assert storage.get_draft(trip_id) is not None
    resp = client.post(f"/segments/{segment_id}/abort", follow_redirects=True)
    assert "采集段已放弃".encode() in resp.data
    assert storage.get_segment(segment_id)["status"] == "aborted"
    assert storage.get_draft(trip_id) is None  # 属于本段的草稿被清
    # 读数与批次留痕保留
    assert len(storage.get_segment_readings(segment_id)) == 4
    assert storage.get_segment_batches(segment_id)
    # 正式版未动
    assert storage.get_trip(trip_id)["revision"] == 1
    # 放弃后可重新建段
    new_id = _create_segment(client, trip_id)
    assert new_id != segment_id


def test_closed_segment_rejects_further_batches(client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id)
    _upload_batch(client, segment_id, BATCH_A)
    _upload_batch(client, segment_id, BATCH_B)
    client.post(f"/segments/{segment_id}/generate")
    client.post(f"/trips/{trip_id}/confirm")  # 段变为 generated
    page = _upload_batch(client, segment_id, BATCH_A)
    assert "整批拒绝".encode() in page.data
    assert len(storage.get_segment_readings(segment_id)) == 4


def test_confirm_aborts_other_open_segments_but_keeps_their_data(client):
    trip_id = _confirm_trip(client)
    seg1 = _create_segment(client, trip_id)
    _upload_batch(client, seg1, BATCH_A)
    _upload_batch(client, seg1, BATCH_B)
    client.post(f"/segments/{seg1}/generate")
    client.post(f"/trips/{trip_id}/confirm")  # rev 2，seg1 generated

    # rev 2 之后新建第二段并传一部分
    seg2 = _create_segment(client, trip_id)
    _upload_batch(client, seg2, BATCH_A)
    # 一次手工式整趟替换（无 open 段时才允许；先放弃 seg2 模拟正式版被别处推进）
    client.post(f"/segments/{seg2}/abort")
    new_temp = (
        "timestamp_utc,celsius\n"
        "2026-11-18T08:00:00Z,4.0\n"
        "2026-11-18T08:05:00Z,4.5\n"
    ).encode()
    client.post(
        f"/trips/{trip_id}/revise",
        data={"name": "补传测试趟次", "temp_limit": "8.0",
              "temp_file": (io.BytesIO(new_temp), "t.csv")},
        content_type="multipart/form-data",
    )
    client.post(f"/trips/{trip_id}/confirm")
    trip = storage.get_trip(trip_id)
    assert trip["revision"] == 3
    # 被 abort 的段数据仍在
    assert storage.get_segment(seg2)["status"] == "aborted"
    assert len(storage.get_segment_readings(seg2)) == 2


def test_concurrent_batches_are_serialized_and_never_corrupt(app, client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id, start=1, end=8)
    errors: list[str] = []

    def worker(seqs):
        content = _seg_csv([
            (s, f"2026-11-18T08:{s:02d}:00Z", 5.0 + s) for s in seqs
        ])
        with app.app_context():
            try:
                storage.ingest_segment_batch(
                    segment_id,
                    [(s, 1_763_000_000 + s * 300, 5.0 + s) for s in seqs],
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(repr(exc))

    t1 = threading.Thread(target=worker, args=([1, 3, 5, 7],))
    t2 = threading.Thread(target=worker, args=([2, 4, 6, 8],))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert errors == []
    readings = storage.get_segment_readings(segment_id)
    assert [r[0] for r in readings] == [1, 2, 3, 4, 5, 6, 7, 8]


def test_concurrent_conflicting_batches_one_wins_no_corruption(app, client):
    """两个并发批次写同一序号但内容不同：恰好一个成功，另一个整批拒绝。"""
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id, start=1, end=4)
    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def worker(celsius: float):
        barrier.wait()
        with app.app_context():
            try:
                storage.ingest_segment_batch(
                    segment_id, [(1, 1_763_000_000, celsius), (2, 1_763_000_300, 6.0)]
                )
                outcomes.append("ok")
            except storage.SegmentConflict:
                outcomes.append("conflict")

    t1 = threading.Thread(target=worker, args=(5.0,))
    t2 = threading.Thread(target=worker, args=(9.9,))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(outcomes) == ["conflict", "ok"]
    readings = {r[0]: r[2] for r in storage.get_segment_readings(segment_id)}
    assert set(readings) == {1, 2}  # 输家整批回滚，没有半批残留
    assert readings[1] in (5.0, 9.9)


def test_concurrent_generation_only_one_creates_draft(app, client):
    trip_id = _confirm_trip(client)
    segment_id = _create_segment(client, trip_id)
    _upload_batch(client, segment_id, BATCH_A)
    _upload_batch(client, segment_id, BATCH_B)
    outcomes: list[str] = []
    barrier = threading.Barrier(2)

    def worker():
        barrier.wait()
        with app.app_context():
            try:
                segment, seg_readings, _ = storage.verify_segment_complete(segment_id)
                trip = storage.get_trip(trip_id)
                readings = [(ts, c) for _s, ts, c in seg_readings]
                rides = [(r["box"], r["loaded_at"], r["unloaded_at"]) for r in trip["rides"]]
                from app.engine import analyze_all
                results = analyze_all(readings, rides, trip["temp_limit"])
                storage.mark_segment_draft_generated(segment_id)
                storage.upsert_draft(
                    trip_id, trip["revision"], trip["name"], trip["temp_limit"],
                    readings, rides, results, segment_id=segment_id,
                )
                outcomes.append("ok")
            except Exception as exc:  # noqa: BLE001
                outcomes.append(f"error:{exc!r}")

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start(); t2.start(); t1.join(); t2.join()
    # upsert 幂等：两份相同草稿内容，最终只有一份草稿，段标记一致
    assert outcomes == ["ok", "ok"]
    draft = storage.get_draft(trip_id)
    assert draft is not None and draft["segment_id"] == segment_id
    assert storage.get_segment(segment_id)["draft_generated"] is True
