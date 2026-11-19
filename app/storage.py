"""SQLite 存储层。

并发/一致性设计：
- 每个 trip 有单调递增 revision；确认提交采用条件更新
  UPDATE trip SET revision=? WHERE id=? AND revision=?，配合 BEGIN IMMEDIATE
  写事务，同一 revision 的并发确认只有一个事务能成功（其余提交 0 行 -> 409）。
- 每次确认在单个事务内整趟替换：删除旧 readings/rides/results 后重插，
  任何一步失败全部回滚；查询永远只能看到完整旧版或完整新版。
- WAL 模式 + busy_timeout，兼顾读写并发与重启后的持久化。
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS trip (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    temp_limit      REAL NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    confirmed_at    TEXT
);
CREATE TABLE IF NOT EXISTS trip_reading (
    trip_id     TEXT NOT NULL,
    revision    INTEGER NOT NULL,
    ts          INTEGER NOT NULL,
    celsius     REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS trip_ride (
    trip_id     TEXT NOT NULL,
    revision    INTEGER NOT NULL,
    box         TEXT NOT NULL,
    loaded_at   INTEGER NOT NULL,
    unloaded_at INTEGER NOT NULL,
    PRIMARY KEY (trip_id, revision, box)
);
CREATE TABLE IF NOT EXISTS trip_result (
    trip_id         TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    box             TEXT NOT NULL,
    hot_seconds     INTEGER NOT NULL,
    unknown_seconds INTEGER NOT NULL,
    total_seconds   INTEGER NOT NULL,
    hot_segments    TEXT NOT NULL,
    unknown_segments TEXT NOT NULL,
    PRIMARY KEY (trip_id, revision, box)
);
CREATE INDEX IF NOT EXISTS idx_ride_trip ON trip_ride(trip_id, revision);
CREATE INDEX IF NOT EXISTS idx_result_trip ON trip_result(trip_id, revision);
CREATE TABLE IF NOT EXISTS trip_draft (
    trip_id TEXT PRIMARY KEY, expected_revision INTEGER NOT NULL,
    name TEXT NOT NULL, temp_limit REAL NOT NULL,
    readings TEXT NOT NULL, rides TEXT NOT NULL, results TEXT NOT NULL,
    segment_id TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE IF NOT EXISTS trip_draft_source (
    trip_id TEXT PRIMARY KEY, temp_csv TEXT NOT NULL, handling_csv TEXT NOT NULL
);
-- 记录仪离线补传的采集段：只允许挂在已确认趟次上，base_revision 固定为创建时的正式版
CREATE TABLE IF NOT EXISTS trip_segment (
    id              TEXT PRIMARY KEY,
    trip_id         TEXT NOT NULL,
    logger_id       TEXT NOT NULL,
    seq_start       INTEGER NOT NULL,
    seq_end         INTEGER NOT NULL,
    base_revision   INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',  -- open | generated | aborted
    draft_generated INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    CHECK (seq_end >= seq_start),
    CHECK (status IN ('open', 'generated', 'aborted'))
);
CREATE INDEX IF NOT EXISTS idx_segment_trip ON trip_segment(trip_id);
-- 已接收读数：(段, 序号) 唯一，重传同内容幂等、异内容整批拒绝
CREATE TABLE IF NOT EXISTS trip_segment_reading (
    segment_id  TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    ts          INTEGER NOT NULL,
    celsius     REAL NOT NULL,
    PRIMARY KEY (segment_id, seq)
);
-- 批次审计：每次上传（含被拒绝的批次）都留痕
CREATE TABLE IF NOT EXISTS trip_segment_batch (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_id  TEXT NOT NULL,
    accepted    INTEGER NOT NULL,
    row_count   INTEGER NOT NULL,
    message     TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_batch_segment ON trip_segment_batch(segment_id);
"""


class RevisionConflict(RuntimeError):
    """同一 revision 的确认已被其他事务抢先提交。"""


class SegmentConflict(RuntimeError):
    """采集段与趟次当前状态冲突（正式版已前进 / 草稿已被占用 / 段已关闭等）。"""


class NotFound(LookupError):
    pass


def get_db_path() -> Path:
    from flask import current_app

    return Path(current_app.config["DATABASE"])


def _connect(db_path: str | Path) -> sqlite3.Connection:
    # isolation_level=None：全程手动事务控制，避免 SELECT 隐式开启的事务
    # 让同一连接上的多条语句停留在旧快照，读到跨版本的混合数据
    conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(db_path: str | Path) -> None:
    conn = _connect(db_path)
    try:
        conn.executescript(SCHEMA)
        # 旧库迁移：trip_draft 补 segment_id 列（CREATE TABLE IF NOT EXISTS 不会改已有表）
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(trip_draft)")}
        if "segment_id" not in cols:
            conn.execute("ALTER TABLE trip_draft ADD COLUMN segment_id TEXT")
    finally:
        conn.close()


@contextmanager
def db_cursor(commit: bool = False):
    conn = _connect(get_db_path())
    try:
        if commit:
            # 写事务立即拿写锁，配合条件更新保证同修订号并发确认只成功一次
            conn.execute("BEGIN IMMEDIATE")
        else:
            # 显式只读事务：一次查询内的多条 SELECT 共享同一快照，
            # 绝不可能读到"新表头 + 旧明细"之类的跨版本混合状态
            conn.execute("BEGIN")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------- 草稿（待确认的修订预览） ----------

def upsert_draft(
    trip_id: str,
    expected_revision: int,
    name: str,
    temp_limit: float,
    readings: list[tuple[int, float]],
    rides: list[tuple[str, int, int]],
    results: dict[str, dict],
    segment_id: str | None = None,
) -> None:
    """保存草稿（同一 trip 反复修正只保留最新一份）。

    草稿带 expected_revision：确认时若 trip.revision 已前进则拒绝。
    segment_id 非空时表示该草稿由采集段补传生成（而非手工上传修订）。
    """
    with db_cursor(commit=True) as conn:
        conn.execute(
            "INSERT INTO trip_draft(trip_id, expected_revision, name, temp_limit, "
            "readings, rides, results, segment_id) VALUES(?,?,?,?,?,?,?,?) "
            "ON CONFLICT(trip_id) DO UPDATE SET expected_revision=excluded.expected_revision, "
            "name=excluded.name, temp_limit=excluded.temp_limit, readings=excluded.readings, "
            "rides=excluded.rides, results=excluded.results, segment_id=excluded.segment_id, "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')",
            (
                trip_id, expected_revision, name, temp_limit,
                json.dumps(readings, separators=(",", ":")),
                json.dumps(rides, separators=(",", ":")),
                json.dumps(results, separators=(",", ":")),
                segment_id,
            ),
        )


def get_draft(trip_id: str) -> dict | None:
    with db_cursor() as conn:
        row = conn.execute(
            "SELECT * FROM trip_draft WHERE trip_id=?", (trip_id,)
        ).fetchone()
    if row is None:
        return None
    return {
        "trip_id": row["trip_id"],
        "expected_revision": row["expected_revision"],
        "name": row["name"],
        "temp_limit": row["temp_limit"],
        "readings": json.loads(row["readings"]),
        "rides": json.loads(row["rides"]),
        "results": json.loads(row["results"]),
        "segment_id": row["segment_id"],
    }


def delete_draft(conn: sqlite3.Connection, trip_id: str) -> None:
    conn.execute("DELETE FROM trip_draft WHERE trip_id=?", (trip_id,))


# ---------- 正式版本（已确认结论，只增不改） ----------

def commit_revision(
    trip_id: str,
    expected_revision: int,
    name: str,
    temp_limit: float,
    readings: list[tuple[int, float]],
    rides: list[tuple[str, int, int]],
    results: dict[str, dict],
    segment_id: str | None = None,
) -> int:
    """整趟替换提交。返回新 revision；expected_revision 过期则抛 RevisionConflict。

    新 trip（expected_revision=0 且不存在）创建为 revision 1。
    若草稿由采集段生成（segment_id 非空），同事务内把该段标记为 generated；
    该趟其余仍打开的采集段因基准正式版失效而标记 aborted，其读数与批次留痕保留。
    """
    with db_cursor(commit=True) as conn:
        trip = conn.execute("SELECT revision FROM trip WHERE id=?", (trip_id,)).fetchone()
        current = trip["revision"] if trip else 0
        if current != expected_revision:
            raise RevisionConflict(
                f"修订号已过期：页面基于 rev {expected_revision}，当前为 rev {current}"
            )
        new_revision = current + 1

        if trip is None:
            conn.execute(
                "INSERT INTO trip(id, name, revision, temp_limit, confirmed_at) "
                "VALUES(?,?,?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
                (trip_id, name, new_revision, temp_limit),
            )
        else:
            updated = conn.execute(
                "UPDATE trip SET revision=?, name=?, temp_limit=?, "
                "confirmed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE id=? AND revision=?",
                (new_revision, name, temp_limit, trip_id, current),
            ).rowcount
            if updated != 1:  # 并发下另一事务已推进 revision
                raise RevisionConflict("同一修订号的并发确认只允许一次")
            # 整趟替换：先删旧数据（含已删除箱号的旧结论），再插新版
            for table in ("trip_reading", "trip_ride", "trip_result"):
                conn.execute(f"DELETE FROM {table} WHERE trip_id=?", (trip_id,))

        conn.executemany(
            "INSERT INTO trip_reading(trip_id, revision, ts, celsius) VALUES(?,?,?,?)",
            [(trip_id, new_revision, ts, c) for ts, c in readings],
        )
        conn.executemany(
            "INSERT INTO trip_ride(trip_id, revision, box, loaded_at, unloaded_at) "
            "VALUES(?,?,?,?,?)",
            [(trip_id, new_revision, box, lo, ul) for box, lo, ul in rides],
        )
        conn.executemany(
            "INSERT INTO trip_result(trip_id, revision, box, hot_seconds, "
            "unknown_seconds, total_seconds, hot_segments, unknown_segments) "
            "VALUES(?,?,?,?,?,?,?,?)",
            [
                (
                    trip_id, new_revision, box, r["hot_seconds"], r["unknown_seconds"],
                    r["total_seconds"],
                    json.dumps(r["hot_segments"], separators=(",", ":")),
                    json.dumps(r["unknown_segments"], separators=(",", ":")),
                )
                for box, r in results.items()
            ],
        )
        if segment_id is not None:
            conn.execute(
                "UPDATE trip_segment SET status='generated', updated_at="
                "strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=? AND trip_id=?",
                (segment_id, trip_id),
            )
        # 基准正式版被新版本替换：其余仍打开的采集段全部失效
        conn.execute(
            "UPDATE trip_segment SET status='aborted', updated_at="
            "strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE trip_id=? AND status='open'",
            (trip_id,),
        )
        delete_draft(conn, trip_id)
        return new_revision


def get_trip(trip_id: str) -> dict:
    with db_cursor() as conn:
        trip = conn.execute("SELECT * FROM trip WHERE id=?", (trip_id,)).fetchone()
        if trip is None:
            raise NotFound(f"运输趟次不存在：{trip_id}")
        revision = trip["revision"]
        rides = conn.execute(
            "SELECT box, loaded_at, unloaded_at FROM trip_ride "
            "WHERE trip_id=? AND revision=? ORDER BY box", (trip_id, revision)
        ).fetchall()
        results = conn.execute(
            "SELECT * FROM trip_result WHERE trip_id=? AND revision=? ORDER BY box",
            (trip_id, revision),
        ).fetchall()
        readings = conn.execute(
            "SELECT ts, celsius FROM trip_reading WHERE trip_id=? AND revision=? ORDER BY ts",
            (trip_id, revision),
        ).fetchall()
    return {
        "id": trip["id"],
        "name": trip["name"],
        "revision": revision,
        "temp_limit": trip["temp_limit"],
        "created_at": trip["created_at"],
        "confirmed_at": trip["confirmed_at"],
        "readings": [(r["ts"], r["celsius"]) for r in readings],
        "rides": [dict(r) for r in rides],
        "results": [
            {
                "box": r["box"],
                "hot_seconds": r["hot_seconds"],
                "unknown_seconds": r["unknown_seconds"],
                "total_seconds": r["total_seconds"],
                "hot_segments": json.loads(r["hot_segments"]),
                "unknown_segments": json.loads(r["unknown_segments"]),
            }
            for r in results
        ],
    }


def list_trips() -> list[dict]:
    with db_cursor() as conn:
        rows = conn.execute(
            "SELECT t.id, t.name, t.revision, t.temp_limit, t.confirmed_at, "
            "(SELECT COUNT(*) FROM trip_ride r WHERE r.trip_id=t.id AND r.revision=t.revision) AS boxes "
            "FROM trip t ORDER BY t.created_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


# ---------- 记录仪离线补传采集段 ----------

def create_segment(
    trip_id: str, logger_id: str, seq_start: int, seq_end: int
) -> dict:
    """在已确认趟次上创建采集段。

    - 趟次必须已确认，base_revision 固定为当前正式版修订号；
    - 同一趟次同时只允许一个 open 段（串行补传，避免半段数据互相干扰）；
    - 已存在待确认草稿时不得开新段（不能覆盖别人刚保存的草稿）。
    """
    segment_id = uuid.uuid4().hex[:12]
    with db_cursor(commit=True) as conn:
        trip = conn.execute("SELECT revision FROM trip WHERE id=?", (trip_id,)).fetchone()
        if trip is None:
            raise NotFound(f"运输趟次不存在或尚未确认：{trip_id}")
        open_seg = conn.execute(
            "SELECT id FROM trip_segment WHERE trip_id=? AND status='open'", (trip_id,)
        ).fetchone()
        if open_seg is not None:
            raise SegmentConflict("该趟次已有进行中的采集段，请先完成或放弃它")
        draft = conn.execute(
            "SELECT segment_id FROM trip_draft WHERE trip_id=?", (trip_id,)
        ).fetchone()
        if draft is not None:
            raise SegmentConflict("该趟次存在待确认的草稿，请先确认或放弃草稿再新建采集段")
        conn.execute(
            "INSERT INTO trip_segment(id, trip_id, logger_id, seq_start, seq_end, "
            "base_revision) VALUES(?,?,?,?,?,?)",
            (segment_id, trip_id, logger_id, seq_start, seq_end, trip["revision"]),
        )
    return get_segment(segment_id)


def _row_to_segment(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "trip_id": row["trip_id"],
        "logger_id": row["logger_id"],
        "seq_start": row["seq_start"],
        "seq_end": row["seq_end"],
        "base_revision": row["base_revision"],
        "status": row["status"],
        "draft_generated": bool(row["draft_generated"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def get_segment(segment_id: str) -> dict:
    with db_cursor() as conn:
        row = conn.execute(
            "SELECT * FROM trip_segment WHERE id=?", (segment_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"采集段不存在：{segment_id}")
        return _row_to_segment(row)


def list_segments(trip_id: str) -> list[dict]:
    with db_cursor() as conn:
        rows = conn.execute(
            "SELECT * FROM trip_segment WHERE trip_id=? ORDER BY created_at", (trip_id,)
        ).fetchall()
    return [_row_to_segment(r) for r in rows]


def get_segment_readings(segment_id: str) -> list[tuple[int, int, float]]:
    """已接收读数，按序号升序返回 (seq, ts, celsius)。"""
    with db_cursor() as conn:
        rows = conn.execute(
            "SELECT seq, ts, celsius FROM trip_segment_reading "
            "WHERE segment_id=? ORDER BY seq", (segment_id,)
        ).fetchall()
    return [(r["seq"], r["ts"], r["celsius"]) for r in rows]


def get_segment_batches(segment_id: str) -> list[dict]:
    with db_cursor() as conn:
        rows = conn.execute(
            "SELECT id, accepted, row_count, message, created_at "
            "FROM trip_segment_batch WHERE segment_id=? ORDER BY id", (segment_id,)
        ).fetchall()
    out = [dict(r) for r in rows]
    for r in out:
        r["accepted"] = bool(r["accepted"])
    return out


def missing_seqs(segment: dict, readings: list[tuple[int, int, float]]) -> list[int]:
    """计算缺口序号（页面列出缺号）。"""
    have = {r[0] for r in readings}
    return [s for s in range(segment["seq_start"], segment["seq_end"] + 1) if s not in have]


def _record_batch_outcome(
    segment_id: str, accepted: bool, row_count: int, message: str
) -> None:
    """批次留痕。拒绝批次的留痕必须在回滚之后单独事务写入，否则会随回滚丢失。"""
    with db_cursor(commit=True) as conn:
        conn.execute(
            "INSERT INTO trip_segment_batch(segment_id, accepted, row_count, message) "
            "VALUES(?,?,?,?)", (segment_id, 1 if accepted else 0, row_count, message),
        )


def ingest_segment_batch(segment_id: str, records: list[tuple[int, int, float]]) -> dict:
    """整批接收/拒绝一个补传批次，单事务原子完成。

    records 为 parse_segment_csv 的结果 (seq, ts, celsius)，允许乱序。
    规则：
    - 段必须仍为 open；基准正式版被后续确认替换后段自动 aborted，禁止再传；
    - 序号越界（不在 [seq_start, seq_end]）整批拒绝；
    - 同序号已有读数且内容不同（时刻或温度）整批拒绝；
    - 同序号同内容重传幂等（不计为新读数，批次仍记录）。
    任何拒绝都先回滚读数变更、再单独写入批次留痕，已接收数据原样保留。
    返回 {"accepted": bool, "message": str, "new_count": int, "duplicate_count": int}。
    """
    try:
        with db_cursor(commit=True) as conn:
            seg = conn.execute(
                "SELECT * FROM trip_segment WHERE id=?", (segment_id,)
            ).fetchone()
            if seg is None:
                raise NotFound(f"采集段不存在：{segment_id}")
            if seg["status"] != "open":
                reason = (
                    "已生成预览并关闭" if seg["status"] == "generated"
                    else "基准正式版已更新，采集段已失效"
                )
                raise SegmentConflict(
                    f"采集段当前状态为 {seg['status']}（{reason}），拒绝该批次"
                )

            lo, hi = seg["seq_start"], seg["seq_end"]
            # 批内自身先做完整校验，任何一条不合法都不动已接收数据
            existing = {
                r["seq"]: (r["ts"], r["celsius"])
                for r in conn.execute(
                    "SELECT seq, ts, celsius FROM trip_segment_reading WHERE segment_id=?",
                    (segment_id,),
                )
            }
            in_batch: dict[int, tuple[int, float]] = {}
            for seq, ts, celsius in records:
                if seq < lo or seq > hi:
                    raise SegmentConflict(
                        f"序号 {seq} 越界：预期范围 [{lo}, {hi}]，整批拒绝"
                    )
                if seq in existing:
                    old_ts, old_c = existing[seq]
                    if old_ts != ts or old_c != celsius:
                        raise SegmentConflict(
                            f"序号 {seq} 已存在且内容不同"
                            f"（已存：{old_ts}/{old_c}，本次：{ts}/{celsius}），整批拒绝"
                        )
                if seq in in_batch and in_batch[seq] != (ts, celsius):
                    raise SegmentConflict(
                        f"批次内序号 {seq} 出现互相冲突的内容，整批拒绝"
                    )
                in_batch[seq] = (ts, celsius)

            new_count = 0
            duplicate_count = 0
            for seq, (ts, celsius) in in_batch.items():
                if seq in existing:
                    duplicate_count += 1
                    continue
                conn.execute(
                    "INSERT INTO trip_segment_reading(segment_id, seq, ts, celsius) "
                    "VALUES(?,?,?,?)", (segment_id, seq, ts, celsius),
                )
                new_count += 1
            conn.execute(
                "UPDATE trip_segment SET updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE id=?", (segment_id,),
            )
            message = f"接收 {new_count} 条新读数，{duplicate_count} 条同内容重传忽略"
            conn.execute(
                "INSERT INTO trip_segment_batch(segment_id, accepted, row_count, message) "
                "VALUES(?,?,?,?)", (segment_id, 1, len(records), message),
            )
    except SegmentConflict as exc:
        # with 块已回滚全部读数变更；留痕在独立事务写入，确保拒绝可审计
        _record_batch_outcome(segment_id, False, len(records), str(exc))
        raise
    return {
        "accepted": True, "message": message,
        "new_count": new_count, "duplicate_count": duplicate_count,
    }


def verify_segment_complete(
    segment_id: str,
) -> tuple[dict, list[tuple[int, int, float]], list[int]]:
    """校验采集段是否可生成预览：段 open、序号齐全、时刻随序号严格递增。

    返回 (segment, readings, missing)；不满足时抛 SegmentConflict，
    消息区分缺号与时刻非严格递增，不修改任何数据。
    """
    with db_cursor() as conn:
        seg = conn.execute(
            "SELECT * FROM trip_segment WHERE id=?", (segment_id,)
        ).fetchone()
        if seg is None:
            raise NotFound(f"采集段不存在：{segment_id}")
        segment = _row_to_segment(seg)
        rows = conn.execute(
            "SELECT seq, ts, celsius FROM trip_segment_reading "
            "WHERE segment_id=? ORDER BY seq", (segment_id,),
        ).fetchall()
    readings = [(r["seq"], r["ts"], r["celsius"]) for r in rows]
    missing = [
        s for s in range(segment["seq_start"], segment["seq_end"] + 1)
        if s not in {r[0] for r in readings}
    ]
    if segment["status"] != "open":
        raise SegmentConflict(f"采集段已关闭（状态 {segment['status']}），不能重复生成预览")
    if missing:
        raise SegmentConflict(f"序号尚未齐全，缺少 {len(missing)} 个序号，不能生成预览")
    for (_, prev_ts, _), (seq, ts, _) in zip(readings, readings[1:]):
        if ts <= prev_ts:
            raise SegmentConflict(
                f"时刻未随序号严格递增：序号 {seq} 的时刻不晚于前一序号，不能生成预览"
            )
    return segment, readings, missing


def mark_segment_draft_generated(segment_id: str) -> None:
    """生成草稿成功后打标记（与草稿写入分属两个事务；失败不影响已接收读数）。"""
    with db_cursor(commit=True) as conn:
        updated = conn.execute(
            "UPDATE trip_segment SET draft_generated=1, "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=? AND status='open'",
            (segment_id,),
        ).rowcount
        if updated != 1:
            raise SegmentConflict("采集段已关闭，生成预览失败；原有草稿与已接收数据保留")


def abort_segment(segment_id: str) -> dict:
    """放弃采集段：open 段标记 aborted；若已生成待确认草稿则一并删除草稿。"""
    with db_cursor(commit=True) as conn:
        seg = conn.execute(
            "SELECT * FROM trip_segment WHERE id=?", (segment_id,)
        ).fetchone()
        if seg is None:
            raise NotFound(f"采集段不存在：{segment_id}")
        if seg["status"] != "open":
            raise SegmentConflict(f"采集段已关闭（状态 {seg['status']}），无需放弃")
        conn.execute(
            "UPDATE trip_segment SET status='aborted', "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (segment_id,),
        )
        # 仅清理属于本采集段的草稿，手工草稿不受影响
        conn.execute(
            "DELETE FROM trip_draft WHERE trip_id=? AND segment_id=?",
            (seg["trip_id"], segment_id),
        )
        # 草稿已删则同步清理其 CSV 原文旁路行；仍有（手工）草稿时保留
        remaining = conn.execute(
            "SELECT 1 FROM trip_draft WHERE trip_id=?", (seg["trip_id"],)
        ).fetchone()
        if remaining is None:
            conn.execute(
                "DELETE FROM trip_draft_source WHERE trip_id=?", (seg["trip_id"],)
            )
    return get_segment(segment_id)
