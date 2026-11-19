"""冷链运输温度合规平台 —— Flask 应用工厂与路由。"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

from flask import (
    Flask, abort, flash, redirect, render_template, request, url_for,
)

from . import storage
from .engine import analyze_all
from .parser import (
    CSVError, parse_handling_csv, parse_segment_csv, parse_temperature_csv,
)


def create_app(db_path: str | Path | None = None) -> Flask:
    app = Flask(__name__)
    default_db = Path(__file__).resolve().parent.parent / "data" / "coldchain.db"
    app.config["DATABASE"] = str(db_path or os.environ.get("DATABASE") or default_db)
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024
    app.secret_key = "cold-chain-demo-secret-change-in-production"
    Path(app.config["DATABASE"]).parent.mkdir(parents=True, exist_ok=True)
    storage.init_db(app.config["DATABASE"])

    # ---------- 辅助 ----------

    def _read_upload(field: str) -> bytes | None:
        f = request.files.get(field)
        if f is None or f.filename == "":
            return None
        return f.read()

    def _build_draft(trip_id: str, expected_revision: int, name: str,
                     temp_limit: float, temp_bytes: bytes | None,
                     handling_bytes: bytes | None,
                     previous: dict | None) -> dict:
        """合并上传与上一版数据，计算预览。previous 为旧草稿或已确认版本。"""
        old_temp_csv = previous["temp_csv"] if previous else ""
        old_handling_csv = previous["handling_csv"] if previous else ""

        if temp_bytes is not None:
            readings = parse_temperature_csv(temp_bytes)
            temp_csv_text = temp_bytes.decode("utf-8-sig")
        else:
            if not previous:
                raise CSVError("请上传温度 CSV（UTC 时刻、摄氏度）")
            readings = previous["readings"]
            temp_csv_text = old_temp_csv

        if handling_bytes is not None:
            rides = parse_handling_csv(handling_bytes)
            handling_csv_text = handling_bytes.decode("utf-8-sig")
        else:
            if not previous:
                raise CSVError("请上传装卸 CSV（箱号、装车时刻、卸车时刻）")
            rides = [tuple(r) for r in previous["rides"]]
            handling_csv_text = old_handling_csv

        results = analyze_all(readings, rides, temp_limit)
        return {
            "trip_id": trip_id,
            "expected_revision": expected_revision,
            "name": name,
            "temp_limit": temp_limit,
            "readings": readings,
            "rides": rides,
            "results": results,
            "temp_csv": temp_csv_text,
            "handling_csv": handling_csv_text,
        }

    def _persist_draft(draft: dict) -> None:
        storage.upsert_draft(
            draft["trip_id"], draft["expected_revision"], draft["name"],
            draft["temp_limit"], draft["readings"], draft["rides"], draft["results"],
            segment_id=draft.get("segment_id"),
        )
        # CSV 原文存在旁路表，供再次修正时回填（采集段草稿无 CSV 原文，写空串占位）
        with storage.db_cursor(commit=True) as conn:
            conn.execute(
                "INSERT INTO trip_draft_source(trip_id, temp_csv, handling_csv) VALUES(?,?,?) "
                "ON CONFLICT(trip_id) DO UPDATE SET temp_csv=excluded.temp_csv, "
                "handling_csv=excluded.handling_csv",
                (draft["trip_id"], draft.get("temp_csv", ""), draft.get("handling_csv", "")),
            )

    def _load_full_draft(trip_id: str) -> dict | None:
        draft = storage.get_draft(trip_id)
        if draft is None:
            return None
        with storage.db_cursor() as conn:
            row = conn.execute(
                "SELECT temp_csv, handling_csv FROM trip_draft_source WHERE trip_id=?",
                (trip_id,),
            ).fetchone()
        if row:
            draft["temp_csv"] = row["temp_csv"]
            draft["handling_csv"] = row["handling_csv"]
        else:
            draft["temp_csv"], draft["handling_csv"] = "", ""
        return draft

    # ---------- 页面路由 ----------

    @app.route("/")
    def index():
        return render_template("index.html", trips=storage.list_trips())

    @app.route("/trips", methods=["POST"])
    def create_trip():
        trip_id = uuid.uuid4().hex[:12]
        name = request.form.get("name", "").strip() or f"运输趟次 {trip_id[:6]}"
        try:
            temp_limit = float(request.form.get("temp_limit", ""))
        except (TypeError, ValueError):
            flash("请输入有效的温度上限（数字，摄氏度）", "error")
            return redirect(url_for("index"))
        try:
            draft = _build_draft(
                trip_id, expected_revision=0, name=name, temp_limit=temp_limit,
                temp_bytes=_read_upload("temp_file"),
                handling_bytes=_read_upload("handling_file"),
                previous=None,
            )
        except CSVError as exc:
            flash(str(exc), "error")
            return redirect(url_for("index"))
        _persist_draft(draft)
        return redirect(url_for("preview", trip_id=trip_id))

    @app.route("/trips/<trip_id>/preview")
    def preview(trip_id: str):
        draft = _load_full_draft(trip_id)
        if draft is None:
            abort(404)
        confirmed = None
        try:
            confirmed = storage.get_trip(trip_id)
        except storage.NotFound:
            pass
        stale = confirmed is not None and confirmed["revision"] != draft["expected_revision"]
        return render_template(
            "preview.html", draft=draft, confirmed=confirmed, stale=stale,
            fmt=fmt_ts, fmt_duration=fmt_duration,
        )

    @app.route("/trips/<trip_id>/revise", methods=["POST"])
    def revise(trip_id: str):
        try:
            confirmed = storage.get_trip(trip_id)
        except storage.NotFound:
            confirmed = None
        current = _load_full_draft(trip_id)
        if current is None and confirmed is None:
            abort(404)
        # 补传与人工修订互斥：进行中的采集段未关闭前，手工修订入口保持关闭
        open_segments = [s for s in storage.list_segments(trip_id) if s["status"] == "open"]
        if open_segments:
            flash(
                "该趟次有进行中的记录仪采集段，补传完成前不能手工修订；"
                "请先在采集段页面完成或放弃采集段。",
                "error",
            )
            return redirect(url_for("trip_detail", trip_id=trip_id))
        # 基准修订号以已确认版本为准；草稿始终针对最新确认版
        if confirmed is not None:
            expected = confirmed["revision"]
            previous = {
                "readings": confirmed["readings"],
                "rides": [(r["box"], r["loaded_at"], r["unloaded_at"]) for r in confirmed["rides"]],
                "temp_csv": current["temp_csv"] if current else "",
                "handling_csv": current["handling_csv"] if current else "",
            }
        else:
            expected = 0
            previous = current

        name = request.form.get("name", "").strip() or (
            current["name"] if current else confirmed["name"]
        )
        fallback_limit = current["temp_limit"] if current else confirmed["temp_limit"]
        try:
            temp_limit = float(request.form.get("temp_limit", fallback_limit))
        except (TypeError, ValueError):
            flash("温度上限不是有效数字", "error")
            return redirect(url_for("preview", trip_id=trip_id))
        try:
            draft = _build_draft(
                trip_id, expected_revision=expected, name=name, temp_limit=temp_limit,
                temp_bytes=_read_upload("temp_file"),
                handling_bytes=_read_upload("handling_file"),
                previous=previous,
            )
        except CSVError as exc:
            flash(str(exc), "error")
            return redirect(url_for("preview", trip_id=trip_id))
        _persist_draft(draft)
        flash("已按修正内容重新计算预览，尚未写入正式结论", "info")
        return redirect(url_for("preview", trip_id=trip_id))

    @app.route("/trips/<trip_id>/confirm", methods=["POST"])
    def confirm(trip_id: str):
        draft = storage.get_draft(trip_id)
        if draft is None:
            abort(404)
        try:
            new_rev = storage.commit_revision(
                trip_id, draft["expected_revision"], draft["name"], draft["temp_limit"],
                draft["readings"], [tuple(r) for r in draft["rides"]], draft["results"],
                segment_id=draft.get("segment_id"),
            )
        except storage.RevisionConflict as exc:
            # 失败后原草稿与采集段已接收数据都保留，用户可基于最新版本重来
            flash(f"确认失败：{exc}。请基于最新版本重新修正。", "error")
            if draft.get("segment_id"):
                return redirect(url_for("segment_page", segment_id=draft["segment_id"]))
            return redirect(url_for("preview", trip_id=trip_id))
        with storage.db_cursor(commit=True) as conn:
            conn.execute("DELETE FROM trip_draft_source WHERE trip_id=?", (trip_id,))
        flash(f"已确认，正式版本号 rev {new_rev}（整趟替换，旧结论已清除）", "success")
        return redirect(url_for("trip_detail", trip_id=trip_id))

    @app.route("/trips/<trip_id>")
    def trip_detail(trip_id: str):
        trip = storage.get_trip(trip_id)
        has_draft = storage.get_draft(trip_id) is not None
        segments = storage.list_segments(trip_id)
        for seg in segments:
            seg["total"] = seg["seq_end"] - seg["seq_start"] + 1
            seg["received"] = len(storage.get_segment_readings(seg["id"]))
        open_segment = next((s for s in segments if s["status"] == "open"), None)
        return render_template(
            "detail.html", trip=trip, has_draft=has_draft,
            segments=segments, open_segment=open_segment,
            fmt=fmt_ts, fmt_duration=fmt_duration,
        )

    @app.route("/trips/<trip_id>/api/results")
    def api_results(trip_id: str):
        trip = storage.get_trip(trip_id)
        return app.response_class(
            json.dumps(trip, ensure_ascii=False, indent=2),
            mimetype="application/json",
        )

    # ---------- 记录仪离线补传采集段 ----------

    @app.route("/segments", methods=["POST"])
    def create_segment_route():
        trip_id = request.form.get("trip_id", "").strip()
        logger_id = request.form.get("logger_id", "").strip()
        if not trip_id:
            flash("请选择一个已确认趟次", "error")
            return redirect(url_for("index"))
        if not logger_id:
            flash("请填写记录仪编号", "error")
            return redirect(url_for("trip_detail", trip_id=trip_id))
        try:
            seq_start = int(request.form.get("seq_start", ""))
            seq_end = int(request.form.get("seq_end", ""))
        except (TypeError, ValueError):
            flash("起止序号必须是整数", "error")
            return redirect(url_for("trip_detail", trip_id=trip_id))
        if seq_start < 0 or seq_end < seq_start:
            flash("预期序号范围非法：要求 0 ≤ 起始序号 ≤ 结束序号", "error")
            return redirect(url_for("trip_detail", trip_id=trip_id))
        try:
            segment = storage.create_segment(trip_id, logger_id, seq_start, seq_end)
        except storage.NotFound:
            abort(404)
        except storage.SegmentConflict as exc:
            flash(f"无法新建采集段：{exc}", "error")
            return redirect(url_for("trip_detail", trip_id=trip_id))
        flash(
            f"采集段已建立（记录仪 {logger_id}，预期序号 {seq_start}–{seq_end}，"
            f"绑定正式版 rev {segment['base_revision']}），可分批补传 CSV。",
            "success",
        )
        return redirect(url_for("segment_page", segment_id=segment["id"]))

    @app.route("/segments/<segment_id>")
    def segment_page(segment_id: str):
        try:
            segment = storage.get_segment(segment_id)
        except storage.NotFound:
            abort(404)
        readings = storage.get_segment_readings(segment_id)
        missing = storage.missing_seqs(segment, readings)
        batches = storage.get_segment_batches(segment_id)
        trip = storage.get_trip(segment["trip_id"])
        draft = storage.get_draft(segment["trip_id"])
        # 只展示属于本采集段的草稿
        segment_draft = draft if draft and draft.get("segment_id") == segment_id else None
        stale = (
            segment_draft is not None
            and trip["revision"] != segment_draft["expected_revision"]
        )
        return render_template(
            "segment.html",
            segment=segment, trip=trip, readings=readings, missing=missing,
            batches=batches, draft=segment_draft, stale=stale,
            fmt=fmt_ts, fmt_duration=fmt_duration,
        )

    @app.route("/segments/<segment_id>/batches", methods=["POST"])
    def upload_segment_batch(segment_id: str):
        try:
            segment = storage.get_segment(segment_id)
        except storage.NotFound:
            abort(404)
        content = _read_upload("batch_file")
        if content is None:
            flash("请选择补传 CSV 文件（列：seq, timestamp_utc, celsius）", "error")
            return redirect(url_for("segment_page", segment_id=segment_id))
        try:
            # 先完整解析，解析失败不写任何批次留痕、不动已接收数据
            records = parse_segment_csv(content)
            outcome = storage.ingest_segment_batch(segment_id, records)
        except CSVError as exc:
            flash(f"批次被拒绝（解析失败）：{exc}。已接收数据保持不变。", "error")
            return redirect(url_for("segment_page", segment_id=segment_id))
        except storage.SegmentConflict as exc:
            flash(f"批次被整批拒绝：{exc}。已接收数据保持不变。", "error")
            return redirect(url_for("segment_page", segment_id=segment_id))
        flash(
            f"批次已接收：{outcome['message']}；当前缺号 {len(storage.missing_seqs(segment, storage.get_segment_readings(segment_id)))} 个。",
            "success",
        )
        return redirect(url_for("segment_page", segment_id=segment_id))

    @app.route("/segments/<segment_id>/generate", methods=["POST"])
    def generate_segment_preview(segment_id: str):
        try:
            segment, seg_readings, _missing = storage.verify_segment_complete(segment_id)
        except storage.NotFound:
            abort(404)
        except storage.SegmentConflict as exc:
            flash(f"无法生成预览：{exc}。已接收数据与现有草稿均保留。", "error")
            return redirect(url_for("segment_page", segment_id=segment_id))
        trip_id = segment["trip_id"]
        # 不能覆盖别人刚保存的草稿（手工草稿或其它段的草稿）
        existing = storage.get_draft(trip_id)
        if existing is not None and existing.get("segment_id") != segment_id:
            flash(
                "无法生成预览：该趟次已有一份待确认草稿（人工修订或其他采集段），"
                "请先确认或放弃它；本采集段已接收数据完整保留。",
                "error",
            )
            return redirect(url_for("segment_page", segment_id=segment_id))
        # 装卸资料与修订号取自同一正式版：基准 rev 已被后续确认替换则拒绝
        try:
            trip = storage.get_trip(trip_id)
        except storage.NotFound:
            abort(404)
        if trip["revision"] != segment["base_revision"]:
            flash(
                f"无法生成预览：采集段绑定 rev {segment['base_revision']}，"
                f"当前正式版已是 rev {trip['revision']}，本采集段已失效。",
                "error",
            )
            return redirect(url_for("segment_page", segment_id=segment_id))
        readings = [(ts, celsius) for _seq, ts, celsius in seg_readings]
        rides = [(r["box"], r["loaded_at"], r["unloaded_at"]) for r in trip["rides"]]
        results = analyze_all(readings, rides, trip["temp_limit"])
        draft = {
            "trip_id": trip_id,
            "expected_revision": trip["revision"],
            "name": trip["name"],
            "temp_limit": trip["temp_limit"],
            "readings": readings,
            "rides": rides,
            "results": results,
            "segment_id": segment_id,
            "temp_csv": "",
            "handling_csv": "",
        }
        try:
            # 先打段标记再写草稿；任一步失败都不覆盖已有草稿、已接收读数原样保留
            storage.mark_segment_draft_generated(segment_id)
            _persist_draft(draft)
        except storage.SegmentConflict as exc:
            flash(f"生成预览失败：{exc}", "error")
            return redirect(url_for("segment_page", segment_id=segment_id))
        flash("序号齐全且时刻严格递增，已基于该段读数生成待确认温度预览（装卸资料沿用同一正式版）。", "success")
        return redirect(url_for("preview", trip_id=trip_id))

    @app.route("/segments/<segment_id>/abort", methods=["POST"])
    def abort_segment_route(segment_id: str):
        try:
            segment = storage.abort_segment(segment_id)
        except storage.NotFound:
            abort(404)
        except storage.SegmentConflict as exc:
            flash(f"放弃失败：{exc}", "error")
            return redirect(url_for("segment_page", segment_id=segment_id))
        flash("采集段已放弃，其读数不再参与任何确认；趟次正式版未改动。", "info")
        return redirect(url_for("trip_detail", trip_id=segment["trip_id"]))

    @app.errorhandler(storage.NotFound)
    def not_found_exc(_):
        return render_template("404.html"), 404

    @app.errorhandler(404)
    def not_found(_):
        return render_template("404.html"), 404

    @app.template_filter("fmt")
    def fmt_ts(epoch: int) -> str:
        return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    @app.template_filter("duration")
    def fmt_duration(seconds: int) -> str:
        sign = "-" if seconds < 0 else ""
        seconds = abs(seconds)
        h, rem = divmod(seconds, 3600)
        m, s = divmod(rem, 60)
        return f"{sign}{h:d}:{m:02d}:{s:02d}"

    return app
