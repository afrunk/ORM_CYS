from __future__ import annotations

from datetime import datetime, timedelta, timezone
from collections import defaultdict
import re

from flask import Blueprint, g, render_template, request
from sqlalchemy import func, case, or_, and_

from ..extensions import db
from ..models import CONVERSION_STATUS_LABELS, Customer, Region, User
from ..permissions import login_required, roles_required
from ..utils.timewindow import get_shift_window_utc, get_yesterday_window_utc

stats_bp = Blueprint("stats", __name__, template_folder="../templates")


@stats_bp.route("/", methods=["GET"])
@login_required
@roles_required(["super_admin", "data_entry"])
def stats_index():
    """数据统计首页。"""
    start = request.args.get("start")
    end = request.args.get("end")
    preset = request.args.get("preset")

    start_dt = None
    end_dt = None

    def _date_floor(dt: datetime) -> datetime:
        return datetime.combine(dt.date(), datetime.min.time())

    def _date_ceiling(dt: datetime) -> datetime:
        return datetime.combine(dt.date(), datetime.max.time())

    def _to_utc_naive(dt: datetime | None) -> datetime | None:
        """
        将用户输入的时间转为 UTC 的 naive 时间。
        - 如果没有 tz 信息，按北京时区解析，再减 8 小时得到 UTC。
        - 如果有 tz 信息，转为 UTC 并去掉 tzinfo。
        """
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt - timedelta(hours=8)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    now = datetime.utcnow()
    
    # 如果指定了快捷 preset，则优先按快捷时间计算（忽略手动时间输入）
    if preset:
        if preset == "today":
            # "今天"按正常24小时制统计：北京时间 00:00:00 ~ 23:59:59
            start_dt, end_dt = get_shift_window_utc(now_utc=now)
        elif preset == "yesterday":
            start_dt, end_dt = get_yesterday_window_utc(now_utc=now)
        elif preset == "7d":
            # 近7天：从6天前的00:00到现在的23:59:59（北京时区）
            beijing_now = now + timedelta(hours=8)
            start_local = _date_floor(beijing_now - timedelta(days=6))
            end_local = _date_ceiling(beijing_now)
            start_dt = start_local - timedelta(hours=8)
            end_dt = end_local - timedelta(hours=8)
        elif preset == "30d":
            # 近30天：从29天前的00:00到现在的23:59:59（北京时区）
            beijing_now = now + timedelta(hours=8)
            start_local = _date_floor(beijing_now - timedelta(days=29))
            end_local = _date_ceiling(beijing_now)
            start_dt = start_local - timedelta(hours=8)
            end_dt = end_local - timedelta(hours=8)
    else:
        # 如果没有 preset，则解析手动输入的时间
        try:
            if start:
                start_dt_raw = datetime.fromisoformat(start)
                start_dt = _to_utc_naive(start_dt_raw)
            if end:
                end_dt_raw = datetime.fromisoformat(end)
                # 如果只有日期和时间，没有秒，添加秒
                if len(end) == 16:  # YYYY-MM-DDTHH:mm
                    end_dt_raw = end_dt_raw.replace(second=59)
                else:
                    # 如果已经有秒，确保是59秒
                    end_dt_raw = end_dt_raw.replace(second=59)
                end_dt = _to_utc_naive(end_dt_raw)
        except ValueError:
            start_dt = end_dt = None

    # 如果完全没有起止时间和 preset，则默认使用"当天窗口"
    if not start_dt and not end_dt and not preset:
        start_dt, end_dt = get_shift_window_utc(now_utc=now)

    filters_created = []
    filters_accepted = []
    filters_dispatched = []

    current = g.current_user

    # 单实例部署，所有数据共用

    if start_dt:
        filters_created.append(Customer.created_at >= start_dt)
        filters_accepted.append(Customer.accepted_time >= start_dt)
        filters_dispatched.append(Customer.dispatch_time >= start_dt)
    if end_dt:
        filters_created.append(Customer.created_at <= end_dt)
        filters_accepted.append(Customer.accepted_time <= end_dt)
        filters_dispatched.append(Customer.dispatch_time <= end_dt)

    # 录入统计（按 creator_id）
    # 同时统计该运营录入的有效/无效订单数（基于 Customer.is_valid 字段）
    data_entry_stats = (
        db.session.query(
            User.id,
            User.username,
            func.count(Customer.id).label("count"),
            func.sum(
                case((Customer.is_valid.is_(True), 1), else_=0)
            ).label("valid_count"),
            func.sum(
                case((Customer.is_valid.is_(False), 1), else_=0)
            ).label("invalid_count"),
        )
        .join(Customer, Customer.creator_id == User.id)
        .filter(*filters_created)
        .group_by(User.id, User.username)
        .all()
    )

    # 销售接单统计
    sales_accept_stats = (
        db.session.query(
            User.id,
            User.username,
            func.count(Customer.id).label("accepted_count"),
            func.sum(case((Customer.is_valid.is_(True), 1), else_=0)).label("valid_count"),
            func.sum(case((Customer.is_valid.is_(False), 1), else_=0)).label("invalid_count"),
        )
        .join(Customer, Customer.sales_id == User.id)
        .filter(Customer.status == "accepted", *filters_accepted)
        .group_by(User.id, User.username)
        .all()
    )

    # 销售转化统计
    sales_conversion_stats = (
        db.session.query(
            User.id,
            User.username,
            func.count(Customer.id).label("converted_count"),
        )
        .join(Customer, Customer.sales_id == User.id)
        .filter(Customer.is_converted.is_(True), *filters_accepted)
        .group_by(User.id, User.username)
        .all()
    )

    def _extract_timeout_sales(remark: str | None) -> list[str]:
        """从备注中提取所有未接单的销售名称。"""
        if not remark:
            return []
        names: list[str] = []
        for segment in re.findall(r"未接单销售:\s*([^\n]+)", remark):
            for raw in re.split(r"[，,]", segment):
                name = raw.strip()
                if name:
                    names.append(name)
        return names

    timeout_threshold = now - timedelta(minutes=5)

    time_filters = []
    if start_dt:
        time_filters.append(
            or_(
                and_(Customer.dispatch_time.isnot(None), Customer.dispatch_time >= start_dt),
                and_(Customer.dispatch_time.is_(None), Customer.created_at >= start_dt),
            )
        )
    if end_dt:
        time_filters.append(
            or_(
                and_(Customer.dispatch_time.isnot(None), Customer.dispatch_time <= end_dt),
                and_(Customer.dispatch_time.is_(None), Customer.created_at <= end_dt),
            )
        )

    timeout_candidates = Customer.query
    if time_filters:
        timeout_candidates = timeout_candidates.filter(*time_filters)
    timeout_candidates = timeout_candidates.all()

    users = User.query.all()
    user_id_to_name = {u.id: u.username for u in users}
    user_name_to_id = {u.username: u.id for u in users}

    timeout_by_user: dict[int, int] = defaultdict(int)

    for customer in timeout_candidates:
        names = _extract_timeout_sales(customer.remark)

        # 当前仍在该销售手上且已超时，也算入（防止尚未写入 remark 的场景）
        reference_ts = customer.dispatch_time or customer.created_at
        if (
            customer.status in ("pending", "timeout")
            and reference_ts
            and reference_ts <= timeout_threshold
            and customer.sales_id
            and customer.sales_id in user_id_to_name
        ):
            names.append(user_id_to_name[customer.sales_id])

        if not names:
            continue

        for name in set(names):  # 去重，避免同一客户多次计入同一销售
            uid = user_name_to_id.get(name)
            if uid:
                timeout_by_user[uid] += 1

    # 汇总转换为便于模板展示的结构
    sales_map = {}
    for row in sales_accept_stats:
        sales_map[row.id] = {
            "username": row.username,
            "accepted": row.accepted_count,
            "valid": row.valid_count or 0,
            "invalid": row.invalid_count or 0,
            "converted": 0,
            "timeout": 0,
            "rate": "-",
        }
    for row in sales_conversion_stats:
        s = sales_map.setdefault(
            row.id,
            {"username": row.username, "accepted": 0, "converted": 0, "timeout": 0, "rate": "-"},
        )
        s["converted"] = row.converted_count
        if s["accepted"]:
            s["rate"] = f"{(s['converted'] / s['accepted'] * 100):.1f}%"
    for uid, count in timeout_by_user.items():
        s = sales_map.setdefault(
            uid,
            {
                "username": user_id_to_name.get(uid, "未知"),
                "accepted": 0,
                "converted": 0,
                "timeout": 0,
                "rate": "-",
            },
        )
        s["timeout"] += count

    # 运营派单统计（按 dispatcher_id），统计所有有派单记录的用户（包括 operator、super_admin、data_entry）
    operator_stats_query = (
        db.session.query(
            User.id,
            User.username,
            User.role,
            func.count(Customer.id).label("dispatch_count"),
            func.sum(
                case((Customer.is_valid.is_(True), 1), else_=0)
            ).label("valid_count"),
            func.sum(
                case((Customer.is_valid.is_(False), 1), else_=0)
            ).label("invalid_count"),
        )
        .join(Customer, Customer.dispatcher_id == User.id)
        .filter(
            User.role.in_(["operator", "super_admin", "data_entry"]),  # 统计运营、超管、数据员的派单
            *filters_dispatched
        )
        .group_by(User.id, User.username, User.role)
        .all()
    )

    entry_rows = [
        {
            "id": row.id,
            "username": row.username,
            "count": row.count,
            "valid_count": row.valid_count or 0,
            "invalid_count": row.invalid_count or 0,
        }
        for row in data_entry_stats
    ]

    sales_rows = sorted(
        (
            {
                "id": sid,
                "username": payload["username"],
                "accepted": payload["accepted"],
                "valid": payload.get("valid", 0),
                "invalid": payload.get("invalid", 0),
                "converted": payload["converted"],
                "timeout": payload["timeout"],
                "rate": payload["rate"],
            }
            for sid, payload in sales_map.items()
        ),
        key=lambda row: row["accepted"],
        reverse=True,
    )

    operator_rows = []
    for row in operator_stats_query:
        dispatch_count = row.dispatch_count or 0
        valid_count = row.valid_count or 0
        invalid_count = row.invalid_count or 0
        rate = f"{(valid_count / dispatch_count * 100):.1f}%" if dispatch_count else "-"
        operator_rows.append(
            {
                "id": row.id,
                "username": row.username,
                "dispatch_count": dispatch_count,
                "valid_count": valid_count,
                "invalid_count": invalid_count,
                "valid_rate": rate,
            }
        )
    operator_total_dispatch = sum(row["dispatch_count"] for row in operator_rows)

    total_created = sum(row["count"] for row in entry_rows)
    total_accepted = sum(row["accepted"] for row in sales_rows)
    total_converted = sum(row["converted"] for row in sales_rows)
    pending = max(total_created - total_accepted, 0)
    
    timeout_count = sum(timeout_by_user.values())

    # 地区统计：按客户地区统计数量与转化情况（基于派单时间窗口 filters_dispatched）
    region_stats_query = (
        db.session.query(
            Customer.region.label("region"),
            func.count(Customer.id).label("total"),
            func.count(func.nullif(Customer.is_converted.is_(True), False)).label(
                "converted"
            ),
        )
        .filter(*filters_dispatched)
        .group_by(Customer.region)
        .order_by(func.count(Customer.id).desc())
        .all()
    )

    region_rows = []
    for row in region_stats_query:
        region_name = row.region or "未填写地区"
        total = row.total or 0
        converted = row.converted or 0
        rate = f"{(converted / total * 100):.1f}%" if total else "-"
        region_rows.append(
            {
                "region": region_name,
                "total": total,
                "converted": converted,
                "rate": rate,
            }
        )

    summary = {
        "total_created": total_created,
        "total_accepted": total_accepted,
        "pending": pending,
        "timeout": timeout_count,
        "conversion_rate": (
            f"{(total_converted / total_accepted * 100):.1f}%"
            if total_accepted
            else "-"
        ),
    }

    return render_template(
        "stats/stats.html",
        data_entry_stats=entry_rows,
        sales_stats=sales_rows,
        operator_stats=operator_rows,
        operator_total_dispatch=operator_total_dispatch,
        summary=summary,
        region_stats=region_rows,
        start=start,
        end=end,
        preset=preset,
        current_user_role=current.role,
    )



@stats_bp.route("/operator/<int:user_id>", methods=["GET"])
@login_required
@roles_required(["super_admin", "data_entry"])
def operator_detail(user_id):
    """
    显示指定运营在当前统计时间窗口（同 stats_index 的时间判定：以北京时间 00:00 为起始）
    的录入明细（仅当天窗口 / 或按查询参数 start/end/preset）。
    """
    start = request.args.get("start")
    end = request.args.get("end")
    preset = request.args.get("preset")

    start_dt = None
    end_dt = None

    def _date_floor(dt: datetime) -> datetime:
        return datetime.combine(dt.date(), datetime.min.time())

    def _date_ceiling(dt: datetime) -> datetime:
        return datetime.combine(dt.date(), datetime.max.time())

    def _to_utc_naive(dt: datetime | None) -> datetime | None:
        if dt is None:
            return None
        if dt.tzinfo is None:
            return dt - timedelta(hours=8)
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

    now = datetime.utcnow()

    if preset:
        if preset == "today":
            start_dt, end_dt = get_shift_window_utc(now_utc=now)
        elif preset == "yesterday":
            start_dt, end_dt = get_yesterday_window_utc(now_utc=now)
        elif preset == "7d":
            beijing_now = now + timedelta(hours=8)
            start_local = _date_floor(beijing_now - timedelta(days=6))
            end_local = _date_ceiling(beijing_now)
            start_dt = start_local - timedelta(hours=8)
            end_dt = end_local - timedelta(hours=8)
        elif preset == "30d":
            beijing_now = now + timedelta(hours=8)
            start_local = _date_floor(beijing_now - timedelta(days=29))
            end_local = _date_ceiling(beijing_now)
            start_dt = start_local - timedelta(hours=8)
            end_dt = end_local - timedelta(hours=8)
    else:
        try:
            if start:
                start_dt_raw = datetime.fromisoformat(start)
                start_dt = _to_utc_naive(start_dt_raw)
            if end:
                end_dt_raw = datetime.fromisoformat(end)
                if len(end) == 16:
                    end_dt_raw = end_dt_raw.replace(second=59)
                else:
                    end_dt_raw = end_dt_raw.replace(second=59)
                end_dt = _to_utc_naive(end_dt_raw)
        except ValueError:
            start_dt = end_dt = None

    # 默认使用当天窗口
    if not start_dt and not end_dt and not preset:
        start_dt, end_dt = get_shift_window_utc(now_utc=now)

    # 查询该运营在窗口内的录入客户
    filters = [Customer.creator_id == user_id]
    if start_dt:
        filters.append(Customer.created_at >= start_dt)
    if end_dt:
        filters.append(Customer.created_at <= end_dt)

    rows = Customer.query.filter(*filters).order_by(Customer.created_at.desc()).all()

    # 收集接单人的名字映射
    sales_ids = {c.sales_id for c in rows if c.sales_id}
    sales_map = {}
    if sales_ids:
        users = User.query.filter(User.id.in_(sales_ids)).all()
        sales_map = {u.id: u.username for u in users}

    detail_rows = []
    for c in rows:
        beijing_time = (c.created_at + timedelta(hours=8)) if c.created_at else None
        created_local = beijing_time.strftime("%Y-%m-%d %H:%M:%S") if beijing_time else "-"
        cds = c.conversion_display_status()
        conv_label = CONVERSION_STATUS_LABELS.get(cds, cds)
        detail_rows.append(
            {
                "id": c.id,
                "region": c.region or "未填写地区",
                "created_at": created_local,
                "is_converted": bool(c.is_converted),
                "conversion_label": conv_label,
                "sales": sales_map.get(c.sales_id, "-") if c.sales_id else "-",
            }
        )

    # 按地区汇总该运营在时间窗口内的数量与转化数
    region_stats_query = (
        db.session.query(
            Customer.region.label("region"),
            func.count(Customer.id).label("total"),
            func.sum(case((Customer.is_converted.is_(True), 1), else_=0)).label("converted"),
        )
        .filter(*filters)
        .group_by(Customer.region)
        .order_by(func.count(Customer.id).desc())
        .all()
    )

    region_rows = []
    for row in region_stats_query:
        region_name = row.region or "未填写地区"
        total = int(row.total or 0)
        converted = int(row.converted or 0)
        rate = f"{(converted / total * 100):.1f}%" if total else "-"
        region_rows.append({"region": region_name, "total": total, "converted": converted, "rate": rate})

    operator_user = User.query.get(user_id)
    operator_name = operator_user.username if operator_user else f"用户 {user_id}"

    return render_template(
        "stats/operator_detail.html",
        operator_id=user_id,
        operator_name=operator_name,
        rows=detail_rows,
        total=len(detail_rows),
        region_stats=region_rows,
        start=start,
        end=end,
        preset=preset,
    )


