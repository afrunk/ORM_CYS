from __future__ import annotations

from datetime import datetime, timedelta, timezone

import os
import re
import unicodedata
from pathlib import Path
from flask import (
    Blueprint,
    flash,
    g,
    redirect,
    render_template,
    request,
    url_for,
    current_app,
    jsonify,
)
from sqlalchemy import func, or_, and_
from werkzeug.utils import secure_filename

from ..extensions import db
from ..models import (
    CONVERSION_STATUS_URGE_ADD,
    Customer,
    Notification,
    Region,
    SalesProfile,
    User,
)
from ..notifications import send_assignment_notification
from ..permissions import login_required
from ..utils.images import ensure_thumbnail, ensure_preview, remove_preview, remove_thumbnail
from ..utils.timewindow import get_shift_window_utc, get_yesterday_window_utc

customer_bp = Blueprint("customer", __name__, template_folder="../templates")


def _list_summary_flags(user: User) -> tuple[bool, bool, bool]:
    """客户列表顶区统计卡片：(超管/数据员块, 运营块, 销售仅接单数).

    角色字符串做 NFKC + 去空白 + 小写，避免库里有不可见字符导致模板分支失效。
    若 role 未识别但存在 sales_profile，按销售展示（仅顶区卡片）。
    """
    raw = getattr(user, "role", None) or ""
    raw = unicodedata.normalize("NFKC", str(raw))
    role_key = "".join(raw.split()).lower()

    if role_key in ("super_admin", "data_entry"):
        return True, False, False
    if role_key == "operator":
        return False, True, False
    if role_key == "sales":
        return False, False, True
    if getattr(user, "sales_profile", None) is not None:
        return False, False, True
    return False, False, False


def _collect_failed_sales_names(remark: str) -> list[str]:
    """从历史备注中解析所有未接单销售的姓名，去重后返回列表。"""
    if not remark:
        return []

    names: list[str] = []

    # 1）找出所有 "未接单销售: xxx" 段，按中/英文逗号拆分
    for segment in re.findall(r"未接单销售:\s*([^\n]+)", remark):
        for raw_name in re.split(r"[，,]", segment):
            name = raw_name.strip()
            if name and name not in names:
                names.append(name)

    # 2）找出所有 "派单给 xxx" 的记录，补充进列表（避免遗漏首次派单）
    for assign_name in re.findall(r"派单给\s+([^\s,\n]+)", remark):
        assign_name = assign_name.strip()
        if assign_name and assign_name not in names:
            names.append(assign_name)

    return names


@customer_bp.route("/sales-by-region")
@login_required
def get_sales_by_region():
    """根据地区获取销售列表（API）。"""
    region_name = request.args.get("region", "").strip()
    
    if not region_name:
        return jsonify({
            "success": True,
            "sales": []
        })
    
    # 查询该地区的销售（服务地区匹配且可用）
    # 使用 outerjoin 以防某些销售没有 SalesProfile
    sales = (
        User.query.join(SalesProfile, SalesProfile.user_id == User.id)
        .filter(
            User.role == "sales",
            User.is_active == True,
            SalesProfile.service_region == region_name,
            SalesProfile.is_available == True
        )
        .order_by(SalesProfile.dispatch_order.asc(), User.id.asc())
        .all()
    )
    
    # 计算每个销售的转化比
    sales_list = []
    for s in sales:
        # 统计接单数和转化数
        total_accepted = Customer.query.filter(
            Customer.sales_id == s.id,
            Customer.status == "accepted"
        ).count()
        total_converted = Customer.query.filter(
            Customer.sales_id == s.id,
            Customer.is_converted.is_(True)
        ).count()
        
        # 计算转化率
        if total_accepted > 0:
            conversion_rate = f"{(total_converted / total_accepted * 100):.1f}%"
        else:
            conversion_rate = "-"
        
        sales_list.append({
            "id": s.id,
            "username": s.username,
            "conversion_rate": conversion_rate,
            "accepted": total_accepted,
            "converted": total_converted,
        })
    
    return jsonify({
        "success": True,
        "sales": sales_list
    })


def _get_sales(include_unavailable: bool = False):
    """按派单序号获取所有销售列表。"""
    query = (
        User.query.join(SalesProfile, SalesProfile.user_id == User.id)
        .filter(
            User.role == "sales",
            User.is_active.is_(True),
        )
    )
    if not include_unavailable:
        query = query.filter(SalesProfile.is_available.is_(True))
    
    return query.order_by(SalesProfile.dispatch_order.asc(), User.id.asc()).all()


def _static_asset_exists(rel_path: str | None) -> bool:
    """Check whether a static asset exists on disk."""
    if not rel_path:
        return False
    rel_path_clean = rel_path.replace("/", os.sep)
    static_folder = current_app.static_folder
    candidate = Path(static_folder) / rel_path_clean
    return candidate.exists()


def _auto_assign_sales(region: str | None = None, exclude_sales_id: int | None = None) -> User | None:
    """自动循环派单给下一个销售。
    
    派单策略（循环分配）：
    1. 如果指定了地区，只在该地区的销售中选择
    2. 按 dispatch_order 排序，循环分配
    3. 查询该地区最近一次自动派单的客户，从下一个 dispatch_order 开始
    4. 只分配给可用状态（is_available=True）的销售
    5. 排除指定销售（用于超时重派时避免重复分配给同一人）
    6. 如果循环完所有销售都没有可用销售，返回 None（放入公海）
    
    Args:
        region: 客户所在地区，如果指定则只在该地区的销售中选择
        exclude_sales_id: 要排除的销售ID（用于超时重派）
    """
    # 安全保护：如果没有地区信息，则不进行跨区自动派单
    # 调用方应将此类客户直接放入公海，由人工处理
    if not region:
        return None

    sales_users = _get_sales(include_unavailable=False)
    if not sales_users:
        return None
    
    # 只选择服务地区与客户地区完全匹配的销售，禁止跨区派单
    sales_users = [
        s
        for s in sales_users
        if s.sales_profile and s.sales_profile.service_region == region
    ]
    if not sales_users:
        return None
    
    # 排除指定销售
    if exclude_sales_id:
        sales_users = [s for s in sales_users if s.id != exclude_sales_id]
        if not sales_users:
            return None
    
    # 按 dispatch_order 排序
    sales_users.sort(key=lambda s: (s.sales_profile.dispatch_order if s.sales_profile else 0, s.id))
    
    # 获取所有 dispatch_order 值（去重并排序）
    dispatch_orders = sorted(set(
        s.sales_profile.dispatch_order if s.sales_profile else 0 
        for s in sales_users
    ))
    
    if not dispatch_orders:
        return None
    
    # 查找该地区最近一次自动派单的客户（dispatcher_id 为 None 表示自动派单）
    # 确定从哪个 dispatch_order 开始
    # 注意：查询所有状态的客户（包括已接单的），因为：
    # - 如果销售1接了单，下一个单子应该派给销售2
    # - 如果销售1没接单（pending），下一个单子也应该派给销售2（因为销售1已经有待接单的）
    start_order = None
    if region:
        # 查询该地区最近一次自动派单的客户（不管状态，包括已接单的）
        # 这样可以确保：如果销售1接了单，下一个单子会派给销售2
        last_auto_customer = (
            Customer.query
            .filter(
                Customer.region == region,
                Customer.dispatcher_id.is_(None),  # 自动派单（dispatcher_id 为 None）
                Customer.sales_id.isnot(None)
            )
            .order_by(Customer.dispatch_time.desc(), Customer.id.desc())
            .first()
        )
        
        if last_auto_customer and last_auto_customer.sales_id:
            last_sales = User.query.get(last_auto_customer.sales_id)
            if last_sales and last_sales.sales_profile:
                last_order = last_sales.sales_profile.dispatch_order
                # 找到下一个 dispatch_order（循环到下一个销售）
                try:
                    current_index = dispatch_orders.index(last_order)
                    # 从下一个开始（如果销售1接了单，下一个就是销售2）
                    if current_index + 1 < len(dispatch_orders):
                        start_order = dispatch_orders[current_index + 1]
                    else:
                        # 如果已经是最后一个，从第一个开始（循环）
                        start_order = dispatch_orders[0]
                except ValueError:
                    # 如果找不到，从第一个开始
                    start_order = dispatch_orders[0]
    
    # 如果没有找到起始点，从第一个开始
    if start_order is None:
        start_order = dispatch_orders[0]
    
    # 从 start_order 开始循环
    start_index = dispatch_orders.index(start_order)
    # 循环两轮，确保能遍历所有销售
    for round_offset in range(len(dispatch_orders) * 2):
        order_index = (start_index + round_offset) % len(dispatch_orders)
        current_order = dispatch_orders[order_index]
        
        # 获取该 dispatch_order 的所有销售
        candidates = [
            s for s in sales_users 
            if (s.sales_profile.dispatch_order if s.sales_profile else 0) == current_order
        ]
        
        # 在相同 dispatch_order 的销售中，选择第一个（因为已经排序了）
        if candidates:
            return candidates[0]
    
    # 如果循环完都没有找到，返回 None
    return None



def _assign_public_pool_to_sales(sales_user: User, limit: int | None = None) -> int:
    """将公海客户分配给指定销售。
    
    Args:
        sales_user: 当前上线的销售
        limit: 最多分配数量（None 表示不限）
    """
    query = Customer.query.filter(Customer.status == "public_pool")

    # 如果销售有配置的服务地区，则只从公海中分配该地区的客户（严格避免跨地域派单）
    profile = getattr(sales_user, "sales_profile", None)
    service_region = getattr(profile, "service_region", None) if profile else None
    if service_region:
        query = query.filter(
            Customer.region == service_region
        )

    query = query.order_by(Customer.dispatch_time.asc(), Customer.id.asc())

    customers = query.limit(limit).all() if limit else query.all()
    if not customers:
        return 0

    now = datetime.utcnow()
    assigned = 0

    for customer in customers:
        # 再次校验地区匹配，防御性保护
        if customer.region and customer.region == service_region:
            customer.sales_id = sales_user.id
            customer.dispatcher_id = None  # 系统自动派单
            customer.dispatch_time = now
            customer.status = "pending"
            customer.retry_count = 0
            _prepend_remark(
                customer,
                f"[系统] 校验匹配：客户地区({customer.region}) == 销售地区({service_region})，销售 {sales_user.username} 上线自动领取公海客户。",
            )
            send_assignment_notification(sales_user, customer)
            assigned += 1
        else:
            # 理论上不会发生，如有异常则保持其在公海，避免跨区误派
            _prepend_remark(
                customer,
                "[系统] 检测到公海自动分配时地区不匹配，已跳过自动分配，等待人工处理。",
            )

    return assigned


def run_auto_dispatch_unassigned(single_customer_id: int | None = None) -> int:
    """对所有（或指定）未分配客户按地区和派单序号进行系统派单。

    规则：
    - 仅处理 status='unassigned' 且 sales_id 为空的客户
    - 按地区分组；每个地区内按 dispatch_order 轮询，每轮每个销售最多 1 单
    - 同一地区内优先把新单派给「最久没有接过单」的销售，保证轮询公平
    - 某地区没有任何可用销售时，该地区客户进入公海（status='public_pool'）
    """
    now = datetime.utcnow()

    base_query = Customer.query.filter(
        Customer.sales_id.is_(None),
        Customer.status == "unassigned",
    )
    if single_customer_id is not None:
        base_query = base_query.filter(Customer.id == single_customer_id)

    unassigned_customers = base_query.order_by(Customer.region.asc(), Customer.id.asc()).all()
    if not unassigned_customers:
        return 0

    # 按地区分组
    region_map: dict[str | None, list[Customer]] = {}
    for c in unassigned_customers:
        region_map.setdefault(c.region, []).append(c)

    assigned_count = 0

    for region, customers in region_map.items():
        # 业务约束：客户必须有明确地区，且只能派给同地区销售
        # - 如果 region 为空 / None：禁止跨区默认派单，直接放入公海
        # - 如果该地区没有在线销售：也直接放入公海
        if not region:
            for c in customers:
                c.status = "public_pool"
                c.dispatch_time = now
                _prepend_remark(
                    c,
                    "[系统] 该地区暂无可匹配销售（客户地区为空），已进入公海，等待销售自助领取。",
                )
            continue

        # 找到该地区所有可用销售（严格按 service_region 匹配）
        sales_users = _get_sales(include_unavailable=False)
        sales_users = [
            s
            for s in sales_users
            if s.sales_profile and s.sales_profile.service_region == region
        ]

        if not sales_users:
            # 没有可用销售：该地区所有未分配客户直接进入公海
            for c in customers:
                c.status = "public_pool"
                c.dispatch_time = now
                _prepend_remark(
                    c,
                    "[系统] 该地区暂无可匹配销售，已进入公海，等待销售自助领取。",
                )
            continue

        # 计算该地区内每个销售最近一次派单时间，用于确定轮询起点
        last_time_map: dict[int, datetime] = {}
        for s in sales_users:
            q = Customer.query.filter(Customer.sales_id == s.id)
            if region:
                q = q.filter(Customer.region == region)
            last_customer = (
                q.order_by(Customer.dispatch_time.desc(), Customer.id.desc())
                .first()
            )
            if last_customer and last_customer.dispatch_time:
                last_time_map[s.id] = last_customer.dispatch_time
            else:
                # 从未派过单的销售优先级最高
                last_time_map[s.id] = datetime.min

        # 轮询顺序：
        # 1) 最近一次派单时间最早的优先（最久没接单）
        # 2) 同时刻按 dispatch_order
        # 3) 再按用户 ID 保证稳定性
        sales_users.sort(
            key=lambda s: (
                last_time_map.get(s.id, datetime.min),
                s.sales_profile.dispatch_order if s.sales_profile else 0,
                s.id,
            )
        )

        # 每轮每个销售最多 1 单；有剩余客户则继续下一轮
        remaining = list(customers)
        while remaining:
            for sales_user in sales_users:
                if not remaining:
                    break

                customer = remaining.pop(0)
                # 再次做一次地区安全校验（防御性编程）
                sales_region = (
                    sales_user.sales_profile.service_region
                    if sales_user.sales_profile
                    else None
                )
                if customer.region and customer.region == sales_region:
                    customer.sales_id = sales_user.id
                    customer.dispatcher_id = None  # 系统派单
                    customer.dispatch_time = now
                    customer.status = "pending"
                    customer.retry_count = customer.retry_count or 0
                    _prepend_remark(
                        customer,
                        f"[系统] 校验匹配：客户地区({customer.region}) == 销售地区({sales_region})，执行系统派单给 {sales_user.username}",
                    )
                    send_assignment_notification(sales_user, customer)
                    assigned_count += 1
                else:
                    # 理论上不会到这里，如出现则直接进入公海，避免跨区误派
                    customer.status = "public_pool"
                    customer.dispatch_time = now
                    _prepend_remark(
                        customer,
                        "[系统] 检测到地区不匹配，已将客户转入公海等待人工处理。",
                    )

    db.session.commit()
    return assigned_count


def _apply_customer_filters(query, current_user: User):
    """根据角色和查询参数对客户列表进行过滤。"""

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

    if current_user.role == "operator":
        query = query.filter(Customer.creator_id == current_user.id)
    elif current_user.role == "sales":
        query = query.filter(Customer.sales_id == current_user.id)

    # 状态筛选
    status = request.args.get("status", "").strip()
    if status:
        query = query.filter(Customer.status == status)

    # 地区筛选
    region = request.args.get("region", "").strip()
    if region:
        query = query.filter(Customer.region == region)

    # 转化/有效筛选
    is_converted = request.args.get("is_converted")
    if is_converted == "true":
        query = query.filter(Customer.is_converted.is_(True))
    elif is_converted == "false":
        query = query.filter(
            or_(Customer.is_converted.is_(False), Customer.is_converted.is_(None))
        )

    is_valid = request.args.get("is_valid")
    if is_valid == "true":
        query = query.filter(Customer.is_valid.is_(True))
    elif is_valid == "false":
        query = query.filter(
            Customer.is_valid.is_(False),
            or_(Customer.conversion_status != CONVERSION_STATUS_URGE_ADD, Customer.conversion_status.is_(None))
        )
    elif is_valid == "urge_add":
        query = query.filter(Customer.conversion_status == CONVERSION_STATUS_URGE_ADD)

    # 二级快速筛选：仅超时订单
    only_timeout = request.args.get("only_timeout")
    if only_timeout and only_timeout.lower() in ("1", "true", "yes"):
        query = query.filter(Customer.status == "timeout")

    # 通用搜索：订单序号或微信号（phone 字段）
    q = request.args.get("q", "").strip()
    if q:
        conds = []
        if q.isdigit():
            try:
                conds.append(Customer.id == int(q))
            except Exception:
                pass
        # 模糊匹配 phone 或 name
        conds.append(Customer.phone.ilike(f"%{q}%"))
        conds.append(Customer.name.ilike(f"%{q}%"))
        query = query.filter(or_(*conds))

    # 时间范围筛选（按派单时间）
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    start = request.args.get("start", "").strip()  # 兼容旧格式
    end = request.args.get("end", "").strip()
    preset = request.args.get("preset", "").strip()

    now = datetime.utcnow()
    start_dt = None
    end_dt = None

    # 如果指定了快捷 preset，则优先按快捷时间计算（忽略手动时间输入）
    if preset:
        if preset == "today":
            # "今天"按正常24小时制统计：北京时间 00:00:00 ~ 23:59:59
            start_dt, end_dt = get_shift_window_utc(now_utc=now)
        elif preset == "yesterday":
            start_dt, end_dt = get_yesterday_window_utc(now)
        elif preset == "7d":
            start_dt = now - timedelta(days=6)
            start_dt = _date_floor(start_dt)
            end_dt = _date_ceiling(now)
        elif preset == "30d":
            start_dt = now - timedelta(days=29)
            start_dt = _date_floor(start_dt)
            end_dt = _date_ceiling(now)

        if start_dt and end_dt:
            # 对于已分配的客户，按派单时间筛选；对于未分配的客户，按创建时间筛选
            query = query.filter(
                or_(
                    and_(
                        Customer.dispatch_time.isnot(None),
                        Customer.dispatch_time >= start_dt,
                        Customer.dispatch_time <= end_dt,
                    ),
                    and_(
                        Customer.dispatch_time.is_(None),
                        Customer.created_at >= start_dt,
                        Customer.created_at <= end_dt,
                    ),
                )
            )
            return query

    # 如果既没有手动时间参数，也没有 preset，则使用"当天窗口"（北京时间 00:00 ~ 23:59）
    if not start_date and not end_date and not start and not end and not preset:
        start_dt, end_dt = get_shift_window_utc()
        # 对于已分配的客户，按派单时间筛选；对于未分配的客户，按创建时间筛选
        query = query.filter(
            or_(
                and_(
                    Customer.dispatch_time.isnot(None),
                    Customer.dispatch_time >= start_dt,
                    Customer.dispatch_time <= end_dt,
                ),
                and_(
                    Customer.dispatch_time.is_(None),
                    Customer.created_at >= start_dt,
                    Customer.created_at <= end_dt,
                ),
            )
        )
        return query
    
    # 优先使用新的日期格式（支持 datetime-local 格式：YYYY-MM-DDTHH:mm）
    if start_date:
        try:
            # 尝试解析 datetime-local 格式 (YYYY-MM-DDTHH:mm)
            if 'T' in start_date:
                # 如果只有日期和时间，没有秒，添加秒
                if len(start_date) == 16:  # YYYY-MM-DDTHH:mm
                    start_date = start_date + ":00"
                start_dt = _to_utc_naive(datetime.fromisoformat(start_date))
            else:
                # 兼容旧的日期格式 (YYYY-MM-DD)
                start_dt = _to_utc_naive(datetime.strptime(start_date, "%Y-%m-%d"))
        except (ValueError, AttributeError) as e:
            current_app.logger.warning(f"Failed to parse start_date: {start_date}, error: {e}")
    elif start:
        try:
            start_dt = _to_utc_naive(datetime.fromisoformat(start.replace("Z", "+00:00")))
        except (ValueError, AttributeError) as e:
            current_app.logger.warning(f"Failed to parse start: {start}, error: {e}")
    
    if end_date:
        try:
            # 尝试解析 datetime-local 格式 (YYYY-MM-DDTHH:mm)
            if 'T' in end_date:
                # 如果只有日期和时间，没有秒，添加秒
                if len(end_date) == 16:  # YYYY-MM-DDTHH:mm
                    end_date = end_date + ":59"
                else:
                    # 如果已经有秒，确保是59秒
                    end_date = end_date.rsplit(':', 1)[0] + ":59"
                end_dt = _to_utc_naive(datetime.fromisoformat(end_date))
            else:
                # 兼容旧的日期格式 (YYYY-MM-DD)，包含整天
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                end_dt = end_dt.replace(hour=23, minute=59, second=59)
                end_dt = _to_utc_naive(end_dt)
        except (ValueError, AttributeError) as e:
            current_app.logger.warning(f"Failed to parse end_date: {end_date}, error: {e}")
    elif end:
        try:
            end_dt = _to_utc_naive(datetime.fromisoformat(end.replace("Z", "+00:00")))
        except (ValueError, AttributeError) as e:
            current_app.logger.warning(f"Failed to parse end: {end}, error: {e}")

    # 统一时间筛选：已分配看 dispatch_time，未分配看 created_at
    if start_dt or end_dt:
        time_filters = []
        if start_dt and end_dt:
            time_filters.append(
                or_(
                    and_(
                        Customer.dispatch_time.isnot(None),
                        Customer.dispatch_time >= start_dt,
                        Customer.dispatch_time <= end_dt,
                    ),
                    and_(
                        Customer.dispatch_time.is_(None),
                        Customer.created_at >= start_dt,
                        Customer.created_at <= end_dt,
                    ),
                )
            )
        elif start_dt:
            time_filters.append(
                or_(
                    and_(
                        Customer.dispatch_time.isnot(None),
                        Customer.dispatch_time >= start_dt,
                    ),
                    and_(
                        Customer.dispatch_time.is_(None),
                        Customer.created_at >= start_dt,
                    ),
                )
            )
        elif end_dt:
            time_filters.append(
                or_(
                    and_(
                        Customer.dispatch_time.isnot(None),
                        Customer.dispatch_time <= end_dt,
                    ),
                    and_(
                        Customer.dispatch_time.is_(None),
                        Customer.created_at <= end_dt,
                    ),
                )
            )

        if time_filters:
            query = query.filter(*time_filters)

    return query


def _prepend_remark(customer: Customer, content: str) -> None:
    """为客户备注添加时间戳并按倒序记录（使用北京时间）。"""
    if not content:
        return

    # 使用北京时间（UTC+8）
    beijing_time = datetime.utcnow() + timedelta(hours=8)
    timestamp = beijing_time.strftime("%Y-%m-%d %H:%M")
    entry = f"[{timestamp}] {content}"
    customer.remark = f"{entry}\n{customer.remark}" if customer.remark else entry


@customer_bp.route("/", methods=["GET"])
@login_required
def customer_list():
    """客户管理列表页。"""
    current = g.current_user
    page = request.args.get("page", default=1, type=int)
    per_page = current_app.config.get("CUSTOMER_LIST_PER_PAGE", 20)
    active_tab = request.args.get("tab", "list")

    query = Customer.query
    
    # 「待分配销售」tab：只显示未分配的客户，且只对超管/数据员可见
    if active_tab == "pending" and current.role in ("super_admin", "data_entry"):
        query = query.filter(Customer.status == "unassigned")
        # 不应用角色筛选，超管/数据员可以看到所有未分配的客户
    else:
        query = _apply_customer_filters(query, current)

    # 默认按派单时间倒序（未派单的放后），同派单时间则按 ID 倒序，最新记录优先
    ordered_query = query.order_by(
        Customer.dispatch_time.desc().nullslast(),
        Customer.id.desc(),
    )
    pagination = ordered_query.paginate(
        page=page,
        per_page=per_page,
        error_out=False,
    )
    customers = pagination.items

    from ..models import SystemConfig
    system_dispatch_enabled = SystemConfig.get_bool("system_dispatch_enabled", default=False)

    thumbnail_map = {}
    preview_map = {}
    for customer in customers:
        if customer.image_path:
            thumb_rel = ensure_thumbnail(customer.image_path)
            if thumb_rel and _static_asset_exists(thumb_rel):
                thumbnail_map[customer.id] = thumb_rel
            preview_rel = ensure_preview(customer.image_path)
            if preview_rel and _static_asset_exists(preview_rel):
                preview_map[customer.id] = preview_rel

    # 如果是待分配销售 tab，需要加载销售列表供手动派单
    sales_users = None
    sales_with_stats = None
    if active_tab == "pending" and current.role in ("super_admin", "data_entry"):
        sales_users = (
            User.query.join(SalesProfile, SalesProfile.user_id == User.id)
            .filter(
                User.role == "sales",
                User.is_active == True,
                SalesProfile.is_available == True
            )
            .order_by(SalesProfile.dispatch_order.asc(), User.id.asc())
            .all()
        )
        
        # 为每个销售计算转化率
        sales_with_stats = []
        for s in sales_users:
            total_accepted = Customer.query.filter(
                Customer.sales_id == s.id,
                Customer.status == "accepted"
            ).count()
            total_converted = Customer.query.filter(
                Customer.sales_id == s.id,
                Customer.is_converted.is_(True)
            ).count()
            
            if total_accepted > 0:
                conversion_rate = f"{(total_converted / total_accepted * 100):.1f}%"
            else:
                conversion_rate = "-"
            
            sales_with_stats.append({
                "user": s,
                "conversion_rate": conversion_rate,
                "accepted": total_accepted,
                "converted": total_converted,
            })

    # 计算「各地区下一位待派销售」预览，仅在客户列表主 Tab 且超管/数据员时展示
    next_sales_by_region: list[dict] | None = None
    if active_tab == "list" and current.role in ("super_admin", "data_entry"):
        next_sales_by_region = []

        # 所有配置了服务地区的可用销售的地区列表
        region_rows = (
            db.session.query(SalesProfile.service_region)
            .filter(SalesProfile.service_region.isnot(None))
            .distinct()
            .all()
        )

        for (region_name,) in region_rows:
            if not region_name:
                continue

            region_sales = (
                User.query.join(SalesProfile, SalesProfile.user_id == User.id)
                .filter(
                    User.role == "sales",
                    User.is_active.is_(True),
                    SalesProfile.is_available.is_(True),
                    SalesProfile.service_region == region_name,
                )
                .order_by(SalesProfile.dispatch_order.asc(), User.id.asc())
                .all()
            )

            if not region_sales:
                continue

            # 与自动派单规则保持一致：最久未在该地区接单的销售优先
            last_time_map: dict[int, datetime] = {}
            for s in region_sales:
                q = Customer.query.filter(
                    Customer.sales_id == s.id,
                    Customer.region == region_name,
                )
                last_customer = (
                    q.order_by(Customer.dispatch_time.desc(), Customer.id.desc())
                    .first()
                )
                if last_customer and last_customer.dispatch_time:
                    last_time_map[s.id] = last_customer.dispatch_time
                else:
                    last_time_map[s.id] = datetime.min

            region_sales_sorted = sorted(
                region_sales,
                key=lambda s: (
                    last_time_map.get(s.id, datetime.min),
                    s.sales_profile.dispatch_order if s.sales_profile else 0,
                    s.id,
                ),
            )

            next_s = region_sales_sorted[0]
            next_sales_by_region.append(
                {
                    "region": region_name,
                    "username": next_s.username,
                    "dispatch_order": next_s.sales_profile.dispatch_order
                    if next_s.sales_profile
                    else None,
                }
            )

    # 获取所有不重复的地区列表，用于筛选下拉框
    regions = (
        db.session.query(Customer.region)
        .filter(Customer.region.isnot(None), Customer.region != "")
        .distinct()
        .order_by(Customer.region.asc())
        .all()
    )
    region_list = [r[0] for r in regions]

    summary_show_admin, summary_show_operator, summary_show_sales_only = _list_summary_flags(
        current
    )

    return render_template(
        "customer/customer_list.html",
        customers=customers,
        pagination=pagination,
        per_page=per_page,
        thumbnail_map=thumbnail_map,
        preview_map=preview_map,
        current_filters=request.args.to_dict(),
        system_dispatch_enabled=system_dispatch_enabled,
        active_tab=active_tab,
        sales_users=sales_users,
        sales_with_stats=sales_with_stats,
        next_sales_by_region=next_sales_by_region,
        region_list=region_list,
        summary_show_admin=summary_show_admin,
        summary_show_operator=summary_show_operator,
        summary_show_sales_only=summary_show_sales_only,
    )


@customer_bp.route("/summary/today-created-count")
@login_required
def today_created_count():
    """返回当天（北京时间 00:00 ~ 23:59）的新增客户总量（按 created_at）。

    说明：
    - 这里统计的是「系统内所有客户」的新增数量
    - 不再根据当前用户角色做任何过滤（运营 / 管理员 / 销售看到的都是同一个总数）
    """

    start_dt, end_dt = get_shift_window_utc()

    count = (
        Customer.query.filter(
            Customer.created_at >= start_dt,
            Customer.created_at <= end_dt,
        )
        .with_entities(func.count(Customer.id))
        .scalar()
    )

    return jsonify({"success": True, "count": int(count or 0)})


@customer_bp.route("/summary/region-stats")
@login_required
def region_stats():
    """返回按地区统计的新增客户数量，以及当前用户的接单/上传数量。

    对于销售：返回个人接单数量（地区列表为空）
    对于运营：返回个人上传数量 + 在所有运营中的排名
    对于管理员/数据员：返回地区统计列表（不过渡到卡片，不展示卡片本身）
    """
    current = g.current_user
    start_dt, end_dt = get_shift_window_utc()
    role_key = (current.role or "").strip().lower()

    personal_count = 0
    personal_label = ""
    personal_rank = None
    operator_ranking = None  # 运营的排名列表，供模板渲染

    # 统计所有运营（operator角色）的上传数量，按降序排列
    all_operator_counts = (
        db.session.query(
            User.id,
            User.username,
            func.count(Customer.id).label("count"),
        )
        .join(Customer, Customer.creator_id == User.id)
        .filter(
            User.role == "operator",
            Customer.created_at >= start_dt,
            Customer.created_at <= end_dt,
        )
        .group_by(User.id, User.username)
        .order_by(func.count(Customer.id).desc())
        .all()
    )

    # 构建运营排名字典
    op_rank_map: dict[int, tuple[int, str, int]] = {}
    for idx, row in enumerate(all_operator_counts, 1):
        op_rank_map[row.id] = (idx, row.username, int(row.count))

    # 按地区统计新增客户数量
    region_counts = (
        db.session.query(
            Customer.region,
            func.count(Customer.id).label("count"),
        )
        .filter(
            Customer.created_at >= start_dt,
            Customer.created_at <= end_dt,
            Customer.region.isnot(None),
            Customer.region != "",
        )
        .group_by(Customer.region)
        .all()
    )

    region_stats_list = [
        {"region": region, "count": int(count)}
        for region, count in region_counts
    ]

    if role_key == "sales":
        personal_count = (
            Customer.query.filter(
                Customer.sales_id == current.id,
                Customer.accepted_time >= start_dt,
                Customer.accepted_time <= end_dt,
            )
            .with_entities(func.count(Customer.id))
            .scalar()
        )
        personal_label = "我的接单数量"
    elif role_key == "operator":
        personal_count = (
            Customer.query.filter(
                Customer.creator_id == current.id,
                Customer.created_at >= start_dt,
                Customer.created_at <= end_dt,
            )
            .with_entities(func.count(Customer.id))
            .scalar()
        )
        personal_label = "我的上传数量"
        # 计算当前运营的排名
        if current.id in op_rank_map:
            rank, _, count = op_rank_map[current.id]
            personal_rank = rank
            personal_count = count
        operator_ranking = [
            {"rank": idx, "username": row.username, "count": int(row.count)}
            for idx, row in enumerate(all_operator_counts, 1)
        ]

    return jsonify({
        "success": True,
        "region_stats": region_stats_list,
        "personal_count": int(personal_count or 0),
        "personal_label": personal_label,
        "personal_rank": personal_rank,
        "operator_ranking": operator_ranking,
    })


@customer_bp.route("/sales/availability", methods=["POST"])
@login_required
def update_sales_availability():
    """销售代表切换接单可用状态。"""
    current = g.current_user
    if current.role != "sales":
        return jsonify({"success": False, "error": "仅销售角色可切换接单状态。"}), 403

    payload = request.get_json(silent=True) or {}
    if "online" not in payload:
        return (
            jsonify({"success": False, "error": "缺少 online 字段，请提供目标状态。"}),
            400,
        )

    desired_state = bool(payload.get("online"))
    profile = current.sales_profile

    # 如果没有销售配置，尝试创建一条默认配置
    if not profile:
        profile = SalesProfile(
            user_id=current.id,
            dispatch_order=0,
            is_available=desired_state,
        )
        db.session.add(profile)
    else:
        profile.is_available = desired_state

    # 不再自动分配公海客户，改为销售手动申领
    # assigned_public_pool = 0
    # if desired_state:
    #     limit = current_app.config.get("PUBLIC_POOL_ASSIGN_ONLINE_LIMIT")
    #     assigned_public_pool = _assign_public_pool_to_sales(
    #         current, limit=limit
    #     )

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Failed to update sales availability: {e}")
        return (
            jsonify(
                {
                    "success": False,
                    "error": "更新状态失败，请稍后重试。如果问题持续，可能是数据库被占用，请检查是否有其他进程在使用数据库。",
                }
            ),
            500,
        )

    return jsonify(
        {
            "success": True,
            "online": desired_state,
            "assigned_customers": 0,  # 不再自动分配，返回0
        }
    )


@customer_bp.route("/create", methods=["GET", "POST"])
@login_required
def customer_create():
    """录入客户信息，并根据角色与系统设置决定是否派单。"""
    current = g.current_user

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        # 联系方式：去掉前后空格后再使用
        phone = (request.form.get("phone") or "").strip()
        region = request.form.get("region", "").strip()
        remark = request.form.get("remark", "").strip()
        sales_id = request.form.get("sales_id", type=int)
        operator_id = request.form.get("operator_id", type=int)

        # 运营只能录入，不能派单：禁止指定销售，名称可空，但必须有联系方式和地区
        if current.role == "operator":
            sales_id = None
            operator_id = current.id
            if not phone or not region:
                flash("运营录入客户时必须填写联系方式和地区。", "danger")
                return redirect(url_for("customer.customer_create"))
        else:
            # 其他角色：仍建议填写姓名
            if not name:
                flash("客户名称不能为空。", "danger")
                return redirect(url_for("customer.customer_create"))

        # 联系方式去重校验：同一个号码只能录入一次（忽略前后空格）
        if phone:
            existing = None
            # 先用数据库函数快速查一遍，避免全表扫太多数据
            try:
                existing = (
                    Customer.query.filter(func.trim(Customer.phone) == phone)
                    .order_by(Customer.id.desc())
                    .first()
                )
            except Exception:
                # 某些 SQLite 版本 / 数据里包含特殊空白符时，fallback 到 Python 侧判断
                pass

            if not existing:
                # 保险起见，再在 Python 侧做一次基于 strip() 的去重判断
                for c in Customer.query.filter(Customer.phone.isnot(None)).all():
                    if (c.phone or "").strip() == phone:
                        existing = c
                        break

            if existing:
                flash(
                    f"该联系方式已存在（客户ID：{existing.id}，姓名：{existing.name}），请勿重复录入。",
                    "danger",
                )
                return redirect(url_for("customer.customer_create"))

        # 处理图片上传
        image_path = None
        if "image" in request.files:
            file = request.files["image"]
            if file and file.filename:
                # 确保上传目录存在
                upload_dir = os.path.join(current_app.root_path, "..", "static", "uploads")
                os.makedirs(upload_dir, exist_ok=True)
                
                # 生成安全的文件名
                filename = secure_filename(file.filename)
                # 添加时间戳避免重名
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{timestamp}_{filename}"
                
                file_path = os.path.join(upload_dir, filename)
                file.save(file_path)
                image_path = filename
                ensure_thumbnail(image_path)
                ensure_preview(image_path)

        # 默认先创建为“未分配”或“待派单”状态
        customer = Customer(
            name=name or None,
            phone=phone,
            region=region,
            remark=remark,
            image_path=image_path,
            creator_id=current.id,
            operator_id=operator_id if operator_id else (current.id if current.role == "operator" else None),
            status="unassigned",
        )

        assigned_sales = None

        # 超级管理员 / 数据员可以手动指定销售，视为“手动派单”
        from ..models import SystemConfig
        system_dispatch_enabled = SystemConfig.get_bool("system_dispatch_enabled", default=False)

        if sales_id and current.role in ("super_admin", "data_entry"):
            assigned_sales = User.query.get(sales_id)
        if assigned_sales:
            customer.sales_id = assigned_sales.id
            customer.dispatcher_id = current.id
            customer.dispatch_time = datetime.utcnow()
            customer.status = "pending"
            _prepend_remark(customer, f"[系统] 手动派单给 {assigned_sales.username}")
        elif system_dispatch_enabled:
            # 系统派单开启：先保存为未分配，稍后统一通过自动派单服务处理
            pass
        else:
            # 系统派单关闭，且未手动指定销售 → 保持 unassigned，等待手动派单
            pass

        db.session.add(customer)
        db.session.commit()

        # 如果系统派单开启且本次没有手动指定销售，则尝试立即为这个客户自动派单
        if system_dispatch_enabled and not assigned_sales:
            from .routes import run_auto_dispatch_unassigned  # 规避循环导入
            run_auto_dispatch_unassigned(single_customer_id=customer.id)

        if assigned_sales:
            send_assignment_notification(assigned_sales, customer)

        flash("客户录入成功。", "success")
        return redirect(url_for("customer.customer_list"))

    # GET 显示表单
    sales_users = (
        User.query.filter_by(role="sales", is_active=True)
        .order_by(User.id.asc())
        .all()
    )
    operator_users = (
        User.query.filter_by(role="operator", is_active=True)
        .order_by(User.id.asc())
        .all()
    )
    
    # 加载地区数据
    regions = Region.query.filter_by(is_active=True).order_by(Region.id.asc()).all()

    return render_template(
        "customer/customer_form.html",
        sales_users=sales_users,
        operator_users=operator_users,
        regions=regions,
    )


@customer_bp.route("/<int:customer_id>/edit", methods=["GET", "POST"])
@login_required
def customer_edit(customer_id: int):
    """编辑客户信息。"""
    current = g.current_user
    customer = Customer.query.get_or_404(customer_id)

    # 权限检查：只有创建者、运营、数据员或超级管理员可以编辑
    if not current.is_super_admin():
        if current.role not in ("operator", "data_entry") and customer.creator_id != current.id:
            flash("无权编辑此客户。", "danger")
            return redirect(url_for("customer.customer_list"))

    if request.method == "POST":
        customer.name = request.form.get("name", "").strip()
        customer.phone = request.form.get("phone", "").strip()
        customer.region = request.form.get("region", "").strip()
        customer.remark = request.form.get("remark", "").strip()

        # 运营角色：联系方式+地区必填
        if current.role == "operator":
            if not customer.phone or not customer.region:
                flash("运营编辑客户时必须填写联系方式和地区。", "danger")
                return redirect(url_for("customer.customer_edit", customer_id=customer.id))
        else:
            # 其他角色：客户名称必填
            if not customer.name:
                flash("客户名称不能为空。", "danger")
                return redirect(url_for("customer.customer_edit", customer_id=customer.id))

        # 超管可修改订单状态
        if current.is_super_admin() and "status" in request.form:
            new_status = request.form.get("status", "").strip()
            if new_status in ("pending", "timeout", "accepted", "public_pool", "unassigned"):
                old_status = customer.status
                customer.status = new_status
                if old_status != new_status:
                    _prepend_remark(
                        customer,
                        f"[系统] 超级管理员 {current.username} 将状态从 {old_status} 修改为 {new_status}",
                    )

        # 处理图片上传
        if "image" in request.files:
            file = request.files["image"]
            if file and file.filename:
                # 确保上传目录存在
                upload_dir = os.path.join(current_app.root_path, "..", "static", "uploads")
                os.makedirs(upload_dir, exist_ok=True)
                
                # 删除旧图片
                if customer.image_path:
                    old_path = os.path.join(upload_dir, customer.image_path)
                    if os.path.exists(old_path):
                        try:
                            os.remove(old_path)
                        except Exception:
                            pass
                    remove_thumbnail(customer.image_path)
                    remove_preview(customer.image_path)
                
                # 生成安全的文件名
                filename = secure_filename(file.filename)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{timestamp}_{filename}"
                
                file_path = os.path.join(upload_dir, filename)
                file.save(file_path)
                customer.image_path = filename
                ensure_thumbnail(filename)
                ensure_preview(filename)

        # 注意：编辑时不修改销售分配和运营人员
        # 销售分配应通过「待分配销售」tab 或重新派单功能完成

        db.session.commit()
        flash("客户信息已更新。", "success")
        return redirect(url_for("customer.customer_detail", customer_id=customer.id))

    # GET 显示编辑表单
    sales_users = (
        User.query.filter_by(role="sales", is_active=True)
        .order_by(User.id.asc())
        .all()
    )
    operator_users = (
        User.query.filter_by(role="operator", is_active=True)
        .order_by(User.id.asc())
        .all()
    )
    
    # 加载地区数据
    regions = Region.query.filter_by(is_active=True).order_by(Region.id.asc()).all()

    return render_template(
        "customer/customer_form.html",
        customer=customer,
        sales_users=sales_users,
        operator_users=operator_users,
        regions=regions,
        is_edit=True,
        can_edit_customer_status=current.is_super_admin(),
    )


@customer_bp.route("/<int:customer_id>", methods=["GET", "POST"])
@login_required
def customer_detail(customer_id: int):
    """客户详情页 + 销售端操作。"""
    current = g.current_user
    customer = Customer.query.get_or_404(customer_id)
    # 与侧栏一致：role 精确为 super_admin 也视为超管（与 User.is_super_admin 双保险）
    is_super_admin_user = current.is_super_admin() or (current.role == "super_admin")

    # 可见范围限制
    if current.role == "operator" and customer.creator_id != current.id:
        flash("只能查看自己录入的客户。", "danger")
        return redirect(url_for("customer.customer_list"))
    if current.role == "sales" and customer.sales_id != current.id:
        flash("只能查看分配给自己的客户。", "danger")
        return redirect(url_for("customer.customer_list"))

    if request.method == "POST":
        _rk = "".join(
            unicodedata.normalize("NFKC", str(current.role or "")).split()
        ).lower()
        sales_follow_up = (
            _rk == "sales"
            and customer.sales_id == current.id
            and customer.status == "accepted"
        )
        if not (sales_follow_up or is_super_admin_user):
            flash("无权执行此操作。", "danger")
            return redirect(url_for("customer.customer_detail", customer_id=customer_id))

        def _tri_state_bool(key: str) -> bool | None:
            v = (request.form.get(key) or "").strip()
            if v == "true":
                return True
            if v == "false":
                return False
            return None

        is_valid_raw = (request.form.get("is_valid") or "").strip()
        if is_valid_raw == "urge_add":
            customer.is_valid = False
            customer.conversion_status = CONVERSION_STATUS_URGE_ADD
            customer.is_converted = False
        else:
            customer.is_valid = _tri_state_bool("is_valid")
            Customer.apply_conversion_from_form(customer, request.form.get("is_converted"))

        # 保存无效客户的佐证截图
        invalid_file = request.files.get("invalid_proof_image")
        if invalid_file and invalid_file.filename:
            upload_dir = os.path.join(current_app.root_path, "..", "static", "uploads")
            os.makedirs(upload_dir, exist_ok=True)

            # 如果已有旧文件，先删除
            if customer.invalid_proof_image:
                old_path = os.path.join(upload_dir, customer.invalid_proof_image)
                if os.path.exists(old_path):
                    try:
                        os.remove(old_path)
                    except OSError:
                        current_app.logger.warning(
                            "Failed to remove old invalid proof image for customer %s",
                            customer.id,
                        )
            remove_preview(customer.invalid_proof_image)

            filename = secure_filename(invalid_file.filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_invalid_{filename}"
            invalid_file.save(os.path.join(upload_dir, filename))
            customer.invalid_proof_image = filename
            ensure_preview(filename)

        new_remark = request.form.get("remark", "").strip()
        if new_remark:
            _prepend_remark(customer, new_remark)

        db.session.commit()
        flash("客户跟进信息已保存。", "success")
        return redirect(url_for("customer.customer_detail", customer_id=customer.id))

    image_preview_path = ensure_preview(customer.image_path) if customer.image_path else None
    invalid_preview_path = (
        ensure_preview(customer.invalid_proof_image) if customer.invalid_proof_image else None
    )

    _role_key = "".join(
        unicodedata.normalize("NFKC", str(current.role or "")).split()
    ).lower()
    show_sales_follow_up = (
        _role_key == "sales"
        and customer.sales_id == current.id
        and customer.status == "accepted"
    ) or is_super_admin_user

    return render_template(
        "customer/customer_detail.html",
        customer=customer,
        image_preview_path=image_preview_path,
        invalid_preview_path=invalid_preview_path,
        show_sales_follow_up=show_sales_follow_up,
        is_super_admin_user=is_super_admin_user,
    )


@customer_bp.route("/<int:customer_id>/accept", methods=["POST"])
@login_required
def customer_accept(customer_id: int):
    """列表页接单操作。"""
    current = g.current_user
    customer = Customer.query.get_or_404(customer_id)

    if current.role != "sales":
        flash("仅销售可以接单。", "danger")
        return redirect(request.referrer or url_for("customer.customer_list"))

    if customer.sales_id != current.id:
        flash("只能接自己名下的客户。", "danger")
        return redirect(request.referrer or url_for("customer.customer_list"))

    if customer.status != "pending":
        flash("该客户已处理，无需再次接单。", "warning")
        return redirect(request.referrer or url_for("customer.customer_list"))

    customer.status = "accepted"
    customer.accepted_time = datetime.utcnow()
    db.session.commit()
    flash("接单成功。", "success")
    return redirect(request.referrer or url_for("customer.customer_list"))


@customer_bp.route("/public_pool")
@login_required
def public_pool():
    """公海客户列表。"""
    current = g.current_user
    query = Customer.query.filter(Customer.status == "public_pool")
    customers = query.order_by(Customer.id.desc()).all()

    thumbnail_map = {}
    preview_map = {}
    for customer in customers:
        if customer.image_path:
            thumb_rel = ensure_thumbnail(customer.image_path)
            if thumb_rel and _static_asset_exists(thumb_rel):
                thumbnail_map[customer.id] = thumb_rel
            preview_rel = ensure_preview(customer.image_path)
            if preview_rel and _static_asset_exists(preview_rel):
                preview_map[customer.id] = preview_rel

    sales_users = []
    show_contact = True
    if current.is_super_admin() or current.role == "operator":
        sales_users = (
            User.query.join(SalesProfile, SalesProfile.user_id == User.id)
            .filter(
                User.role == "sales",
                User.is_active.is_(True),
                SalesProfile.is_available.is_(True),
            )
            .order_by(SalesProfile.dispatch_order.asc(), User.id.asc())
            .all()
        )
    elif current.role == "sales":
        show_contact = False

    return render_template(
        "customer/public_pool.html",
        customers=customers,
        thumbnail_map=thumbnail_map,
        preview_map=preview_map,
        sales_users=sales_users,
        show_contact=show_contact,
    )


@customer_bp.route("/pending/<int:customer_id>/assign", methods=["POST"])
@login_required
def pending_assign(customer_id: int):
    """将待分配客户手动派单给指定销售（仅超管/数据员）。"""
    current = g.current_user
    
    if current.role not in ("super_admin", "data_entry"):
        flash("只有超级管理员和数据员可以手动派单。", "danger")
        return redirect(url_for("customer.customer_list"))
    
    customer = Customer.query.get_or_404(customer_id)
    
    if customer.status != "unassigned":
        flash("该客户已被分配，无法再次派单。", "warning")
        return redirect(url_for("customer.customer_list", tab="pending"))
    
    sales_id = request.form.get("sales_id", type=int)
    if not sales_id:
        flash("请选择销售代表。", "danger")
        return redirect(url_for("customer.customer_list", tab="pending"))
    
    sales = User.query.get(sales_id)
    if not sales or sales.role != "sales":
        flash("请选择有效的销售代表。", "danger")
        return redirect(url_for("customer.customer_list", tab="pending"))
    
    profile = sales.sales_profile
    if not profile or not profile.is_available:
        flash("该销售当前离线，无法分配。", "warning")
        return redirect(url_for("customer.customer_list", tab="pending"))
    
    customer.sales_id = sales.id
    customer.dispatcher_id = current.id
    customer.dispatch_time = datetime.utcnow()
    customer.status = "pending"
    
    # 记录派单信息
    dispatcher_role = "超级管理员" if current.role == "super_admin" else "数据员"
    _prepend_remark(customer, f"[系统] {dispatcher_role}手动派单给 {sales.username}")
    
    db.session.commit()
    send_assignment_notification(sales, customer)
    
    flash(f"客户已派单给 {sales.username}。", "success")
    return redirect(url_for("customer.customer_list", tab="pending"))


@customer_bp.route("/public_pool/<int:customer_id>/assign", methods=["POST"])
@login_required
def public_pool_assign(customer_id: int):
    """将公海客户分配给指定销售。"""
    current = g.current_user
    customer = Customer.query.get_or_404(customer_id)

    if not (current.is_super_admin() or current.role == "operator"):
        flash("只有超级管理员或运营可以从公海分配客户。", "danger")
        return redirect(url_for("customer.public_pool"))

    sales_id = request.form.get("sales_id", type=int)
    sales = User.query.get_or_404(sales_id)
    if sales.role != "sales":
        flash("请选择销售代表。", "danger")
        return redirect(url_for("customer.public_pool"))

    profile = sales.sales_profile
    if not profile or not profile.is_available:
        flash("该销售当前离线，无法分配。", "warning")
        return redirect(url_for("customer.public_pool"))

    customer.sales_id = sales.id
    customer.dispatcher_id = current.id
    customer.dispatch_time = datetime.utcnow()
    customer.status = "pending"
    # 记录派单信息
    _prepend_remark(customer, f"[系统] 公海客户已分配给 {sales.username}")
    db.session.commit()

    send_assignment_notification(sales, customer)

    flash("公海客户已分配给销售。", "success")
    return redirect(url_for("customer.public_pool"))


@customer_bp.route("/<int:customer_id>/release-to-public-pool", methods=["POST"])
@login_required
def release_to_public_pool(customer_id: int):
    """将系统派单的客户释放到公海（仅超管/数据员/运营）。"""
    current = g.current_user
    
    if current.role not in ("super_admin", "data_entry", "operator"):
        flash("只有超级管理员、数据员或运营可以释放客户到公海。", "danger")
        return redirect(request.referrer or url_for("customer.customer_list"))
    
    customer = Customer.query.get_or_404(customer_id)
    
    # 只允许释放系统派单且状态为pending的客户
    if customer.dispatcher_id is not None:
        flash("只能释放系统派单的客户到公海。", "warning")
        return redirect(request.referrer or url_for("customer.customer_list"))
    
    if customer.status != "pending":
        flash("只能释放待接单状态的客户到公海。", "warning")
        return redirect(request.referrer or url_for("customer.customer_list"))
    
    # 释放到公海
    customer.status = "public_pool"
    customer.sales_id = None  # 清除销售分配
    customer.dispatcher_id = None
    customer.retry_count = 0
    
    operator_name = "超级管理员" if current.role == "super_admin" else ("数据员" if current.role == "data_entry" else "运营")
    _prepend_remark(customer, f"[系统] {operator_name} {current.username} 将客户释放到公海。")
    
    db.session.commit()
    
    flash("客户已释放到公海。", "success")
    return redirect(request.referrer or url_for("customer.customer_list"))


@customer_bp.route("/public_pool/<int:customer_id>/claim", methods=["POST"])
@login_required
def public_pool_claim(customer_id: int):
    """销售自助从公海领取客户。"""
    current = g.current_user
    if current.role != "sales":
        flash("只有销售角色可以领取公海客户。", "danger")
        return redirect(url_for("customer.public_pool"))

    profile = current.sales_profile
    if not profile or not profile.is_available:
        flash("请先在客户列表页切换为在线状态。", "warning")
        return redirect(url_for("customer.public_pool"))

    customer = Customer.query.get_or_404(customer_id)

    if customer.status != "public_pool":
        flash("该客户已被其他人领取。", "warning")
        return redirect(url_for("customer.public_pool"))

    customer.sales_id = current.id
    customer.dispatcher_id = current.id
    customer.dispatch_time = datetime.utcnow()
    customer.status = "accepted"  # 领取后直接接单，无需再次确认
    customer.accepted_time = datetime.utcnow()
    customer.retry_count = 0
    _prepend_remark(
        customer, f"[系统] 销售 {current.username} 从公海自助领取并接单。"
    )
    db.session.commit()
    send_assignment_notification(current, customer)

    flash("领取并接单成功，请尽快跟进。", "success")
    return redirect(url_for("customer.public_pool"))


def reassign_timeouts(max_retries: int = 3, timeout_minutes: int = 5) -> int:
    """超时单重派逻辑，可在 CLI / 定时任务中调用。

    处理流程：
    1. 查找所有 status='pending' 且超过 timeout_minutes 未接单的客户
    2. 如果重派次数 >= max_retries，则放入公海
    3. 否则，按客户所在地区，在该地区的销售中按派单序号重新分配给下一个销售（排除原销售）
    4. 记录哪些销售没有接单
    5. 如果该地区没有其他可用销售，则放入公海
    6. 更新重派次数、派单时间等字段
    7. 发送通知给新销售

    Args:
        max_retries: 最大重派次数，超过后放入公海
        timeout_minutes: 超时时间（分钟）

    Returns:
        本次成功重派的客户数量
    """
    from flask import current_app
    from sqlalchemy.exc import SQLAlchemyError

    now = datetime.utcnow()
    threshold = now - timedelta(minutes=timeout_minutes)

    reassigned = 0

    try:
        # 查找所有超时的待接单客户
        timed_out_customers = (
            Customer.query.filter(
                Customer.status == "pending",
                Customer.dispatch_time <= threshold,
            ).all()
        )

        for c in timed_out_customers:
            # 获取原销售信息（用于记录）
            original_sales = User.query.get(c.sales_id) if c.sales_id else None
            original_sales_name = original_sales.username if original_sales else "未知"

            # 如果客户没有地区信息，则不做跨区重派，直接放入公海
            if not c.region:
                c.status = "public_pool"
                _prepend_remark(
                    c,
                    f"[系统] 客户无地区信息，无法匹配销售，已放入公海。未接单销售: {original_sales_name}",
                )
                continue

            # 检查重派次数
            if c.retry_count >= max_retries:
                # 超过最大重派次数，放入公海
                c.status = "public_pool"
                # 记录所有未接单的销售（历史 + 当前），并去重
                failed_sales_list = _collect_failed_sales_names(c.remark or "")
                if original_sales_name not in failed_sales_list:
                    failed_sales_list.append(original_sales_name)
                failed_sales_str = ", ".join(failed_sales_list)
                _prepend_remark(
                    c,
                    f"[系统] 超过{max_retries}次重派未接单，已放入公海。未接单销售: {failed_sales_str}",
                )
                continue

            # 尝试重新分配（按客户所在地区，排除原销售），禁止跨地区
            exclude_sales_id = c.sales_id
            next_sales = _auto_assign_sales(
                region=c.region, exclude_sales_id=exclude_sales_id
            )

            if not next_sales:
                # 该地区没有其他可用销售，放入公海
                # 记录所有未接单的销售（历史 + 当前），并去重
                failed_sales_list = _collect_failed_sales_names(c.remark or "")
                if original_sales_name not in failed_sales_list:
                    failed_sales_list.append(original_sales_name)
                failed_sales_str = ", ".join(failed_sales_list)
                c.status = "public_pool"
                _prepend_remark(
                    c,
                    f"[系统] {c.region}地区无其他可匹配销售，已放入公海。未接单销售: {failed_sales_str}",
                )
                continue

            # 记录原销售未接单（历史 + 当前），并去重
            failed_sales_list = _collect_failed_sales_names(c.remark or "")
            # 不要把本次的新销售（next_sales）算成未接单
            if next_sales.username in failed_sales_list:
                failed_sales_list = [
                    n for n in failed_sales_list if n != next_sales.username
                ]
            if original_sales_name not in failed_sales_list:
                failed_sales_list.append(original_sales_name)
            failed_sales_str = ", ".join(failed_sales_list)

            # 重新分配（再次做地区匹配校验）
            next_region = (
                next_sales.sales_profile.service_region
                if next_sales.sales_profile
                else None
            )
            if c.region == next_region:
                c.status = "pending"
                c.sales_id = next_sales.id
                c.dispatcher_id = None  # 系统自动重派
                c.dispatch_time = now
                c.retry_count = (c.retry_count or 0) + 1
                _prepend_remark(
                    c,
                    f"[系统] 校验匹配：客户地区({c.region}) == 销售地区({next_region})，第{c.retry_count}次自动重派给 {next_sales.username}。未接单销售: {failed_sales_str}",
                )

                # 发送通知
                send_assignment_notification(next_sales, c)
                reassigned += 1
            else:
                # 理论上不会发生，如有异常则直接放入公海
                c.status = "public_pool"
                _prepend_remark(
                    c,
                    f"[系统] 检测到重派时地区不匹配，已放入公海。未接单销售: {failed_sales_str}",
                )

        db.session.commit()
        return reassigned
    except SQLAlchemyError as e:
        # 数据库层面的异常：回滚事务并记录日志，但避免拖垮整个连接池
        db.session.rollback()
        if current_app:
            current_app.logger.error(
                f"[reassign_timeouts] 数据库异常，已回滚事务：{e}", exc_info=True
            )
        return reassigned
    except Exception as e:
        # 其他未预期异常：同样回滚并记录，抛出给上层（由调度器或 CLI 处理）
        db.session.rollback()
        if current_app:
            current_app.logger.error(
                f"[reassign_timeouts] 执行过程中出现异常：{e}", exc_info=True
            )
        raise
    finally:
        # 确保无论如何都释放 Session，将连接归还给连接池
        db.session.remove()


@customer_bp.route("/<int:customer_id>/delete", methods=["POST"])
@login_required
def customer_delete(customer_id: int):
    """删除客户（物理删除：从数据库中彻底删除）。"""
    current = g.current_user
    customer = Customer.query.get_or_404(customer_id)

    # 权限检查：只有创建者、运营、数据员或超级管理员可以删除
    if not current.is_super_admin():
        if current.role not in ("operator", "data_entry") and customer.creator_id != current.id:
            flash("无权删除此客户。", "danger")
            return redirect(url_for("customer.customer_list"))

    customer_name = customer.name
    
    # 1. 删除关联的通知记录
    Notification.query.filter_by(customer_id=customer.id).delete()
    
    # 2. 删除客户图片文件（如果存在）
    if customer.image_path:
        upload_dir = os.path.join(current_app.root_path, "..", "static", "uploads")
        image_path = os.path.join(upload_dir, customer.image_path)
        if os.path.exists(image_path):
            try:
                os.remove(image_path)
            except Exception as e:
                current_app.logger.warning(f"Failed to delete customer image: {e}")
        remove_thumbnail(customer.image_path)
        remove_preview(customer.image_path)
    
    # 3. 删除无效证明图片（如果存在）
    if customer.invalid_proof_image:
        upload_dir = os.path.join(current_app.root_path, "..", "static", "uploads")
        invalid_image_path = os.path.join(upload_dir, customer.invalid_proof_image)
        if os.path.exists(invalid_image_path):
            try:
                os.remove(invalid_image_path)
            except Exception as e:
                current_app.logger.warning(f"Failed to delete invalid proof image: {e}")
        remove_preview(customer.invalid_proof_image)
    
    # 4. 删除客户记录
    db.session.delete(customer)
    db.session.commit()
    
    flash(f"客户 {customer_name} 已删除。", "success")
    return redirect(url_for("customer.customer_list"))


