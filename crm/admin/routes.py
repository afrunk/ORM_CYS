from __future__ import annotations

from flask import Blueprint, flash, g, redirect, render_template, request, url_for
from werkzeug.security import generate_password_hash

from ..extensions import db
from ..models import SalesProfile, User, Customer, Region, Notification, SystemConfig
from ..permissions import login_required, roles_required

admin_bp = Blueprint("admin", __name__, template_folder="../templates")


@admin_bp.route("/settings")
@login_required
@roles_required(["super_admin"])
def settings():
    """统一的系统设置页面，包含用户管理和地区管理两个tab。"""
    current = g.current_user
    
    # 读取筛选条件
    role_filter = request.args.get("role", "").strip()
    region_filter = request.args.get("region", "").strip()
    status_filter = request.args.get("status", "").strip()

    # 基础查询：加载所有用户，可按条件筛选
    query = User.query

    # 按角色筛选
    if role_filter:
        query = query.filter(User.role == role_filter)

    # 按启用/停用状态筛选
    if status_filter == "active":
        query = query.filter(User.is_active.is_(True))
    elif status_filter == "inactive":
        query = query.filter(User.is_active.is_(False))

    # 按地区筛选（通过销售资料里的 service_region）
    if region_filter:
        query = (
            query.join(SalesProfile, SalesProfile.user_id == User.id)
            .filter(SalesProfile.service_region == region_filter)
        )

    # 加载用户数据（包含启用和停用），启用的排在前面
    users = (
        query.order_by(User.is_active.desc(), User.id.asc())
        .all()
    )
    
    # 统计该用户作为销售时的接单数与转化数
    stats_map = {}
    for u in users:
        total_accepted = Customer.query.filter(
            Customer.sales_id == u.id, Customer.status == "accepted"
        ).count()
        total_converted = Customer.query.filter(
            Customer.sales_id == u.id, Customer.is_converted.is_(True)
        ).count()
        conversion_rate = (
            f"{(total_converted / total_accepted * 100):.1f}%" if total_accepted else "-"
        )
        stats_map[u.id] = {
            "accepted": total_accepted,
            "converted": total_converted,
            "rate": conversion_rate,
        }
    
    # 加载地区数据
    regions = Region.query.order_by(Region.id.asc()).all()

    # 读取系统派单开关
    system_dispatch_enabled = SystemConfig.get_bool("system_dispatch_enabled", default=False)
    
    return render_template(
        "admin/settings.html",
        users=users,
        stats_map=stats_map,
        regions=regions,
        system_dispatch_enabled=system_dispatch_enabled,
        current_role_filter=role_filter,
        current_region_filter=region_filter,
        current_status_filter=status_filter,
    )


@admin_bp.route("/users/create", methods=["POST"])
@login_required
@roles_required(["super_admin"])
def user_create():
    from flask import jsonify
    
    current = g.current_user
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    role = request.form.get("role", "sales")

    if not username or not password:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
            return jsonify({"success": False, "error": "用户名和密码不能为空。"}), 400
        flash("用户名和密码不能为空。", "danger")
        return redirect(url_for("admin.settings") + "#user-tab")

    if User.query.filter_by(username=username).first():
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
            return jsonify({"success": False, "error": "用户名已存在。"}), 400
        flash("用户名已存在。", "warning")
        return redirect(url_for("admin.settings") + "#user-tab")

    phone = request.form.get("phone", "").strip() or None
    email = request.form.get("email", "").strip() or None
    
    # 只有销售角色才需要邮箱
    if role != "sales":
        email = None
    
    user = User(
        username=username,
        password_hash=generate_password_hash(password),
        role=role,
        is_active=True,
        phone=phone,
        email=email,
        temp_password=password,  # 保存临时密码用于显示
    )
    db.session.add(user)
    db.session.flush()

    # 为销售角色创建 SalesProfile，以记录派单序号等配置
    if role == "sales":
        is_available = request.form.get("is_available") == "on"
        service_region = request.form.get("service_region", "").strip() or None
        
        # 自动计算派单序号：在全局所有销售中，最大序号 + 1（不按地区拆分）
        from sqlalchemy import func
        max_order = db.session.query(func.max(SalesProfile.dispatch_order)).scalar()
        dispatch_order = (max_order + 1) if max_order is not None else 0
        
        profile = SalesProfile(
            user_id=user.id,
            dispatch_order=dispatch_order,
            is_available=is_available,
            service_region=service_region,
        )
        db.session.add(profile)

    db.session.commit()
    
    # 如果是AJAX请求，返回JSON（包含密码）
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        return jsonify({
            "success": True,
            "message": "用户创建成功。",
            "username": username,
            "password": password  # 返回原始密码，以便显示给管理员
        })
    
    flash("用户创建成功。", "success")
    return redirect(url_for("admin.settings") + "#user-tab")


@admin_bp.route("/users/<int:user_id>", methods=["GET"])
@login_required
@roles_required(["super_admin"])
def user_detail(user_id: int):
    """获取用户详情（用于编辑表单）。"""
    from flask import jsonify
    
    user = User.query.get_or_404(user_id)

    data = {
        "id": user.id,
        "username": user.username,
        "role": user.role,
        "is_active": user.is_active,
        "phone": user.phone,
        "email": user.email,
    }
    
    if user.sales_profile:
        data["dispatch_order"] = user.sales_profile.dispatch_order
        data["is_available"] = user.sales_profile.is_available
        data["service_region"] = user.sales_profile.service_region
    
    return jsonify(data)


@admin_bp.route("/users/<int:user_id>/update", methods=["POST"])
@login_required
@roles_required(["super_admin"])
def user_update(user_id: int):
    from flask import jsonify
    
    user = User.query.get_or_404(user_id)

    # 先获取新角色，用于判断是否需要处理销售配置
    new_role = request.form.get("role", user.role)
    
    # ---------- 更新用户名 ----------
    new_username = request.form.get("username", "").strip() or user.username
    if new_username != user.username:
        # 检查是否重复
        exists = User.query.filter(
            User.username == new_username,
            User.id != user.id,
        ).first()
        if exists:
            msg = "用户名已存在，请换一个。"
            if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.is_json:
                return jsonify({"success": False, "error": msg}), 400
            flash(msg, "danger")
            return redirect(url_for("admin.settings") + "#user-tab")
        user.username = new_username
    
    user.role = new_role
    user.is_active = request.form.get("is_active") == "on"
    
    # 更新电话
    user.phone = request.form.get("phone", "").strip() or None
    
    # 更新邮箱（只有销售角色才需要）
    if new_role == "sales":
        user.email = request.form.get("email", "").strip() or None
    else:
        user.email = None
    
    # 更新密码（如果提供了新密码）
    new_password = request.form.get("password", "").strip()
    password_changed = False
    if new_password:
        user.password_hash = generate_password_hash(new_password)
        user.temp_password = new_password  # 保存临时密码用于显示
        password_changed = True

    # 更新销售配置（派单序号、可用状态、服务地区）
    # 只有当角色是销售时才处理
    if new_role == "sales":
        # 从表单获取值，确保正确处理
        dispatch_order_str = request.form.get("dispatch_order", "").strip()
        dispatch_order = int(dispatch_order_str) if dispatch_order_str else 0
        is_available = request.form.get("is_available") == "on" or request.form.get("is_available") == "true"
        service_region = request.form.get("service_region", "").strip() or None
        
        if user.sales_profile:
            # 更新现有配置
            user.sales_profile.dispatch_order = dispatch_order
            user.sales_profile.is_available = is_available
            user.sales_profile.service_region = service_region
        else:
            # 创建新的销售配置
            profile = SalesProfile(
                user_id=user.id,
                dispatch_order=dispatch_order,
                is_available=is_available,
                service_region=service_region,
            )
            db.session.add(profile)
    else:
        # 如果角色不是销售，或者销售角色但没有公司ID，删除销售配置
        if user.sales_profile:
            db.session.delete(user.sales_profile)

    db.session.commit()
    
    # 如果是AJAX请求，返回JSON
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        result = {
            "success": True,
            "message": "用户信息已更新。"
        }
        # 如果密码被修改，返回新密码
        if password_changed:
            result["password"] = new_password
            result["username"] = user.username
        return jsonify(result)
    
    flash("用户信息已更新。", "success")
    return redirect(url_for("admin.settings") + "#user-tab")


@admin_bp.route("/users/<int:user_id>/delete", methods=["POST"])
@login_required
@roles_required(["super_admin"])
def user_delete(user_id: int):
    """删除用户（物理删除：从数据库中彻底删除）。"""
    current = g.current_user
    user = User.query.get_or_404(user_id)

    # 防止删除自己
    if user.id == current.id:
        flash("不能删除自己的账户。", "warning")
        return redirect(url_for("admin.settings") + "#user-tab")

    username = user.username
    
    # 1. 删除销售配置（如果存在），并调整全局派单序号
    if user.sales_profile:
        removed_order = user.sales_profile.dispatch_order
        db.session.delete(user.sales_profile)

        # 将全局中大于该序号的销售派单序号统一减 1，保持连续
        shift_q = SalesProfile.query.filter(
            SalesProfile.dispatch_order > removed_order
        )
        for sp in shift_q.order_by(SalesProfile.dispatch_order.asc()).all():
            sp.dispatch_order = sp.dispatch_order - 1
    
    # 2. 将客户表中相关的外键设置为 NULL
    Customer.query.filter_by(sales_id=user.id).update({"sales_id": None})
    Customer.query.filter_by(operator_id=user.id).update({"operator_id": None})
    Customer.query.filter_by(dispatcher_id=user.id).update({"dispatcher_id": None})
    Customer.query.filter_by(creator_id=user.id).update({"creator_id": None})
    
    # 3. 删除通知记录（sales_id 指向该用户的）
    Notification.query.filter_by(sales_id=user.id).delete()
    
    # 4. 删除用户记录
    db.session.delete(user)
    db.session.commit()
    
    flash(f"用户 {username} 已删除。", "success")
    return redirect(url_for("admin.settings") + "#user-tab")


@admin_bp.route("/users/<int:user_id>/toggle-active", methods=["POST"])
@login_required
@roles_required(["super_admin"])
def user_toggle_active(user_id: int):
    """一键启用/停用账号（影响登录权限）。"""
    current = g.current_user
    user = User.query.get_or_404(user_id)

    # 不允许把自己停用，避免把自己锁死在系统外
    if user.id == current.id:
        flash("不能停用自己的账户。", "warning")
        return redirect(url_for("admin.settings") + "#user-tab")

    user.is_active = not bool(user.is_active)

    # 如果是销售账号，停用时顺便标记为不可用，避免继续参与派单
    if user.sales_profile and not user.is_active:
        user.sales_profile.is_available = False

    db.session.commit()

    action = "启用" if user.is_active else "停用"
    flash(f"已{action}用户 {user.username} 的账号。", "success")
    return redirect(url_for("admin.settings") + "#user-tab")


@admin_bp.route("/system-dispatch/toggle", methods=["POST"])
@login_required
@roles_required(["super_admin", "data_entry"])
def toggle_system_dispatch():
    """切换系统派单总开关，并可触发一次自动派单。"""
    from flask import jsonify
    from ..models import Customer
    from ..customer.routes import run_auto_dispatch_unassigned

    enabled = request.form.get("enabled") == "true"

    SystemConfig.set_bool("system_dispatch_enabled", enabled)
    db.session.commit()

    # 如果开启系统派单，顺便对当前未分配客户跑一轮自动派单
    dispatched_count = 0
    if enabled:
        result = run_auto_dispatch_unassigned()
        dispatched_count = result[0]

    if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.is_json:
        return jsonify(
            {"success": True, "enabled": enabled, "dispatched": dispatched_count}
        )

    flash(
        f"系统派单已{'开启' if enabled else '关闭'}（本次自动派单 {dispatched_count} 个客户）。",
        "success",
    )
    return redirect(url_for("customer.customer_list", tab="dispatch"))


# ========== 地区管理路由 ==========

@admin_bp.route("/regions")
@login_required
@roles_required(["super_admin"])
def region_list():
    """获取地区列表（API）。"""
    from flask import jsonify
    
    regions = Region.query.order_by(Region.id.asc()).all()
    
    return jsonify({
        "success": True,
        "regions": [{
            "id": r.id,
            "name": r.name,
            "is_active": r.is_active,
        } for r in regions]
    })


@admin_bp.route("/regions/<region_name>/max-dispatch-order")
@login_required
@roles_required(["super_admin"])
def get_max_dispatch_order(region_name: str):
    """获取指定地区的最大派单序号（API）。"""
    from flask import jsonify
    from sqlalchemy import func
    
    max_order = db.session.query(func.max(SalesProfile.dispatch_order)).filter(
        SalesProfile.service_region == region_name
    ).scalar()
    
    next_order = (max_order + 1) if max_order is not None else 0
    
    return jsonify({
        "success": True,
        "max_order": max_order,
        "next_order": next_order
    })


@admin_bp.route("/regions/create", methods=["POST"])
@login_required
@roles_required(["super_admin"])
def region_create():
    """创建地区。"""
    from flask import jsonify
    
    name = request.form.get("name", "").strip()
    
    if not name:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
            return jsonify({"success": False, "error": "地区名称不能为空。"}), 400
        flash("地区名称不能为空。", "danger")
        return redirect(url_for("admin.settings") + "#region-tab")
    
    if Region.query.filter_by(name=name).first():
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
            return jsonify({"success": False, "error": "地区名称已存在。"}), 400
        flash("地区名称已存在。", "warning")
        return redirect(url_for("admin.settings") + "#region-tab")
    
    region = Region(
        name=name,
        is_active=True,
    )
    db.session.add(region)
    db.session.commit()
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        return jsonify({
            "success": True,
            "message": "地区创建成功。",
            "region": {
                "id": region.id,
                "name": region.name,
                "is_active": region.is_active,
            }
        })
    
    flash("地区创建成功。", "success")
    return redirect(url_for("admin.settings") + "#region-tab")


@admin_bp.route("/regions/<int:region_id>", methods=["GET"])
@login_required
@roles_required(["super_admin"])
def region_detail(region_id: int):
    """获取地区详情（用于编辑表单）。"""
    from flask import jsonify
    
    region = Region.query.get_or_404(region_id)
    
    return jsonify({
        "id": region.id,
        "name": region.name,
        "is_active": region.is_active,
    })


@admin_bp.route("/regions/<int:region_id>/update", methods=["POST"])
@login_required
@roles_required(["super_admin"])
def region_update(region_id: int):
    """更新地区。"""
    from flask import jsonify
    
    region = Region.query.get_or_404(region_id)
    
    name = request.form.get("name", "").strip()
    is_active = request.form.get("is_active") == "on"
    
    if not name:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
            return jsonify({"success": False, "error": "地区名称不能为空。"}), 400
        flash("地区名称不能为空。", "danger")
        return redirect(url_for("admin.settings") + "#region-tab")
    
    # 检查名称是否与其他地区重复
    existing = Region.query.filter_by(name=name).first()
    if existing and existing.id != region_id:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
            return jsonify({"success": False, "error": "地区名称已存在。"}), 400
        flash("地区名称已存在。", "warning")
        return redirect(url_for("admin.settings") + "#region-tab")
    
    region.name = name
    region.is_active = is_active
    db.session.commit()
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        return jsonify({
            "success": True,
            "message": "地区信息已更新。",
            "region": {
                "id": region.id,
                "name": region.name,
                "is_active": region.is_active,
            }
        })
    
    flash("地区信息已更新。", "success")
    return redirect(url_for("admin.settings") + "#region-tab")


@admin_bp.route("/regions/<int:region_id>/delete", methods=["POST"])
@login_required
@roles_required(["super_admin"])
def region_delete(region_id: int):
    """删除地区。"""
    from flask import jsonify
    
    region = Region.query.get_or_404(region_id)
    
    # 检查是否有销售正在使用该地区
    using_sales = SalesProfile.query.filter_by(service_region=region.name).first()
    if using_sales:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
            return jsonify({"success": False, "error": f"无法删除：有销售正在使用地区「{region.name}」。请先修改相关销售的服务地区。"}), 400
        flash(f"无法删除：有销售正在使用地区「{region.name}」。请先修改相关销售的服务地区。", "warning")
        return redirect(url_for("admin.settings") + "#region-tab")
    
    db.session.delete(region)
    db.session.commit()
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        return jsonify({
            "success": True,
            "message": "地区已删除。"
        })
    
    flash(f"地区「{region.name}」已删除。", "success")
    return redirect(url_for("admin.settings") + "#region-tab")


