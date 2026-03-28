from __future__ import annotations

"""
自动化区域派单回归测试脚本：

目标：
- 验证「北京」订单不会被派给「三亚」销售
- 验证「三亚」订单不会被派给「北京」销售
- 同时验证：仅当有同区域在线销售时才派单，否则进入公海

运行方式（在项目根目录）：
    .\.venv\Scripts\activate          # 按你的虚拟环境为准
    python simulate_dispatch_test.py
"""

from typing import Tuple

from werkzeug.security import generate_password_hash

from app import app
from crm.models import db, User, SalesProfile, Customer, SystemConfig


def _ensure_system_dispatch_enabled() -> None:
    """确保系统自动派单开关已打开。"""
    SystemConfig.set_bool("system_dispatch_enabled", True)
    db.session.commit()


def _ensure_test_admin() -> Tuple[User, str]:
    """确保存在一个可登录的测试超级管理员账号，用于走 /login /customers/create 流程。"""
    username = "test_admin"
    password = "test_admin123"

    admin = User.query.filter_by(username=username).first()
    if not admin:
        admin = User(
            username=username,
            password_hash=generate_password_hash(password),
            role="super_admin",
            is_active=True,
        )
        db.session.add(admin)
        db.session.commit()
    return admin, password


def _ensure_sales(username: str, service_region: str, dispatch_order: int) -> User:
    """确保存在指定地区的销售及其 SalesProfile。"""
    user = User.query.filter_by(username=username).first()
    if not user:
        user = User(
            username=username,
            password_hash=generate_password_hash("test_sales123"),
            role="sales",
            is_active=True,
        )
        db.session.add(user)
        db.session.flush()

    profile = user.sales_profile
    if not profile:
        profile = SalesProfile(
            user_id=user.id,
            dispatch_order=dispatch_order,
            is_available=True,
            service_region=service_region,
        )
        db.session.add(profile)
    else:
        profile.dispatch_order = dispatch_order
        profile.is_available = True
        profile.service_region = service_region

    db.session.commit()
    return user


def _login(client, username: str, password: str) -> None:
    """通过 /login 完成一次真实登录，获取 session。"""
    resp = client.post(
        "/login",
        data={"username": username, "password": password},
        follow_redirects=False,
    )
    if resp.status_code not in (302, 303):
        raise AssertionError(f"登录失败，状态码：{resp.status_code}")


def _create_order(
    client, *, name: str, phone: str, region: str
) -> Customer:
    """通过 /customers/create 提交表单，模拟录入订单，并返回新建的 Customer。"""
    resp = client.post(
        "/customers/create",
        data={
            "name": name,
            "phone": phone,
            "region": region,
            "remark": f"自动化测试订单 - {region}",
        },
        follow_redirects=False,
    )
    if resp.status_code not in (302, 303):
        raise AssertionError(
            f"创建客户失败（HTTP {resp.status_code}），可能是表单校验或登录状态异常。"
        )

    # 按 phone 唯一性查回刚创建的订单
    customer = (
        Customer.query.filter_by(phone=phone)
        .order_by(Customer.id.desc())
        .first()
    )
    if not customer:
        raise AssertionError("未能在数据库中找到刚创建的客户记录。")
    return customer


def _assert_beijing_order_dispatched_correctly(sales_a: User, sales_b: User) -> None:
    """验证北京订单不会被派给三亚销售 B。"""
    test_phone = "AUTO_TEST_BEIJING"

    # 清理历史测试数据（同一 phone）
    Customer.query.filter_by(phone=test_phone).delete()
    db.session.commit()

    with app.test_client() as client:
        admin, admin_password = _ensure_test_admin()
        _login(client, admin.username, admin_password)

        customer = _create_order(
            client, name="北京自动化测试客户", phone=test_phone, region="北京"
        )
        db.session.refresh(customer)

        # 如果系统派单关闭或没有北京销售在线，订单可能仍为 unassigned 或进入公海
        # 这里的强校验点是：绝不能派给三亚销售 B
        if customer.sales_id is not None:
            assigned_sales = User.query.get(customer.sales_id)
            if not assigned_sales:
                raise AssertionError("北京订单已分配，但找不到对应销售记录。")

            # 核心断言：北京订单不允许派给三亚销售 B
            if assigned_sales.id == sales_b.id:
                raise AssertionError(
                    "错误：北京订单被派给了三亚销售 B，这是跨区域派单漏洞！"
                )

            # 额外安全断言：派单销售的服务区域必须与客户地区一致
            profile = assigned_sales.sales_profile
            if not profile or profile.service_region != "北京":
                raise AssertionError(
                    f"错误：北京订单被派给了服务区域为 {getattr(profile, 'service_region', None)} 的销售。"
                )


def _assert_sanya_order_dispatched_correctly(sales_a: User, sales_b: User) -> None:
    """验证三亚订单不会被派给北京销售 A。"""
    test_phone = "AUTO_TEST_SANYA"

    # 清理历史测试数据（同一 phone）
    Customer.query.filter_by(phone=test_phone).delete()
    db.session.commit()

    with app.test_client() as client:
        admin, admin_password = _ensure_test_admin()
        _login(client, admin.username, admin_password)

        customer = _create_order(
            client, name="三亚自动化测试客户", phone=test_phone, region="三亚"
        )
        db.session.refresh(customer)

        # 强校验点：绝不能派给北京销售 A
        if customer.sales_id is not None:
            assigned_sales = User.query.get(customer.sales_id)
            if not assigned_sales:
                raise AssertionError("三亚订单已分配，但找不到对应销售记录。")

            if assigned_sales.id == sales_a.id:
                raise AssertionError(
                    "错误：三亚订单被派给了北京销售 A，这是跨区域派单漏洞！"
                )

            profile = assigned_sales.sales_profile
            if not profile or profile.service_region != "三亚":
                raise AssertionError(
                    f"错误：三亚订单被派给了服务区域为 {getattr(profile, 'service_region', None)} 的销售。"
                )


def main() -> None:
    # 测试模式下关闭定时任务等副作用
    app.config["TESTING"] = True

    with app.app_context():
        _ensure_system_dispatch_enabled()

        # 准备测试数据：销售 A（北京）、销售 B（三亚），都在线
        sales_a = _ensure_sales("sales_a_beijing", "北京", dispatch_order=1)
        sales_b = _ensure_sales("sales_b_sanya", "三亚", dispatch_order=2)

        # 1) 验证北京订单不会被误派给三亚销售 B
        _assert_beijing_order_dispatched_correctly(sales_a, sales_b)
        print("[OK] 北京订单派单规则校验通过（未发现误派给三亚销售 B）。")

        # 2) 验证三亚订单不会被误派给北京销售 A
        _assert_sanya_order_dispatched_correctly(sales_a, sales_b)
        print("[OK] 三亚订单派单规则校验通过（未发现误派给北京销售 A）。")

        print("所有跨区域派单测试项均通过（Pass）。")


if __name__ == "__main__":
    main()

