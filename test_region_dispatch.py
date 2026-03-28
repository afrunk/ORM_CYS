from __future__ import annotations

"""
简单的终端测试脚本：
在终端中运行它，用于检查是否存在“客户地区 != 销售服务地区”的派单记录。

用法（在项目根目录）：
    .\.venv\Scripts\activate      # 按你本地虚拟环境为准
    python test_region_dispatch.py
"""

from app import app

try:
    # 按项目现有结构导入模型（如有差异，可根据你实际项目路径微调）
    from crm.models import db, Customer, User, SalesProfile
except ImportError as e:  # 防御性提示
    raise RuntimeError(
        "导入模型失败，请确认 crm.models 中是否定义了 db, Customer, User, SalesProfile。"
    ) from e


def main() -> None:
    """检查是否存在跨地区派单记录。"""
    with app.app_context():
        # 查询所有 已分配销售 的客户，筛选出 客户地区 != 销售服务地区 的记录
        rows = (
            db.session.query(
                Customer.id,
                Customer.name,
                Customer.region,
                User.username,
                SalesProfile.service_region,
            )
            .join(User, Customer.sales_id == User.id)
            .join(SalesProfile, SalesProfile.user_id == User.id)
            .filter(
                Customer.sales_id.isnot(None),
                Customer.region.isnot(None),
                Customer.region != "",
                Customer.region != SalesProfile.service_region,
            )
            .all()
        )

        if not rows:
            print("✅ 没有发现跨地区派单记录，一切正常。")
        else:
            print("⚠ 发现以下跨地区派单记录：")
            for r in rows:
                print(
                    f"客户ID={r.id}, 客户名={r.name}, "
                    f"客户地区={r.region}, 销售={r.username}, 销售地区={r.service_region}"
                )


if __name__ == "__main__":
    main()

