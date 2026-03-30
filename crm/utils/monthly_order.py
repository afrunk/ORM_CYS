"""客户月度序号：按北京时间自然月自增，新月份从 1 重新计数。"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import func, or_
from sqlalchemy.orm import Session


def utc_naive_to_beijing_ym(dt: datetime) -> str:
    """将库内 naive UTC 时间转为北京日历 YYYYMM 字符串。"""
    if dt is None:
        dt = datetime.utcnow()
    beijing = dt + timedelta(hours=8)
    return f"{beijing.year:04d}{beijing.month:02d}"


def current_beijing_ym() -> str:
    return utc_naive_to_beijing_ym(datetime.utcnow())


def next_monthly_sequence(session: Session):
    """分配下一个 (ym, seq)，并更新计数表。与当前请求同一事务内调用。"""
    from ..models import MonthlyCustomerSeq

    ym = current_beijing_ym()
    row = (
        session.query(MonthlyCustomerSeq)
        .filter_by(ym=ym)
        .with_for_update()
        .one_or_none()
    )
    if row is None:
        row = MonthlyCustomerSeq(ym=ym, last_seq=0)
        session.add(row)
        session.flush()
    row.last_seq += 1
    return ym, row.last_seq


def assign_monthly_order_fields(session: Session, customer) -> None:
    ym, seq = next_monthly_sequence(session)
    customer.monthly_order_ym = ym
    customer.monthly_order_key = seq


def sync_monthly_seq_counters_from_customers(session: Session) -> None:
    """根据 customers 表各月最大序号校准计数表（回填后必须调用）。"""
    from ..models import Customer, MonthlyCustomerSeq

    rows = (
        session.query(
            Customer.monthly_order_ym,
            func.max(Customer.monthly_order_key),
        )
        .filter(
            Customer.monthly_order_ym.isnot(None),
            Customer.monthly_order_key.isnot(None),
        )
        .group_by(Customer.monthly_order_ym)
        .all()
    )
    for ym, max_k in rows:
        if not ym or max_k is None:
            continue
        row = session.query(MonthlyCustomerSeq).filter_by(ym=ym).one_or_none()
        if row is None:
            session.add(MonthlyCustomerSeq(ym=ym, last_seq=int(max_k)))
        else:
            row.last_seq = max(int(row.last_seq or 0), int(max_k))


def backfill_customer_monthly_ids_if_needed(app) -> None:
    """为缺少月度序号的旧数据按创建时间（北京月）依次编号，并同步计数表。幂等。

    须在 Flask 应用上下文中调用（例如 create_app 初始化阶段）。
    """
    from ..extensions import db
    from ..models import Customer

    has_null = (
        Customer.query.filter(
            or_(
                Customer.monthly_order_key.is_(None),
                Customer.monthly_order_ym.is_(None),
            )
        ).first()
    )
    if has_null is None:
        return

    from collections import defaultdict

    customers = (
        Customer.query.filter(
            or_(
                Customer.monthly_order_key.is_(None),
                Customer.monthly_order_ym.is_(None),
            )
        )
        .order_by(Customer.id.asc())
        .all()
    )

    by_ym: dict[str, list] = defaultdict(list)
    for c in customers:
        dt = c.created_at or datetime.utcnow()
        by_ym[utc_naive_to_beijing_ym(dt)].append(c)

    for ym, group in by_ym.items():
        group.sort(key=lambda x: (x.created_at or datetime.min, x.id))
        for i, c in enumerate(group, start=1):
            c.monthly_order_ym = ym
            c.monthly_order_key = i

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        app.logger.exception("[迁移] 回填客户月度编号失败")
        raise

    sync_monthly_seq_counters_from_customers(db.session)
    try:
        db.session.commit()
        app.logger.info("[迁移] 已回填 %s 条客户的月度编号", len(customers))
    except Exception:
        db.session.rollback()
        app.logger.exception("[迁移] 同步月度计数表失败")
        raise
