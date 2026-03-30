from __future__ import annotations

from datetime import datetime, timedelta


def get_shift_window_utc(now_utc: datetime | None = None) -> tuple[datetime, datetime]:
    """返回"当天"（北京时间 0:00~23:59:59）的 UTC 时间范围。

    业务定义的"当天"：北京时间 0:00 ~ 23:59:59。

    为了便于与数据库中的 UTC 时间比较，这里直接返回 UTC 范围：
    start_utc <= ts <= end_utc
    """
    if now_utc is None:
        now_utc = datetime.utcnow()

    # 转换到北京时间
    beijing_now = now_utc + timedelta(hours=8)

    # 当天 00:00 北京时间
    start_local = beijing_now.replace(hour=0, minute=0, second=0, microsecond=0)
    # 当天 23:59:59 北京时间
    end_local = beijing_now.replace(hour=23, minute=59, second=59, microsecond=999999)

    # 转回 UTC
    start_utc = start_local - timedelta(hours=8)
    end_utc = end_local - timedelta(hours=8)
    return start_utc, end_utc


def get_yesterday_window_utc(now_utc: datetime | None = None) -> tuple[datetime, datetime]:
    """返回"昨天"（北京时间 0:00~23:59:59）对应的 UTC 时间范围。"""
    if now_utc is None:
        now_utc = datetime.utcnow()

    # 转换到北京时间以便按"当天"日期计算
    beijing_now = now_utc + timedelta(hours=8)

    # 昨天 00:00 北京时间
    start_local = beijing_now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    # 昨天 23:59:59 北京时间
    end_local = beijing_now.replace(hour=23, minute=59, second=59, microsecond=999999) - timedelta(days=1)

    start_utc = start_local - timedelta(hours=8)
    end_utc = end_local - timedelta(hours=8)
    return start_utc, end_utc
