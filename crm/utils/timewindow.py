from __future__ import annotations

from datetime import datetime, timedelta


def get_shift_window_utc(now_utc: datetime | None = None) -> tuple[datetime, datetime]:
    """返回“当天班次窗口”的 UTC 时间范围。

    业务定义的“当天”：前一天 18:00 到 第二天 18:00（北京时间）。

    为了便于与数据库中的 UTC 时间比较，这里直接返回 UTC 范围：
    start_utc <= ts < end_utc
    """
    if now_utc is None:
        now_utc = datetime.utcnow()

    # 转换到北京时间
    beijing_now = now_utc + timedelta(hours=8)

    cutoff = beijing_now.replace(hour=18, minute=0, second=0, microsecond=0)

    if beijing_now < cutoff:
        # 还没到今天 18:00：窗口是 昨天18:00 ~ 今天18:00
        end_local = cutoff
        start_local = end_local - timedelta(days=1)
    else:
        # 已经过了今天 18:00：窗口是 今天18:00 ~ 明天18:00
        start_local = cutoff
        end_local = start_local + timedelta(days=1)

    # 转回 UTC
    start_utc = start_local - timedelta(hours=8)
    end_utc = end_local - timedelta(hours=8)
    return start_utc, end_utc


def get_yesterday_window_utc(now_utc: datetime | None = None) -> tuple[datetime, datetime]:
    """返回“昨天”（北京时区定义的 18:00~18:00）对应的 UTC 时间范围。"""
    if now_utc is None:
        now_utc = datetime.utcnow()

    # 转换到北京时间以便按“当天”日期计算
    beijing_now = now_utc + timedelta(hours=8)
    today_18_local = beijing_now.replace(hour=18, minute=0, second=0, microsecond=0)

    end_local = today_18_local
    start_local = end_local - timedelta(days=1)

    start_utc = start_local - timedelta(hours=8)
    end_utc = end_local - timedelta(hours=8)
    return start_utc, end_utc


