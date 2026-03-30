from __future__ import annotations

from functools import wraps
from typing import Iterable

from flask import abort, g, redirect, session, url_for, flash


def login_required(view):
    """登录校验装饰器。"""

    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if not session.get("user_id"):
            flash("请先登录系统。", "warning")
            return redirect(url_for("auth.login"))
        # session 里可能有 user_id，但用户已删或库不一致时 g.current_user 会为 None
        user = getattr(g, "current_user", None)
        if user is None:
            session.pop("user_id", None)
            flash("登录已失效，请重新登录。", "warning")
            return redirect(url_for("auth.login"))
        return view(*args, **kwargs)

    return wrapped_view


def roles_required(roles: Iterable[str]):
    """角色校验装饰器。"""

    roles_set = set(roles)

    def decorator(view):
        @wraps(view)
        def wrapped_view(*args, **kwargs):
            user = getattr(g, "current_user", None)
            if user is None:
                flash("请先登录系统。", "warning")
                return redirect(url_for("auth.login"))
            if user.role not in roles_set:
                abort(403)
            return view(*args, **kwargs)

        return wrapped_view

    return decorator


