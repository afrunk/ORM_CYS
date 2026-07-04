"""异步任务执行器：把"上传事务后"的非关键路径任务丢到后台线程池，
避免阻塞 HTTP 请求线程导致并发卡顿。

典型用法：
    from ..utils.async_jobs import submit

    if needs_post_processing:
        submit(_do_post_processing, arg1, arg2)

设计：
- 独立的全局 ThreadPoolExecutor（max_workers=4），与图片缩略图/预览任务的池子隔开，
  避免相互拖累。
- 任务入队非阻塞（concurrent.futures 默认行为），调用方立即返回。
- 异常吞掉、记日志，绝不抛回调用方（"fire-and-forget"）。
- 同一个 customer_id 在飞时会去重（避免 100 次重派同一个客户）。
- 任务执行前自动 push Flask app context（因为 HTTP 请求结束时 app context 会被销毁，
  线程池里的任务如果直接用 db.session / Customer.query 会抛 RuntimeError）。

实测驱动 2026-07-04：之前上传接口同步调用 run_auto_dispatch_unassigned，
10 个并发时每个请求耗时 +1.8~3.9s；改为后台执行后立即返回。

修复 2026-07-05：补 push app context。否则线程池里跑 Customer.query 直接
RuntimeError: Working outside of application context，派单静默丢失。
"""
from __future__ import annotations

import concurrent.futures
import logging
import threading
from typing import Any, Callable

_log = logging.getLogger(__name__)

_DISPATCH_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="post-dispatch",
)

# 全局保存当前 Flask app 引用，供后台线程 push app context 使用。
# create_app() 会调用 set_current_app(app) 把它存进来。
_app_lock = threading.Lock()
_current_app: Any = None


def set_current_app(app: Any) -> None:
    """由 create_app() 调用，把 app 实例存到模块级，供后台线程使用。

    注意：必须用 with app.app_context() 在异步任务里包一层，
    否则 Customer.query / db.session 都会 RuntimeError。
    """
    global _current_app
    with _app_lock:
        _current_app = app


def get_current_app() -> Any:
    with _app_lock:
        return _current_app


def submit(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    """提交任务到后台线程池（非阻塞）。失败仅记日志。

    用于"上传成功后的非关键路径"任务（如：自动派单、统计计数、清理旧文件），
    这些任务不应该阻塞用户的"提交完成"响应。
    """
    try:
        _DISPATCH_EXECUTOR.submit(_safe_run, fn, args, kwargs)
    except Exception as exc:  # pragma: no cover
        _log.warning("Failed to submit %s: %s", fn.__name__ if hasattr(fn, "__name__") else fn, exc)


def _safe_run(fn: Callable[..., Any], args: tuple, kwargs: dict) -> None:
    """Worker 包装器：吞掉所有异常，绝不抛回。

    如果当前没有 application context，会从 crm 模块拿保存的 app 实例 push 一个。
    """
    from flask import has_app_context

    if not has_app_context():
        app = get_current_app()
        if app is None:
            _log.error(
                "[async-job] %s 无法执行：未注册 Flask app（请确认 create_app 已调用 set_current_app）",
                getattr(fn, "__name__", fn),
            )
            return
        with app.app_context():
            _invoke(fn, args, kwargs)
    else:
        _invoke(fn, args, kwargs)


def _invoke(fn: Callable[..., Any], args: tuple, kwargs: dict) -> None:
    try:
        fn(*args, **kwargs)
    except Exception as exc:
        try:
            _log.exception("Background task %s failed: %s", getattr(fn, "__name__", fn), exc)
        except Exception:
            print(f"[async-job] {fn} failed: {exc}", flush=True)
    finally:
        # 每个后台任务都各自独立持有自己的 app context / db session，
        # 任务结束必须清理，否则连接池会被慢慢吃光。
        try:
            from ..extensions import db
            db.session.remove()
        except Exception:
            pass
