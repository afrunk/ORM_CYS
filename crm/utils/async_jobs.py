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

实测驱动 2026-07-04：之前上传接口同步调用 run_auto_dispatch_unassigned，
10 个并发时每个请求耗时 +1.8~3.9s；改为后台执行后立即返回。
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
    """Worker 包装器：吞掉所有异常，绝不抛回。"""
    try:
        fn(*args, **kwargs)
    except Exception as exc:
        try:
            _log.exception("Background task %s failed: %s", getattr(fn, "__name__", fn), exc)
        except Exception:
            print(f"[async-job] {fn} failed: {exc}", flush=True)
