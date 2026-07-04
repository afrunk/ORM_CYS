"""Image helper utilities (thumbnail/preview generation & removal).

性能说明：
- WEBP method=6 + optimize=True 压缩率最好但 CPU 最重，常见 1080P 手机拍的原图（5MB+）
  在单核上单张耗时 200-800ms。列表页一页 20 条 × 同步转码 = 容易卡住整个请求线程。
- 本模块做了两点优化：
  1) 提供 schedule_async_preview() 接口把"生成大预览图"丢到后台线程，避免阻塞请求。
  2) 缩略图保留 method=6（很小，不卡）；大预览如果调用方同步调用，仍然是同一个慢路径。

并发设计（重要）：
- 用一个全局有界 ThreadPoolExecutor（max_workers=2）替代「每个请求每个文件开一个 daemon 线程 + BoundedSemaphore」。
- 原因：werkzeug threaded 模式下，HTTP 请求线程本身已经多个；如果预览/缩略图任务每个文件都开 daemon 线程，
  进程内线程数会爆炸（一次列表页 20 张图 = 20 个 daemon worker），所有线程抢 GIL，反而比同步执行还慢。
- Executor 内部 worker 数量固定为 2（CPU 密集任务再多的 worker 也只是争 GIL），
  任务通过有界队列排队。Submit 是非阻塞的，调用方请求线程立即返回。
"""
from __future__ import annotations

import concurrent.futures
import os
import threading
from pathlib import Path
from typing import Optional, Tuple

from flask import current_app
from PIL import Image


# Thumbnail (list view)
THUMB_SUBDIR = "thumbs"
THUMB_PREFIX = "thumb_"
THUMB_SIZE: Tuple[int, int] = (160, 160)

# Large preview (overlay / detail)
PREVIEW_SUBDIR = "previews"
PREVIEW_PREFIX = "preview_"
PREVIEW_SIZE: Tuple[int, int] = (1080, 1080)

WEBP_QUALITY_THUMB = 74
WEBP_QUALITY_PREVIEW = 82

# ============================================================================
# 全局有界线程池：替代旧版「每文件开 daemon 线程 + BoundedSemaphore」。
# - max_workers=2 是经验值：WEBP 转码是 CPU 密集任务，再多的 worker 也只是争 GIL。
# - 队列有上限（通过 ThreadPoolExecutor 默认实现，submit 不阻塞），调用方立即返回。
# ============================================================================
_PREVIEW_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="img-preview",
)
_THUMB_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=2,
    thread_name_prefix="img-thumb",
)
# 统计已派发的任务数（用于诊断）
_preview_dispatch_count = 0
_preview_done_count = 0
_preview_lock = threading.Lock()
_thumb_dispatch_count = 0
_thumb_done_count = 0
_thumb_lock = threading.Lock()


def _static_root() -> str:
    # 优先用 current_app.root_path（app 上下文内有效）；
    # 如果在 app 上下文外（daemon 后台线程），fallback 到相对于这个文件的固定路径。
    try:
        return os.path.abspath(os.path.join(current_app.root_path, "..", "static"))
    except RuntimeError:
        # 兜底：crm/utils/images.py 上两级就是项目根，static 在那里
        return os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "static")
        )


def _uploads_root() -> str:
    return os.path.join(_static_root(), "uploads")


def _variant_names(image_filename: str, prefix: str) -> tuple[str, str]:
    """Return (webp_name, legacy_name) for the resized file."""
    stem = Path(image_filename).stem or os.path.splitext(image_filename)[0]
    webp_name = f"{prefix}{stem}.webp"
    legacy_name = f"{prefix}{image_filename}"
    return webp_name, legacy_name


def _ensure_variant(
    image_filename: str,
    *,
    prefix: str,
    subdir: str,
    size: Tuple[int, int],
    quality: int,
) -> Optional[str]:
    """Create (if needed) and return the relative static path for a resized WEBP variant."""
    if not image_filename:
        return None

    uploads_dir = _uploads_root()
    original_path = os.path.join(uploads_dir, image_filename)
    if not os.path.exists(original_path):
        return None

    variant_dir = os.path.join(uploads_dir, subdir)
    os.makedirs(variant_dir, exist_ok=True)

    variant_name, legacy_name = _variant_names(image_filename, prefix)
    variant_path = os.path.join(variant_dir, variant_name)
    legacy_path = os.path.join(variant_dir, legacy_name)

    try:
        regenerate = True
        if os.path.exists(variant_path):
            regenerate = os.path.getmtime(original_path) > os.path.getmtime(variant_path)

        if regenerate:
            with Image.open(original_path) as img:
                img_copy = img.copy()
                img_copy.thumbnail(size, Image.Resampling.LANCZOS)

                # Convert to a WEBP-friendly mode
                if img_copy.mode not in ("RGB", "L"):
                    img_copy = img_copy.convert("RGB")

                img_copy.save(
                    variant_path,
                    format="WEBP",
                    optimize=True,
                    quality=quality,
                    method=6,
                )

        # Best-effort: clean legacy file if it exists and is not the same as the new path
        if os.path.exists(legacy_path) and legacy_path != variant_path:
            try:
                os.remove(legacy_path)
            except OSError:
                pass

        return os.path.join("uploads", subdir, variant_name).replace("\\", "/")
    except Exception as exc:  # pragma: no cover - best effort logging
        try:
            current_app.logger.warning(
                "Failed to generate %s variant for %s: %s", prefix, image_filename, exc
            )
        except Exception:
            pass
        if os.path.exists(variant_path):
            return os.path.join("uploads", subdir, variant_name).replace("\\", "/")
        if os.path.exists(legacy_path):
            return os.path.join("uploads", subdir, legacy_name).replace("\\", "/")
        return None


def ensure_thumbnail(image_filename: str) -> Optional[str]:
    """Create (if needed) and return the relative static path for the thumbnail."""
    return _ensure_variant(
        image_filename,
        prefix=THUMB_PREFIX,
        subdir=THUMB_SUBDIR,
        size=THUMB_SIZE,
        quality=WEBP_QUALITY_THUMB,
    )


def ensure_preview(image_filename: str) -> Optional[str]:
    """Create (if needed) and return the relative static path for the preview image.

    注意：此函数同步执行会调用较慢的 WEBP 转码（method=6, optimize=True）。
    列表页等批量场景请改用 schedule_async_preview()。
    """
    try:
        current_app.logger.debug("[preview] ensure_preview called: %s", image_filename)
    except Exception:
        pass
    return _ensure_variant(
        image_filename,
        prefix=PREVIEW_PREFIX,
        subdir=PREVIEW_SUBDIR,
        size=PREVIEW_SIZE,
        quality=WEBP_QUALITY_PREVIEW,
    )


def _preview_worker(image_filename: str) -> None:
    """Executor worker 函数：跑一次 WEBP 转码。

    由 _PREVIEW_EXECUTOR 调度，executor 内部已经限制了并发（max_workers=2），
    不再需要额外的信号量。

    注意：worker 线程里没有 Flask app context，所有 current_app.* 调用必须 try/except。
    """
    global _preview_done_count
    try:
        result = ensure_preview(image_filename)
        try:
            current_app.logger.debug(
                "[async-preview] done: %s -> %s", image_filename, result
            )
        except RuntimeError:
            pass  # 没有 app context，正常
    except Exception as exc:  # pragma: no cover
        try:
            current_app.logger.warning(
                "Async preview worker failed for %s: %s", image_filename, exc
            )
        except RuntimeError:
            # executor 线程里没有 app context；用 stderr 兜底
            import sys
            print(
                f"[async-preview] worker failed: {image_filename}: {exc}",
                file=sys.stderr,
            )
        except Exception:
            pass
    finally:
        with _preview_lock:
            _preview_done_count += 1


def schedule_async_preview(image_filename: Optional[str]) -> None:
    """后台异步生成 preview，不阻塞调用方。

    用全局有界 ThreadPoolExecutor（max_workers=2）替代旧的「每文件开 daemon 线程 + 信号量」。
    Submit 是非阻塞的，HTTP 请求线程立即返回，不会因为本任务被 GIL 抢占而卡死其他请求。

    用于：
    - 列表页 hover/click 触发
    - 详情页：打开页面时立刻返回占位，背景慢慢生成
    - 上传接口：保存原图后立刻返回，preview 延后生成

    失败仅记日志，绝不抛到调用方。
    """
    global _preview_dispatch_count
    if not image_filename:
        return
    try:
        with _preview_lock:
            _preview_dispatch_count += 1
            dispatched = _preview_dispatch_count
            done = _preview_done_count
        _PREVIEW_EXECUTOR.submit(_preview_worker, image_filename)
        try:
            current_app.logger.debug(
                "[async-preview] scheduled: %s (dispatched=%d done=%d)",
                image_filename, dispatched, done,
            )
        except Exception:
            pass
    except Exception as exc:  # pragma: no cover
        try:
            current_app.logger.warning(
                "Failed to schedule async preview for %s: %s", image_filename, exc
            )
        except Exception:
            pass


# ============================================================================
# 异步缩略图：缩略图生成（CPU 密集）也已异步化，列表页请求路径不再阻塞。
# 列表页只检查「缩略图是否已经存在」，不存在则直接返回原图 URL 占位，
# 同时后台补生成；下次访问时缩略图已就绪。
# ============================================================================
_thumb_dispatch_count = 0
_thumb_done_count = 0
_thumb_lock = threading.Lock()


def _thumb_worker(image_filename: str) -> None:
    """后台线程：跑一次 thumbnail 生成。"""
    global _thumb_done_count
    try:
        ensure_thumbnail(image_filename)
    except Exception as exc:
        try:
            current_app.logger.warning(
                "Async thumbnail worker failed for %s: %s", image_filename, exc
            )
        except Exception:
            pass
    finally:
        with _thumb_lock:
            _thumb_done_count += 1


def schedule_async_thumbnail(image_filename: Optional[str]) -> None:
    """后台异步生成缩略图。仅在确实需要时才排队。"""
    global _thumb_dispatch_count
    if not image_filename:
        return
    try:
        # 快速去重：如果缩略图已经存在且 mtime 正常，就不调度。
        thumb_rel = _thumbnail_path_if_exists(image_filename)
        if thumb_rel:
            return
        with _thumb_lock:
            _thumb_dispatch_count += 1
        _THUMB_EXECUTOR.submit(_thumb_worker, image_filename)
    except Exception:
        pass


def _thumbnail_path_if_exists(image_filename: str) -> Optional[str]:
    """如果缩略图已经生成（且原图 mtime 没变化），返回相对路径；否则返回 None。"""
    try:
        uploads_dir = _uploads_root()
        original_path = os.path.join(uploads_dir, image_filename)
        if not os.path.exists(original_path):
            return None
        variant_name, legacy_name = _variant_names(image_filename, THUMB_PREFIX)
        variant_dir = os.path.join(uploads_dir, THUMB_SUBDIR)
        variant_path = os.path.join(variant_dir, variant_name)
        legacy_path = os.path.join(variant_dir, legacy_name)
        # 优先 webp
        for candidate in (variant_path, legacy_path):
            if os.path.exists(candidate):
                # 检查 mtime：如果原图更新了，缩略图失效（视为不存在）
                try:
                    if os.path.getmtime(original_path) > os.path.getmtime(candidate):
                        continue
                except OSError:
                    pass
                return os.path.join("uploads", THUMB_SUBDIR,
                                    os.path.basename(candidate)).replace("\\", "/")
        return None
    except Exception:
        return None


def ensure_thumbnail_async_or_fallback(image_filename: str) -> Optional[str]:
    """返回缩略图相对路径；如果还没生成，返回 None 并异步补做。

    列表页模板可这样用：
        {% set t = thumb_or_fallback(c.image_path) %}
        {% if t %}<img src=...thumb...>{% else %}<img src=...原图...>{% endif %}
    """
    rel = _thumbnail_path_if_exists(image_filename)
    if rel:
        return rel
    schedule_async_thumbnail(image_filename)
    return None


def _remove_variant(image_filename: Optional[str], *, prefix: str, subdir: str) -> None:
    """Delete generated variant files (WEBP and legacy) for a given original image."""
    if not image_filename:
        return

    variant_name, legacy_name = _variant_names(image_filename, prefix)
    variant_dir = os.path.join(_uploads_root(), subdir)

    for name in (variant_name, legacy_name):
        variant_path = os.path.join(variant_dir, name)
        if os.path.exists(variant_path):
            try:
                os.remove(variant_path)
            except OSError:
                try:
                    current_app.logger.debug("Failed to delete variant %s", variant_path)
                except Exception:
                    pass


def remove_thumbnail(image_filename: Optional[str]) -> None:
    """Remove thumbnail if it exists."""
    _remove_variant(image_filename, prefix=THUMB_PREFIX, subdir=THUMB_SUBDIR)


def remove_preview(image_filename: Optional[str]) -> None:
    """Remove preview image if it exists."""
    _remove_variant(image_filename, prefix=PREVIEW_PREFIX, subdir=PREVIEW_SUBDIR)