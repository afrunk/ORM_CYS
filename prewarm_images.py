"""后台预热进程：一次性把所有 uploads/ 原图预生成为 thumbs/ 和 previews/。

设计原则（2026-07-04 方案 II）：
- 用独立的 Python 进程在后台运行，不影响 Flask 主进程
- 单线程顺序执行（WEBP 转码是 GIL-bound，多线程没意义）
- 每张生成完后 sleep 一小段时间让 Flask CPU 不会被完全抢走
- 进度日志打到 logs/prewarm.out，方便诊断
- 文件存在则跳过（重启时不会重复转）

用法（不需要重启 Flask）：
    /root/ORM_CYS/venv/bin/python /root/ORM_CYS/prewarm_images.py &

停止：kill PID（不是 Flask）
"""
import os
import sys
import time
from pathlib import Path

# 直接调用 Pillow，不 import Flask（避免任何 app context 负担）
from PIL import Image

UPLOADS = '/root/ORM_CYS/static/uploads'
THUMBS = os.path.join(UPLOADS, 'thumbs')
PREVIEWS = os.path.join(UPLOADS, 'previews')
LOG_FILE = '/root/ORM_CYS/logs/prewarm.out'

THUMB_SIZE = (160, 160)
PREVIEW_SIZE = (1080, 1080)
THUMB_QUALITY = 74
PREVIEW_QUALITY = 82

os.makedirs(THUMBS, exist_ok=True)
os.makedirs(PREVIEWS, exist_ok=True)


def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')


def stamp(name: str) -> str:
    return Path(name).stem


def has_thumb(orig: str) -> bool:
    p = os.path.join(THUMBS, f'thumb_{stamp(orig)}.webp')
    return os.path.exists(p)


def has_preview(orig: str) -> bool:
    p = os.path.join(PREVIEWS, f'preview_{stamp(orig)}.webp')
    return os.path.exists(p)


def make_thumb(orig: str) -> bool:
    """生成 thumb。returns True if newly generated."""
    target = os.path.join(THUMBS, f'thumb_{stamp(orig)}.webp')
    if os.path.exists(target):
        return False
    src = os.path.join(UPLOADS, orig)
    try:
        with Image.open(src) as img:
            img_copy = img.copy()
            img_copy.thumbnail(THUMB_SIZE, Image.Resampling.LANCZOS)
            if img_copy.mode not in ('RGB', 'L'):
                img_copy = img_copy.convert('RGB')
            img_copy.save(target, format='WEBP', optimize=False, quality=THUMB_QUALITY, method=4)
        return True
    except Exception as e:
        log(f'  ERR thumb {orig}: {e}')
        return False


def make_preview(orig: str) -> bool:
    target = os.path.join(PREVIEWS, f'preview_{stamp(orig)}.webp')
    if os.path.exists(target):
        return False
    src = os.path.join(UPLOADS, orig)
    try:
        with Image.open(src) as img:
            img_copy = img.copy()
            img_copy.thumbnail(PREVIEW_SIZE, Image.Resampling.LANCZOS)
            if img_copy.mode not in ('RGB', 'L'):
                img_copy = img_copy.convert('RGB')
            img_copy.save(target, format='WEBP', optimize=False, quality=PREVIEW_QUALITY, method=4)
        return True
    except Exception as e:
        log(f'  ERR preview {orig}: {e}')
        return False


def main():
    log('=' * 60)
    log(f'预热启动 PID={os.getpid()}')
    log('=' * 60)

    files = sorted(
        f for f in os.listdir(UPLOADS)
        if f.lower().endswith(('.jpg', '.jpeg', '.png'))
    )
    log(f'找到原图 {len(files)} 张')

    total = len(files)
    need_thumb = sum(1 for f in files if not has_thumb(f))
    need_prev = sum(1 for f in files if not has_preview(f))
    log(f'待生成 thumb={need_thumb}, preview={need_prev}')

    started = time.time()
    done_thumb = 0
    done_prev = 0
    skip_thumb = 0
    skip_prev = 0
    err_count = 0

    for i, f in enumerate(files):
        if not has_thumb(f):
            if make_thumb(f):
                done_thumb += 1
        else:
            skip_thumb += 1

        if not has_preview(f):
            if make_preview(f):
                done_prev += 1
        else:
            skip_prev += 1

        # 每 100 张打印一次进度
        if (i + 1) % 100 == 0:
            el = time.time() - started
            rate = (i + 1) / el if el > 0 else 0
            remain = (total - i - 1) / rate if rate > 0 else 0
            log(f'[{i+1}/{total}] thumb +{done_thumb} preview +{done_prev} err={err_count} 速率={rate:.1f}/s 剩余≈{remain/60:.1f}min')

        # 每张图让出 5ms CPU，避免和 Flask 抢太狠
        time.sleep(0.005)

    el = time.time() - started
    log('=' * 60)
    log(f'完成！耗时 {el/60:.1f}分钟 (PID={os.getpid()})')
    log(f'thumb 新生成 {done_thumb}, skip {skip_thumb}')
    log(f'preview 新生成 {done_prev}, skip {skip_prev}')
    log(f'错误 {err_count}')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log('用户中断')
        sys.exit(0)
