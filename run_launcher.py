"""
Flask 应用守护进程：自动检测端口/500 错误并重启。

功能：
  - 启动 Flask 服务（默认 0.0.0.0:5000）
  - 每 30 秒对健康检查端点发起一次请求
  - 遇到连接拒绝（进程崩溃）或 HTTP 500 时，立即杀掉旧进程并重新启动
  - 日志输出到 logs/launcher.log（同样使用 RotatingFileHandler）

用法（项目根目录下执行）：
    python run_launcher.py
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ============ 配置 ============
HOST = "127.0.0.1"
PORT = 5000
CHECK_INTERVAL = 300          # 健康检查间隔（秒）
HEALTH_ENDPOINT = f"http://{HOST}:{PORT}/login"  # 使用登录页作为存活检查
KILL_WAIT = 3               # 杀掉进程后等待秒数再重启
LAUNCHER_LOG = os.path.join(os.path.dirname(__file__), "logs", "launcher.log")

# ============ 日志配置 ============
def _setup_logging() -> logging.Logger:
    os.makedirs(os.path.dirname(LAUNCHER_LOG), exist_ok=True)
    logger = logging.getLogger("launcher")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = RotatingFileHandler(LAUNCHER_LOG, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    fh.setFormatter(formatter)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def _is_port_open(host: str, port: int) -> bool:
    """检测端口是否被监听。"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3)
    try:
        result = sock.connect_ex((host, port))
        return result == 0
    finally:
        sock.close()


def _check_http_status(logger: logging.Logger) -> bool:
    """请求健康检查端点，检测是否返回 500。"""
    try:
        req = Request(HEALTH_ENDPOINT, headers={"User-Agent": "LauncherHealthCheck/1.0"})
        with urlopen(req, timeout=5) as resp:
            status = resp.getcode()
            if status == 500:
                logger.warning(f"健康检查返回 HTTP 500，将重启服务")
                return False
            return True
    except HTTPError as e:
        if e.code == 500:
            logger.warning(f"健康检查返回 HTTP {e.code}，将重启服务")
            return False
        return True
    except (URLError, OSError) as e:
        logger.warning(f"健康检查连接失败（{type(e).__name__}: {e}），将重启服务")
        return False


def _kill_process_by_port(logger: logging.Logger) -> None:
    """通过端口号杀掉对应进程（Windows/Linux 兼容）。"""
    if sys.platform == "win32":
        try:
            # Windows: 使用 netstat + taskkill
            result = subprocess.run(
                f'netstat -ano | findstr :{PORT} | findstr LISTENING',
                shell=True, capture_output=True, text=True
            )
            for line in result.stdout.strip().splitlines():
                parts = line.split()
                if parts and parts[-1].isdigit():
                    pid = parts[-1]
                    logger.info(f"正在杀掉旧进程 PID={pid} ...")
                    subprocess.run(f"taskkill /F /PID {pid}", shell=True, check=False)
        except Exception as e:
            logger.warning(f"taskkill 执行异常: {e}")
    else:
        try:
            # Linux/macOS: 使用 fuser 或 lsof
            result = subprocess.run(f"fuser -k {PORT}/tcp", shell=True, capture_output=True, text=True)
            logger.info(f"fuser -k 输出: {result.stdout.strip()}")
        except Exception:
            try:
                result = subprocess.run(
                    f"lsof -ti:{PORT} | xargs kill -9",
                    shell=True, capture_output=True, text=True
                )
            except Exception as e:
                logger.warning(f"fuser/lsof 清理异常: {e}")


def _launch_flask(logger: logging.Logger) -> subprocess.Popen:
    """启动 Flask 子进程，返回进程对象。"""
    # 优先使用当前目录下的 .venv
    venv_python = os.path.join(os.path.dirname(__file__), ".venv", "Scripts", "python.exe")
    if not os.path.isfile(venv_python):
        venv_python = sys.executable  # fallback 到当前 Python

    log_file = os.path.join(os.path.dirname(__file__), "logs", "app.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # 重定向 stdout/stderr 到 app.log
    fout = open(log_file, "a", encoding="utf-8")
    ferr = open(log_file, "a", encoding="utf-8")

    proc = subprocess.Popen(
        [venv_python, "app.py"],
        cwd=os.path.dirname(__file__),
        stdout=fout,
        stderr=ferr,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
    )
    logger.info(f"Flask 已启动，PID={proc.pid}，日志 → {log_file}")
    return proc


def _wait_until_ready(logger: logging.Logger, timeout: int = 30) -> bool:
    """等待服务端口就绪。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _is_port_open(HOST, PORT):
            time.sleep(1)  # 稍微等一下确保完全就绪
            return True
        time.sleep(1)
    return False


def main() -> None:
    logger = _setup_logging()
    logger.info(f"=" * 50)
    logger.info(f"Flask 守护进程启动 | 检查间隔={CHECK_INTERVAL}s | 端口={PORT}")
    logger.info(f"=" * 50)

    proc: subprocess.Popen | None = None
    restart_count = 0

    while True:
        # --- 启动阶段 ---
        if proc is None or proc.poll() is not None:
            proc = _launch_flask(logger)
            ready = _wait_until_ready(logger)
            if not ready:
                logger.error("Flask 启动后端口未就绪，继续等待...")
                time.sleep(CHECK_INTERVAL)
                continue
            logger.info("Flask 服务已就绪")

        # --- 健康检查阶段 ---
        time.sleep(CHECK_INTERVAL)

        if proc.poll() is not None:
            logger.warning("Flask 进程已退出（退出码=%s），准备重启", proc.returncode)
            proc = None
            restart_count += 1
            logger.info(f"重启次数: {restart_count}")
            continue

        if not _is_port_open(HOST, PORT):
            logger.warning("端口 %s 未监听，Flask 可能已崩溃，准备重启", PORT)
            proc = None
            restart_count += 1
            logger.info(f"重启次数: {restart_count}")
            continue

        if not _check_http_status(logger):
            logger.warning("HTTP 500 错误，准备重启")
            _kill_process_by_port(logger)
            proc = None
            restart_count += 1
            logger.info(f"重启次数: {restart_count}")
            time.sleep(KILL_WAIT)
            continue

        logger.info(f"健康检查 OK | PID={proc.pid if proc else 'N/A'}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n守护进程已停止")
        sys.exit(0)
