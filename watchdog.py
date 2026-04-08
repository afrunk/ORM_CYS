"""
Flask 应用监控脚本：检测 500 错误后自动重启进程。

工作原理：
1. 启动 Flask 应用作为子进程
2. 定期向健康检查端点发送请求
3. 若连续 N 次请求失败（超时或非 200），判定进程已崩溃
4. 杀死旧进程，重新启动
"""
import subprocess
import time
import sys
import os
import signal
import requests
import logging
import logging.handlers
from pathlib import Path
from datetime import datetime
from threading import Lock
import io

# ============ 配置 ============
BIND_HOST = "0.0.0.0"
APP_PORT = 8000
CHECK_HOST = "127.0.0.1"
HEALTH_URL = f"http://{CHECK_HOST}:{APP_PORT}/health"
STARTUP_URL = f"http://{CHECK_HOST}:{APP_PORT}/"

STARTUP_TIMEOUT = 20
HEALTH_INTERVAL = 5
MAX_RETRIES = 3
KILL_TIMEOUT = 10

LOG_DIR = Path(__file__).parent
LOG_FILE = LOG_DIR / "watchdog.log"
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_COUNT = 5
# ==============================

# 全局日志锁，确保线程安全
_log_lock = Lock()

# 全局 logger 引用（延迟初始化）
_logger: logging.Logger | None = None


def _ensure_log_file() -> None:
    """确保日志文件存在，如果被删除则重建。"""
    if not LOG_FILE.exists():
        LOG_FILE.touch()
        LOG_FILE.chmod(0o644)


def _get_logger() -> logging.Logger:
    """获取或创建 logger 实例（单例模式）。"""
    global _logger

    if _logger is not None:
        return _logger

    _ensure_log_file()

    logger = logging.getLogger("Watchdog")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # RotatingFileHandler - 自动轮转
    file_handler = logging.handlers.RotatingFileHandler(
        filename=str(LOG_FILE),
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
        delay=True,
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # StreamHandler - 同时输出到 stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    _logger = logger
    return logger


def log(msg: str, level: str = "INFO") -> None:
    """线程安全的日志写入函数。"""
    _ensure_log_file()

    with _log_lock:
        logger = _get_logger()
        log_func = getattr(logger, level.lower(), logger.info)
        log_func(msg)


def log_flask_output(line: str) -> None:
    """记录 Flask 子进程的输出。"""
    if not line.strip():
        return
    _ensure_log_file()

    with _log_lock:
        logger = _get_logger()
        logger.debug(f"[FLASK] {line.strip()}")


class FlaskOutputReader:
    """非阻塞读取子进程输出并写入日志。"""

    def __init__(self, process: subprocess.Popen):
        self.process = process
        self.buffer = io.StringIO()

    def read_available(self) -> None:
        """读取所有可用的输出（非阻塞）。"""
        if self.process.stdout is None:
            return

        import select

        try:
            if sys.platform != "win32":
                # Unix 系统：使用 select 检测是否有数据可读
                if select.select([self.process.stdout], [], [], 0.1)[0]:
                    line = self.process.stdout.readline()
                    if line:
                        log_flask_output(line)
            else:
                # Windows 系统：直接读取
                while True:
                    char = self.process.stdout.read(1)
                    if not char:
                        break
                    if char == "\n":
                        log_flask_output(self.buffer.getvalue())
                        self.buffer = io.StringIO()
                    else:
                        self.buffer.write(char)
        except Exception:
            pass


class FlaskWatcher:
    def __init__(self):
        self.process: subprocess.Popen | None = None
        self.pid: int | None = None
        self.fail_count = 0
        self.restart_count = 0
        self.output_reader: FlaskOutputReader | None = None

    def _is_process_alive(self, pid: int) -> bool:
        """检查指定 PID 的进程是否存活。"""
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _get_child_pids(self, pid: int) -> list[int]:
        """获取指定进程的所有直接子进程 PID。"""
        child_pids = []
        try:
            result = subprocess.run(
                ["ps", "--ppid", str(pid), "-o", "pid=", "--no-headers"],
                capture_output=True,
                text=True,
                timeout=5
            )
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if line:
                    child_pids.append(int(line))
        except Exception:
            pass
        return child_pids

    def _safe_kill(self, pid: int, sig: int = signal.SIGTERM) -> bool:
        """安全地向进程发送信号，返回是否成功。"""
        try:
            os.kill(pid, sig)
            return True
        except OSError:
            return False

    def _redirect_flask_output(self) -> None:
        """将 Flask 输出重定向到日志文件。"""
        if self.process and self.process.stdout:
            try:
                log_file = open(LOG_FILE, "a", encoding="utf-8")
                # 创建 Tee 风格的写入器
                original_write = log_file.write

                def tee_write(s: str) -> int:
                    original_write(s)
                    if _logger:
                        for line in s.split("\n"):
                            if line.strip():
                                log_flask_output(line)
                    return len(s)

                log_file.write = tee_write
                self.process.stdout = log_file
            except Exception as e:
                log(f"[START] 重定向 Flask 输出失败: {e}", "WARNING")

    def start_flask(self) -> bool:
        """启动 Flask 子进程，返回是否成功。"""
        import shutil

        python_bin = "python3" if shutil.which("python3") else sys.executable

        log("=" * 60)
        log(f"[START] 准备启动 Flask 应用...")
        log(f"        Python: {python_bin}")
        log(f"        监听地址: {BIND_HOST}:{APP_PORT}")
        log("=" * 60)

        try:
            self.process = subprocess.Popen(
                [python_bin, "app.py"],
                cwd=Path(__file__).parent,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                text=True,
                bufsize=1,
            )
            self.pid = self.process.pid
            self.output_reader = FlaskOutputReader(self.process)
            log(f"[START] Flask 进程已启动 (PID: {self.pid})")
            return True
        except Exception as e:
            log(f"[START] 启动 Flask 失败: {e}", "ERROR")
            return False

    def kill_flask(self) -> None:
        """强制终止 Flask 进程及其所有子进程。"""
        if not self.pid:
            log("[KILL] 没有记录的进程 PID，跳过")
            return

        log(f"[KILL] 开始终止进程树，根 PID: {self.pid}")

        # 关闭输出重定向
        if self.output_reader:
            self.output_reader = None

        # 第一步：收集进程信息
        child_pids = self._get_child_pids(self.pid)
        all_pids = [self.pid] + child_pids
        log(f"[KILL] 进程树包含: {all_pids}")
        if child_pids:
            log(f"[KILL] 子进程: {child_pids}")

        # 第二步：发送 SIGTERM（优雅终止）
        log("[KILL] 步骤 1/3: 发送 SIGTERM 信号...")
        for pid in all_pids:
            self._safe_kill(pid, signal.SIGTERM)

        time.sleep(2)

        # 第三步：检查是否还有存活的进程
        alive_pids = [pid for pid in all_pids if self._is_process_alive(pid)]
        if alive_pids:
            log(f"[KILL] 仍有进程存活: {alive_pids}，发送 SIGKILL 强制终止...")
            for pid in alive_pids:
                self._safe_kill(pid, signal.SIGKILL)

        # 第四步：验证所有进程已终止
        time.sleep(1)
        still_alive = [pid for pid in all_pids if self._is_process_alive(pid)]
        if still_alive:
            log(f"[KILL] 警告: 以下进程仍无法终止: {still_alive}", "WARNING")
        else:
            log("[KILL] 所有进程已成功终止")

        # 第五步：等待 subprocess poll 结果
        if self.process:
            try:
                self.process.wait(timeout=KILL_TIMEOUT)
                log(f"[KILL] subprocess 已回收，返回码: {self.process.returncode}")
            except subprocess.TimeoutExpired:
                log("[KILL] subprocess.wait 超时，强制终止")
                self.process.kill()
            finally:
                self.process = None
                self.pid = None

    def wait_for_ready(self) -> bool:
        """等待 Flask 启动就绪（请求成功或超时）。"""
        log("[READY] 开始等待 Flask 启动就绪...")
        deadline = time.time() + STARTUP_TIMEOUT
        attempt = 0

        while time.time() < deadline:
            attempt += 1

            # 读取 Flask 输出
            if self.output_reader:
                self.output_reader.read_available()

            # 检查进程是否已退出
            if self.process and self.process.poll() is not None:
                retcode = self.process.poll()
                log(f"[READY] Flask 进程已异常退出 (返回码: {retcode})", "ERROR")
                return False

            try:
                log(f"[READY] 尝试连接 ({attempt}): {STARTUP_URL}")
                resp = requests.get(STARTUP_URL, timeout=5)
                if resp.status_code < 500:
                    log(f"[READY] Flask 应用已就绪 (HTTP {resp.status_code})")
                    return True
                else:
                    log(f"[READY] 服务器返回错误码: {resp.status_code}", "WARNING")
            except requests.exceptions.Timeout:
                log("[READY] 连接超时 (5秒)，继续等待...")
            except ConnectionRefusedError:
                log("[READY] 连接被拒绝，Flask 尚未开始监听...")
            except requests.exceptions.RequestException as e:
                log(f"[READY] 请求异常: {type(e).__name__} - {e}", "WARNING")

            time.sleep(1)

        log(f"[READY] 启动超时 ({STARTUP_TIMEOUT}秒)", "WARNING")
        return False

    def check_health(self) -> bool:
        """发送健康检查请求，返回 True 表示正常。"""
        try:
            resp = requests.get(HEALTH_URL, timeout=5)
            return resp.status_code == 200
        except requests.exceptions.Timeout:
            log("[HEALTH] 健康检查超时 (5秒)", "WARNING")
            return False
        except ConnectionRefusedError:
            log("[HEALTH] 连接被拒绝", "WARNING")
            return False
        except requests.exceptions.RequestException:
            return False

    def run(self) -> None:
        log("=" * 60)
        log("Flask Watchdog 启动")
        log(f"日志文件: {LOG_FILE}")
        log(f"日志轮转: {LOG_MAX_BYTES // (1024*1024)}MB/文件，保留 {LOG_BACKUP_COUNT} 个备份")
        log(f"检查地址: {CHECK_HOST}:{APP_PORT}")
        log(f"外部地址: {BIND_HOST}:{APP_PORT}")
        log(f"健康检查: {HEALTH_URL}")
        log(f"失败阈值: 连续 {MAX_RETRIES} 次")
        log("=" * 60)

        # 首次启动
        if not self.start_flask():
            log("[ERROR] 首次启动失败，退出", "ERROR")
            sys.exit(1)

        if not self.wait_for_ready():
            log("[ERROR] Flask 启动超时，正在终止...", "ERROR")
            self.kill_flask()
            sys.exit(1)

        log("[OK] Flask 应用运行正常，进入监控循环")

        # 主循环
        while True:
            time.sleep(HEALTH_INTERVAL)

            # 读取 Flask 输出
            if self.output_reader:
                self.output_reader.read_available()

            # 检查进程状态
            if self.process and self.process.poll() is not None:
                retcode = self.process.poll()
                log(f"[WATCH] 检测到 Flask 进程异常退出 (返回码: {retcode})", "WARNING")
                self.fail_count = MAX_RETRIES
            else:
                ok = self.check_health()
                if ok:
                    if self.fail_count > 0:
                        log(f"[WATCH] Flask 恢复正常 (之前连续失败 {self.fail_count} 次)")
                    self.fail_count = 0
                else:
                    self.fail_count += 1
                    log(f"[WATCH] Flask 健康检查失败 ({self.fail_count}/{MAX_RETRIES})", "WARNING")

            # 触发重启
            if self.fail_count >= MAX_RETRIES:
                self.restart_count += 1
                log("=" * 60)
                log(f"[RESTART] 第 {self.restart_count} 次自动重启", "WARNING")
                log("=" * 60)

                log("[RESTART] 步骤 1: 终止旧进程...")
                self.kill_flask()

                log("[RESTART] 步骤 2: 等待 3 秒让端口释放...")
                time.sleep(3)

                log("[RESTART] 步骤 3: 启动新进程...")
                if self.start_flask():
                    log("[RESTART] 步骤 4: 等待新进程就绪...")
                    if self.wait_for_ready():
                        self.fail_count = 0
                        log("[RESTART] 成功！Flask 已恢复正常服务")
                    else:
                        log("[RESTART] 新进程启动超时，稍后将重试...", "WARNING")
                        time.sleep(5)
                else:
                    log("[RESTART] 启动命令执行失败，10 秒后重试...", "WARNING")
                    time.sleep(10)


if __name__ == "__main__":
    log("=" * 60)
    log("Watchdog 进程启动")
    log("=" * 60)

    watcher = FlaskWatcher()
    try:
        watcher.run()
    except KeyboardInterrupt:
        log("收到 Ctrl+C，停止 Watchdog...")
        watcher.kill_flask()
    except Exception as e:
        log(f"Watchdog 发生未捕获异常: {e}", "ERROR")
        raise
