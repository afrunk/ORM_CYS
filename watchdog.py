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
APP_PORT = 5000
CHECK_HOST = "127.0.0.1"
HEALTH_URL = f"http://{CHECK_HOST}:{APP_PORT}/health"
STARTUP_URL = f"http://{CHECK_HOST}:{APP_PORT}/"
ONLINE_USERS_URL = f"http://{CHECK_HOST}:{APP_PORT}/metrics/online-users"

STARTUP_TIMEOUT = 20
HEALTH_INTERVAL = 120          # 借鉴 custom-ormfor5：放宽到 2 分钟，慢请求不再误杀
HEALTH_TIMEOUT = 8             # 单次超时（HEALTH_INTERVAL 放宽后可稍短）
MAX_RETRIES = 3                # 借鉴 custom-ormfor5：端口死了就直接重启，不要"先 N 次失败再重启"
KILL_TIMEOUT = 10
# 熔断：如果连续 CRASH_LIMIT 次重启后新进程在 READY_GRACE 秒内仍然立刻崩，
# 判定为不可恢复故障，停止 watchdog 重启，避免 1 秒一次循环浪费资源。
CRASH_LIMIT = 5
READY_GRACE = 60
# 卡顿检测：单次 /health 请求耗时超过 SLOW_THRESHOLD 秒算"慢"。
# 连续 SLOW_CONSECUTIVE 次慢请求视为"卡顿"，只打 WARN 不重启
# （重启解决不了慢的问题，反而会丢弃所有在线用户会话）。
SLOW_THRESHOLD = 5.0
SLOW_CONSECUTIVE = 3
# 在线用户统计上报间隔（秒）
ONLINE_REPORT_INTERVAL = 60

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
        self.crash_count = 0           # 连续启动后立即崩溃的次数
        self.last_start_time: float = 0.0
        self.output_reader: FlaskOutputReader | None = None
        # 标记当前 Flask 是否被 watchdog 自己主动终止（与"崩溃"区分），
        # 避免把正常 kill 当成异常退出，导致 kill 后立刻又重启、再 kill 的死循环。
        self.killed_by_us: bool = False
        # 卡顿检测：连续 SLOW_CONSECUTIVE 次响应慢时 +1，正常响应后清零。
        # 注意：slow_count 只触发 WARN 日志，不计入 fail_count，不重启。
        self.slow_count = 0
        # 在线用户数上报时间戳
        self.last_metrics_report: float = 0.0

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

    def _is_port_open(self) -> bool:
        """借鉴 custom-ormfor5：探测端口是否被监听。

        与 HTTP /health 探测的区别：
        - HTTP 探测会被 werkzeug 队列阻塞（慢请求时）
        - TCP 端口探测是 OS 内核直接返回，毫秒级
        如果进程死透了端口就立即没人 listen，返回 False。
        """
        import socket
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                return sock.connect_ex((CHECK_HOST, APP_PORT)) == 0
        except Exception:
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
        import secrets

        # 优先使用 venv 内的 python（项目依赖在 venv 里）；
        # 否则退回系统 python3。
        # 关键修复：之前始终用系统 python3 跑 app.py，结果缺依赖启动失败。
        venv_dir = Path(__file__).parent / "venv"
        venv_python = (
            venv_dir / "bin" / "python"
            if (venv_dir / "bin" / "python").exists()
            else None
        )
        if venv_python and venv_python.exists():
            python_bin = str(venv_python)
        elif shutil.which("python3"):
            python_bin = "python3"
        else:
            python_bin = sys.executable

        # 生成 watchdog 与 Flask 之间的共享 token，写到 instance/watchdog_token
        # 让 metrics 接口只能被本机 watchdog 读取，避免任意本地进程读到用户 IP。
        # 注：每次 watchdog 启动 Flask 时都重新生成，旧的自动失效。
        instance_dir = Path(__file__).parent / "instance"
        token_path = instance_dir / "watchdog_token"
        watchdog_token = secrets.token_urlsafe(32)
        try:
            instance_dir.mkdir(parents=True, exist_ok=True)
            token_path.write_text(watchdog_token, encoding="utf-8")
            # 让 metrics 端点能读到
            self._watchdog_token = watchdog_token
        except OSError as e:
            log(f"[START] 无法写 watchdog_token: {e}，metrics 接口将拒绝所有本地请求", "WARNING")
            self._watchdog_token = ""

        log("=" * 60)
        log(f"[START] 准备启动 Flask 应用...")
        log(f"        Python: {python_bin}")
        log(f"        监听地址: {BIND_HOST}:{APP_PORT}")
        log("=" * 60)

        # 把 token 通过环境变量传给 Flask 子进程
        env = os.environ.copy()
        if self._watchdog_token:
            env["CRM_WATCHDOG_TOKEN"] = self._watchdog_token

        # ============================================================
        # 历史教训（2026-07-04）：
        # 之前用 stdout=subprocess.PIPE 把 Flask stdout 接进管道，
        # 然后 watchdog 每 HEALTH_INTERVAL=120s 才 drain 一次。
        # 高并发时管道（默认 64KB）一旦塞满，Flask 内部 logger 的
        # StreamHandler.emit() 就会在 pipe_write 上 sleep —— 而它
        # 同时持有 Handler.lock。结果是所有 werkzeug 请求线程都卡在
        # logging.Handler.acquire()，连 /health 都返回不了。
        #
        # 修复：stdout 直接重定向到 logs/app.out（真实文件，page cache
        # 可以缓冲，永远不会因为消费者跟不上而阻塞 Flask）。
        # ============================================================
        flask_stdout_path = LOG_DIR / "logs" / "app.out"
        flask_stdout_path.parent.mkdir(parents=True, exist_ok=True)
        flask_stdout_file = open(flask_stdout_path, "ab", buffering=0)  # 行缓冲不阻塞
        try:
            self.process = subprocess.Popen(
                [python_bin, "app.py"],
                cwd=Path(__file__).parent,
                stdout=flask_stdout_file,    # ← 真实文件，不再用 PIPE
                stderr=flask_stdout_file,    # ← 也直接进同一文件，方便排查
                start_new_session=True,
                env=env,
                # 不再需要 text=True / bufsize=1：二进制流到文件
            )
            self._flask_stdout_file = flask_stdout_file
        except Exception:
            flask_stdout_file.close()
            raise
        self.pid = self.process.pid
        self.killed_by_us = False  # 新一轮生命周期，重置主动终止标志
        # 旧版 FlaskOutputReader 设计给 PIPE 用；现在写文件，不需要它
        self.output_reader = None
        log(f"[START] Flask 进程已启动 (PID: {self.pid})")
        log(f"[START] Flask stdout → {flask_stdout_path}（直写文件，不再走管道）")
        self.last_start_time = time.time()
        return True

    def kill_flask(self) -> None:
        """强制终止 Flask 进程及其所有子进程。"""
        if not self.pid:
            log("[KILL] 没有记录的进程 PID，跳过")
            return

        # 关键修复：在发送信号前先打上"主动终止"标记，
        # 主循环看到 process 退出时就不会把它误判为"崩溃 → 立刻重启"。
        self.killed_by_us = True
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
                resp = requests.get(STARTUP_URL, timeout=HEALTH_TIMEOUT)
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

    def probe_health(self) -> tuple[bool, float, int]:
        """真探测 /health：返回 (ok, latency_seconds, status_code)。

        ok=True 仅当：进程存活 且 /health 在 HEALTH_TIMEOUT 内返回 2xx。
        """
        if self.process is None or self.process.poll() is not None:
            return (False, 0.0, 0)
        start = time.monotonic()
        try:
            resp = requests.get(HEALTH_URL, timeout=HEALTH_TIMEOUT)
            latency = time.monotonic() - start
            if 200 <= resp.status_code < 300:
                return (True, latency, resp.status_code)
            return (False, latency, resp.status_code)
        except requests.exceptions.Timeout:
            return (False, time.monotonic() - start, 0)
        except Exception:
            return (False, time.monotonic() - start, 0)

    def check_health(self) -> bool:
        """仅检查 Flask 进程是否存在（兼容原方法）。"""
        if self.process is None or self.process.poll() is not None:
            log("[HEALTH] Flask 进程不存在", "WARNING")
            return False
        try:
            result = subprocess.run(
                ["pgrep", "-f", f"python.*app.py"],
                capture_output=True,
                text=True,
                timeout=3
            )
            if result.returncode == 0:
                return True
            log("[HEALTH] Flask 进程未找到", "WARNING")
            return False
        except subprocess.TimeoutExpired:
            log("[HEALTH] 进程检查超时", "WARNING")
            return False
        except Exception:
            return False

    def report_online_users(self) -> None:
        """拉一次 /metrics/online-users，把在线 IP 数和列表打到 watchdog.log。

        调用方需要保证 self.last_metrics_report 非零（避免启动后立即打一行）。
        自动带 X-Watchdog-Token 头，由 Flask 端校验。
        """
        try:
            headers = {}
            token = getattr(self, "_watchdog_token", "")
            if token:
                headers["X-Watchdog-Token"] = token
            resp = requests.get(ONLINE_USERS_URL, timeout=5, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                count = data.get("count", 0)
                ips = data.get("ips", [])
                ttl = data.get("ttl_seconds", 0)
                log(f"[ONLINE] 当前在线用户数={count} (TTL={ttl}s, IP: {ips})")
            elif resp.status_code == 403:
                log("[ONLINE] metrics 接口拒绝访问 (403)，token 不匹配或未配置", "WARNING")
            else:
                log(f"[ONLINE] metrics 返回 {resp.status_code}", "WARNING")
        except requests.exceptions.Timeout:
            log("[ONLINE] metrics 请求超时，跳过本轮上报", "WARNING")
        except Exception as e:
            log(f"[ONLINE] 上报失败: {type(e).__name__} - {e}", "WARNING")

    def run(self) -> None:
        log("=" * 60)
        log("Flask Watchdog 启动")
        log(f"日志文件: {LOG_FILE}")
        log(f"日志轮转: {LOG_MAX_BYTES // (1024*1024)}MB/文件，保留 {LOG_BACKUP_COUNT} 个备份")
        log(f"检查地址: {CHECK_HOST}:{APP_PORT}")
        log(f"外部地址: {BIND_HOST}:{APP_PORT}")
        log(f"健康检查: 进程检查 (pgrep)")
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

            # 如果当前进程已稳定运行超过 READY_GRACE 秒，重置熔断计数
            if (
                self.process
                and self.process.poll() is None
                and self.last_start_time
                and (time.time() - self.last_start_time) > READY_GRACE
                and self.crash_count > 0
            ):
                log(f"[WATCH] 进程已稳定运行 {READY_GRACE} 秒以上，重置熔断计数")
                self.crash_count = 0

            # 检查进程状态
            if self.process and self.process.poll() is not None:
                retcode = self.process.poll()
                alive_for = time.time() - self.last_start_time if self.last_start_time else 0
                if self.killed_by_us:
                    # 是我们自己刚刚 kill 的，不算异常退出，避免无意义重启。
                    log(f"[WATCH] Flask 已被主动终止 (存活 {alive_for:.1f} 秒)")
                    # 不动 fail_count，让上层决定何时（是否）重启。
                else:
                    log(f"[WATCH] 检测到 Flask 进程异常退出 (返回码: {retcode}, 存活 {alive_for:.1f} 秒)", "WARNING")
                    self.fail_count = MAX_RETRIES
                    # 熔断：短时间内连续崩溃，直接放弃重启
                    if alive_for < READY_GRACE:
                        self.crash_count += 1
                        if self.crash_count >= CRASH_LIMIT:
                            log("=" * 60, "ERROR")
                            log(f"[FATAL] 连续 {self.crash_count} 次启动后立即崩溃，疑似配置/代码故障", "ERROR")
                            log("[FATAL] 停止自动重启，请人工排查日志后手动恢复", "ERROR")
                            log("=" * 60, "ERROR")
                            self.kill_flask()
                            sys.exit(2)
                    else:
                        self.crash_count = 0
            else:
                # 进程存活 → 真探测 /health 的响应时间
                # 借鉴 custom-ormfor5 的设计哲学：
                # 1) 端口活着 ≠ 一定健康，但端口死了 = 一定不健康
                # 2) 慢请求/超时不算"故障"，只 WARN
                # 3) 只有"HTTP 持续 500"或"连接被拒绝"才重启
                ok, latency, code = self.probe_health()
                if ok:
                    if self.fail_count > 0:
                        log(f"[WATCH] Flask 恢复正常 (之前连续失败 {self.fail_count} 次)")
                    self.fail_count = 0
                    # 卡顿检测：仅记录 + 打 WARN，不触发重启
                    if latency > SLOW_THRESHOLD:
                        self.slow_count += 1
                        if self.slow_count >= SLOW_CONSECUTIVE:
                            log(
                                f"[SLOW] Flask 响应慢: 连续 {self.slow_count} 次耗时 > {SLOW_THRESHOLD}s "
                                f"(本次 {latency:.2f}s, HTTP {code})。建议检查数据库/磁盘/CPU，"
                                f"但不会自动重启（重启解决不了慢，反而丢失在线会话）",
                                "WARNING",
                            )
                    else:
                        if self.slow_count > 0:
                            log(f"[SLOW] 恢复正常 (之前连续慢 {self.slow_count} 次)")
                        self.slow_count = 0
                elif code == 0:
                    # code=0 = 连接被拒绝或超时（端口可能死了或 werkzeug 队列卡死）
                    # 借鉴 custom-ormfor5：先确认端口是否真的没监听（=进程死了）
                    # 如果端口还活着，只是慢/超时，就只 WARN 不重启
                    if self._is_port_open():
                        self.slow_count += 1
                        log(
                            f"[WATCH] /health 超时/失败但端口仍存活 "
                            f"(latency={latency:.2f}s, slow_count={self.slow_count}) "
                            f"——只 WARN 不重启",
                            "WARNING",
                        )
                    else:
                        # 端口死了 = 进程真的挂了 → 立即计入 fail_count
                        self.fail_count += 1
                        log(
                            f"[WATCH] 端口 {APP_PORT} 已不监听 (fail_count={self.fail_count}/{MAX_RETRIES})",
                            "ERROR",
                        )
                else:
                    # code != 0 (例如 5xx)：HTTP 真出错了，但和 custom-ormfor5 一样
                    # 连续多次 5xx 才重启
                    self.fail_count += 1
                    log(f"[WATCH] Flask 返回 HTTP {code} ({self.fail_count}/{MAX_RETRIES}, latency={latency:.2f}s)", "WARNING")

            # 在线用户统计：每 ONLINE_REPORT_INTERVAL 秒打一次
            now = time.time()
            if self.last_metrics_report > 0 and now - self.last_metrics_report >= ONLINE_REPORT_INTERVAL:
                self.report_online_users()
                self.last_metrics_report = now
            elif self.last_metrics_report == 0:
                # 第一次循环开始计时，下一个周期才打
                self.last_metrics_report = now

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
                        # 仅当新进程稳定运行超过 READY_GRACE 秒，才认为恢复正常
                        # 否则下一次崩溃会被计入熔断
                        log(f"[RESTART] 成功！Flask 已恢复正常服务（将观察 {READY_GRACE} 秒确认稳定）")
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
