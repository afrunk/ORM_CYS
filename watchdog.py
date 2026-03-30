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
from pathlib import Path

# ============ 配置 ============
APP_HOST = "127.0.0.1"
APP_PORT = 8000
HEALTH_URL = f"http://{APP_HOST}:{APP_PORT}/health"
STARTUP_URL = f"http://{APP_HOST}:{APP_PORT}/"
STARTUP_TIMEOUT = 15  # 启动等待秒数
HEALTH_INTERVAL = 5   # 健康检查间隔秒数
MAX_RETRIES = 3       # 连续失败 N 次才重启（防止误判）
LOG_FILE = Path(__file__).parent / "watchdog.log"
MAX_LOG_LINES = 200  # 日志文件最大行数
# ==============================


def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)

    # 写入日志文件
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

        # 控制日志文件大小
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > MAX_LOG_LINES:
            with open(LOG_FILE, "w", encoding="utf-8") as f:
                f.writelines(lines[-MAX_LOG_LINES:])
    except Exception:
        pass


class FlaskWatcher:
    def __init__(self):
        self.process: subprocess.Popen | None = None
        self.fail_count = 0
        self.restart_count = 0

    def start_flask(self) -> bool:
        """启动 Flask 子进程，返回是否成功。"""
        import shutil

        python_bin = "python3" if shutil.which("python3") else sys.executable

        if self.process and self.process.poll() is None:
            log("Flask 进程仍在运行，先终止...")
            self.kill_flask()

        log("正在启动 Flask 应用...")
        try:
            self.process = subprocess.Popen(
                [python_bin, "app.py"],
                cwd=Path(__file__).parent,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            log(f"Flask 进程已启动 (PID: {self.process.pid})")
            return True
        except Exception as e:
            log(f"启动 Flask 失败：{e}")
            return False

    def kill_flask(self) -> None:
        """强制终止 Flask 进程。"""
        if not self.process:
            return

        try:
            if sys.platform == "win32":
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(self.process.pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                )
            else:
                pgid = os.getpgid(self.process.pid)
                os.killpg(pgid, signal.SIGTERM)
                self.process.wait(timeout=10)
            log("Flask 进程已终止")
        except Exception as e:
            log(f"终止进程时出错：{e}")
        finally:
            self.process = None

    def wait_for_ready(self) -> bool:
        """等待 Flask 启动就绪（请求成功或超时）。"""
        deadline = time.time() + STARTUP_TIMEOUT
        while time.time() < deadline:
            if self.process and self.process.poll() is not None:
                # 进程已退出
                return False
            try:
                resp = requests.get(STARTUP_URL, timeout=3)
                if resp.status_code < 500:
                    log("Flask 应用已就绪")
                    return True
            except requests.RequestException:
                pass
            time.sleep(1)
        return False

    def check_health(self) -> bool:
        """发送健康检查请求，返回 True 表示正常。"""
        try:
            resp = requests.get(HEALTH_URL, timeout=5)
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def run(self) -> None:
        log("=" * 40)
        log("Flask Watchdog 启动")
        log(f"健康检查地址：{HEALTH_URL}")
        log(f"失败阈值：连续 {MAX_RETRIES} 次")
        log("=" * 40)

        # 首次启动
        if not self.start_flask():
            log("首次启动失败，退出")
            sys.exit(1)
        if not self.wait_for_ready():
            log("Flask 启动超时，杀死并退出")
            self.kill_flask()
            sys.exit(1)

        # 主循环
        while True:
            time.sleep(HEALTH_INTERVAL)

            if self.process and self.process.poll() is not None:
                # 进程已崩溃退出
                retcode = self.process.poll()
                log(f"检测到 Flask 进程异常退出 (返回码: {retcode})")
                self.fail_count = MAX_RETRIES
            else:
                ok = self.check_health()
                if ok:
                    if self.fail_count > 0:
                        log(f"Flask 恢复正常 (之前连续失败 {self.fail_count} 次)")
                    self.fail_count = 0
                else:
                    self.fail_count += 1
                    log(f"Flask 健康检查失败 ({self.fail_count}/{MAX_RETRIES})")

            if self.fail_count >= MAX_RETRIES:
                self.restart_count += 1
                log(f"=" * 40)
                log(f"触发重启条件！第 {self.restart_count} 次自动重启")
                log("=" * 40)
                self.kill_flask()
                time.sleep(2)

                if self.start_flask():
                    if self.wait_for_ready():
                        self.fail_count = 0
                        log("Flask 重启成功，已恢复正常服务")
                    else:
                        log("Flask 重启后启动超时，稍后将重试...")
                        time.sleep(5)
                else:
                    log("重启命令执行失败，10 秒后重试...")
                    time.sleep(10)


if __name__ == "__main__":
    watcher = FlaskWatcher()
    try:
        watcher.run()
    except KeyboardInterrupt:
        log("收到 Ctrl+C，停止 Watchdog...")
        watcher.kill_flask()
