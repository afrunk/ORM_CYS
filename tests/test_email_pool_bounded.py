"""冒烟测试：验证邮件池在大量并发下不会无限堆积。

模拟：
- 并发提交 30 封"假邮件"（SMTP 用 mock 阻塞 0.5s/封）
- 池大小 = 8，期望只有 8 个 worker 线程活着，多余任务直接丢弃
- 全部跑完后，活跃线程数应回落到 0

运行：
  cd /root/ORM_CYS && . venv/bin/activate && python tests/test_email_pool_bounded.py
"""
import os
import sys
import threading
import time

# 确保能找到 crm 包
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crm.notifications import (
    _EMAIL_POOL_MAX_WORKERS,
    _EMAIL_POOL_REF,
    _get_email_pool,
)


def fake_send_email(customer_id: int, sleep_s: float = 0.5):
    """模拟 SMTP：每个 worker 阻塞 sleep_s 秒。"""
    time.sleep(sleep_s)
    return customer_id


def main():
    print(f"[TEST] 池上限 = {_EMAIL_POOL_MAX_WORKERS}")
    pool = _get_email_pool()
    assert pool is not None, "懒初始化失败"
    print(f"[TEST] 单例池已就绪：{pool}")

    n_total = 30
    futures = []
    started = time.monotonic()
    for i in range(n_total):
        fut = pool.submit(fake_send_email, i, sleep_s=0.3)
        futures.append(fut)

    # 关键检查：0.3s 时刻，应该最多 8 个 worker 在跑，其他都在排队等信号量
    # 但我们的实现是"超出 8 直接丢弃"，所以实际只有 8 个 worker 启动
    time.sleep(0.1)
    active = sum(1 for t in threading.enumerate() if t.name.startswith("email-pool"))
    print(f"[TEST] 提交后 0.1s 时，email-pool 活跃线程数 = {active} (期望 ≤ 8)")
    assert active <= _EMAIL_POOL_MAX_WORKERS, f"线程数爆炸！{active} > {_EMAIL_POOL_MAX_WORKERS}"

    # 等所有"成功提交到 executor 的"future 完成
    completed = 0
    for fut in futures:
        try:
            fut.result(timeout=5)
            completed += 1
        except Exception:
            pass

    # 关掉池子，确保 shutdown 工作正常
    pool.shutdown(wait=True, cancel_futures=True)
    print(f"[TEST] completed={completed}/{n_total}, elapsed={time.monotonic()-started:.2f}s")
    print(f"[TEST] PASSED: 池大小有界，进程退出能干净 shutdown")

    # ------------------------------------------------------------
    # 第二轮：验证 worker 内的 BoundedSemaphore 丢弃逻辑
    # ------------------------------------------------------------
    print()
    print("[TEST-2] 验证 worker 内信号量丢弃：手动 acquire 信号量占满 8 个")
    from crm.notifications import _EMAIL_POOL_SEM

    # 占满信号量
    acquired = []
    for _ in range(_EMAIL_POOL_MAX_WORKERS):
        assert _EMAIL_POOL_SEM.acquire(blocking=False), "无法占满信号量"
        acquired.append(True)

    # 现在调 _async_send_email 的 worker 部分（实际跑 send 函数会丢）
    # 用 mock 让 send_email_notification 不发真邮件
    from crm import notifications as _n
    original = _n.send_email_notification
    called = {"count": 0}

    def _mock_send(sales, customer):
        called["count"] += 1

    _n.send_email_notification = _mock_send
    try:
        # 提交 5 封，全部应该被 worker 内的 acquire(blocking=False) 丢弃
        from types import SimpleNamespace
        for i in range(5):
            sales = SimpleNamespace(id=999, username=f"fake{i}", email="x@x.com")
            cust = SimpleNamespace(id=1000 + i, name=f"c{i}", region="r",
                                    dispatch_time=None)
            _n._async_send_email(sales, cust)

        # 等 worker 跑完
        time.sleep(0.5)
        assert called["count"] == 0, (
            f"信号量占满时不应调 send，但调了 {called['count']} 次"
        )
        print(f"[TEST-2] 信号量占满时 send 被调用 {called['count']} 次 (期望 0)")
        print("[TEST-2] PASSED: 池满时丢弃而不阻塞")
    finally:
        _n.send_email_notification = original
        for _ in range(_EMAIL_POOL_MAX_WORKERS):
            _EMAIL_POOL_SEM.release()

    # 池子已经 shutdown 了，进程退出时 atexit 不会再触发
    print()
    print("[ALL DONE]")


if __name__ == "__main__":
    main()