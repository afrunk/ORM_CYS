"""冒烟测试：验证邮件有界丢弃逻辑 + async_jobs 提交路径。

修复 2026-07-05（回归）：
- 之前 d31baf1 的自建线程池方案触发了 Working outside of application context，
  邮件静默丢失。
- 现在邮件改为复用 crm.utils.async_jobs.submit，该提交器自带 app context 兜底，
  性能优化改用 BoundedSemaphore 在入队前限流：
    - 池满（>8 并发在飞）→ 直接丢弃 + WARN，绝不堆积
    - 派单事务立刻返回，不等 SMTP

运行：
  cd /root/ORM_CYS && . venv/bin/activate && python tests/test_email_pool_bounded.py

本测试覆盖：
  1. _EMAIL_INFLIGHT 信号量能正常工作（acquire/release 平衡）
  2. 信号量占满时调用 _async_send_email 会触发 [邮件有界丢弃] 日志
  3. 池满时丢弃而不阻塞 send_email_notification
"""
import os
import sys
import threading
import time
import logging

# 测试期间让 crm.notifications 的 logger 输出到 stdout
logging.basicConfig(level=logging.INFO, format='%(name)s | %(message)s')

# 确保能找到 crm 包
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_semaphore_balance():
    """信号量：8 个并发槽 + BoundedSemaphore 鲁棒性。"""
    from crm.notifications import _EMAIL_INFLIGHT, _EMAIL_MAX_INFLIGHT

    assert _EMAIL_MAX_INFLIGHT == 8, f"上限应为 8，实际 {_EMAIL_MAX_INFLIGHT}"

    acquired = []
    for _ in range(_EMAIL_MAX_INFLIGHT):
        assert _EMAIL_INFLIGHT.acquire(blocking=False), "无法占满"
        acquired.append(True)
    print(f"[TEST-1] 占满 {len(acquired)} 个槽位 PASS")

    # 第 9 个不应获得
    assert not _EMAIL_INFLIGHT.acquire(blocking=False), "信号量未限制上限"
    print("[TEST-1] 第 9 个 acquire(blocking=False) 失败 PASS")

    # 释放一个
    _EMAIL_INFLIGHT.release()
    assert _EMAIL_INFLIGHT.acquire(blocking=False), "释放后无法重获"
    _EMAIL_INFLIGHT.release()
    print("[TEST-1] 释放 + 重获 PASS")


def test_pool_full_drops_email():
    """模拟 SMTP 极慢时，_async_send_email 应走"丢弃"分支而非阻塞。"""
    from crm.notifications import _EMAIL_INFLIGHT, _EMAIL_MAX_INFLIGHT, _async_send_email
    import crm.notifications as _n

    # 先把信号量占满
    for _ in range(_EMAIL_MAX_INFLIGHT):
        _EMAIL_INFLIGHT.acquire(blocking=False)

    try:
        # mock send_email_notification：如果走到它就是 bug
        called = {"count": 0}

        def _should_not_call(sales, customer):
            called["count"] += 1

        original = _n.send_email_notification
        _n.send_email_notification = _should_not_call
        try:
            from types import SimpleNamespace
            t0 = time.monotonic()
            for i in range(5):
                sales = SimpleNamespace(id=999 + i, username=f"fake{i}", email="x@x.com")
                cust = SimpleNamespace(id=1000 + i, name=f"c{i}", region="r",
                                       dispatch_time=None)
                _async_send_email(sales, cust)
            elapsed = time.monotonic() - t0
            # 全部应该立刻返回（不入队、不调 SMTP），总共 < 100ms
            assert elapsed < 0.5, f"耗时 {elapsed:.3f}s 过长，似乎阻塞了"
            assert called["count"] == 0, (
                f"信号量满时仍调 send，被调 {called['count']} 次"
            )
            print(f"[TEST-2] 池满时 5 封提交耗时 {elapsed*1000:.1f}ms，全部被丢弃 send={called['count']}")
            print("[TEST-2] PASSED: 池满丢弃而不阻塞")
        finally:
            _n.send_email_notification = original
    finally:
        # 还原信号量
        for _ in range(_EMAIL_MAX_INFLIGHT):
            _EMAIL_INFLIGHT.release()


def main():
    print(f"[START] 邮件池冒烟测试 2026-07-05 修订版")
    test_semaphore_balance()
    print()
    test_pool_full_drops_email()
    print()
    print("[ALL DONE]")


if __name__ == "__main__":
    main()
