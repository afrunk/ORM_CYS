from __future__ import annotations

"""
简单并发压测脚本：

- 使用 requests + 线程池模拟大量用户登录后访问 /customers/ 列表页
- 主要用于观察数据库连接池是否会被耗尽、应用是否抛错

运行方式（在项目根目录）：
    .\.venv\Scripts\activate          # 按你的虚拟环境为准
    pip install requests              # 如未安装
    python stress_test_customers.py
"""

import concurrent.futures
import time
from typing import Any

import requests

BASE_URL = "http://127.0.0.1:5000"
LOGIN_URL = f"{BASE_URL}/login"
CUSTOMERS_URL = f"{BASE_URL}/customers/"

# 使用项目中已有的超级管理员账号（见 app.py 注释）
USERNAME = "superadmin"
PASSWORD = "superadmin123"


def worker(_) -> Any:
    """单个并发用户：先登录，再访问 /customers/。"""
    s = requests.Session()
    try:
        resp_login = s.post(
            LOGIN_URL,
            data={"username": USERNAME, "password": PASSWORD},
            timeout=5,
        )
        # 登录通常会 302 跳转到首页或客户列表
        if resp_login.status_code not in (200, 302, 303):
            return f"login_fail:{resp_login.status_code}"

        resp = s.get(CUSTOMERS_URL, timeout=5)
        return resp.status_code
    except Exception as e:  # noqa: BLE001
        return f"error:{type(e).__name__}"
    finally:
        s.close()


def main() -> None:
    # 你可以根据机器性能和需要自由调整下面两个参数
    concurrency = 50      # 线程池大小（同时并发的“用户”数）
    total_requests = 1000 # 总请求次数

    print(
        f"开始压测：并发 {concurrency}，总请求 {total_requests}，"
        f"目标地址：{CUSTOMERS_URL}"
    )

    start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        results = list(executor.map(worker, range(total_requests)))

    cost = time.time() - start
    ok = sum(1 for r in results if r == 200)
    print(f"OK: {ok}/{total_requests}, 耗时: {cost:.2f}s")

    from collections import Counter

    counter = Counter(results)
    print("结果统计：")
    for key, value in counter.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()

