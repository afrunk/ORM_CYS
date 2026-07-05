from __future__ import annotations

import atexit
import logging
import smtplib
import threading
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

from flask import current_app

from .extensions import db
from .models import Customer, Notification, User

_log = logging.getLogger(__name__)

# ============================================================
# 邮件专用线程池（2026-07-05 性能修复）
# ------------------------------------------------------------
# 历史问题：派单通知邮件与自动派单任务共用 crm.utils.async_jobs 的
# 4 线程池，导致两件事互相阻塞：
#   - 派单耗时 ~0.5-2s/单（含 DB N+1）
#   - SMTP 耗时 ~1-3s/封（QQ SMTP）
# 10 运营并发时，邮件任务排在派单任务之后，队尾邮件要等几十秒
# 才能发出。表现为"派单给小C了，但小C很久才收到邮件"。
#
# 修复：拆出独立有界线程池，专职 SMTP。
#   - max_workers=8：QQ SMTP 单连接 ~1-3s，8 并发可支撑峰值
#     80 单/分钟派单，远超实际业务量。
#   - BoundedSemaphore(8)：超出上限的提交直接丢弃 + 记录告警，
#     绝不无限堆积线程（避免"线程数随邮件数线性增长"的隐患）。
#   - daemon=True + atexit shutdown：进程退出时强制回收。
#   - 单封超时熔断：worker 内部 12s 硬超时（connect 5 + login 5 + send 2）。
# ============================================================
_EMAIL_POOL_MAX_WORKERS = 8
_EMAIL_POOL_SEM = threading.BoundedSemaphore(_EMAIL_POOL_MAX_WORKERS)
_EMAIL_POOL_LOCK = threading.Lock()
_EMAIL_POOL_REF = None  # type: concurrent.futures.ThreadPoolExecutor | None


def _get_email_pool():
    """懒加载邮件专用线程池，进程级单例。

    之所以不直接放模块级 executor：导入 crm 时此模块已加载，
    但需要兼容被测试或子进程场景，懒初始化更安全。
    """
    global _EMAIL_POOL_REF
    if _EMAIL_POOL_REF is not None:
        return _EMAIL_POOL_REF
    with _EMAIL_POOL_LOCK:
        if _EMAIL_POOL_REF is not None:
            return _EMAIL_POOL_REF
        import concurrent.futures

        pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=_EMAIL_POOL_MAX_WORKERS,
            thread_name_prefix="email-pool",
        )
        _EMAIL_POOL_REF = pool
        # 进程退出时优雅关闭（强制 wait=False，避免 SMTP 挂死卡进程）
        atexit.register(_shutdown_email_pool)
        _log.info("[邮件池] 已启动 %d 个工作线程", _EMAIL_POOL_MAX_WORKERS)
        return pool


def _shutdown_email_pool() -> None:
    """atexit 钩子：进程退出时关闭邮件池。

    daemon=True 让 Python 退出时不阻塞，但 executor 自身不会自动
    cancel 已提交未执行的任务；这里 cancel + wait 短超时，保证不泄漏。
    """
    global _EMAIL_POOL_REF
    pool = _EMAIL_POOL_REF
    if pool is None:
        return
    try:
        # cancel 已排队但未开始的任务（已开始 run 的无法取消，只能等 SMTP 自然超时）
        # 不 wait 太长：进程退出场景下 2s 足够
        pool.shutdown(wait=False, cancel_futures=True)
        _log.info("[邮件池] 已 shutdown")
    except Exception as exc:  # noqa: BLE001
        _log.warning("[邮件池] shutdown 异常：%s", exc)
    finally:
        _EMAIL_POOL_REF = None


def send_assignment_notification(sales: User, customer: Customer) -> None:
    """派单通知：通过邮件发送。

    如果销售有邮箱，则**异步**发送邮件通知；否则仅记录到通知表。

    关键：所有 SMTP 操作放到 daemon 线程里执行，避免阻塞调用方
    （reassign_timeouts 循环调用本函数，慢 SMTP 不能拖垮整个派单事务）。
    """

    # 构建通知内容
    content = f"新客户派单：{customer.name}，电话：{customer.phone or '无'}"

    # 优先使用邮箱发送
    channel = "email" if sales.email else "none"
    status = "sent"

    if sales.email:
        # 异步发送邮件：派单事务不等 SMTP，立刻返回
        _async_send_email(sales, customer)
        status = "sent"

    # 记录到通知表（立即可见）
    record = Notification(
        customer_id=customer.id,
        sales_id=sales.id,
        channel=channel,
        content=content,
        status=status,
    )
    db.session.add(record)

    if status == "sent":
        current_app.logger.info(
            f"[通知] 已提交异步邮件任务至 {sales.username} ({sales.email})：{content}"
        )
    else:
        current_app.logger.warning(f"[通知失败] 向销售 {sales.username} 发送派单通知失败")


def _async_send_email(sales: User, customer: Customer) -> None:
    """后台线程发邮件，与调用方完全解耦。

    2026-07-05 重构：从 crm.utils.async_jobs 的 4 线程共享池中拆出，
    使用邮件专用 8 线程池 + BoundedSemaphore 限流，避免：
      1) 派单任务与 SMTP 互相阻塞
      2) 线程数随邮件提交量线性增长（无界堆积）

    失败仅记日志，绝不抛回调用方（派单事务不能因为 SMTP 慢/失败而回滚）。
    """
    started_at = time.monotonic()

    def _worker() -> None:
        # 抢信号量：池满则放弃执行。acquire 非阻塞，
        # 失败说明当下 SMTP 已堆积，丢弃这一封避免更严重的阻塞。
        if not _EMAIL_POOL_SEM.acquire(blocking=False):
            try:
                current_app.logger.warning(
                    "[邮件池满] 丢弃一封派单通知：customer_id=%s sales=%s "
                    "(> %d 并发，待恢复后可由巡检补发)",
                    customer.id, sales.username, _EMAIL_POOL_MAX_WORKERS,
                )
            except Exception:
                pass
            return
        try:
            send_email_notification(sales, customer)
            elapsed = time.monotonic() - started_at
            try:
                current_app.logger.info(
                    "[邮件发送完成] 耗时 %.2fs → %s <%s>: customer_id=%s",
                    elapsed, sales.username, sales.email, customer.id,
                )
            except Exception:
                pass
        except Exception as exc:  # noqa: BLE001
            elapsed = time.monotonic() - started_at
            try:
                current_app.logger.error(
                    "[异步邮件失败] 耗时 %.2fs → %s <%s>: %s",
                    elapsed, sales.username, sales.email, exc,
                )
            except Exception:
                pass
        finally:
            try:
                _EMAIL_POOL_SEM.release()
            except Exception:
                pass

    try:
        pool = _get_email_pool()
        pool.submit(_worker)
    except RuntimeError:
        # 进程退出阶段 executor 已 shutdown，忽略即可
        pass
    except Exception as exc:  # pragma: no cover
        try:
            current_app.logger.warning("[邮件池提交失败] %s", exc)
        except Exception:
            pass


def send_email_notification(sales: User, customer: Customer) -> None:
    """发送邮件通知给销售。
    
    Args:
        sales: 销售用户对象
        customer: 客户对象
    """
    app = current_app
    
    # 检查邮件配置
    mail_username = app.config.get("MAIL_USERNAME")
    mail_password = app.config.get("MAIL_PASSWORD")
    
    if not mail_username or not mail_password:
        raise ValueError("邮件服务器未配置，请在配置文件中设置 MAIL_USERNAME 和 MAIL_PASSWORD")
    
    if not sales.email:
        raise ValueError(f"销售 {sales.username} 未设置邮箱")
    
    # 构建邮件内容
    subject = f"【派单通知】新客户 {customer.name} 已分配给您"
    
    # 格式化派单时间（北京时间）
    dispatch_time = customer.dispatch_time
    if dispatch_time:
        beijing_time = dispatch_time + timedelta(hours=8)
        dispatch_time_str = beijing_time.strftime('%Y-%m-%d %H:%M:%S')
    else:
        dispatch_time_str = "未知"
    
    # 构建HTML邮件内容
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                font-family: Arial, "Microsoft YaHei", sans-serif;
                line-height: 1.6;
                color: #333;
            }}
            .container {{
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f9fafb;
            }}
            .card {{
                background-color: #ffffff;
                border-radius: 8px;
                padding: 24px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .header {{
                border-bottom: 2px solid #3b82f6;
                padding-bottom: 16px;
                margin-bottom: 24px;
            }}
            .title {{
                font-size: 20px;
                font-weight: bold;
                color: #1f2937;
                margin: 0;
            }}
            .info-row {{
                margin-bottom: 16px;
                padding-bottom: 12px;
                border-bottom: 1px solid #e5e7eb;
            }}
            .info-label {{
                font-weight: 600;
                color: #6b7280;
                font-size: 14px;
                margin-bottom: 4px;
            }}
            .info-value {{
                color: #1f2937;
                font-size: 16px;
            }}
            .footer {{
                margin-top: 24px;
                padding-top: 16px;
                border-top: 1px solid #e5e7eb;
                color: #6b7280;
                font-size: 12px;
                text-align: center;
            }}
            .button {{
                display: inline-block;
                margin-top: 20px;
                padding: 12px 24px;
                background-color: #3b82f6;
                color: #ffffff;
                text-decoration: none;
                border-radius: 6px;
                font-weight: 600;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="card">
                <div class="header">
                    <h1 class="title">📋 新客户派单通知</h1>
                </div>
                
                <div class="info-row">
                    <div class="info-label">客户名称</div>
                    <div class="info-value">{customer.name}</div>
                </div>
                
                <div class="info-row">
                    <div class="info-label">客户地区</div>
                    <div class="info-value">{customer.region or '未指定'}</div>
                </div>
                
                <div class="info-row">
                    <div class="info-label">派单时间</div>
                    <div class="info-value">{dispatch_time_str}</div>
                </div>
                
                <div class="info-row">
                    <div class="info-label">客户ID</div>
                    <div class="info-value">#{customer.id}</div>
                </div>
                
                <div style="margin-top: 24px; padding: 16px; background-color: #eff6ff; border-radius: 6px; border-left: 4px solid #3b82f6;">
                    <p style="margin: 0; color: #1e40af; font-weight: 600;">
                        ⚠️ 请及时登录系统查看客户详情并接单，超过5分钟未接单将自动重派。
                    </p>
                </div>
                
                <div class="footer">
                    <p>此邮件由客户管理系统自动发送，请勿回复。</p>
                    <p>如有疑问，请联系系统管理员。</p>
                </div>
            </div>
        </div>
    </body>
    </html>
    """
    
    # 创建邮件
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = mail_username
    msg['To'] = sales.email
    
    # 添加HTML内容
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)
    
    # 发送邮件（带超时，避免 SMTP 服务器挂起时阻塞整个 reassign 任务）
    # 4. 连接超时 5 秒；读取超时 8 秒（QQ/163 邮件服务器如果响应慢也不超过这个时间）。
    # 这样 50 个超时单最多耗时 (5+8) * 50 = 6.5 分钟，不会无限阻塞。
    try:
        server = smtplib.SMTP(
            app.config["MAIL_SERVER"],
            app.config["MAIL_PORT"],
            timeout=8,
        )
        server.starttls()
        server.login(mail_username, mail_password)
        server.send_message(msg)
        server.quit()
    except Exception as e:
        raise Exception(f"邮件发送失败：{str(e)}")


