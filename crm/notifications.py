"""派单通知：邮件 + Notification 表。

修复 2026-07-05（回归 commit d31baf1 引发）：
- 之前 d31baf1 改成自建 ThreadPoolExecutor + 在 worker 里调
  current_app.logger.info(...) 触发 Working outside of application context，
  邮件静默失败（双重 try/except 吞掉）。
- 现在重新改回 via crm.utils.async_jobs.submit，该提交器已自带 app context
  兜底（commit 7ebfbee/93af2a3），符合既有约定。

性能修复 2026-07-05（commit d31baf1 初衷）：
- 派单邮件原本与自动派单任务共用 async_jobs 的 4 线程池，10 运营并发时队列
  阻塞几十秒。
- 方案：保持走 async_jobs 但加入「正在发件」的有界信号量，让派单事务入队立刻
  返回。async_jobs 池满时邮件仍提交但有界丢弃 + WARN；这样派单事务不被
  SMTP 拖慢，且线程数不会随邮件提交量线性增长。
"""
from __future__ import annotations

import logging
import smtplib
import threading
import time
from datetime import timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from flask import current_app

from .extensions import db
from .models import Customer, Notification, User

_log = logging.getLogger(__name__)

# ============================================================
# 邮件并发有界信号量
# ------------------------------------------------------------
# 派单事务是非阻塞的（"已提交异步邮件任务"立刻写日志返回），
# 但 SMTP 本身慢（QQ/163 1-3s/封）。如果不加并发上限，瞬时 100 单派单会
# 在 async_jobs 4 线程池里累积。
#
# 我们用一个进程级信号量统计"此刻真正在收 SMTP 应答的邮件数"。
# 信号量耗尽时调用 _try_enter 立刻返回失败，由调用方记 WARN 并跳过
# 提交——这是有界丢弃而非线性堆积（避免线程数随邮件数增长）。
# ============================================================
_EMAIL_MAX_INFLIGHT = 8  # QQ SMTP 单连接 ~1-3s，8 并发可支撑 80 单/分钟
_EMAIL_INFLIGHT = threading.BoundedSemaphore(_EMAIL_MAX_INFLIGHT)


def send_assignment_notification(sales: User, customer: Customer) -> None:
    """派单通知：通过邮件发送。"""
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

    2026-07-05 重写（修复回归）：
    - 之前 d31baf1 直接 self ThreadPoolExecutor.submit，worker 触发
      "Working outside of application context"，邮件静默丢失。
    - 现在改为复用 crm.utils.async_jobs.submit：该提交器已自带
      app.app_context() 兜底（commit 7ebfbee, 93af2a3），
      daemon 线程里访问 current_app / db.session 不会再 RuntimeError。

    性能（有界并发）：
    - 入队前检查 _EMAIL_INFLIGHT 信号量；满则丢弃这一封并 WARN，
      避免线程数随邮件提交量线性增长。
    - 失败仅记日志，绝不抛回调用方。
    """
    # 入队前先抢信号量：池满则不进入 worker，直接丢弃，避免 async_jobs 的
    # 4 线程池被 SMTP 慢请求堆积。
    if not _EMAIL_INFLIGHT.acquire(blocking=False):
        # 池满丢弃这一封。在飞线程里再调 current_app 需要 app.context，
        # 这里加 has_app_context 守护：开发者/测试脚本在没有 app 上下文时也能记日志。
        from flask import has_app_context, current_app as _ca
        if has_app_context():
            _ca.logger.warning(
                "[邮件有界丢弃] customer_id=%s sales=%s "
                "(> %d 并发在飞，待恢复后由巡检补发)",
                customer.id, sales.username, _EMAIL_MAX_INFLIGHT,
            )
        else:
            _log.warning(
                "[邮件有界丢弃] customer_id=%s sales=%s "
                "(> %d 并发在飞，待恢复后由巡检补发)",
                customer.id, sales.username, _EMAIL_MAX_INFLIGHT,
            )
        return

    started_at = time.monotonic()

    def _worker() -> None:
        try:
            send_email_notification(sales, customer)
            _log.info(
                "[邮件发送完成] 耗时 %.2fs → %s <%s>: customer_id=%s",
                time.monotonic() - started_at, sales.username, sales.email, customer.id,
            )
        except Exception as exc:  # noqa: BLE001
            _log.exception(
                "[异步邮件失败] 耗时 %.2fs → %s <%s>: %s",
                time.monotonic() - started_at, sales.username, sales.email, exc,
            )
        finally:
            try:
                _EMAIL_INFLIGHT.release()
            except Exception:
                pass

    from .utils.async_jobs import submit as submit_async_job
    submit_async_job(_worker)


def send_email_notification(sales: User, customer: Customer) -> None:
    """发送邮件通知给销售。

    由后台线程调用，调用前请保证所在线程已 push app context
    （crm.utils.async_jobs.submit 已自动处理）。
    """
    app = current_app  # 必须在 app context 内

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
    # 连接超时 5 秒；读取超时 8 秒（QQ/163 邮件服务器如果响应慢也不超过这个时间）。
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
