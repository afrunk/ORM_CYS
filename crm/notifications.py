from __future__ import annotations

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

from flask import current_app
from sqlalchemy import select

from .extensions import db
from .models import Customer, Notification, User


def send_assignment_notification(sales: User, customer: Customer) -> None:
    """派单通知：通过邮件发送。

    如果销售有邮箱，则发送邮件通知；否则仅记录到通知表。
    """
    sales_id = sales.id
    # 强制用当前库中最新数据覆盖会话内对象（避免 email 等在内存里仍是旧值/None）
    stmt = (
        select(User)
        .where(User.id == sales_id)
        .execution_options(populate_existing=True)
    )
    sales_row = db.session.execute(stmt).scalar_one_or_none()
    if not sales_row:
        current_app.logger.warning("派单通知跳过：销售 id=%s 不存在", sales_id)
        return
    sales = sales_row

    # 构建通知内容
    content = f"新客户派单：{customer.name}，电话：{customer.phone or '无'}"

    # 优先使用邮箱发送
    channel = "email" if sales.email else "none"
    status = "sent"

    if not sales.email:
        current_app.logger.warning(
            "派单通知：销售 %s（id=%s）未设置邮箱，已跳过发信，仅写入通知表",
            sales.username,
            sales.id,
        )

    if sales.email:
        try:
            send_email_notification(sales, customer)
            status = "sent"
        except Exception as e:
            current_app.logger.error(f"发送邮件通知失败：{e}")
            status = "failed"
            channel = "email_failed"
    
    # 记录到通知表
    record = Notification(
        customer_id=customer.id,
        sales_id=sales.id,
        channel=channel,
        content=content,
        status=status,
    )
    db.session.add(record)
    
    if status == "sent":
        current_app.logger.info(f"[通知] 向销售 {sales.username} ({sales.email}) 发送派单通知：{content}")
    else:
        current_app.logger.warning(f"[通知失败] 向销售 {sales.username} 发送派单通知失败")


def _smtp_send(app, msg: MIMEMultipart, mail_username: str, mail_password: str) -> None:
    """通过 SMTP 发送邮件，支持重试与端口 fallback。"""
    mail_server = app.config["MAIL_SERVER"]
    ports_to_try = [int(app.config.get("MAIL_PORT", 587)), 587, 465]

    last_error = None
    for attempt in range(3):
        server = None
        try:
            port = ports_to_try[attempt] if attempt < len(ports_to_try) else ports_to_try[-1]
            server = smtplib.SMTP(mail_server, port, timeout=20)
            server.ehlo()
            if port in (587, 25):
                server.starttls()
                server.ehlo()
            server.login(mail_username, mail_password)
            server.send_message(msg)
            server.quit()
            return
        except smtplib.SMTPException as e:
            last_error = e
            if server:
                try:
                    server.quit()
                except Exception:
                    try:
                        server.close()
                    except Exception:
                        pass
            import time
            time.sleep(2)  # 等待 2 秒后重试（QQ SMTP 偶发断连）
        except OSError as e:
            last_error = e
            if server:
                try:
                    server.close()
                except Exception:
                    pass
            import time
            time.sleep(2)

    raise Exception(f"邮件发送失败（已重试 3 次）：{last_error}") from last_error


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
                    <div class="info-label">联系电话</div>
                    <div class="info-value">{customer.phone or '未提供'}</div>
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
                    <div class="info-label">客户月度编号</div>
                    <div class="info-value">{customer.monthly_display_id}</div>
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

    # 发送邮件（内部已处理重试）
    _smtp_send(app, msg, mail_username, mail_password)


