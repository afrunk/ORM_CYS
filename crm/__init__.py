from __future__ import annotations

import os
from datetime import timedelta
from logging.handlers import RotatingFileHandler

from flask import Flask

from .extensions import db


def _migrate_schema(app: Flask) -> None:
    """增量迁移：给已有表添加新字段（幂等，安全重复调用）。

    仅在首次部署或表结构变更时生效，不影响已有数据。
    """
    from sqlalchemy import inspect, text

    with app.app_context():
        inspector = inspect(db.engine)
        table_names = inspector.get_table_names()

        # --- users 表 ---
        if "users" in table_names:
            columns = [c["name"] for c in inspector.get_columns("users")]
            for field, col_type in [
                ("temp_password", "VARCHAR(128)"),
                ("phone", "VARCHAR(32)"),
                ("email", "VARCHAR(128)"),
            ]:
                if field not in columns:
                    try:
                        db.session.execute(text(f"ALTER TABLE users ADD COLUMN {field} {col_type}"))
                        db.session.commit()
                        app.logger.info(f"[迁移] 已添加字段 users.{field}")
                    except Exception:
                        db.session.rollback()

        # --- customers 表 ---
        if "customers" in table_names:
            customer_columns = [c["name"] for c in inspector.get_columns("customers")]
            if "operator_id" not in customer_columns:
                try:
                    db.session.execute(text("ALTER TABLE customers ADD COLUMN operator_id INTEGER"))
                    db.session.commit()
                    app.logger.info("[迁移] 已添加字段 customers.operator_id")
                except Exception:
                    db.session.rollback()

            for field, col_type in [
                ("monthly_order_ym", "VARCHAR(6)"),
                ("monthly_order_key", "INTEGER"),
            ]:
                if field not in customer_columns:
                    try:
                        db.session.execute(
                            text(f"ALTER TABLE customers ADD COLUMN {field} {col_type}")
                        )
                        db.session.commit()
                        app.logger.info(f"[迁移] 已添加字段 customers.{field}")
                    except Exception:
                        db.session.rollback()
                    customer_columns.append(field)

        # --- 确保 ORM 中新增的表（如 monthly_customer_seq）已创建 ---
        try:
            from .models import MonthlyCustomerSeq  # noqa: F401 — 注册到 metadata

            db.create_all()
        except Exception:
            db.session.rollback()

        # --- 回填客户月度编号（仅当有旧数据缺字段时执行，幂等） ---
        try:
            from .utils.monthly_order import backfill_customer_monthly_ids_if_needed

            backfill_customer_monthly_ids_if_needed(app)
        except Exception:
            app.logger.exception("[迁移] 客户月度编号回填失败，请检查数据库；旧数据可能暂显示为 —")

        # --- regions 表（可能尚未创建） ---
        if "regions" not in table_names:
            try:
                from .models import Region
                db.create_all()
                app.logger.info("[迁移] 已创建 regions 表")
            except Exception:
                pass

        app.logger.info("[迁移] 数据库结构检查完成")


def _ensure_superadmin(app: Flask) -> None:
    """确保数据库已创建且 superadmin 账号存在（幂等，安全重复调用）。

    在 create_app() 阶段调用，无需额外手动命令；
    所有逻辑走 db session，回滚可靠。
    """
    from werkzeug.security import generate_password_hash
    from .models import User

    with app.app_context():
        db.create_all()
        _migrate_schema(app)

        existing = User.query.filter_by(role="super_admin").first()
        if not existing:
            super_user = User(
                username="superadmin",
                password_hash=generate_password_hash("superadmin123"),
                role="super_admin",
                is_active=True,
                temp_password="superadmin123",
            )
            db.session.add(super_user)
            db.session.commit()
            app.logger.info("✓ 已创建默认超级管理员：superadmin / superadmin123")
        else:
            app.logger.info("✓ 超级管理员已存在，跳过初始化")


def create_app() -> Flask:
    """应用工厂，创建并配置 Flask 实例。"""

    # templates 和 static 目录在项目根目录，因此这里显式指定上一级的目录
    app = Flask(
        __name__,
        instance_relative_config=True,
        template_folder="../templates",
        static_folder="../static",
    )
    # 基础配置，这里使用 SQLite，后续可替换为 MySQL
    # 显式设置 SECRET_KEY，确保 session 可用
    app.config["SECRET_KEY"] = "dev-secret-key"
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///crm.db"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # 日志滚动配置：单个日志最大 10MB，保留 5 个历史文件
    _configure_rotating_log(app)

    # SQLAlchemy 连接池配置（适配 200 人并发）
    # pool_size=50 基础连接，max_overflow=100 峰值额外连接，共 150 并发上限
    # pool_recycle=3600 每小时回收防 MySQL/PostgreSQL 断连
    # pool_pre_ping=True 每次借出前 ping，保证断连不被误用
    # pool_timeout=30 等待连接超时报错而非无限阻塞
    app.config.setdefault(
        "SQLALCHEMY_ENGINE_OPTIONS",
        {
            "pool_size": 50,
            "max_overflow": 100,
            "pool_recycle": 3600,
            "pool_pre_ping": True,
            "pool_timeout": 30,
        },
    )
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)
    
    # 邮件配置（QQ邮箱SMTP）
    app.config["MAIL_SERVER"] = "smtp.qq.com"
    app.config["MAIL_PORT"] = 587
    app.config["MAIL_USE_TLS"] = True
    # 使用环境变量或默认配置
    app.config["MAIL_USERNAME"] = os.environ.get("MAIL_USERNAME", "afrunk@foxmail.com")
    app.config["MAIL_PASSWORD"] = os.environ.get("MAIL_PASSWORD", "wkqrgooalktzjjic")
    app.config["MAIL_DEFAULT_SENDER"] = app.config["MAIL_USERNAME"]

    # 初始化扩展
    db.init_app(app)
    
    # 初始化定时任务（仅在非测试环境且主进程运行）
    if not app.config.get("TESTING"):
        _init_scheduler(app)

    # 延迟导入，避免循环引用
    from .auth.routes import auth_bp
    from .admin.routes import admin_bp
    from .customer.routes import customer_bp
    from .stats.routes import stats_bp

    # 注册蓝图
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(customer_bp, url_prefix="/customers")
    app.register_blueprint(stats_bp, url_prefix="/stats")

    # 每个请求前从 session 中加载当前用户（应用级别钩子，确保所有路由都能访问 g.current_user）
    from flask import g, session
    from .models import User

    @app.before_request
    def load_logged_in_user():
        user_id = session.get("user_id")
        if not user_id:
            g.current_user = None
            return
        user = User.query.get(user_id)
        if user is None or not user.is_active:
            # 避免「session 有效但用户不存在/已停用」导致 g.current_user 为 None 却通过 login_required
            session.pop("user_id", None)
            g.current_user = None
            return
        g.current_user = user

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        """每个请求结束后强制关闭 Session，防止连接泄露。"""
        db.session.remove()

    # 上下文处理：注入当前用户
    @app.context_processor
    def inject_user():
        from flask import g

        return {"current_user": getattr(g, "current_user", None)}
    
    # 注册时间转换过滤器：UTC转北京时间
    @app.template_filter('beijing_time')
    def beijing_time_filter(dt):
        """将UTC时间转换为北京时间（UTC+8）。"""
        if dt is None:
            return None
        from datetime import timedelta
        beijing_time = dt + timedelta(hours=8)
        return beijing_time

    # 注册 CLI 命令之前，先确保数据库和超管已初始化（幂等操作）
    _ensure_superadmin(app)

    # CLI 命令：手动触发初始化（覆盖已存在行为）
    @app.cli.command("init-db")
    def init_db_command():
        """初始化数据库并创建默认超级管理员账户（手动触发版）。"""
        from click import echo

        _ensure_superadmin(app)
        echo("✓ 数据库初始化完成（详见上方日志）")

    # CLI 命令：清空数据库并创建指定超管（危险操作）
    @app.cli.command("reset-db-and-superadmin")
    def reset_db_and_superadmin_command():
        """
        清空数据库所有表，并创建用户名为 echo 的超级管理员。
        密码默认 echo123，temp_password 同步为 echo123。
        """
        from click import echo
        from werkzeug.security import generate_password_hash
        from .models import User

        with app.app_context():
            echo("⚠️ 将要清空所有表并重新创建，正在执行...")
            # 清空并重建表结构
            db.drop_all()
            db.create_all()

            # 创建新的超级管理员
            super_user = User(
                username="echo",
                password_hash=generate_password_hash("echo123"),
                role="super_admin",
                is_active=True,
                temp_password="echo123",
            )
            db.session.add(super_user)
            db.session.commit()
            echo("✓ 数据库已重置")
            echo("✓ 已创建超级管理员：echo / echo123")

    # @app.cli.command("change-superadmin")
    # def change_superadmin_command():
    #     """
    #     修改现有超级管理员的账号密码为 echo / echo123。
    #     如果不存在超级管理员，则创建一个新的。
    #     """
    #     from click import echo
    #     from werkzeug.security import generate_password_hash
    #     from .models import User
    #
    #     with app.app_context():
    #         # 查找现有的超级管理员
    #         superadmin = User.query.filter_by(role="super_admin").first()
    #         
    #         if superadmin:
    #             old_username = superadmin.username
    #             superadmin.username = "echo"
    #             superadmin.password_hash = generate_password_hash("echo123")
    #             superadmin.temp_password = "echo123"
    #             superadmin.is_active = True
    #             db.session.commit()
    #             echo(f"✓ 已修改超级管理员账号：{old_username} → echo")
    #             echo("✓ 密码已更新为：echo123")
    #         else:
    #             # 如果不存在，创建一个新的
    #             super_user = User(
    #                 username="echo",
    #                 password_hash=generate_password_hash("echo123"),
    #                 role="super_admin",
    #                 is_active=True,
    #                 temp_password="echo123",
    #             )
    #             db.session.add(super_user)
    #             db.session.commit()
    #             echo("✓ 已创建新的超级管理员：echo / echo123")

    # CLI 命令：迁移数据库，添加新字段
    @app.cli.command("migrate-db")
    def migrate_db_command():
        """迁移数据库，添加缺失字段。"""
        from click import echo

        _migrate_schema(app)
        echo("✓ 数据库迁移完成（详见上方日志）")

    # CLI 命令：初始化现有用户的 temp_password
    @app.cli.command("init-temp-passwords")
    def init_temp_passwords_command():
        """为现有用户初始化 temp_password 字段（用于显示密码）。"""
        from click import echo
        from sqlalchemy import or_
        from .models import User

        with app.app_context():
            # 查找所有 temp_password 为空的用户（使用 or_ 确保正确匹配）
            users_without_temp_password = User.query.filter(
                or_(User.temp_password.is_(None), User.temp_password == "")
            ).all()
            
            if not users_without_temp_password:
                echo("✓ 所有用户的 temp_password 都已设置")
                # 即使都设置了，也检查 superadmin 是否需要更新
                superadmin = User.query.filter_by(username="superadmin", role="super_admin").first()
                if superadmin and (not superadmin.temp_password or superadmin.temp_password == ""):
                    superadmin.temp_password = "superadmin123"
                    db.session.commit()
                    echo("✓ 已为 superadmin 更新 temp_password")
                return
            
            echo(f"发现 {len(users_without_temp_password)} 个用户的 temp_password 为空，正在初始化...")
            updated_count = 0
            
            for user in users_without_temp_password:
                # 对于 superadmin，使用默认密码 superadmin123
                if user.username == "superadmin" and user.role == "super_admin":
                    user.temp_password = "superadmin123"
                    updated_count += 1
                    echo(f"  ✓ 已为 superadmin 设置默认密码到 temp_password")
                # 对于其他用户，保持为空（用户需要手动编辑设置密码）
            
            if updated_count > 0:
                try:
                    db.session.commit()
                    echo(f"\n✓ 成功为 {updated_count} 个用户初始化了 temp_password")
                except Exception as e:
                    db.session.rollback()
                    echo(f"\n✗ 初始化 temp_password 失败：{e}")
            else:
                echo("\n✓ 没有需要初始化的用户")

    @app.cli.command("flatten-tenancy")
    def flatten_tenancy_command():
        """将多租户数据结构重建为单实例版本（会重建表结构）。"""
        from click import echo
        from sqlalchemy import inspect, text
        from .models import User, Customer, SalesProfile, Notification

        with app.app_context():
            echo("→ 备份现有数据...")
            users_payload = []
            for user in User.query.order_by(User.id.asc()).all():
                role = "super_admin" if user.role == "company_admin" else user.role
                profile = None
                if user.sales_profile:
                    profile = {
                        "id": user.sales_profile.id,
                        "dispatch_order": user.sales_profile.dispatch_order,
                        "is_available": user.sales_profile.is_available,
                        "service_region": user.sales_profile.service_region,
                    }
                users_payload.append(
                    {
                        "id": user.id,
                        "username": user.username,
                        "password_hash": user.password_hash,
                        "role": role,
                        "is_active": user.is_active,
                        "phone": user.phone,
                        "email": user.email,
                        "wechat_openid": user.wechat_openid,
                        "temp_password": user.temp_password,
                        "created_at": user.created_at,
                        "profile": profile,
                    }
                )

            customers_payload = [
                {
                    "id": c.id,
                    "name": c.name,
                    "phone": c.phone,
                    "region": c.region,
                    "fans_count": c.fans_count,
                    "image_path": c.image_path,
                    "status": c.status,
                    "created_at": c.created_at,
                    "dispatch_time": c.dispatch_time,
                    "accepted_time": c.accepted_time,
                    "sales_id": c.sales_id,
                    "operator_id": c.operator_id,
                    "dispatcher_id": c.dispatcher_id,
                    "creator_id": c.creator_id,
                    "is_converted": c.is_converted,
                    "is_valid": c.is_valid,
                    "invalid_proof_image": c.invalid_proof_image,
                    "remark": c.remark,
                    "retry_count": c.retry_count,
                    "monthly_order_ym": c.monthly_order_ym,
                    "monthly_order_key": c.monthly_order_key,
                }
                for c in Customer.query.order_by(Customer.id.asc()).all()
            ]

            notifications_payload = [
                {
                    "id": n.id,
                    "customer_id": n.customer_id,
                    "sales_id": n.sales_id,
                    "channel": n.channel,
                    "content": n.content,
                    "status": n.status,
                    "created_at": n.created_at,
                }
                for n in Notification.query.order_by(Notification.id.asc()).all()
            ]

            echo("→ 重建数据表...")
            db.drop_all()
            inspector = inspect(db.engine)
            if "companies" in inspector.get_table_names():
                db.session.execute(text("DROP TABLE IF EXISTS companies"))
                db.session.commit()
            db.create_all()

            echo("→ 恢复用户与配置...")
            for data in users_payload:
                user = User(
                    id=data["id"],
                    username=data["username"],
                    password_hash=data["password_hash"],
                    role=data["role"],
                    is_active=data["is_active"],
                    phone=data.get("phone"),
                    email=data.get("email"),
                    wechat_openid=data.get("wechat_openid"),
                    temp_password=data.get("temp_password"),
                )
                if data["created_at"]:
                    user.created_at = data["created_at"]
                db.session.add(user)
            db.session.flush()

            for data in users_payload:
                profile = data.get("profile")
                if profile:
                    db.session.add(
                        SalesProfile(
                            id=profile["id"],
                            user_id=data["id"],
                            dispatch_order=profile["dispatch_order"],
                            is_available=profile["is_available"],
                            service_region=profile["service_region"],
                        )
                    )

            echo("→ 恢复客户数据...")
            for c in customers_payload:
                customer = Customer(
                    id=c["id"],
                    name=c["name"],
                    phone=c["phone"],
                    region=c["region"],
                    fans_count=c["fans_count"],
                    image_path=c["image_path"],
                    status=c["status"],
                    dispatch_time=c["dispatch_time"],
                    accepted_time=c["accepted_time"],
                    sales_id=c["sales_id"],
                    operator_id=c["operator_id"],
                    dispatcher_id=c["dispatcher_id"],
                    creator_id=c["creator_id"],
                    is_converted=c["is_converted"],
                    is_valid=c["is_valid"],
                    invalid_proof_image=c["invalid_proof_image"],
                    remark=c["remark"],
                    retry_count=c["retry_count"],
                    monthly_order_ym=c.get("monthly_order_ym"),
                    monthly_order_key=c.get("monthly_order_key"),
                )
                if c["created_at"]:
                    customer.created_at = c["created_at"]
                db.session.add(customer)

            echo("→ 恢复通知记录...")
            for n in notifications_payload:
                record = Notification(
                    id=n["id"],
                    customer_id=n["customer_id"],
                    sales_id=n["sales_id"],
                    channel=n["channel"],
                    content=n["content"],
                    status=n["status"],
                )
                if n["created_at"]:
                    record.created_at = n["created_at"]
                db.session.add(record)

            echo("→ 同步客户月度计数表...")
            from .utils.monthly_order import sync_monthly_seq_counters_from_customers

            sync_monthly_seq_counters_from_customers(db.session)

            db.session.commit()
            echo(
                f"✓ 租户结构重建完成：{len(users_payload)} 个用户、{len(customers_payload)} 条客户、{len(notifications_payload)} 条通知已保留。"
            )

    return app


def _configure_rotating_log(app: Flask) -> None:
    """配置日志滚动：单文件最大 10MB，保留 5 个历史文件。"""
    import logging

    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "app.log")

    handler = RotatingFileHandler(
        filename=log_path,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    # 同时输出到控制台（保持原行为）
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    app.logger.handlers.clear()
    app.logger.addHandler(handler)
    app.logger.addHandler(console_handler)
    app.logger.setLevel(logging.INFO)


def _init_scheduler(app: Flask) -> None:
    """初始化 APScheduler 定时任务。
    
    定时任务：
    - 每1分钟扫描一次超时单并自动重派
    """
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.interval import IntervalTrigger
        from .customer.routes import reassign_timeouts
        
        scheduler = BackgroundScheduler()

        def _run_reassign_job() -> None:
            """确保定时任务在应用上下文中执行，并做好异常与连接回收保护。"""
            from flask import current_app as _current_app

            with app.app_context():
                try:
                    reassign_timeouts()
                except Exception as e:  # noqa: BLE001
                    # 记录异常但不让调度器崩掉
                    if _current_app:
                        _current_app.logger.error(
                            f"定时任务 reassign_timeouts 执行失败：{e}", exc_info=True
                        )
        
        # 添加超时单重派任务：每1分钟执行一次
        scheduler.add_job(
            func=_run_reassign_job,
            trigger=IntervalTrigger(minutes=1),
            id="reassign_timeouts",
            name="超时单自动重派",
            replace_existing=True,
        )
        
        scheduler.start()
        app.logger.info("定时任务已启动：超时单自动重派（每1分钟）")
    except ImportError:
        app.logger.warning("APScheduler 未安装，定时任务功能不可用")
    except Exception as e:
        app.logger.error(f"定时任务启动失败：{e}")
