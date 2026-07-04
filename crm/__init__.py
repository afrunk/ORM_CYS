from __future__ import annotations

import io
import os
import sys
from datetime import timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

from flask import Flask

from .extensions import db

# 项目根目录（crm/ 的父级），用于定位 templates/static/logs 等资源，
# 这样无论从哪个 cwd 启动 Flask，日志路径都稳定。
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOGS_DIR = PROJECT_ROOT / "logs"


def _safe_stdout():
    """返回一个强制 UTF-8 的 stdout 包装器。

    Windows 终端默认 GBK，直接 print/log 中文 + emoji 必然 UnicodeEncodeError。
    用 reconfigure / 重新包装一层 UTF-8 解决；其他平台原样返回。
    """
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name, None)
        if stream is None:
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            try:
                wrapper = io.TextIOWrapper(
                    stream.buffer,
                    encoding="utf-8",
                    errors="replace",
                    line_buffering=True,
                )
                setattr(sys, name, wrapper)
            except Exception:
                pass
    return sys.stdout


def _configure_logger(app: Flask) -> None:
    """配置 RotatingFileHandler，防止日志文件无限膨胀。

    日志目录固定为 <项目根>/logs，不依赖启动时的 cwd，
    避免被写到上层目录（例如 ORM_VFOR7_F/logs）。

    - 单文件最大 10MB，超出自动切分
    - 最多保留 5 个历史备份文件（.log.1 ~ .log.5）
    - 格式：时间戳 | 级别 | 模块名 | 消息
    """
    import logging

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOGS_DIR / "app.log"

    handler = RotatingFileHandler(
        str(log_path),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,
        encoding="utf-8",
    )
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(fmt)
    handler.setLevel(logging.INFO)

    # 接管 Flask 自身日志 + 所有通过 app.logger 输出的日志
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.INFO)
    # 防止日志向上游 root logger 重复输出
    app.logger.propagate = False

    # 同时输出到 stdout（方便 Docker / systemd journal 采集）。
    # 关键：Windows 默认 GBK，直接写中文 + emoji 会崩溃；用 _safe_stdout 强制 UTF-8。
    console = logging.StreamHandler(_safe_stdout())
    console.setFormatter(fmt)
    console.setLevel(logging.INFO)
    app.logger.addHandler(console)

    # ============================================================
    # 静默 werkzeug 访问日志（[GET /health 200 ...] 那一行）。
    #
    # 历史教训（2026-07-04）：
    #   在 watchdog 下，app.py 的 stdout 是管道，werkzeug 每次请求
    #   都通过 logging.info() 写访问日志。高并发下管道填满、watchdog
    #   还没及时 drain 时，logger 会持锁 sleep → 所有请求线程卡死。
    #   静默 werkzeug 可彻底规避这一类日志阻塞问题。
    #
    # 需要排查具体请求时，临时改成 logging.INFO 即可。
    # ============================================================
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    app.logger.info(f"[日志] 文件日志已配置：{log_path}")

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
            if "conversion_status" not in customer_columns:
                try:
                    db.session.execute(
                        text("ALTER TABLE customers ADD COLUMN conversion_status VARCHAR(32)")
                    )
                    db.session.commit()
                    app.logger.info("[迁移] 已添加字段 customers.conversion_status")
                    from .models import (
                        CONVERSION_STATUS_CONVERTED,
                        CONVERSION_STATUS_NOT_CONVERTED,
                        Customer,
                    )

                    for row in Customer.query.all():
                        if row.conversion_status is not None:
                            continue
                        if row.is_converted is True:
                            row.conversion_status = CONVERSION_STATUS_CONVERTED
                        elif row.is_converted is False:
                            row.conversion_status = CONVERSION_STATUS_NOT_CONVERTED
                        else:
                            row.conversion_status = None
                    db.session.commit()
                    app.logger.info("[迁移] 已根据 is_converted 回填 conversion_status")
                except Exception:
                    db.session.rollback()

        # --- regions 表（可能尚未创建） ---
        if "regions" not in table_names:
            try:
                from .models import Region
                db.create_all()
                app.logger.info("[迁移] 已创建 regions 表")
            except Exception:
                pass

        # ============================================================
        # customers 性能索引（2026-07-04）
        #
        # 历史教训：customers 表此前无任何业务索引。11224 条数据 + ORDER BY dispatch_time DESC
        # + OFFSET N 翻页，每次翻页都要全表扫到 OFFSET。瓶颈观察：
        #   - 列表页 SQL: 5~7ms / page（看着不大，但并发翻页会叠加）
        #   - 主因是 OFFSET 不能走索引，page=10 实际比 page=2 慢（线性）
        #   - 加索引后预期降到 <1ms / page
        #
        # 索引策略（按 _apply_customer_filters / customer_list 真实用到的列设计）：
        # - idx_dispatch_id：服务 ORDER BY dispatch_time DESC NULLS LAST, id DESC
        #   复合索引既覆盖排序又能被 OFFSET 走
        # - idx_status：服务 status=unassigned/timeout 等过滤
        # - idx_region：服务 region=... 过滤
        # - idx_sales_id：服务销售角色只看自己 (sales_id = self)
        # - idx_creator_id：服务运营角色只看自己 (creator_id = self)
        # - idx_dispatch_time：服务时间范围过滤 (start/end)，且 NULLS LAST 排序走它也快
        #
        # 用 IF NOT EXISTS 等价的方式：先查 sqlite_master，存在则跳过。
        # SQLite 不支持 CREATE INDEX IF NOT EXISTS 在所有版本上都干净，幂等用查表方式实现。
        # ============================================================
        if "customers" in table_names:
            try:
                existing = {
                    row[0]
                    for row in db.session.execute(
                        text("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='customers'")
                    )
                }
                desired_indexes = [
                    ("idx_customers_dispatch_id",  "dispatch_time DESC, id DESC"),
                    ("idx_customers_dispatch_time", "dispatch_time"),
                    ("idx_customers_status",        "status"),
                    ("idx_customers_region",        "region"),
                    ("idx_customers_sales_id",      "sales_id"),
                    ("idx_customers_creator_id",    "creator_id"),
                ]
                created_count = 0
                for idx_name, cols in desired_indexes:
                    if idx_name in existing:
                        continue
                    # SQLite DESC keyword 在索引里允许。NULLS LAST 不在 SQL 索引里支持，
                    # 但 planner 走这个索引 + ORDER BY 会得到正确 DESC 顺序，
                    # NULL 顺序由 storage engine 后处理（SQLite 文档明确）。
                    db.session.execute(
                        text(f"CREATE INDEX {idx_name} ON customers({cols})")
                    )
                    created_count += 1
                db.session.commit()
                if created_count:
                    app.logger.info(
                        f"[迁移] customers 新增 {created_count} 个索引："
                        + ", ".join(n for n, _ in desired_indexes if n not in existing)
                    )
                else:
                    app.logger.info("[迁移] customers 索引已存在，跳过")
            except Exception as exc:
                db.session.rollback()
                app.logger.error(f"[迁移] customers 索引创建失败：{exc}")

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
            app.logger.info("[OK] 已创建默认超级管理员：superadmin / superadmin123")
        else:
            app.logger.info("[OK] 超级管理员已存在，跳过初始化")


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
    # SECRET_KEY 必须强随机；优先读环境变量，其次回退到 instance/secret_key 文件，
    # 最后兜底为一个固定开发值（生产环境必须覆盖）。
    _secret = os.environ.get("CRM_SECRET_KEY")
    if not _secret:
        _sk_path = Path(app.instance_path) / "secret_key"
        if _sk_path.exists():
            _secret = _sk_path.read_text().strip()
        else:
            _secret = "dev-secret-key"
            try:
                Path(app.instance_path).mkdir(parents=True, exist_ok=True)
                _sk_path.write_text(_secret)
            except OSError:
                pass
    app.config["SECRET_KEY"] = _secret
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///crm.db"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # 配置 RotatingFileHandler：单文件最大 10MB，保留 5 个轮转备份
    _configure_logger(app)

    # gzip 压缩：HTML/CSS/JS 走服务端压缩后传输，3Mbps 链路下首屏时间显著下降
    try:
        from flask_compress import Compress
        Compress(app)
    except ImportError:
        app.logger.warning("flask-compress 未安装，HTTP 响应不会走 gzip")

    # ============================================================
    # 静态资源缓存头（性能优化 2026-07-04）
    #
    # 痛点：列表页会加载 6 个静态资源，其中 vendor 文件（bootstrap.min.css、
    # bootstrap-icons.css、bootstrap.bundle.min.js）合计 ~410KB，每次翻页、
    # 每次刷新浏览器都要重新下载一次，造成「点击下一页一直转圈」的用户体验。
    #
    # 优化策略（按文件名后缀分流）：
    # - vendor/*（*.min.css / *.min.js / *.woff2 等）→ max-age=1年, immutable
    #   因为已经是 .min 版本且文件名带版本号后，重命名 = 改版本，不会被覆盖
    # - 业务 CSS/JS（main.css, main.js）→ max-age=5分钟
    #   偶尔会改，但允许用户拿到旧版本 5 分钟
    # - 图片（thumb/preview/*.webp、uploads/*）→ max-age=1天
    #
    # 注意：必须分开设置，不能统一 1 年，否则改 main.css 用户拿不到新版。
    # ============================================================
    import re as _re
    from flask import request as _flask_request

    @app.after_request
    def _set_static_cache_headers(response):
        path = _flask_request.path
        # 只处理 /static/ 路径，业务路径不干扰
        if not path.startswith("/static/"):
            return response
        # vendor 资源永久缓存
        if "/static/vendor/" in path:
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        # 业务 CSS/JS 短缓存
        elif path.endswith(("/main.css", "/main.js")):
            response.headers["Cache-Control"] = "public, max-age=300, must-revalidate"
        # 图片缩略图 / 预览 / 用户上传
        elif _re.search(r"\.(webp|png|jpe?g|gif|svg|woff2?|ttf|eot|ico)(\?.*)?$", path, _re.I):
            response.headers["Cache-Control"] = "public, max-age=86400"
        # 其它静态（极少见兜底）给短缓存
        else:
            response.headers["Cache-Control"] = "public, max-age=300"
        return response

    # 在线访客跟踪：每个请求的 remote_addr 加到 TTL 集合里，5 分钟内还活跃就算"在线"
    # 用于 watchdog 每分钟统计在线 IP 数和具体 IP 列表
    ONLINE_TTL_SECONDS = 5 * 60
    _online_ips: dict[str, float] = {}
    _online_lock = __import__("threading").Lock()

    def _track_visitor():
        from flask import request as _req
        from time import time as _t
        ip = _req.headers.get("X-Forwarded-For", _req.remote_addr or "")
        # X-Forwarded-For 可能含多个 IP（反向代理链），取第一个
        if "," in ip:
            ip = ip.split(",", 1)[0].strip()
        if not ip:
            return
        now = _t()
        with _online_lock:
            _online_ips[ip] = now
            # 顺手清理过期（这里只清到期的，遍历开销 O(n) 但 n 很小）
            expired = [k for k, v in _online_ips.items() if now - v > ONLINE_TTL_SECONDS]
            for k in expired:
                _online_ips.pop(k, None)

    app.before_request(_track_visitor)

    @app.route("/metrics/online-users")
    def _metrics_online_users():
        from time import time as _t
        now = _t()
        with _online_lock:
            alive = {ip: ts for ip, ts in _online_ips.items()
                     if now - ts <= ONLINE_TTL_SECONDS}
            ips = sorted(alive.keys())
        # 仅暴露给本地 watchdog；外网请求走 auth 守卫。
        # 共享密钥机制：watchdog 必须带 X-Watchdog-Token 头，且值等于 CRM_WATCHDOG_TOKEN 环境变量。
        # 这避免"任何能 curl 127.0.0.1 的进程都能读到用户 IP"的安全漏洞。
        from flask import request as _req
        import os as _os
        token = _os.environ.get("CRM_WATCHDOG_TOKEN", "")
        if _req.remote_addr in ("127.0.0.1", "::1"):
            # 本机调用：必须带正确 token（防止被其他本地进程误读）
            if not token or _req.headers.get("X-Watchdog-Token") != token:
                return {"error": "forbidden"}, 403
        else:
            # 外网调用：必须 super_admin 登录
            from flask_login import current_user
            if not (current_user.is_authenticated and current_user.is_super_admin()):
                return {"error": "forbidden"}, 403
        return {"count": len(ips), "ips": ips,
                "ttl_seconds": ONLINE_TTL_SECONDS,
                "ts": int(now)}

    # SQLAlchemy 连接池配置
    # SQLite 是文件级锁，不适合大量连接池；使用 NullPool 避免连接堆积导致 database is locked
    is_sqlite = app.config["SQLALCHEMY_DATABASE_URI"].startswith("sqlite")
    if is_sqlite:
        from sqlalchemy.pool import NullPool
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
            "poolclass": NullPool,
            "connect_args": {
                "timeout": 30,
                "check_same_thread": False,
            },
        }
    else:
        # MySQL / PostgreSQL：使用连接池
        app.config.setdefault(
            "SQLALCHEMY_ENGINE_OPTIONS",
            {
                "pool_size": 50,
                "max_overflow": 100,
                "pool_pre_ping": True,
                "pool_timeout": 30,
                "pool_recycle": 3600,
            },
        )
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=8)
    # 即使未开 DEBUG，也每次请求重载模板，避免改 HTML 后必须重启进程
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    
    # 邮件配置（QQ邮箱SMTP）
    app.config["MAIL_SERVER"] = "smtp.qq.com"
    app.config["MAIL_PORT"] = 587
    app.config["MAIL_USE_TLS"] = True
    # 使用环境变量或默认配置
    app.config["MAIL_USERNAME"] = os.environ.get("MAIL_USERNAME", "1377153898@qq.com")
    app.config["MAIL_PASSWORD"] = os.environ.get("MAIL_PASSWORD", "wkqrgooalktzjjic")
    app.config["MAIL_DEFAULT_SENDER"] = app.config["MAIL_USERNAME"]

    # 初始化扩展
    db.init_app(app)

    # SQLite 优化：在 app context 内注册 PRAGMA listener（开启 WAL + busy_timeout）
    if is_sqlite:
        from sqlalchemy import event
        with app.app_context():
            @event.listens_for(db.engine, "connect")
            def _set_sqlite_pragma(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA busy_timeout=30000")
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

    # 请求结束后强制关闭 session，将连接归还给连接池（防止连接泄漏）
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db.session.remove()

    # 静态资源与 vendor CSS/JS 永久缓存：文件名带 hash 的资源用一年，
    # vendor 与 css/js 设 1 天；HTML 走 no-cache（避免改模板后用户看不到新版）。
    STATIC_CACHE_MAX_AGE = 60 * 60 * 24  # 1 天
    VENDOR_CACHE_MAX_AGE = 60 * 60 * 24 * 30  # 30 天
    UPLOADS_CACHE_MAX_AGE = 60 * 60 * 24 * 7  # 7 天

    @app.after_request
    def _add_cache_headers(response):
        from flask import request
        path = request.path or ""
        # HTML：不缓存
        if response.mimetype == "text/html" or path.endswith(".html"):
            response.headers.setdefault("Cache-Control", "no-cache")
            return response
        # 静态资源路径（/static/...）
        if path.startswith("/static/"):
            if "/vendor/" in path:
                response.headers.setdefault(
                    "Cache-Control", f"public, max-age={VENDOR_CACHE_MAX_AGE}"
                )
            elif path.startswith("/static/uploads/"):
                response.headers.setdefault(
                    "Cache-Control", f"public, max-age={UPLOADS_CACHE_MAX_AGE}"
                )
            else:
                response.headers.setdefault(
                    "Cache-Control", f"public, max-age={STATIC_CACHE_MAX_AGE}"
                )
        return response

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
        echo("[OK] 数据库初始化完成（详见上方日志）")

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
            echo("[WARN] 将要清空所有表并重新创建，正在执行...")
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
            echo("[OK] 数据库已重置")
            echo("[OK] 已创建超级管理员：echo / echo123")

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
    #             echo(f"[OK] 已修改超级管理员账号：{old_username} >> echo")
    #             echo("[OK] 密码已更新为：echo123")
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
    #             echo("[OK] 已创建新的超级管理员：echo / echo123")

    # CLI 命令：迁移数据库，添加新字段
    @app.cli.command("migrate-db")
    def migrate_db_command():
        """迁移数据库，添加缺失字段。"""
        from click import echo

        _migrate_schema(app)
        echo("[OK] 数据库迁移完成（详见上方日志）")

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
                echo("[OK] 所有用户的 temp_password 都已设置")
                # 即使都设置了，也检查 superadmin 是否需要更新
                superadmin = User.query.filter_by(username="superadmin", role="super_admin").first()
                if superadmin and (not superadmin.temp_password or superadmin.temp_password == ""):
                    superadmin.temp_password = "superadmin123"
                    db.session.commit()
                    echo("[OK] 已为 superadmin 更新 temp_password")
                return
            
            echo(f"发现 {len(users_without_temp_password)} 个用户的 temp_password 为空，正在初始化...")
            updated_count = 0
            
            for user in users_without_temp_password:
                # 对于 superadmin，使用默认密码 superadmin123
                if user.username == "superadmin" and user.role == "super_admin":
                    user.temp_password = "superadmin123"
                    updated_count += 1
                    echo(f"  [OK] 已为 superadmin 设置默认密码到 temp_password")
                # 对于其他用户，保持为空（用户需要手动编辑设置密码）
            
            if updated_count > 0:
                try:
                    db.session.commit()
                    echo(f"\n[OK] 成功为 {updated_count} 个用户初始化了 temp_password")
                except Exception as e:
                    db.session.rollback()
                    echo(f"\n✗ 初始化 temp_password 失败：{e}")
            else:
                echo("\n[OK] 没有需要初始化的用户")

    @app.cli.command("flatten-tenancy")
    def flatten_tenancy_command():
        """将多租户数据结构重建为单实例版本（会重建表结构）。"""
        from click import echo
        from sqlalchemy import inspect, text
        from .models import User, Customer, SalesProfile, Notification

        with app.app_context():
            echo(">> 备份现有数据...")
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
                    "conversion_status": getattr(c, "conversion_status", None),
                    "is_valid": c.is_valid,
                    "invalid_proof_image": c.invalid_proof_image,
                    "remark": c.remark,
                    "retry_count": c.retry_count,
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

            echo(">> 重建数据表...")
            db.drop_all()
            inspector = inspect(db.engine)
            if "companies" in inspector.get_table_names():
                db.session.execute(text("DROP TABLE IF EXISTS companies"))
                db.session.commit()
            db.create_all()

            echo(">> 恢复用户与配置...")
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

            echo(">> 恢复客户数据...")
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
                    conversion_status=c.get("conversion_status"),
                    is_valid=c["is_valid"],
                    invalid_proof_image=c["invalid_proof_image"],
                    remark=c["remark"],
                    retry_count=c["retry_count"],
                )
                if c["created_at"]:
                    customer.created_at = c["created_at"]
                db.session.add(customer)

            echo(">> 恢复通知记录...")
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

            db.session.commit()
            echo(
                f"[OK] 租户结构重建完成：{len(users_payload)} 个用户、{len(customers_payload)} 条客户、{len(notifications_payload)} 条通知已保留。"
            )

    return app


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
            max_instances=1,           # 上一次还没跑完就不开新实例
            coalesce=True,             # 错过的多次触发合并成一次
            misfire_grace_time=120,    # 最多容忍 2 分钟的延迟
        )
        
        scheduler.start()
        app.logger.info("定时任务已启动：超时单自动重派（每1分钟）")
    except ImportError:
        app.logger.warning("APScheduler 未安装，定时任务功能不可用")
    except Exception as e:
        app.logger.error(f"定时任务启动失败：{e}")
