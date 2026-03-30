from __future__ import annotations

from sqlalchemy import func

from .extensions import db


class User(db.Model):
    """用户表，按公司归属，角色控制权限。"""

    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), nullable=False, unique=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(
        db.String(32),
        nullable=False,
        default="sales",
    )  # super_admin / data_entry / operator / sales
    is_active = db.Column(db.Boolean, default=True)
    phone = db.Column(db.String(32))  # 电话
    email = db.Column(db.String(128))  # 邮箱（主要用于销售代表接收派单通知）
    # 通知相关预留字段
    wechat_openid = db.Column(db.String(128))
    # 临时密码字段（用于显示，不加密存储，仅用于管理员查看）
    temp_password = db.Column(db.String(128), nullable=True)

    created_at = db.Column(db.DateTime, server_default=func.now())

    sales_profile = db.relationship(
        "SalesProfile", back_populates="user", uselist=False, lazy="joined"
    )

    created_customers = db.relationship(
        "Customer",
        back_populates="creator",
        foreign_keys="Customer.creator_id",
        lazy="dynamic",
    )
    dispatched_customers = db.relationship(
        "Customer",
        back_populates="dispatcher",
        foreign_keys="Customer.dispatcher_id",
        lazy="dynamic",
    )
    owned_customers = db.relationship(
        "Customer",
        back_populates="sales",
        foreign_keys="Customer.sales_id",
        lazy="dynamic",
    )

    def is_super_admin(self) -> bool:
        return self.role == "super_admin"

class SalesProfile(db.Model):
    """销售扩展表，记录派单序号等配置。"""

    __tablename__ = "sales_profiles"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, unique=True)
    dispatch_order = db.Column(db.Integer, nullable=False, default=0)
    is_available = db.Column(db.Boolean, default=True, nullable=False)  # 可用状态
    service_region = db.Column(db.String(128))  # 服务地区

    user = db.relationship("User", back_populates="sales_profile")
class Customer(db.Model):
    """客户表，包含派单和销售反馈信息。"""

    __tablename__ = "customers"

    id = db.Column(db.Integer, primary_key=True)

    name = db.Column(db.String(128), nullable=False)
    phone = db.Column(db.String(32))  # 联系方式（电话/微信号等）
    region = db.Column(db.String(64))
    fans_count = db.Column(db.Integer)
    image_path = db.Column(db.String(255))

    status = db.Column(
        db.String(32),
        nullable=False,
        default="pending",
    )  # pending / timeout / accepted / public_pool

    created_at = db.Column(db.DateTime, server_default=func.now())
    dispatch_time = db.Column(db.DateTime)
    accepted_time = db.Column(db.DateTime)

    sales_id = db.Column(db.Integer, db.ForeignKey("users.id"))
    operator_id = db.Column(db.Integer, db.ForeignKey("users.id"))  # 运营人员
    dispatcher_id = db.Column(db.Integer, db.ForeignKey("users.id"))
    creator_id = db.Column(db.Integer, db.ForeignKey("users.id"))

    is_converted = db.Column(db.Boolean, default=False)
    is_valid = db.Column(db.Boolean, default=True)
    invalid_proof_image = db.Column(db.String(255))
    remark = db.Column(db.Text)
    retry_count = db.Column(db.Integer, default=0, nullable=False)  # 重派次数

    # 业务展示用月度序号（北京自然月内从 1 递增，新月份重置）
    monthly_order_ym = db.Column(db.String(6), nullable=True, index=True)  # YYYYMM
    monthly_order_key = db.Column(db.Integer, nullable=True)

    sales = db.relationship("User", foreign_keys=[sales_id], back_populates="owned_customers")
    operator = db.relationship("User", foreign_keys=[operator_id])
    dispatcher = db.relationship(
        "User", foreign_keys=[dispatcher_id], back_populates="dispatched_customers"
    )
    creator = db.relationship(
        "User", foreign_keys=[creator_id], back_populates="created_customers"
    )

    notifications = db.relationship(
        "Notification", back_populates="customer", lazy="dynamic"
    )

    @property
    def monthly_display_id(self) -> str:
        if self.monthly_order_ym and self.monthly_order_key is not None:
            return f"{self.monthly_order_ym}-{self.monthly_order_key}"
        return "—"


class MonthlyCustomerSeq(db.Model):
    """按北京 YYYYMM 维度的月度客户序号分配（与 Customer 月度字段一致）。"""

    __tablename__ = "monthly_customer_seq"

    ym = db.Column(db.String(6), primary_key=True)
    last_seq = db.Column(db.Integer, nullable=False, default=0)


class Notification(db.Model):
    """通知记录表，用于记录派单通知发送结果。"""

    __tablename__ = "notifications"

    id = db.Column(db.Integer, primary_key=True)
    customer_id = db.Column(db.Integer, db.ForeignKey("customers.id"), nullable=False)
    sales_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)

    channel = db.Column(db.String(32))  # wechat / none
    content = db.Column(db.Text)
    status = db.Column(db.String(32), default="sent")  # sent / failed
    created_at = db.Column(db.DateTime, server_default=func.now())

    customer = db.relationship("Customer", back_populates="notifications")


class SystemConfig(db.Model):
    """简单的系统配置表，用于存储全局开关等设置。"""

    __tablename__ = "system_configs"

    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(64), unique=True, nullable=False)
    value = db.Column(db.String(256), nullable=True)

    @staticmethod
    def get_bool(key: str, default: bool = False) -> bool:
        """读取布尔配置，如果表不存在或出错则返回默认值并自动创建表。"""
        try:
            cfg = SystemConfig.query.filter_by(key=key).first()
        except Exception as exc:  # 表可能尚未创建
            from flask import current_app
            current_app.logger.warning("SystemConfig.get_bool failed, trying to create tables: %s", exc)
            # 自动创建缺失的表，然后重试一次
            db.create_all()
            cfg = SystemConfig.query.filter_by(key=key).first()

        if not cfg or cfg.value is None:
            return default
        return cfg.value == "1"

    @staticmethod
    def set_bool(key: str, value: bool) -> None:
        cfg = SystemConfig.query.filter_by(key=key).first()
        if not cfg:
            cfg = SystemConfig(key=key)
            db.session.add(cfg)
        cfg.value = "1" if value else "0"

class Region(db.Model):
    """地区表，用于管理自定义地区。"""

    __tablename__ = "regions"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False, unique=True)  # 地区名称
    is_active = db.Column(db.Boolean, default=True, nullable=False)  # 是否启用
    display_order = db.Column(db.Integer, nullable=False, default=0)  # 排序序号
    created_at = db.Column(db.DateTime, server_default=func.now())

