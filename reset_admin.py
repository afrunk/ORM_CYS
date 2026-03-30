"""
重置超级管理员账号为：
  username : superadmin
  password : superadmin123
  role     : super_admin
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from crm import create_app
from crm.extensions import db
from crm.models import User
from werkzeug.security import generate_password_hash

app = create_app()

with app.app_context():
    password_hash = generate_password_hash("superadmin123")
    user = User.query.filter_by(username="superadmin").first()

    if user:
        user.password_hash = password_hash
        user.role = "super_admin"
        user.is_active = True
        print(f"[OK] 更新已有账号 superadmin → role={user.role}, is_active={user.is_active}")
    else:
        user = User(
            username="superadmin",
            password_hash=password_hash,
            role="super_admin",
            is_active=True,
        )
        db.session.add(user)
        print("[OK] 新建账号 superadmin → role=super_admin, is_active=True")

    db.session.commit()
    print("超级管理员账号已重置完成。")
    print("  登录地址 : http://<your-server>:5000/login")
    print("  用户名   : superadmin")
    print("  密码     : superadmin123")
