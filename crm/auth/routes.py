from __future__ import annotations

from flask import (
    Blueprint,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash

from ..models import User

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/", methods=["GET", "POST"])
@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        user = User.query.filter_by(username=username).first()
        if not user or not check_password_hash(user.password_hash, password):
            flash("用户名或密码错误。", "danger")
            return redirect(url_for("auth.login"))

        if not user.is_active:
            flash("账户已被禁用，请联系管理员。", "warning")
            return redirect(url_for("auth.login"))

        session["user_id"] = user.id
        session.permanent = True
        flash("登录成功。", "success")
        return redirect(url_for("customer.customer_list"))

    return render_template("auth/login.html")


@auth_bp.route("/logout")
def logout():
    session.pop("user_id", None)
    flash("您已退出登录。", "info")
    return redirect(url_for("auth.login"))


