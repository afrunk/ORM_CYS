from __future__ import annotations

from flask import jsonify

from crm import create_app
from crm.customer.routes import reassign_timeouts


def create_flask_app() -> "Flask":
    """兼容 WSGI 的应用创建函数。"""
    from flask import Flask
    from flask import Flask as _Flask

    app: "Flask" = create_app()

    # 健康检查端点（供 watchdog 监控使用）
    @app.route("/health")
    def health_check():
        return jsonify({"status": "ok"})

    # 注册一个简单的 CLI 命令，用于执行超时单重派
    @app.cli.command("reassign-timeouts")
    def reassign_timeouts_command():
        """扫描 pending 且超时的客户并进行重派。"""
        from click import echo

        with app.app_context():
            count = reassign_timeouts()
            echo(f"本次共重派超时客户 {count} 个。")

    return app


app = create_flask_app()

if __name__ == "__main__":
    # 对外开放 8000 端口
    app.run(host="0.0.0.0", port=8000, debug=False)

