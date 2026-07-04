from __future__ import annotations

from flask import jsonify

from crm import create_app
from crm.customer.routes import reassign_timeouts


def create_flask_app() -> "Flask":
    """兼容 WSGI 的应用创建函数。

    注意：不要在模块顶层调用 create_app()！
    - Gunicorn 通过 `app:create_flask_app` 加载时，模块顶层执行会让工厂被调用两次
      （顶层一次 + Gunicorn 加载时又一次），导致 scheduler、蓝图重复注册。
    - Flask debug 模式（reloader）下也会让模块被执行两次。
    所以这里只提供工厂函数，调用方按需调用。
    """
    from flask import Flask

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


if __name__ == "__main__":
    # 直接 python app.py 启动时，正常构造并运行
    app = create_flask_app()
    # 对外开放 8000 端口。
    # 注意：threaded=True（多线程模式）。
    # 历史教训：之前用单线程 (threaded=False) 时，慢业务请求（如 /customers/ 列表页）
    # 会独占唯一的请求线程，导致所有其他用户的请求（甚至 /health）排队到 8s 超时。
    # 多线程下，需要保证 CPU 密集型后台任务（图片预览/缩略图生成）不抢占 GIL；
    # 这由 crm/utils/images.py 的 ThreadPoolExecutor (max_workers=2) 控制。
    from werkzeug.serving import make_server

    server = make_server(host="0.0.0.0", port=8000, app=app, threaded=True)
    server.serve_forever()

