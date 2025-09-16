# /app/__init__.py
import os
import json
import logging
from flask import Flask
from dotenv import load_dotenv

from services.database import init_db_command, create_db_manager
from services.config_service import ConfigManager

load_dotenv()


def create_app(config_name: str = ""):
    import time
    from flask import g

    app = Flask(__name__)

    # Secret
    app.secret_key = os.getenv("FLASK_SECRET", os.urandom(24))

    # Load base config (if you use config.py classes like DevelopmentConfig/ProductionConfig)
    if config_name:
        app.config.from_object(f"config.{config_name}Config")

    # Merge JSON config
    app.config.update(ConfigManager().config)

    # Project root (parent of /app)
    root_path = os.path.dirname(os.path.dirname(__file__))

    # Ensure database path is absolute
    db_cfg = app.config.get("database", "buz_data.db")
    if not os.path.isabs(db_cfg):
        db_cfg = os.path.abspath(os.path.join(root_path, db_cfg))
    app.config["database"] = db_cfg

    # Ensure upload_folder is absolute
    up_cfg = app.config.get("upload_folder", "uploads")
    if not os.path.isabs(up_cfg):
        up_cfg = os.path.abspath(os.path.join(root_path, up_cfg))
    app.config["upload_folder"] = up_cfg

    # Ensure UPLOAD_OUTPUT_DIR is set and absolute
    out_cfg = app.config.get("UPLOAD_OUTPUT_DIR", app.config["upload_folder"])
    if not os.path.isabs(out_cfg):
        out_cfg = os.path.abspath(os.path.join(root_path, out_cfg))
    app.config["UPLOAD_OUTPUT_DIR"] = out_cfg

    # Create output dir if missing
    os.makedirs(app.config["UPLOAD_OUTPUT_DIR"], exist_ok=True)

    # Logging (basic)
    logging.basicConfig(
        level=logging.DEBUG if app.config.get("DEBUG") else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,  # prevent duplicate handlers in some reload scenarios
    )
    logging.debug("upload_folder=%s  output_dir=%s  database=%s",
                  app.config["upload_folder"], app.config["UPLOAD_OUTPUT_DIR"], app.config["database"])

    # Users from env (optional)
    app.config["USERS"] = json.loads(os.getenv("USERS", "{}"))

    # App-wide DB manager (optional utility; you’re opening per-request below)
    app.extensions["db_manager"] = create_db_manager(app.config["database"])

    @app.before_request
    def before_request():
        """Per-request DB + request timing."""
        g.db = create_db_manager(app.config["database"])
        g.start_time = time.time()

    @app.after_request
    def after_request(response):
        if hasattr(g, "start_time"):
            duration = time.time() - g.start_time
            g.request_duration = f"{duration:.3f} seconds"
            if response.content_type and response.content_type.startswith("text/html"):
                html = response.get_data(as_text=True)
                if "[[ request_duration ]]" in html:
                    response.set_data(html.replace("[[ request_duration ]]", g.request_duration))
        return response

    @app.teardown_request
    def teardown_request(_):
        db = getattr(g, "db", None)
        if db is not None:
            db.close()

    # Blueprints
    from app.routes import main_routes
    from services.fabric_routes import fabrics_blueprint
    app.register_blueprint(main_routes)
    app.register_blueprint(fabrics_blueprint)

    # CLI
    app.cli.add_command(init_db_command)  # type: ignore

    return app
