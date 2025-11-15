# /app/__init__.py
import os
import json
import logging
from flask import Flask
from dotenv import load_dotenv
from config import ProductionConfig
from services.database import init_db_command, create_db_manager
from services.config_service import ConfigManager
from pathlib import Path
from app.routes import main_routes_bp, fabrics_bp, discount_groups_bp, lead_times_bp, excel_tools_bp, customer_automation_bp, max_discount_review_bp, quote_scraper_bp


load_dotenv()


def init_sentry():
    """Initialize Sentry error tracking if SENTRY_DSN is configured."""
    sentry_dsn = os.getenv("SENTRY_DSN")
    if not sentry_dsn:
        return

    try:
        import sentry_sdk
        from sentry_sdk.integrations.flask import FlaskIntegration
        from sentry_sdk.integrations.logging import LoggingIntegration

        # Determine environment from ENV variable or default to development
        environment = os.getenv("FLASK_ENV") or os.getenv("ENV") or "development"

        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=environment,
            integrations=[
                FlaskIntegration(),
                LoggingIntegration(
                    level=logging.INFO,       # Capture info and above as breadcrumbs
                    event_level=logging.ERROR  # Send errors and above as events
                ),
            ],
            # Set traces_sample_rate to 1.0 to capture 100% of transactions for performance monitoring.
            # Adjust this value in production (e.g., 0.1 for 10%)
            traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.1")),

            # Send default PII (Personally Identifiable Information) like user IP, etc.
            send_default_pii=True,

            # Attach stack traces to all messages
            attach_stacktrace=True,

            # Disable Sentry's debug logging (very verbose, only needed when debugging Sentry itself)
            # Set SENTRY_DEBUG=1 in .env to enable if needed
            debug=os.getenv("SENTRY_DEBUG", "0") == "1",
        )
        logging.info(f"Sentry initialized for environment: {environment}")
    except ImportError:
        logging.warning("sentry-sdk not installed, error tracking disabled")
    except Exception as e:
        logging.error(f"Failed to initialize Sentry: {e}")


# call this during app startup
def cleanup_stale_jobs(db):
    db.execute_query(
        "UPDATE jobs SET status='aborted', pct=0 WHERE status='running'"
    )
    db.commit()


def create_app(config_name: str = ""):
    import time
    from flask import g

    # Initialize Sentry before creating app to catch initialization errors
    init_sentry()

    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(ProductionConfig)

    # Secret
    app.secret_key = os.getenv("FLASK_SECRET", os.urandom(24))

    # Load base config (if you use config.py classes like DevelopmentConfig/ProductionConfig)
    if config_name:
        app.config.from_object(f"config.{config_name}Config")

    # Merge JSON config
    app.config.update(ConfigManager().config)

    # base dirs
    project_root = Path(__file__).resolve().parent.parent
    instance_root = Path(app.instance_path)
    instance_root.mkdir(parents=True, exist_ok=True)
    app.config["PROJECT_ROOT"] = str(project_root)
    app.config["INSTANCE_ROOT"] = str(instance_root)

    def _set_path(key: str, default_rel: str | Path, *, base: str = "instance", is_file: bool = False):
        """
        Resolve a config path and ensure its directory exists.
        - If app.config[key] is absolute, use it as-is.
        - If it's relative, anchor to `instance` (default) or `project`.
        - If it's missing, use `default_rel` anchored to the chosen base.
        - If `is_file=True`, create the parent dir; else create the dir itself.
        """
        val = app.config.get(key)
        base_dir = instance_root if base == "instance" else project_root

        if val:
            p = Path(val)
            if not p.is_absolute():
                p = (base_dir / p).resolve()
        else:
            p = (base_dir / Path(default_rel)).resolve()

        (p.parent if is_file else p).mkdir(parents=True, exist_ok=True)
        app.config[key] = str(p)
        return p

    # 1) Generic export root (folder)
    _set_path("EXPORT_ROOT", "exports", base="instance", is_file=False)

    # 2) Database (file)
    _set_path("database", "buz_data.db", base="instance", is_file=True)

    # 3) Upload folder (folder)
    _set_path("upload_folder", "uploads", base="instance", is_file=False)

    # 4) Upload output dir (defaults to upload_folder if not set)
    if not app.config.get("UPLOAD_OUTPUT_DIR"):
        app.config["UPLOAD_OUTPUT_DIR"] = app.config["upload_folder"]
    _set_path("UPLOAD_OUTPUT_DIR", "uploads", base="instance", is_file=False)

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
    app.register_blueprint(main_routes_bp)
    app.register_blueprint(fabrics_bp)
    app.register_blueprint(discount_groups_bp)
    app.register_blueprint(lead_times_bp)
    app.register_blueprint(excel_tools_bp)
    app.register_blueprint(customer_automation_bp)
    app.register_blueprint(max_discount_review_bp)
    app.register_blueprint(quote_scraper_bp)

    # CLI
    app.cli.add_command(init_db_command)  # type: ignore

    cleanup_stale_jobs(app.extensions["db_manager"])

    return app
