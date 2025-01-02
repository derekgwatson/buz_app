# /app/__init__.py
import os
import json
import logging
from flask import Flask
from dotenv import load_dotenv

# Import services and routes
from services.database import init_db_command, create_db_manager
from services.config_service import ConfigManager


# immediately load environment variables to they're available even before create_app is run
load_dotenv()


def create_app(config_name=''):
    import time
    from flask import g

    app = Flask(__name__)

    # Load environment variables
    app.secret_key = os.getenv("FLASK_SECRET", os.urandom(24))

    @app.before_request
    def before_request():
        """
        Initialize and close database connection for each request
        """
        from services.database import create_db_manager

        g.db = create_db_manager(app.config['database'])

        """Track the start time of each request."""
        g.start_time = time.time()

    @app.after_request
    def after_request(response):
        if hasattr(g, 'start_time'):
            duration = time.time() - g.start_time
            g.request_duration = f"{duration:.3f} seconds"

            if response.content_type == "text/html; charset=utf-8":
                response_data = response.get_data(as_text=True)
                if "[[ request_duration ]]" in response_data:
                    response_data = response_data.replace("[[ request_duration ]]", g.request_duration)
                    response.set_data(response_data)

        return response

    @app.teardown_request
    def teardown_request(exception):
        db = getattr(g, 'db', None)
        if db is not None:
            db.close()

    # Configure Flask app (no duplication)
    app.config.from_object(f'config.{config_name}Config')
    app.config.update(ConfigManager().config)  # Merge custom config

    # Resolve the root path of the project
    root_path = os.path.dirname(os.path.dirname(__file__))

    # Add the root path and other paths to the app's config
    app.config['upload_folder'] = os.path.join(root_path, app.config['upload_folder'])

    # Set up logging (only once)
    logging.basicConfig(
        level=logging.DEBUG if app.config['DEBUG'] else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logging.debug(f"upload folder set in app create is {app.config['upload_folder']}")

    # Load users from the .env file (if present)
    app.config['USERS'] = json.loads(os.getenv("USERS", "{}"))

    # Initialize database manager and store in app extensions
    app.extensions['db_manager'] = create_db_manager(app.config['database'])

    # Register CLI commands (for Flask CLI)
    app.cli.add_command(init_db_command)  # type: ignore

    # Register Blueprints
    from app.routes import main_routes
    from services.fabric_routes import fabrics_blueprint

    app.register_blueprint(main_routes)
    app.register_blueprint(fabrics_blueprint)

    return app
