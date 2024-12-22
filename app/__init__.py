# /app/__init__.py
import os
import json
import logging
from flask import Flask
from dotenv import load_dotenv

# Import services and routes
from services.database import init_db_command, create_db_manager
from services.config_service import ConfigManager
from app.routes import main_routes


# immediately load environment variables to they're available even before create_app is run
load_dotenv()


def create_app(config_name='default'):
    app = Flask(__name__)

    # Load environment variables
    app.secret_key = os.getenv("FLASK_SECRET", os.urandom(24))

    # Configure Flask app (no duplication)
    app.config.from_object(f'config.{config_name.capitalize()}Config')
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
    app.register_blueprint(main_routes)

    return app
