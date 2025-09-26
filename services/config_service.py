import json
import os
import logging
from typing import Callable, List


# Configure logging
logger = logging.getLogger(__name__)


class ConfigManager:
    def __init__(self, config_path="config.json"):
        self._config_path = config_path
        self._resolved_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), self._config_path)
        self._last_load_error = None
        self.config = self._load_config()
        self._observers = []

    @property
    def last_load_error(self):
        return self._last_load_error

    @property
    def resolved_path(self):
        return self._resolved_path

    def register_observer(self, observer: Callable[[List[str], object], None]):
        """Register an observer to be notified on configuration changes."""
        self._observers.append(observer)

    def _notify_observers(self, keys: List[str], value):
        """Notify all registered observers about a configuration change."""
        for observer in self._observers:
            observer(keys, value)

    def _load_config(self):
        """Load configuration from the file or initialize an empty config."""
        path = self._resolved_path
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in {path}. Loading empty configuration.")
        return {}

    def save_config(self):
        """Save the current configuration to the file."""
        try:
            with open(self._resolved_path, "w") as f:
                json.dump(self.config, f, indent=4)
        except (OSError, IOError) as e:
            logger.error(f"Unable to save configuration to {self._config_path}. {e}")

    def update_config(self, keys: List[str], value):
        """Update a nested configuration key with a new value if it has changed."""
        config = self.config
        for key in keys[:-1]:
            if key not in config or not isinstance(config[key], dict):
                config[key] = {}
            config = config[key]

        last_key = keys[-1]
        if config.get(last_key) != value:
            config[last_key] = value
            self.save_config()
            self._notify_observers(keys, value)
            return True
        return False

    # services/config_service.py

    def get(self, *keys, default=None):
        """
        Access nested values.
        Supports both:
          get("a", "b", "c")  and  get("a.b.c")
        """
        # Allow a single dotted string
        if len(keys) == 1 and isinstance(keys[0], str) and "." in keys[0]:
            keys = keys[0].split(".")

        node = self.config
        for k in keys:
            if isinstance(node, dict) and k in node:
                node = node[k]
            else:
                return default
        return node


class SpreadsheetConfigUpdater:
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager

    def update_spreadsheet_config(self, spreadsheet_name, new_id=None, new_range=None):
        """Update the spreadsheet configuration and log changes."""
        updated = False
        if new_id:
            if self.config_manager.update_config(["spreadsheets", spreadsheet_name, "id"], new_id):
                logger.info(f"Updated {spreadsheet_name} spreadsheet ID to {new_id}")
                updated = True
        if new_range:
            if self.config_manager.update_config(["spreadsheets", spreadsheet_name, "range"], new_range):
                logger.info(f"Updated {spreadsheet_name} spreadsheet range to {new_range}")
                updated = True
        return updated

# Example of using the class externally
# app.config.update(ConfigManager().get_flat_config())
