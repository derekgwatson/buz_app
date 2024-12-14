import json
import os


class ConfigManager:
    def __init__(self, app=None, config_path="config.json"):
        """Initialize ConfigManager with the path to the config file."""
        self._app = app
        self._config_path = config_path
        self.config = self._load_config()
        self._refresh_app()

    def _load_config(self):
        """Load configuration from the file or initialize an empty config."""
        if os.path.exists(self._config_path):
            with open(self._config_path, "r") as f:
                return json.load(f)
        return {}

    def _flatten_config(self, config=None, prefix=""):
        """
        Flatten the nested config dictionary into dot-separated keys.

        :param config: The configuration to flatten (defaults to self.config).
        :param prefix: The prefix for nested keys during recursion.
        :return: A flattened dictionary with dot-separated keys.
        """
        if config is None:
            config = self.config

        flat_config = {}
        for key, value in config.items():
            if isinstance(value, dict):
                flat_config.update(self._flatten_config(value, f"{prefix}{key}."))
            else:
                flat_config[f"{prefix}{key}"] = value
        return flat_config

    def _refresh_app(self):
        """
        Refresh Flask's app config with current config (if it's been passed)

        :param app: The Flask app instance.
        """
        if self._app:
            self._app.config.update(self._flatten_config())

    def _save_config(self):
        """Save the current configuration to the file."""
        with open(self._config_path, "w") as f:
            json.dump(self.config, f, indent=4)

    def _update_config(self, keys, value):
        """
        Update a nested configuration key with a new value if it has changed.
        Saves the file only if an update occurs.

        :param keys: A list of keys representing the nested path.
        :type keys: list[str] | tuple[str]
        :param value: The new value to set.
        :return: True if the configuration was updated, False otherwise.
        :rtype: bool
        """
        # Traverse to the deepest nested key
        config = self.config
        for key in keys[:-1]:
            if key not in config or not isinstance(config[key], dict):
                config[key] = {}
            config = config[key]

        # Update the final key if the value has changed
        last_key = keys[-1]
        if config.get(last_key) != value:
            config[last_key] = value
            self._save_config()
            return True
        return False

    def get(self, *keys, default=None):
        """
        Safely access nested configuration values.

        Example:
            If config.json contains:
            {
                "spreadsheets": {
                    "backorders": {
                        "id": "spreadsheet_123",
                        "range": "A:B"
                    }
                }
            }

            # Access a nested key
            spreadsheet_id = config_manager.get("spreadsheets", "backorders", "id")
            print(spreadsheet_id)  # Output: spreadsheet_123

            # Use a default if a key doesn't exist
            unknown_value = config_manager.get("spreadsheets", "unknown", "id", default="Not Found")
            print(unknown_value)  # Output: Not Found

        :param keys: The sequence of keys to access nested values.
        :param default: The default value if the key path doesn't exist.
        :return: The nested value or the default.
        """
        config = self.config
        for key in keys:
            if isinstance(config, dict):
                config = config.get(key, default)
            else:
                return default
        return config

    def _update_config_spreadsheet(self, spreadsheet_name, new_spreadsheet_id=None, new_spreadsheet_range=None):
        """
        Update the backorder spreadsheet ID and range in the configuration.
        Saves the file only if updates occur.
        Args:
            new_spreadsheet_id (str): The new spreadsheet ID.
            new_spreadsheet_range (str): The new spreadsheet range.
        Returns:
            bool: True if any updates occurred, False otherwise.
        """

        if new_spreadsheet_id:
            updated_id = self._update_config(
                ["spreadsheets", spreadsheet_name, "id"],
                new_spreadsheet_id
            )
            if updated_id:
                print(f"{spreadsheet_name} spreadsheet ID updated!")
        else:
            updated_id = False

        if new_spreadsheet_range:
            updated_range = self._update_config(
                ["spreadsheets", spreadsheet_name, "range"],
                new_spreadsheet_range
            )
            if updated_range:
                print(f"{spreadsheet_name} spreadsheet range updated!")
        else:
            updated_range = False

        if updated_id or updated_range:
            self._refresh_app()
            return True
        return False

    def update_config_unleashed_data_extract(self, new_spreadsheet_id=None, new_spreadsheet_range=None):
        """
        Update the unleashed_data_extract spreadsheet ID and range in the configuration.
        Saves the file only if updates occur.
        Args:
            new_spreadsheet_id (str): The new spreadsheet ID.
            new_spreadsheet_range (str): The new spreadsheet range.
        Returns:
            bool: True if any updates occurred, False otherwise.
        """
        return self._update_config_spreadsheet("unleashed_data_extract", new_spreadsheet_id, new_spreadsheet_range)

    def update_config_backorders(self, new_spreadsheet_id=None, new_spreadsheet_range=None):
        """
        Update the backorders spreadsheet ID and range in the configuration.
        Saves the file only if updates occur.
        Args:
            new_spreadsheet_id (str): The new spreadsheet ID.
            new_spreadsheet_range (str): The new spreadsheet range.
        Returns:
            bool: True if any updates occurred, False otherwise.
        """
        return self._update_config_spreadsheet("backorders", new_spreadsheet_id, new_spreadsheet_range)
