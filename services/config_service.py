import json
import os


class ConfigManager:
    def __init__(self, config_path="config.json"):
        """Initialize ConfigManager with the path to the config file."""
        self._config_path = config_path
        self.config = self._load_config()

    def _load_config(self):
        """Load configuration from the file."""
        if not os.path.exists(self._config_path):
            return {}
        with open(self._config_path, "r") as f:
            return json.load(f)

    def _save_config(self):
        """Save the current configuration to the file."""
        with open(self._config_path, "w") as f:
            json.dump(self.config, f, indent=4)

    def _update_config(self, key, value):
        """
        Update a configuration key with a new value if it has changed.
        Saves the file only if an update occurs.
        Returns:
            bool: True if the configuration was updated, False otherwise.
        """
        if self.config.get(key) != value:
            self.config[key] = value
            self._save_config()
            return True
        return False

    def update_config_backorder(self, spreadsheet_id=None, spreadsheet_range=None):
        """
        Update the backorder spreadsheet ID and range in the configuration.
        Saves the file only if updates occur.
        Args:
            spreadsheet_id (str): The new spreadsheet ID.
            spreadsheet_range (str): The new spreadsheet range.
        Returns:
            bool: True if any updates occurred, False otherwise.
        """
        return any([
            self._update_config('backorder_spreadsheet_id', spreadsheet_id),
            self._update_config('backorder_spreadsheet_range', spreadsheet_range)
        ])

    def update_config_unleashed_data_extract(self, spreadsheet_id=None, spreadsheet_range=None):
        """
        Update the unleashed_data_extract spreadsheet ID and range in the configuration.
        Saves the file only if updates occur.
        Args:
            spreadsheet_id (str): The new spreadsheet ID.
            spreadsheet_range (str): The new spreadsheet range.
        Returns:
            bool: True if any updates occurred, False otherwise.
        """
        return any([
            self._update_config('unleashed_data_extract_spreadsheet_id', spreadsheet_id),
            self._update_config('unleashed_data_extract_range', spreadsheet_range)
        ])
