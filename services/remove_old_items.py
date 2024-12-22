import logging
import os
from services.database import DatabaseManager
from services.data_processing import get_old_buz_items_unleashed
from services.excel import OpenPyXLFileHandler


# Configure logging
logger = logging.getLogger(__name__)


def get_headers_config(app_config, file_key):
    """
    Retrieve and transform headers configuration from Flask app.config.

    Args:
        app_config (dict): The Flask app configuration containing headers JSON.
        file_key (str): Key to select the specific header configuration (e.g., 'buz_inventory_item_file').

    Returns:
        list[dict]: Transformed headers configuration mapping database fields to headers.
    """
    # Access the headers configuration
    raw_headers = app_config.get("headers", {}).get(file_key, [])

    # Transform to the required format
    headers_config = [
        {
            "database_field": header["database_field"],
            "spreadsheet_column": header["spreadsheet_column"],
            "column_letter": header["column_letter"]
        }
        for header in raw_headers
    ]
    return headers_config


def remove_old_items(db_manager: DatabaseManager, app_config, output_file: str):
    # get the items to remove
    items = get_old_buz_items_unleashed(db_manager=db_manager)
    logger.debug(f"Items is {items}")
    if not items:
        return None

    headers_config = get_headers_config(app_config, "buz_inventory_item_file")
    logger.debug(f"Headers config is {headers_config}")
    file_manager = OpenPyXLFileHandler.from_items(items=items, headers_config=headers_config, header_row=2)
    logger.debug(f"File Manager is {file_manager}")

    file_manager.save_workbook(os.path.join(app_config['upload_folder'], output_file))

    return items
