from services.data_processing import insert_unleashed_data, update_table_history, get_last_upload_time
from services.process_buz_workbooks import process_workbook
from services.excel import OpenPyXLFileHandler
from werkzeug.datastructures import FileStorage
from services.database import DatabaseManager
import os


def upload(
        db_manager: DatabaseManager,
        inventory_file: FileStorage,
        inventory_file_expected_headers: list[str],
        inventory_file_db_fields: list[str],
        pricing_file: FileStorage,
        pricing_file_expected_headers: list[str],
        pricing_file_db_fields: list[str],
        unleashed_file: FileStorage,
        unleashed_file_expected_headers: list[str],
        upload_folder: str,
        invalid_pkid: str,
        override_friendly_descriptions_id: str,
        override_friendly_descriptions_range: str,
        ignored_groups: list[str],
):
    uploaded_files = {}

    if inventory_file:
        inventory_file_path = os.path.join(upload_folder, inventory_file.filename)
        inventory_file.save(inventory_file_path)
        process_workbook(
            db_manager=db_manager,
            file_handler=OpenPyXLFileHandler.from_file(file_path=inventory_file_path),
            table_name='inventory_items',
            expected_headers=inventory_file_expected_headers,
            db_fields=inventory_file_db_fields,
            header_row=2,
            invalid_pkid=invalid_pkid,
            ignored_groups=ignored_groups
        )
        update_table_history(db_manager=db_manager, table_name='inventory_items')
        last_upload = get_last_upload_time(db_manager, 'inventory_items')
        uploaded_files['inventory_file'] = last_upload

    if pricing_file:
        pricing_file_path = os.path.join(upload_folder, pricing_file.filename)
        pricing_file.save(pricing_file_path)
        process_workbook(
            db_manager=db_manager,
            file_handler=OpenPyXLFileHandler.from_file(file_path=pricing_file_path),
            table_name='pricing_data',
            expected_headers=pricing_file_expected_headers,
            db_fields=pricing_file_db_fields,
            header_row=1,
            invalid_pkid=invalid_pkid,
            ignored_groups=ignored_groups
        )
        update_table_history(db_manager=db_manager, table_name='pricing_data')
        last_upload = get_last_upload_time(db_manager, 'pricing_data')
        uploaded_files['pricing_file'] = last_upload

    if unleashed_file:
        # Load friendly descriptions from Google Sheet
        from services.google_sheets_service import GoogleSheetsService
        from services.fabric_helpers import load_friendly_descriptions_from_google_sheet

        g_sheets_service = GoogleSheetsService("credentials/buz-app-439103-b6ae046c4723.json")
        friendly_overrides = load_friendly_descriptions_from_google_sheet(
            g_sheets_service,
            spreadsheet_id=override_friendly_descriptions_id,
            range_name=override_friendly_descriptions_range
        )

        unleashed_file_path = os.path.join(upload_folder, unleashed_file.filename)
        unleashed_file.save(unleashed_file_path)
        insert_unleashed_data(
            db_manager=db_manager,
            file_path=unleashed_file_path,
            expected_headers=unleashed_file_expected_headers,
            overrides=friendly_overrides
        )
        update_table_history(db_manager=db_manager, table_name='unleashed_products')
        last_upload = get_last_upload_time(db_manager, 'unleashed_products')
        uploaded_files['unleashed_file'] = last_upload

    return uploaded_files


def init_last_upload_times(db_manager):
    """Initialize last upload times for each file."""
    return {
        'inventory_file': get_last_upload_time(db_manager, 'inventory_items'),
        'pricing_file': get_last_upload_time(db_manager, 'pricing_data'),
        'unleashed_file': get_last_upload_time(db_manager, 'unleashed_products')
    }
