import pytest
from unittest.mock import MagicMock, patch
from werkzeug.datastructures import FileStorage
from services.upload import upload


@pytest.fixture
def mock_files(mocker):
    """Fixture for creating mock file storage objects."""
    inventory_file = MagicMock(spec=FileStorage, filename='inventory.xlsx')
    pricing_file = MagicMock(spec=FileStorage, filename='pricing.xlsx')
    unleashed_file = MagicMock(spec=FileStorage, filename='unleashed.csv')

    # Mock save methods
    inventory_file.save = MagicMock()
    pricing_file.save = MagicMock()
    unleashed_file.save = MagicMock()

    return {
        'inventory_file': inventory_file,
        'pricing_file': pricing_file,
        'unleashed_file': unleashed_file
    }


@patch('upload_module.process_workbook')
@patch('upload_module.insert_unleashed_data')
@patch('upload_module.OpenPyXLFileHandler')
def test_upload_all_files(mock_file_handler, mock_insert, mock_process, mock_files, app_config):
    """Test uploading all files successfully."""
    files = mock_files
    upload_folder = app_config['upload_folder']

    # Call the function
    result = upload(
        inventory_file=files['inventory_file'],
        inventory_file_expected_headers=app_config['headers']['buz_inventory_item_file'],
        inventory_file_db_fields=app_config['fields']['buz_inventory_item_file'],
        pricing_file=files['pricing_file'],
        pricing_file_expected_headers=app_config['headers']['buz_pricing_file'],
        pricing_file_db_fields=app_config['fields']['buz_pricing_file'],
        unleashed_file=files['unleashed_file'],
        unleashed_file_expected_headers=app_config['headers']['unleashed_csv_file'],
        upload_folder=upload_folder,
        invalid_pkid=app_config['invalid_pkid']
    )

    # Assertions for file saving
    files['inventory_file'].save.assert_called_once_with(f"{upload_folder}/inventory.xlsx")
    files['pricing_file'].save.assert_called_once_with(f"{upload_folder}/pricing.xlsx")
    files['unleashed_file'].save.assert_called_once_with(f"{upload_folder}/unleashed.csv")

    # Assertions for file processing
    mock_file_handler.assert_any_call(file_path=f"{upload_folder}/inventory.xlsx")
    mock_process.assert_any_call(
        file_handler=mock_file_handler.return_value,
        table_name='inventory_items',
        expected_headers=app_config['headers']['buz_inventory_item_file'],
        header_row=2
    )
    mock_process.assert_any_call(
        file_handler=mock_file_handler.return_value,
        table_name='pricing_data',
        expected_headers=app_config['headers']['buz_pricing_file'],
        header_row=1
    )

    # Assertions for Unleashed file insertion
    mock_insert.assert_called_once_with(f"{upload_folder}/unleashed.csv")

    # Final result assertion
    assert result == ['inventory_file', 'pricing_file', 'unleashed_file'], "Result file list mismatch."


def test_upload_no_files(app_config):
    """Test uploading with no files provided."""
    upload_folder = app_config['upload_folder']
    result = upload(
        inventory_file=None,
        inventory_file_expected_headers=app_config['headers']['buz_inventory_item_file'],
        inventory_file_db_fields=app_config['fields']['buz_inventory_item_file'],
        pricing_file=None,
        pricing_file_expected_headers=app_config['headers']['buz_pricing_file'],
        pricing_file_db_fields=app_config['fields']['buz_pricing_file'],
        unleashed_file=None,
        unleashed_file_expected_headers=app_config['headers']['unleashed_csv_file'],
        upload_folder=upload_folder,
        invalid_pkid=app_config['invalid_pkid']
    )
    assert result == [], "Result should be an empty list when no files are provided."
