from app import create_app
import unittest
from unittest.mock import MagicMock, patch
from services.remove_old_items import delete_deprecated_items_request, delete_deprecated_items, process_sheet
from tests.test_helpers import get_dummy_unleashed_data, get_dummy_inventory_items


class TestDeleteDeprecatedItems(unittest.TestCase):
    def setUp(self):
        # Create a test-specific app instance
        self.app = create_app(upload_folder="tests/uploads")

    @patch("services.google_sheets_service.GoogleSheetsService.__init__", return_value=None)
    @patch("services.google_sheets_service.GoogleSheetsService.fetch_sheet_data")
    @patch("services.excel.OpenPyXLFileHandler")
    @patch("services.config_service.ConfigManager")
    def test_delete_deprecated_items_request(
            self, MockConfigManager, MockFileHandler, MockFetchSheetData, MockAuthenticateGoogleSheets
    ):
        # Mock Unleashed data from google sheet
        MockFetchSheetData.return_value = get_dummy_unleashed_data()

        # Mock request object
        mock_request = MagicMock()
        mock_request.files.get.return_value = get_dummy_inventory_items()

        # Mock ConfigManager
        mock_config_manager = MockConfigManager.return_value
        mock_config_manager.config = {
            "unleashed_data_extract_spreadsheet_id": "mock_spreadsheet_id",
            "unleashed_data_extract_range": "mock_range",
        }

        # Mock OpenPyXLFileHandler
        mock_file_handler = MockFileHandler.return_value
        mock_file_handler.load_workbook.return_value = None  # Simulate loading a workbook
        mock_file_handler.get_sheet_names.return_value = ["Sheet1", "Sheet2"]  # Simulate sheet names
        mock_file_handler.get_sheet.side_effect = lambda name: MagicMock()  # Simulate sheet objects

        # Call function
        output_file = delete_deprecated_items_request(
            request=mock_request,
            output_file="mock_output.xlsx",
            json_file="dummy_service_account.json",
        )

        # Assertions
        MockFetchSheetData.assert_called_once_with("mock_spreadsheet_id", "mock_range")
        mock_file_handler.load_workbook.assert_called_once()
        self.assertEqual(output_file, "mock_output.xlsx")

    @patch("services.excel.OpenPyXLFileHandler")
    @patch("services.google_sheets_service.GoogleSheetsService")
    def test_delete_deprecated_items(self, MockGoogleSheetsService, MockFileHandler):
        # Mock dependencies
        mock_file_handler = MockFileHandler.return_value
        mock_file_handler.get_sheet_names.return_value = ["Sheet1", "Sheet2"]
        mock_file_handler.get_sheet.side_effect = [MagicMock(), MagicMock()]

        mock_sheets_service = MockGoogleSheetsService.return_value
        mock_sheets_service.fetch_sheet_data.return_value = [["Code1"], ["Code2"]]

        google_product_codes = {"code1", "code2"}

        # Call function
        delete_deprecated_items(
            mock_file_handler,
            mock_sheets_service,
            spreadsheet_id="mock_spreadsheet_id",
            range_name="mock_range",
            output_file="output.xlsx",
        )

        # Assertions
        mock_file_handler.load_workbook.assert_called_once()
        mock_sheets_service.fetch_sheet_data.assert_called_once_with("mock_spreadsheet_id", "mock_range")
        mock_file_handler.get_sheet_names.assert_called_once()

    def test_process_sheet(self):
        # Mock sheet object
        mock_sheet = MagicMock()
        mock_sheet.iter_rows.return_value = [
            [MagicMock(value="UNLEASHED"), MagicMock(value="Code1")],
            [MagicMock(value="UNLEASHED"), MagicMock(value=None)],
            [MagicMock(value="OtherSupplier"), MagicMock(value="Code3")]
        ]

        # Mock Google product codes
        google_product_codes = {"code1", "code2"}

        # Call function
        retained_rows = process_sheet(mock_sheet, google_product_codes)

        # Assertions
        self.assertEqual(len(retained_rows), 0)  # No rows retained
        mock_sheet.delete_rows.assert_called()

    def test_delete_deprecated_items_empty_workbook(self):
        mock_file_handler = MagicMock()
        mock_file_handler.get_sheet_names.return_value = []

        mock_sheets_service = MagicMock()
        mock_sheets_service.fetch_sheet_data.return_value = [["Code1"], ["Code2"]]

        with self.assertLogs("services.remove_old_items", level="INFO") as log:
            delete_deprecated_items(
                mock_file_handler,
                mock_sheets_service,
                spreadsheet_id="mock_spreadsheet_id",
                range_name="mock_range",
                output_file="output.xlsx",
            )

        self.assertIn("No sheets to process", log.output)


    def test_process_sheet_empty_supplier_code(self):
        mock_sheet = MagicMock()
        mock_sheet.iter_rows.return_value = [
            [MagicMock(value=None), MagicMock(value="Code1")]
        ]
        google_product_codes = {"code1", "code2"}

        retained_rows = process_sheet(mock_sheet, google_product_codes)
        self.assertEqual(len(retained_rows), 0)
        mock_sheet.delete_rows.assert_called_once_with(3)

