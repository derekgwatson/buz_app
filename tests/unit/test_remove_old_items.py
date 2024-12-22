from app import create_app
import unittest
from unittest.mock import MagicMock, patch
from services.remove_old_items import (
    delete_deprecated_items_request,
    delete_deprecated_items,
    process_sheet,
)


class TestDeleteDeprecatedItems(unittest.TestCase):
    def setUp(self):
        """Set up a test-specific app instance."""
        self.app = create_app()

    @patch("services.google_sheets_service.GoogleSheetsService.__init__", return_value=None)
    @patch("services.google_sheets_service.GoogleSheetsService.fetch_sheet_data")
    @patch("services.excel.OpenPyXLFileHandler")
    @patch("services.config_service.ConfigManager")
    def test_delete_deprecated_items_request(
        self, mock_config_manager, mock_file_handler, mock_unleashed_data, MockAuthenticateGoogleSheets
    ):
        ########################################################################################
        # Set up for the test
        """Test the delete_deprecated_items_request function."""
        # Mock Unleashed data
        MockFetchSheetData.return_value = get_dummy_unleashed_data()

        headers_config = [
          {"spreadsheet_column": "PkId", "database_field": "PkId", "column_letter": "A"},
          {"spreadsheet_column": "Code", "database_field": "Code", "column_letter": "B"},
          {"spreadsheet_column": "Description", "database_field": "Description", "column_letter": "C"},
          {"spreadsheet_column": "DescnPart1 (Material)", "database_field": "DescnPart1", "column_letter": "D"},
          {"spreadsheet_column": "DescnPart2 (Material Types)", "database_field": "DescnPart2", "column_letter": "E"},
          {"spreadsheet_column": "Operation", "database_field": "Operation", "column_letter": "F"},
        ]

        # Mock request object
        mock_request = MagicMock()
        mock_request.files.get.return_value = get_dummy_inventory_items()

        # Mock ConfigManager
        mock_config = mock_config_manager.return_value
        mock_config.config = {
            "unleashed_data_extract_spreadsheet_id": "mock_spreadsheet_id",
            "unleashed_data_extract_range": "mock_range",
        }

        # Mock OpenPyXLFileHandler
        mock_file_handler = MockFileHandler.return_value
        mock_file_handler.get_sheet_names.return_value = ["Sheet1", "Sheet2"]
        mock_file_handler.get_sheet.side_effect = lambda name: MagicMock()
        ########################################################################################

        ########################################################################################
        # Now call the actual function being tested
        output_file = delete_deprecated_items_request(
            request=mock_inveotory_file,
            output_file="mock_output.xlsx",
            json_file="dummy_service_account.json",
        )
        ########################################################################################

        ########################################################################################
        # And check everything is as it should be
        MockFetchSheetData.assert_called_once_with("mock_spreadsheet_id", "mock_range")
        self.assertEqual(output_file, "mock_output.xlsx")
        ########################################################################################

    @patch("services.excel.OpenPyXLFileHandler")
    @patch("services.google_sheets_service.GoogleSheetsService")
    def test_delete_deprecated_items(self, MockGoogleSheetsService, MockFileHandler):
        """Test the delete_deprecated_items function."""
        # Mock dependencies
        mock_file_handler = MockFileHandler.return_value
        mock_file_handler.get_sheet_names.return_value = ["Sheet1", "Sheet2"]
        mock_file_handler.get_sheet.side_effect = [MagicMock(), MagicMock()]

        mock_sheets_service = MockGoogleSheetsService.return_value
        mock_sheets_service.fetch_sheet_data.return_value = [["Code1"], ["Code2"]]

        # Call function
        delete_deprecated_items(
            mock_file_handler,
            mock_sheets_service,
            spreadsheet_id="mock_spreadsheet_id",
            range_name="mock_range",
            output_file="output.xlsx",
        )

        # Assertions
        mock_sheets_service.fetch_sheet_data.assert_called_once_with("mock_spreadsheet_id", "mock_range")
        mock_file_handler.get_sheet_names.assert_called_once()

    def test_process_sheet(self):
        """Test the process_sheet function."""
        # Mock sheet object
        mock_sheet = MagicMock()
        mock_sheet.iter_rows.return_value = [
            [MagicMock(value="UNLEASHED"), MagicMock(value="Code1")],
            [MagicMock(value="UNLEASHED"), MagicMock(value=None)],
            [MagicMock(value="OtherSupplier"), MagicMock(value="Code3")],
        ]

        # Mock Google product codes
        google_product_codes = {"code1", "code2"}

        # Call function
        retained_rows = process_sheet(mock_sheet, google_product_codes)

        # Assertions
        self.assertEqual(len(retained_rows), 0)
        mock_sheet.delete_rows.assert_called()

    def test_delete_deprecated_items_empty_workbook(self):
        """Test delete_deprecated_items with an empty workbook."""
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
        """Test process_sheet with empty supplier codes."""
        mock_sheet = MagicMock()
        mock_sheet.iter_rows.return_value = [
            [MagicMock(value=None), MagicMock(value="Code1")]
        ]
        google_product_codes = {"code1", "code2"}

        retained_rows = process_sheet(mock_sheet, google_product_codes)

        # Assertions
        self.assertEqual(len(retained_rows), 0)
        mock_sheet.delete_rows.assert_called_once_with(3)
