import unittest
from unittest.mock import Mock
from services.excel import OpenPyXLFileHandler
from services.google_sheets_service import GoogleSheetsService
from services.backorders import process_inventory_backorder_with_services


class TestInventoryBackorder(unittest.TestCase):

    def setUp(self):
        self.file_handler = Mock(spec=OpenPyXLFileHandler)
        self.sheets_service = Mock(spec=GoogleSheetsService)
        self.spreadsheet_id = "test_spreadsheet_id"
        self.range_name = "Sheet1!A1:E10"
        self.original_filename = "original.xlsx"
        self.upload_filename = "upload.xlsx"

    def test_missing_columns_in_google_sheet(self):
        # Simulate missing required columns
        self.sheets_service.fetch_sheet_data.return_value = [
            ["Some Column", "Another Column"]
        ]

        with self.assertRaises(ValueError) as context:
            upload_wb, original_wb = process_inventory_backorder_with_services(
                self.file_handler,
                self.sheets_service,
                self.spreadsheet_id,
                self.range_name,
            )
        self.assertIn("Missing required columns", str(context.exception))

    def test_successful_backorder_processing(self):
        self.file_handler.read_sheet_to_dict.return_value = {
            "Product1": [
                {
                    "PkId": "1",
                    "Supplier Product Code": "SUP123",
                    "Warning": "",
                    "DescnPart1 (Material)": "Cotton",
                    "DescnPart2 (Material Types)": "Shirt",
                    "DescnPart3 (Colour)": "Blue",
                    "Operation": "",
                }
            ],
            "Product2": [
                {
                    "PkId": "1",
                    "Supplier Product Code": "SUP123",
                    "Warning": "",
                    "DescnPart1 (Material)": "Cotton",
                    "DescnPart2 (Material Types)": "Shirt",
                    "DescnPart3 (Colour)": "Blue",
                    "Operation": "",
        }
            ]
        }

        self.sheets_service.fetch_sheet_data.return_value = [
            ["Unleashed Code", "On backorder until"],
            ["SUP123", "2050-12-30"]
        ]

        upload_wb, original_wb = process_inventory_backorder_with_services(
            self.file_handler,
            self.sheets_service,
            self.spreadsheet_id,
            self.range_name,
        )

        self.file_handler.read_sheet_to_dict.assert_called_once_with(header_row=1)

        # Assertions: Upload file checks
        upload_sheet1 = upload_wb["Product1"]
        upload_sheet2 = upload_wb["Product2"]

        expected_warning = "Cotton Shirt Blue on backorder until 30 Dec 2050."

        # Check first product upload row
        upload_row1 = list(upload_sheet1.iter_rows(values_only=True))[1]
        self.assertEqual(upload_row1[2], expected_warning, "Upload sheet Product1 warning mismatch.")
        self.assertEqual(upload_row1[6], "E", "Upload sheet should have edit operation.")

        # Check second product upload row
        upload_row2 = list(upload_sheet2.iter_rows(values_only=True))[1]
        self.assertEqual(upload_row2[2], expected_warning, "Upload sheet Product2 warning mismatch.")
        self.assertEqual(upload_row2[6], "E", "Upload sheet should have edit operation.")

        # Assertions: Original file checks
        original_sheet1 = original_wb["Product1"]
        original_sheet2 = original_wb["Product2"]

        # Verify original rows are intact
        original_row1 = list(original_sheet1.iter_rows(values_only=True))[1]
        original_row2 = list(original_sheet2.iter_rows(values_only=True))[1]

        self.assertEqual(original_row1[2], "", "Original sheet Product1 warning should be empty.")
        self.assertEqual(original_row2[2], "", "Original sheet Product2 warning should be empty.")

    def test_expired_backorder_message(self):
        self.file_handler.read_sheet_to_dict.return_value = {
            "Inventory": [
                {
                    "PkId": "1",
                    "Supplier Product Code": "SUP123",
                    "Warning": "On backorder until 01 Dec 2023.",
                    "DescnPart1 (Material)": "Cotton",
                    "DescnPart2 (Material Types)": "Shirt",
                    "DescnPart3 (Colour)": "Blue"
                }
            ]
        }

        self.sheets_service.fetch_sheet_data.return_value = [
            ["Unleashed Code", "On backorder until"],
            ["SUP123", "2024-12-30"]
        ]

        upload_wb, original_wb = process_inventory_backorder_with_services(
            self.file_handler,
            self.sheets_service,
            self.spreadsheet_id,
            self.range_name,
        )

        self.file_handler.read_sheet_to_dict.assert_called_once_with(header_row=1)


if __name__ == '__main__':
    unittest.main()
