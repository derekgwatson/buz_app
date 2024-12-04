import unittest
from unittest.mock import patch, MagicMock
from services.google_sheets_service import GoogleSheetsService


class TestGoogleSheetsService(unittest.TestCase):
    @patch('google_sheets_service.Credentials.from_service_account_info')
    @patch('google_sheets_service.gspread.authorize')
    def setUp(self, mock_authorize, mock_credentials):
        # Mock the credentials and gspread client
        mock_credentials.return_value = MagicMock()
        self.mock_client = MagicMock()
        mock_authorize.return_value = self.mock_client

        # Initialize GoogleSheetsService
        self.service = GoogleSheetsService(json_file="./static/dummy_service_account.json")

    def test_fetch_sheet_data_success(self):
        # Mock the worksheet and its method
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [['Name', 'Age'], ['Alice', '30']]
        self.mock_client.open_by_key.return_value.worksheet.return_value = mock_worksheet

        # Call fetch_sheet_data
        result = self.service.fetch_sheet_data('sheet_id', 'Sheet1!A1:B10')

        # Assert the result matches the mocked data
        self.assertEqual(result, [['Name', 'Age'], ['Alice', '30']])
        self.mock_client.open_by_key.assert_called_once_with('sheet_id')
        mock_worksheet.get_all_values.assert_called_once()

    def test_fetch_sheet_data_failure(self):
        # Simulate an APIError being raised by gspread
        self.mock_client.open_by_key.side_effect = Exception("APIError: Sheet not found")

        # Assert that fetch_sheet_data handles the exception gracefully
        result = self.service.fetch_sheet_data('invalid_sheet_id', 'Sheet1!A1:B10')

        # Assert that the result is an empty list
        self.assertEqual(result, [])

        # Assert that open_by_key was called once with the invalid sheet ID
        self.mock_client.open_by_key.assert_called_once_with('invalid_sheet_id')

    def test_insert_row_success(self):
        # Mock the worksheet and its method
        mock_worksheet = MagicMock()
        self.mock_client.open_by_key.return_value.worksheet.return_value = mock_worksheet

        # Call insert_row
        self.service.insert_row('sheet_id', ['Alice', '30'], worksheet_name='Sheet1')

        # Assert the mocked methods were called
        self.mock_client.open_by_key.assert_called_once_with('sheet_id')
        mock_worksheet.append_row.assert_called_once_with(['Alice', '30'])

    def test_insert_row_failure(self):
        # Simulate an APIError being raised by gspread
        self.mock_client.open_by_key.side_effect = Exception("APIError: Unable to insert row")

        # Call insert_row and ensure it handles the error
        with self.assertLogs(level='ERROR') as log:
            self.service.insert_row('invalid_sheet_id', ['Alice', '30'], worksheet_name='Sheet1')

        # Assert that the error log contains the expected message
        self.assertIn(
            "Error inserting row into spreadsheet invalid_sheet_id",
            log.output[0]
        )

        # Assert that open_by_key was called once with the invalid sheet ID
        self.mock_client.open_by_key.assert_called_once_with('invalid_sheet_id')


if __name__ == '__main__':
    unittest.main()
