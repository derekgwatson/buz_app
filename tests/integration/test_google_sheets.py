import unittest
from services.google_sheets_service import GoogleSheetsService


class TestGoogleSheetsServiceIntegration(unittest.TestCase):
    def setUp(self):
        # Initialize GoogleSheetsService with actual credentials
        self.json_file = "./static/service_account.json"  # Path to your real service account credentials
        self.spreadsheet_id = "1Z2hwnG9EqTvP2lW-zmR62pZEgfNl6lb-pC6zyCMoXzU"  # Replace with your test Google Sheets ID
        self.service = GoogleSheetsService(json_file=self.json_file)

    def test_fetch_sheet_data_success(self):
        # Ensure the test sheet has some data in the specified range
        range_name = "Sheet1!A1:B21"

        # Call fetch_sheet_data
        result = self.service.fetch_sheet_data(self.spreadsheet_id, range_name)

        # Assert that the result is not empty and matches the expected structure
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)  # Ensure at least one row of data
        self.assertEqual(result[1][1], "Galaxy")

    def test_insert_row_success(self):
        # Insert a row into the sheet
        row_data = ["25", "Test Entry"]
        worksheet_name = "Sheet1"

        self.service.insert_row(self.spreadsheet_id, row_data, worksheet_name=worksheet_name)

        # Fetch data again to verify the row was inserted
        result = self.service.fetch_sheet_data(self.spreadsheet_id, "Sheet1!A1:B10")

        # Assert the row is present in the data
        self.assertIn(row_data, result)

    def tearDown(self):
        # Optionally clean up the test sheet after each test
        # For example, delete any rows added during testing
        pass


if __name__ == "__main__":
    unittest.main()
