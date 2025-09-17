import json
import logging
import os
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path

logger = logging.getLogger(__name__)


class GoogleSheetsService:
    """
    A class to handle Google Sheets operations.
    Automatically uses the service account JSON from the app's main folder (NOT inside /app).
    """

    @staticmethod
    def _get_service_account_path():
        env = os.environ.get("GSHEETS_CREDENTIALS") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if env and os.path.isfile(env):
            return env

        candidates = [
            Path(__file__).resolve().parents[1] / "credentials" / "service_account.json",
            Path(__file__).resolve().parents[2] / "credentials" / "service_account.json",
            Path.cwd() / "credentials" / "service_account.json",
            Path.cwd() / "service_account.json",
        ]
        for p in candidates:
            if p.is_file():
                return str(p)

        raise FileNotFoundError(
            "service_account.json not found. Set GSHEETS_CREDENTIALS or GOOGLE_APPLICATION_CREDENTIALS.")

    @staticmethod
    def _authenticate_google_sheets(json_file: str) -> Credentials:
        """
        Authenticate with the Google Sheets API.

        :param json_file: JSON configuration file containing service account credentials.
        :type json_file: str
        :return: Credentials object for use in authorization.
        :rtype: google.oauth2.service_account.Credentials
        """
        with open(json_file) as f:
            creds = json.load(f)
        scope = [
            "https://www.googleapis.com/auth/spreadsheets"
        ]
        return Credentials.from_service_account_info(creds, scopes=scope)

    def __init__(self, json_file: str = None):
        """
        Initialize the GoogleSheetsService with an authorized gspread client.
        If no json_file is provided, uses the app's default credentials path.

        :param json_file: Path to the JSON configuration file.
        :type json_file: str
        """
        if json_file is None:
            json_file = self._get_service_account_path()
        creds = self._authenticate_google_sheets(json_file)
        self._client = gspread.authorize(creds)

    def fetch_sheet_data(self, spreadsheet_id: str, range_name: str) -> list[list[str]]:
        """
        Fetch data from the specified range of a Google Sheet.

        :param spreadsheet_id: The ID of the spreadsheet to fetch data from.
        :type spreadsheet_id: str
        :param range_name: The range within the spreadsheet to fetch data from (e.g., "Sheet1!A1:D10").
        :type range_name: str
        :return: A list of rows where each row is represented as a list of cell values.
        :rtype: list[list[str]]
        """
        try:
            sheet = self._client.open_by_key(spreadsheet_id)
            worksheet = sheet.worksheet(range_name.split('!')[0])
            return worksheet.get_all_values()
        except Exception as e:
            logger.error(f"Error fetching data from spreadsheet {spreadsheet_id}: {e}")
            return []

    def insert_row(self, spreadsheet_id: str, row_data: list[str], worksheet_name: str = 'Sheet1'):
        """
        Insert a row into a specific worksheet.
        """
        try:
            sheet = self._client.open_by_key(spreadsheet_id)
            worksheet = sheet.worksheet(worksheet_name)
            worksheet.append_row(row_data)
        except Exception as e:
            logger.error(f"Error inserting row into spreadsheet {spreadsheet_id}: {e}")


def filter_google_sheet_second_column_numeric(
        sheets_service: GoogleSheetsService, spreadsheet_id: str, range_name: str = 'Sheet1!A:B'
) -> list[tuple[str, str]]:

    """
    Get data from a Google Sheet and filter rows based on numeric values in the second column.

    :param sheets_service: An instance of GoogleSheetsService.
    :type sheets_service: GoogleSheetsService
    :param spreadsheet_id: The ID of the Google Sheet to fetch data from.
    :type spreadsheet_id: str
    :param range_name: The range of data to fetch (default is 'Sheet1!A:B').
    :type range_name: str
    :return: A list of tuples (column A, column B) where column B contains numeric values.
    :rtype: list[tuple[str, str]]
    """

    # Fetch data from the specified range
    data = sheets_service.fetch_sheet_data(spreadsheet_id, range_name)
    if not data:
        logger.warning("No data fetched from Google Sheets.")
        return []

    def is_numeric(value: str) -> bool:
        """
        Function to check if a value is numeric (including percentages)

        :param value: value to check if it's numeric
        :type value: str
        :return: true if the value is numeric
        :rtype: bool
        """
        try:
            # Remove percentage sign and try converting to float
            if value.endswith('%'):
                value = value[:-1]  # Remove the '%' character
            float(value)  # Try converting to float
            return True
        except (ValueError, AttributeError):
            return False

    # Filter rows where column B has a numeric value (including percentages)
    return [
        (row[0], row[1])
        for row in data
        if len(row) > 1 and row[0] and row[1] and is_numeric(row[1])
    ]



