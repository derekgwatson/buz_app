import openpyxl
from io import BytesIO
import logging


# Configure logging
logger = logging.getLogger(__name__)


class OpenPyXLFileHandler:
    """
    A file handler class that abstracts operations for reading Excel files using openpyxl.
    """

    def __init__(self, file=None, file_path=None):
        """
        Initialize the file handler with either a file-like object or a file path.

        :param file: A file-like object (e.g., from `request.files`)
        :type file: file-like object
        :param file_path: Path to the Excel file
        :type file_path: str
        """
        if file and file_path:
            raise ValueError("Provide either a file or a file_path, not both.")
        if not file and not file_path:
            raise ValueError("You must provide either a file or a file_path.")

        self.file = file
        self.file_path = file_path
        self.workbook = None

    def load_workbook(self, data_only=True):
        """
        Load the workbook into memory.

        :param data_only: Whether to read the values instead of formulas
        :type data_only: bool
        """
        if self.file_path:
            self.workbook = openpyxl.load_workbook(self.file_path, data_only=data_only)
        elif self.file:
            self.workbook = openpyxl.load_workbook(BytesIO(self.file.read()), data_only=data_only)

    def get_sheet_names(self):
        """
        Get the names of all sheets in the workbook.

        :return: List of sheet names
        :rtype: list[str]
        """
        if self.workbook is None:
            raise ValueError("Workbook is not loaded. Call load_workbook() first.")
        return self.workbook.sheetnames

    def get_sheet(self, sheet_name):
        """
        Get a specific sheet by name.

        :param sheet_name: Name of the sheet
        :type sheet_name: str
        :return: The sheet object
        :rtype: openpyxl.worksheet.worksheet.Worksheet
        """
        if self.workbook is None:
            raise ValueError("Workbook is not loaded. Call load_workbook() first.")
        return self.workbook[sheet_name]

    def get_headers(self, sheet, header_row):
        """
        Get headers from a specific row in a sheet.

        :param sheet: The sheet object
        :type sheet: openpyxl.worksheet.worksheet.Worksheet
        :param header_row: The row number containing headers
        :type header_row: int
        :return: List of headers
        :rtype: list[str]
        """
        return [sheet.cell(row=header_row, column=col).value for col in range(1, sheet.max_column + 1)]

    def get_rows(self, sheet, start_row):
        """
        Get all rows starting from a specific row.

        :param sheet: The sheet object
        :type sheet: openpyxl.worksheet.worksheet.Worksheet
        :param start_row: The starting row number
        :type start_row: int
        :return: List of rows, where each row is a tuple of cell values
        :rtype: list[tuple]
        """
        return list(sheet.iter_rows(min_row=start_row, values_only=True))

    def read_sheet_to_dict(self, header_row: int = 1):
        """
        Read all sheets in the workbook into dictionaries, with configurable header rows.

        :param header_row: A dictionary mapping sheet names to header row numbers.
                            If None, defaults to the first row for all sheets.
        :type header_row: int
        :return: A dictionary where keys are sheet names, and values are lists of row dictionaries
        :rtype: dict[str, list[dict]]
        """
        if self.workbook is None:
            raise ValueError("Workbook is not loaded. Call load_workbook() first.")

        # Default to header row 1 for all sheets if no configuration is provided
        all_data = {}

        for sheet_name in self.get_sheet_names():
            sheet = self.get_sheet(sheet_name)
            headers = self.get_headers(sheet, header_row)
            rows = self.get_rows(sheet, header_row + 1)
            all_data[sheet_name] = [dict(zip(headers, row)) for row in rows if any(row)]

        return all_data
