import openpyxl
from io import BytesIO
import logging


# Configure logging
logger = logging.getLogger(__name__)


class OpenPyXLFileHandler:
    """
    A file handler class that abstracts operations for reading Excel files using openpyxl.
    """

    def __init__(self, workbook=None):
        """
        Initialize the file handler with an existing workbook.
        """
        self.workbook = workbook

    @classmethod
    def from_file(cls, file_path, data_only=True):
        """
        Initialize the file handler with an Excel file from disk.

        Args:
            file_path (str): Path to the Excel file.
            data_only (bool): Whether to read the values instead of formulas.

        Returns:
            OpenPyXLFileHandler: An initialized file handler.
        """
        logger.debug(f"File path we're loading the excel from is {file_path}")
        workbook = openpyxl.load_workbook(file_path, data_only=data_only)
        return cls(workbook=workbook)

    @classmethod
    def from_file_like(cls, file, data_only=True):
        """
        Initialize the file handler with a file-like object.

        Args:
            file: A file-like object (e.g., from `request.files`).
            data_only (bool): Whether to read the values instead of formulas.

        Returns:
            OpenPyXLFileHandler: An initialized file handler.
        """
        workbook = openpyxl.load_workbook(BytesIO(file.read()), data_only=data_only)
        return cls(workbook=workbook)

    @classmethod
    def from_sheets_data(cls, sheets_data, sheets_header_data):
        """
        Initialize the file handler with sheets data.

        Args:
            sheets_data (dict): Dictionary where keys are sheet names, and values are lists of rows.
            sheets_header_data (dict): Dictionary with:
                - `headers`: List of column headers (shared by all sheets).
                - `header_row`: Row index where headers should appear.

        Returns:
            OpenPyXLFileHandler: An initialized file handler.
        """
        handler = cls()
        handler._create_excel_file(sheets_data, sheets_header_data)
        return handler

    @classmethod
    def from_items(cls, items, headers_config, header_row=1):
        """
        Initialize the file handler with items and headers configuration.

        Args:
            items (list[dict]): Inventory items to process.
            headers_config (list[dict]): Configuration mapping database fields to headers.
            header_row (int): Row where headers are located in the Excel sheet.

        Returns:
            OpenPyXLFileHandler: An initialized file handler.
        """
        sheets_data = cls.transform_items_to_sheets_data(items, headers_config, header_row)
        return cls.from_sheets_data(sheets_data)

    @staticmethod
    def transform_items_to_sheets_data(items, headers_config, header_row=1):
        """
        Transform items into sheets data format for Excel creation.
        """
        headers = [header["spreadsheet_column"] for header in headers_config]
        database_fields = [header["database_field"] for header in headers_config]

        grouped_data = {}
        for item in items.values():
            group_code = item["inventory_group_code"]
            if group_code not in grouped_data:
                grouped_data[group_code] = []
            grouped_data[group_code].append({field: item[field] for field in database_fields})

        sheets_data = {}
        for group_code, group_items in grouped_data.items():
            sheet_data = [
                [item[field] for field in database_fields]
                for item in group_items
            ]
            sheets_data[group_code] = (sheet_data, headers, header_row)

        return sheets_data

    def get_sheet_names(self):
        """
        Get the names of all sheets in the workbook.

        :return: List of sheet names
        :rtype: list[str]
        """
        if self.workbook is None:
            raise ValueError("Workbook is not loaded.")
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
            raise ValueError("Workbook is not loaded.")
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
            raise ValueError("Workbook is not loaded.")

        # Default to header row 1 for all sheets if no configuration is provided
        all_data = {}

        for sheet_name in self.get_sheet_names():
            sheet = self.get_sheet(sheet_name)
            headers = self.get_headers(sheet, header_row)
            rows = self.get_rows(sheet, header_row + 1)
            all_data[sheet_name] = [dict(zip(headers, row)) for row in rows if any(row)]

        return all_data

    def _create_excel_file(self, sheets_data, sheets_header_data):
        """
        Internal method to create a new Excel workbook with multiple sheets.

        Args:
            sheets_data (dict): Dictionary where keys are sheet names, and values are lists of rows.
            sheets_header_data (dict): Contains:
                - `headers`: List of column headers.
                - `header_row`: Row index for headers (default is 1).

        Modifies:
            self.workbook: Sets this attribute to the newly created workbook.
        """
        self.workbook = openpyxl.Workbook()

        headers = sheets_header_data["headers"]
        header_row = sheets_header_data.get("header_row", 1)

        for idx, (sheet_name, rows) in enumerate(sheets_data.items(), start=1):
            # Add a new sheet or use the default active sheet
            if idx == 1:
                sheet = self.workbook.active
                sheet.title = sheet_name
            else:
                sheet = self.workbook.create_sheet(title=sheet_name)

            # Write headers
            for col_num, header in enumerate(headers, start=1):
                sheet.cell(row=header_row, column=col_num, value=header)

            # Write data starting below the header row
            data_start_row = header_row + 1
            for row_idx, row in enumerate(rows, start=data_start_row):
                for col_idx, value in enumerate(row, start=1):
                    sheet.cell(row=row_idx, column=col_idx, value=value)

    def save_workbook(self, save_path):
        """
        Save the current workbook to the specified file path.

        :param save_path: The file path where the workbook should be saved.
        :type save_path: str
        """
        if self.workbook is None:
            raise ValueError("No workbook is loaded or created to save.")

        self.workbook.save(save_path)
        logger.info(f"Workbook saved to {save_path}")

    def get_column_by_header(self, sheet_name, header_name, header_row=1):
        """
        Get all values in a column by its header text.

        Args:
            sheet_name (str): The name of the sheet to search in.
            header_name (str): The header text of the column.
            header_row (int): The row number containing the headers. Defaults to 1.

        Returns:
            list: A list of values from the specified column (excluding the header).

        Raises:
            ValueError: If the header or sheet is not found.
        """
        # Get the sheet
        sheet = self.get_sheet(sheet_name)

        # Get the headers
        headers = self.get_headers(sheet, header_row)

        # Find the column index of the header
        if header_name not in headers:
            raise ValueError(f"Header '{header_name}' not found in sheet '{sheet_name}'.")
        column_index = headers.index(header_name) + 1  # 1-based index

        # Collect values from the column, excluding the header
        values = [
            sheet.cell(row=row, column=column_index).value
            for row in range(header_row + 1, sheet.max_row + 1)
        ]
        return values

    def set_value_by_header(self, sheet_name, header_name, row, value, header_row=1):
        """
        Set a single value in a specific row of a column identified by its header text.

        Args:
            sheet_name (str): The name of the sheet to search in.
            header_name (str): The header text of the column.
            row (int): The row number (1-based) where the value should be set.
            value: The value to set in the specified cell.
            header_row (int): The row number containing the headers. Defaults to 1.

        Raises:
            ValueError: If the header or sheet is not found.
            IndexError: If the specified row is outside the range of the sheet.
        """
        # Get the sheet
        sheet = self.get_sheet(sheet_name)

        # Get the headers
        headers = self.get_headers(sheet, header_row)

        # Find the column index of the header
        if header_name not in headers:
            raise ValueError(f"Header '{header_name}' not found in sheet '{sheet_name}'.")
        column_index = headers.index(header_name) + 1  # 1-based index

        # Validate the row number
        if row < header_row + 1 or row > sheet.max_row:
            raise IndexError(f"Row {row} is out of range for sheet '{sheet_name}'.")

        # Set the value in the specified cell
        sheet.cell(row=row, column=column_index, value=value)

    @classmethod
    def create_blank_pricing_upload_from_config(cls, config: list[dict], group_codes: list[str], header_row: int = 1):
        """
        Create a blank pricing upload workbook using the buz_pricing_file config and inventory group codes.

        :param config: The buz_pricing_file config section (list of dicts with spreadsheet_column keys).
        :param group_codes: List of inventory_group_codes to create sheets for.
        :param header_row: Row number to place headers (default is 1).
        :return: OpenPyXLFileHandler with empty sheets per group.
        """
        headers = [entry["spreadsheet_column"] for entry in config]
        sheets_data = {group: [] for group in group_codes}
        sheets_header_data = {
            "headers": headers,
            "header_row": header_row
        }
        return cls.from_sheets_data(sheets_data, sheets_header_data)
