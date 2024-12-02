import logging
from google_sheets_service import GoogleSheetsService
from excel import OpenPyXLFileHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_sheet(sheet, google_product_codes):
    """
    Process a single sheet: mark rows for deletion if the supplier product code is not in Google Sheets.
    Remove rows with empty supplier product codes or those found in Google Sheets.

    Args:
        sheet: The Excel sheet to process.
        google_product_codes: Set of valid supplier product codes from Google Sheets.

    Returns:
        list: Rows retained in the sheet after processing (marked for deletion).
    """
    rows_to_keep = []
    to_delete = []  # Collect rows to delete

    for row_idx, row in enumerate(sheet.iter_rows(min_row=3, max_row=sheet.max_row), start=3):
        supplier = row[26].value  # Column AA (27th column, zero-indexed)
        supplier_product_code_cell = row[27]  # Column AB (28th column, zero-indexed)
        supplier_product_code = (
            str(supplier_product_code_cell.value).strip().lower()
            if supplier_product_code_cell.value
            else None
        )
        operation_cell = sheet.cell(row=row_idx, column=41)  # Column AO (41st column, 1-based index)

        if not supplier_product_code:
            # Mark for deletion if supplier product code is empty
            to_delete.append(row_idx)
            logger.info(f"Removing row {row_idx} in {sheet.title} - supplier product code empty.")
            continue

        if supplier_product_code in google_product_codes:
            # Mark for deletion if supplier product code exists in Google Sheets
            to_delete.append(row_idx)
            logger.info(f"Removing row {row_idx} in {sheet.title} - {supplier_product_code} is valid.")
            continue

        if supplier != 'UNLEASHED':
            # Mark for deletion if supplier isn't Unleashed
            to_delete.append(row_idx)
            logger.info(f"Removing row {row_idx} in {sheet.title} - it's not an Unleashed code.")
            continue

        # Mark for deletion in column AO if not in Google Sheets
        operation_cell.value = 'D'
        rows_to_keep.append(row_idx)

    # Delete rows in a single pass
    for row_idx in sorted(to_delete, reverse=True):
        sheet.delete_rows(row_idx)

    return rows_to_keep


def process_workbook_with_google_sheets_and_handler(
    _file_handler: OpenPyXLFileHandler,
    _sheets_service: GoogleSheetsService,
    spreadsheet_id: str,
    range_name: str,
    output_file: str
):
    """
    Process an Excel workbook and mark rows for deletion based on Google Sheets lookup.

    Args:
        _file_handler (OpenPyXLFileHandler): An instance of OpenPyXLFileHandler for interacting with the workbook.
        _sheets_service (GoogleSheetsService): An instance of GoogleSheetsService for interacting with Google Sheets.
        spreadsheet_id (str): The ID of the Google Sheet to fetch data from.
        range_name (str): The range within the Google Sheet to fetch data from.
        output_file (str): Path to save the updated Excel workbook.
    """
    logger.info("Loading workbook...")
    _file_handler.load_workbook()
    workbook = _file_handler.workbook

    logger.info("Fetching Google Sheets data...")
    google_sheet_data = _sheets_service.fetch_sheet_data(spreadsheet_id, range_name)
    google_product_codes = {
        row[0].strip().lower()
        for row in google_sheet_data
        if row
            and row[0]  # Product Code exists
            and len(row) > 50  # Ensure AX (49) and AY (50) exist
            and row[49].strip().lower() != "yes"  # IsObsoleted != "Yes"
            and row[50].strip().lower() != "no"  # Is Sellable != "No"
    }
    logger.info(f"Retrieved {len(google_product_codes)} valid supplier product codes from Google Sheets.")

    sheets_to_delete = []

    for sheet_name in _file_handler.get_sheet_names():
        logger.info(f"Processing sheet: {sheet_name}")
        sheet = _file_handler.get_sheet(sheet_name)

        # Delete sheet if 'PkId' is not in A2
        if sheet["A2"].value != "PkId":
            logger.info(f"Deleting sheet: {sheet_name} - 'Pkid' not found in A2.")
            sheets_to_delete.append(sheet_name)
            continue

        # Remove trailing '*' from row 2, columns B and C
        for col in ["B", "C"]:
            cell = sheet[f"{col}2"]
            if cell.value and cell.value.endswith("*"):
                original_value = cell.value
                cell.value = cell.value[:-1]  # Remove the trailing '*'
                logger.info(f"Removed trailing '*' from {col}2 in sheet '{sheet_name}' (was '{original_value}')")

        rows_to_keep = process_sheet(sheet, google_product_codes)

        # Mark sheet for deletion if empty (excluding headers)
        if len(rows_to_keep) == 0:
            sheets_to_delete.append(sheet_name)

    # Delete empty sheets
    for sheet_name in sheets_to_delete:
        logger.info(f"Deleting empty sheet: {sheet_name}")
        del workbook[sheet_name]

    # Save the updated workbook
    logger.info(f"Saving updated workbook to {output_file}")
    workbook.save(output_file)

