import logging
import pandas as pd
from io import BytesIO
from services.excel import OpenPyXLFileHandler
import re


# Configure logging
logger = logging.getLogger(__name__)


def validate_uploaded_file(uploaded_file):
    """Validate the uploaded file."""
    if not uploaded_file or not hasattr(uploaded_file.workbook, 'sheetnames'):
        raise ValueError("No file uploaded.")
    if not isinstance(uploaded_file, OpenPyXLFileHandler):
        raise TypeError("Unsupported file type. Expected an OpenPyXLFileHandler object.")


def extract_headers(sheet, header_row):
    """Extract and clean headers from the specified row."""
    def clean_header(cell_value):
        """Trim the header and remove a trailing '*' if it exists."""
        if not cell_value:
            return ""
        return cell_value.strip().rstrip("*")

    # Extract and clean headers
    raw_headers = [cell.value for cell in sheet[header_row]]
    headers = {clean_header(cell): idx for idx, cell in enumerate(raw_headers) if clean_header(cell)}

    return headers


def filter_rows(sheet, supplier_product_codes, supplier_product_code_col_idx, operation_col_idx, header_row):
    """Filter rows based on supplier codes and update the 'Operation' column."""
    filtered_rows = []

    for row_index, row in enumerate(sheet.iter_rows(min_row=header_row + 1, values_only=True), start=header_row + 1):
        if len(row) <= max(supplier_product_code_col_idx, operation_col_idx):
            logger.warning(f"Row {row_index} skipped: insufficient columns.")
            continue

        supplier_product_code_value = row[supplier_product_code_col_idx]
        if supplier_product_code_value in supplier_product_codes:
            row = list(row)
            row[operation_col_idx] = 'E'
            filtered_rows.append(row)

    return filtered_rows


def process_single_sheet(sheet, supplier_product_codes, header_row):
    """Process a single sheet, returning filtered rows."""
    headers = extract_headers(sheet, header_row)

    if "Operation" not in headers or "Supplier Product Code" not in headers:
        logger.warning(f"Required headers missing in sheet '{sheet.title}'. Skipping.")
        return None

    filtered_rows = filter_rows(
        sheet,
        supplier_product_codes,
        headers["Supplier Product Code"],
        headers["Operation"],
        header_row
    )

    if not filtered_rows:
        return None

    # Add header rows back to the filtered rows
    for row_num in range(1, header_row + 1):
        filtered_rows.insert(row_num - 1, [cell.value for cell in sheet[row_num]])

    return filtered_rows


def save_filtered_sheets_to_excel(filtered_sheets):
    """Save filtered sheets to a new Excel file."""
    if not filtered_sheets:
        logger.warning("No sheets met the criteria for processing.")
        return None

    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, rows in filtered_sheets.items():
            df = pd.DataFrame(rows)
            df.to_excel(writer, index=False, header=False, sheet_name=sheet_name)
    output.seek(0)
    return output


def process_buz_items_by_supplier_product_codes(
        uploaded_file: OpenPyXLFileHandler,
        supplier_product_codes: list[str],
        header_row: int = 2):
    """
    Process all sheets in the uploaded Excel file to filter rows based on supplier codes.

    :param uploaded_file: OpenPyXLFileHandler object for the uploaded Excel file.
    :param supplier_product_codes: List of supplier codes to filter by.
    :param header_row: Row number containing the headers (1-based index). Defaults to 2.
    :return: BytesIO object containing the filtered Excel file, or None if no valid sheets.
    """
    validate_uploaded_file(uploaded_file)

    filtered_sheets = {}
    for sheet_name in uploaded_file.workbook.sheetnames:
        logger.debug(f"Processing sheet: {sheet_name}")
        sheet = uploaded_file.get_sheet(sheet_name)

        filtered_rows = process_single_sheet(sheet, supplier_product_codes, header_row)
        if filtered_rows:
            filtered_sheets[sheet_name] = filtered_rows

    return save_filtered_sheets_to_excel(filtered_sheets)
