from datetime import datetime
from openpyxl import Workbook
from services.database import DatabaseManager
from services.google_sheets_service import GoogleSheetsService
from services.data_processing import get_all_inventory_items_by_group
import logging


# Configure logging
logger = logging.getLogger(__name__)


def process_inventory_backorder_with_services(
    _db_manager: DatabaseManager,
    _sheets_service: GoogleSheetsService,
    spreadsheet_id: str,
    range_name: str
):
    """
    Process inventory items for backorder messages using custom service classes and generate upload and original files.

    Args:
        _db_manager: Database Manager
        _sheets_service (GoogleSheetsService): Instance of GoogleSheetsService for interacting with Google Sheets.
        spreadsheet_id (str): Google Sheet ID containing supplier codes and backorder dates.
        range_name (str): Range in the Google Sheet to fetch data from.
        header_row (int): Row that header is on.
    """
    # Step 1: Query inventory items from the database
    inventory_data = get_all_inventory_items_by_group(_db_manager)

    # Step 2: Read supplier codes and backorder dates from Google Sheets
    google_sheet_data = _sheets_service.fetch_sheet_data(spreadsheet_id, range_name)
    logger.debug(f"Google Sheet Data: {google_sheet_data}")

    # Extract headers and map them to indices
    headers = google_sheet_data[0]
    header_map = {header: index for index, header in enumerate(headers)}

    # Ensure required columns exist
    required_columns = ['Unleashed Code', 'On backorder until']
    missing_columns = [col for col in required_columns if col not in header_map]
    if missing_columns:
        raise ValueError(f"Missing required columns in Google Sheet: {', '.join(missing_columns)}")

    # Convert Google Sheet data into a lookup dictionary
    backorder_lookup = {
        row[header_map['Unleashed Code']]: row[header_map['On backorder until']]
        for row in google_sheet_data[1:]  # Skip the header row
        if row[header_map['Unleashed Code']] and row[header_map['On backorder until']]  # Ensure non-empty values
    }

    # Step 3: Process each sheet in the inventory workbook
    upload_workbook = Workbook()
    original_workbook = Workbook()
    upload_workbook.remove(upload_workbook.active)  # Remove the default sheet
    original_workbook.remove(original_workbook.active)

    # Read all sheets at once
    processed_sheets = set()  # Track groups that are processed

    for group_code, rows in inventory_data.items():
        updated_rows = []
        original_rows = []

        for row in rows:
            original_row = dict(row)  # Preserve the original row before any changes (and convert to dict)
            new_row = dict(row)  # Preserve the original row before any changes (and convert to dict)
            supplier_code = row['SupplierProductCode']
            warning_message = row['Warning'] or ''

            # Concatenate columns D, E, and F with spaces
            # Safely concatenate columns D, E, and F with spaces
            description_parts = [
                (row["DescnPart1"] or "").strip(),
                (row["DescnPart2"] or "").strip(),
                (row["DescnPart3"] or "").strip(),
            ]
            description = " ".join(part for part in description_parts if part)

            # Check for existing backorder message
            if "on backorder until" in warning_message:
                existing_date_str = ""
                try:
                    # Parse the date from the existing message (new format: "d mmm yyyy")
                    existing_date_str = warning_message.split("until")[1].strip().rstrip(".")
                    existing_date = datetime.strptime(existing_date_str, "%d %b %Y")  # New format
                    if existing_date < datetime.now():
                        # Remove the outdated warning
                        new_row['Warning'] = ''
                        new_row['Operation'] = 'E'
                        updated_rows.append(new_row)
                        original_rows.append(original_row)  # Keep the unmodified row in the original file
                        continue
                except ValueError:
                    # If parsing fails, skip date comparison
                    logger.warning(f"Failed to parse date: {existing_date_str}")
                    pass

            if supplier_code in backorder_lookup:
                # Format backorder date
                raw_date = backorder_lookup[supplier_code]
                try:
                    formatted_date = datetime.strptime(raw_date, "%d/%m/%Y").strftime("%d %b %Y")
                except ValueError:
                    logger.warning(f'Failed to convert {raw_date} to date format')
                    formatted_date = raw_date  # Fallback to raw value if formatting fails
                logger.debug(f'Date was {raw_date}, converted to {formatted_date}')

                warning = f"{description} on backorder until {formatted_date}."
                new_row['Warning'] = warning
                logger.debug(f'Warning is: {warning}')
                new_row['Operation'] = 'E'
                updated_rows.append(new_row)
                original_rows.append(original_row)  # Keep the unmodified row in the original file

        # Only add updated rows to the upload workbook
        if updated_rows:
            processed_sheets.add(group_code)
            upload_sheet = upload_workbook.create_sheet(title=group_code)
            upload_sheet.append(list(updated_rows[0].keys()))  # Write header
            for updated_row in updated_rows:
                upload_sheet.append(list(updated_row.values()))

        # Add original rows to the original workbook
        if original_rows:
            original_sheet = original_workbook.create_sheet(title=group_code)
            original_sheet.append(list(original_rows[0].keys()))  # Write header
            for original_row in original_rows:
                original_sheet.append(list(original_row.values()))

    # Step 4: return the workbooks (to save them, or inspect them if testing)
    return upload_workbook, original_workbook
