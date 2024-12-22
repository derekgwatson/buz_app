from services.database import DatabaseManager
import logging
from services.excel import OpenPyXLFileHandler
from services.config_service import ConfigManager
from typing import Sequence, Optional
import re
from datetime import datetime


logger = logging.getLogger(__name__)
config = ConfigManager()


def parse_excel_date(cell_value):
    """
    Convert Excel cell value to a proper date string.
    """
    if isinstance(cell_value, datetime):
        return cell_value.strftime('%Y-%m-%d')
    if isinstance(cell_value, str):
        try:
            parsed_date = datetime.strptime(cell_value, '%d/%m/%Y')  # Adjust format if needed
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            pass  # Not a valid date string, return as is
    return cell_value  # Return the original if not recognized


def is_group_allowed(db_manager: DatabaseManager, inventory_group_code: str) -> bool:
    """
    Check if the inventory group code exists in the allowed list.

    :param db_manager: Instance of database manager
    :type db_manager: DatabaseManager
    :param inventory_group_code: Inventory group code
    :type inventory_group_code: str
    :return: true if inventory group is allowed
    :rtype: bool
    """

    return db_manager.get_item("inventory_groups", {"group_code": inventory_group_code}) is not None


def process_workbook(
    file_handler: OpenPyXLFileHandler,
    table_name: str,
    expected_headers: list[str],
    db_fields: list[str],
    header_row: int,
    db_manager: DatabaseManager,
    invalid_pkid: str
) -> dict[str, int]:
    """
    Process an Excel workbook and insert its data into the specified database table.

    This function reads data from an Excel workbook, validates its sheets against
    expected headers, and inserts rows into a database table. It skips sheets that
    are not in the allowed list, have invalid headers, or contain no data. Existing
    rows for the corresponding inventory group are deleted before inserting new data.

    :param invalid_pkid:
    :param file_handler: An instance of `OpenPyXLFileHandler` used to load and interact
                         with the Excel workbook.
    :param table_name: The name of the database table where the data will be inserted.
    :param expected_headers: A list of expected column headers for validation.
    :param db_fields: A list of corresponding database fields to be updated.
    :param header_row: The row number in the Excel sheet that contains the headers.
    :param db_manager: An instance of `DatabaseManager` used for database operations.

    :return: A summary dictionary containing:
             - `processed_sheets`: Number of sheets successfully processed.
             - `skipped_sheets`: Number of sheets skipped due to errors or validation failure.
             - `rows_inserted`: Total number of rows inserted into the database.
    """
    summary = {"processed_sheets": 0, "skipped_sheets": 0, "rows_inserted": 0}

    for sheet_name in file_handler.workbook.sheetnames:
        if not is_group_allowed(db_manager, sheet_name):
            logger.warning(f"Skipping sheet {sheet_name} as it is not in the allowed list.")
            summary["skipped_sheets"] += 1
            continue

        logger.info(f"Processing sheet: {sheet_name}")
        sheet = file_handler.workbook[sheet_name]

        # Validate headers
        actual_headers = [
            str(sheet.cell(row=header_row, column=col).value).strip().rstrip('*')
            if sheet.cell(row=header_row, column=col).value is not None else ""
            for col in range(1, sheet.max_column + 1)
        ]

        if actual_headers != expected_headers:
            logger.warning(
                f"Skipping sheet '{sheet_name}': Incorrect headers.\n"
                f"Expected: {expected_headers}, Found: {actual_headers}"
            )
            summary["skipped_sheets"] += 1
            continue

        # Extract and validate rows
        rows = [
            (sheet_name, *[parse_excel_date(cell) if actual_headers[idx] == 'Last Edit Date' else cell for idx, cell in enumerate(row)])
            for row in sheet.iter_rows(min_row=header_row + 1, values_only=True)
            if any(row) and row[0] is not None  # Skip empty rows or rows with None in the first column
        ]
        if not rows:
            logger.warning(f"Skipping sheet '{sheet_name}' as it contains no data.")
            summary["skipped_sheets"] += 1
            continue

        try:
            # Delete existing data for this sheet (inventory_group_code)
            delete_existing_data(db_manager, table_name, sheet_name)

            # Insert all valid rows into the table
            insert_data(db_manager, table_name, ['inventory_group_code'] + db_fields, rows)

            # Delete rows with invalid PKID
            delete_invalid_rows(db_manager, table_name, sheet_name, invalid_pkid)

            summary["processed_sheets"] += 1
            summary["rows_inserted"] += len(rows)

        except Exception as e:
            logger.error(f"Error processing sheet '{sheet_name}': {e}")

    return summary


def delete_existing_data(db_manager: DatabaseManager, table_name: str, group_code: str) -> int:
    """Delete existing data for a specific inventory group code from a database table.

    This function removes all rows in the specified database table where the
    `inventory_group_code` matches the provided group code.

    :param db_manager: An instance of `DatabaseManager` used to execute the query.
    :param table_name: The name of the database table from which data will be deleted.
    :param group_code: The inventory group code used as the filter for deletion.

    :return: The number of rows deleted from the database.

    :raises Exception: If the database query fails, the exception is logged and re-raised.
    """
    query = f'DELETE FROM {table_name} WHERE inventory_group_code = ?'
    try:
        return db_manager.execute_query(query, [group_code]).rowcount
    except Exception as e:
        logger.error(f"Failed to delete data for group code {group_code}: {e}")
        raise


def insert_data(
    db_manager: DatabaseManager,
    table_name: str,
    known_columns: list[str],
    rows: list[tuple]
) -> None:
    """
    Insert multiple rows into a database table using known column names.

    This function inserts the given rows into the specified database table using a predefined
    list of known column names. Input rows are validated to ensure they align with the schema.

    :param db_manager: An instance of `DatabaseManager` used to execute the query.
    :param table_name: The name of the database table where the rows will be inserted.
    :param known_columns: A list of predefined column names for the database table.
    :param rows: A list of tuples, where each tuple represents a row of data to be inserted.

    :raises ValueError: If rows do not match the number of columns in `known_columns`.
    :raises Exception: If the database query fails, the exception is logged and re-raised.
    """
    # Ensure rows match the column count
    for i, row in enumerate(rows):
        if len(row) != len(known_columns):
            raise ValueError(
                f"Row {i + 1} has {len(row)} columns, but {len(known_columns)} were expected: {row}"
            )

    placeholders = ', '.join(['?'] * len(known_columns))
    quoted_headers = [f'[{col}]' for col in known_columns]
    query = f'INSERT INTO {table_name} ({", ".join(quoted_headers)}) VALUES ({placeholders})'

    try:
        db_manager.executemany(query, rows)
    except Exception as e:
        logger.error(f"Failed to insert rows into {table_name}: {e}")
        raise


def delete_invalid_rows(
    db_manager: DatabaseManager, table_name: str, inventory_group_code: str, invalid_value: str
) -> int:
    """Delete rows with an invalid value in a specific column from a database table.

    This function removes all rows in the specified database table where the
    `inventory_group_code` matches the provided value and the `PkId` column contains
    the specified invalid value.

    :param db_manager: An instance of `DatabaseManager` used to execute the query.
    :param table_name: The name of the database table from which invalid rows will be deleted.
    :param inventory_group_code: The inventory group code used to identify the relevant rows.
    :param invalid_value: The value considered invalid in the `PkId` column.

    :return: The number of rows deleted from the database.

    :raises Exception: If the database query fails, the exception is logged and re-raised.
    """
    query = f'''
        DELETE FROM {table_name}
        WHERE inventory_group_code = ? AND PkId = ?
    '''
    try:
        return db_manager.execute_query(query, [inventory_group_code, invalid_value]).rowcount
    except Exception as e:
        logger.error(f"Failed to delete invalid rows for group {inventory_group_code}: {e}")
        raise
