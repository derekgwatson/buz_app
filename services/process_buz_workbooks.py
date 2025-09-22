from services.database import DatabaseManager
import logging
from services.excel import OpenPyXLFileHandler
from services.config_service import ConfigManager
from typing import Callable
from datetime import datetime
from typing import Iterable, List, Tuple, Optional
from time import perf_counter


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
    Check if the inventory group code is allowed (not in ignored list and exists in the allowed table).
    """
    ignored_groups = config.config.get("ignored_inventory_groups", [])

    if inventory_group_code in ignored_groups:
        logger.info(f"Skipping group {inventory_group_code} because it's in the ignored list.")
        return False

    return db_manager.get_item("inventory_groups", {"group_code": inventory_group_code}) is not None


ProgressCb = Optional[Callable[[str, Optional[int]], None]]


def _chunked(iterable: Iterable, size: int) -> Iterable[List]:
    """Yield lists of up to `size` items from `iterable`."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def process_workbook(
    file_handler: OpenPyXLFileHandler,
    table_name: str,
    expected_headers: list[str],
    db_fields: list[str],
    header_row: int,
    db_manager: DatabaseManager,
    invalid_pkid: str,
    ignored_groups: list[str],
    batch_size: int = 1000,
) -> dict[str, int]:
    """
    Process an Excel workbook and insert its data into the specified database table.
    """
    summary = {"processed_sheets": 0, "skipped_sheets": 0, "rows_inserted": 0}

    # Purge ignored groups up front
    purge_result = purge_ignored_groups(db_manager, "inventory_items", ignored_groups)
    logger.info("Purged ignored inventory groups: %s", purge_result)

    known_columns = ["inventory_group_code"] + db_fields
    max_cols = len(expected_headers)  # hard limit — avoids huge formatted rows

    for sheet_name in file_handler.workbook.sheetnames:
        if not is_group_allowed(db_manager, sheet_name):
            logger.warning("Skipping sheet %s as it is not in the allowed list.", sheet_name)
            summary["skipped_sheets"] += 1
            continue

        t0 = perf_counter()
        logger.info("Processing sheet: %s", sheet_name)
        sheet = file_handler.workbook[sheet_name]

        # ---- Header validation (only read what we expect) ----
        header_row_vals = next(
            sheet.iter_rows(
                min_row=header_row,
                max_row=header_row,
                max_col=max_cols,
                values_only=True,
            )
        )
        actual_headers = [
            (str(v).strip().rstrip("*") if v is not None else "")
            for v in header_row_vals
        ]

        if actual_headers != expected_headers:
            logger.warning(
                "Skipping sheet '%s': Incorrect headers. Expected: %s, Found: %s",
                sheet_name, expected_headers, actual_headers,
            )
            summary["skipped_sheets"] += 1
            continue

        # ---- Delete existing data for this group ----
        try:
            delete_existing_data(db_manager, table_name, sheet_name)
        except Exception as exc:
            logger.error(
                "Failed to delete existing data for '%s': %s", sheet_name, exc
            )
            summary["skipped_sheets"] += 1
            continue

        # ---- Stream rows and insert in batches ----
        total_inserted = 0
        try:
            row_iter = sheet.iter_rows(
                min_row=header_row + 1,
                max_col=max_cols,           # hard cap
                values_only=True,
            )

            for batch in _chunked(row_iter, size=1000):
                to_insert: List[Tuple] = []
                for row in batch:
                    if not row or all(v is None for v in row):
                        continue
                    if row[0] is None:  # require a value in first column
                        continue

                    transformed = [
                        parse_excel_date(val) if actual_headers[idx] == "Last Edit Date" else val
                        for idx, val in enumerate(row)
                    ]
                    # Prepend inventory_group_code
                    to_insert.append((sheet_name, *transformed))

                if to_insert:
                    insert_data(db_manager, table_name, known_columns, to_insert)
                    total_inserted += len(to_insert)

            # Remove rows with invalid PKID
            delete_invalid_rows(db_manager, table_name, sheet_name, invalid_pkid)

        except Exception as exc:
            logger.error("Error processing sheet '%s': %s", sheet_name, exc)
            summary["skipped_sheets"] += 1
            # Best-effort: continue other sheets
            continue

        summary["processed_sheets"] += 1
        summary["rows_inserted"] += total_inserted
        logger.info(
            "Sheet '%s' done. Inserted %d rows in %.2fs",
            sheet_name, total_inserted, perf_counter() - t0
        )

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


def purge_ignored_groups(db_manager: DatabaseManager, table_name: str, ignored_groups: list[str]) -> dict[str, int]:
    """
    Delete all rows from the given table where the inventory group is in the ignored list.

    :param db_manager: Instance of DatabaseManager
    :param table_name: Name of the table to purge
    :param ignored_groups: List of group codes to purge
    :return: Dict summarizing how many rows were deleted per group
    """
    results = {}
    for group_code in ignored_groups:
        try:
            rowcount = delete_existing_data(db_manager, table_name, group_code)
            results[group_code] = rowcount
            logger.info(f"Purged {rowcount} rows from '{table_name}' for ignored group: {group_code}")
        except Exception as e:
            logger.error(f"Failed to purge group {group_code} from {table_name}: {e}")
            results[group_code] = -1
    return results
