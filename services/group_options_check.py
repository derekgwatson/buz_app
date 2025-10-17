from collections import defaultdict
from services.excel import OpenPyXLFileHandler
from typing import List, Tuple
import logging
from collections import Counter


logger = logging.getLogger(__name__)


def build_views(dupes: list[tuple[str, list[str]]]):
    """
    dupes = [(code, ['GROUPA','GROUPB', ...]), ...]
    """
    # Normalise and sort
    code_to_groups = {code: sorted(set(groups)) for code, groups in dupes}
    all_groups = sorted({g for gs in code_to_groups.values() for g in gs})

    # Overview stats
    group_counts = {g: sum(1 for gs in code_to_groups.values() if g in gs) for g in all_groups}
    degree_counts = Counter(len(gs) for gs in code_to_groups.values())

    # Optional: group-centric view
    group_to_codes = {g: [] for g in all_groups}
    for code, gs in code_to_groups.items():
        for g in gs:
            group_to_codes[g].append(code)
    for g in all_groups:
        group_to_codes[g].sort()

    return code_to_groups, all_groups, group_counts, degree_counts, group_to_codes


def extract_codes_from_excel_flat_dedup(file_handler: OpenPyXLFileHandler) -> List[Tuple[str, str]]:
    """
    Extracts all codes from a 'group options' Excel file, based on the rules, and returns
    a deduplicated flat list with each code paired with the sheet name.

    Rules:
    - Row 2: Contains codes directly (no splitting by '|').
    - Rows 17 onwards:
        - If row 6 of a column has a value, extract the part after the second '|', if it exists.
        - Otherwise, extract the part after the first '|', if it exists.
    - Ignore column A.
    - Ignore sheets if text "Inventory Code for Pricing" is not found in A2.

    Args:
        file_handler (OpenPyXLFileHandler): The file containing group options.

    Returns:
        List[Tuple[str, str]]: A deduplicated flat list where each tuple contains:
            - The sheet name.
            - A single code from that sheet.
    """
    results = set()  # Use a set to ensure uniqueness

    for sheet_name in file_handler.workbook.sheetnames:
        sheet = file_handler.workbook[sheet_name]

        # Ignore sheet if text "Inventory Code for Pricing" is not found in A2
        if sheet["A2"].value != "Inventory Code for Pricing":
            continue

        # Extract codes from row 2
        for col in sheet.iter_cols(min_row=2, max_row=2, min_col=2):
            cell_value = col[0].value
            if cell_value:
                results.add((sheet_name, cell_value.strip()))

        # Extract codes from row 17 onwards
        for row in sheet.iter_rows(min_row=17, min_col=2):
            for cell in row:
                cell_value = cell.value
                if cell_value:
                    # Check the rule for row 6
                    col_letter = cell.column_letter
                    row_6_value = sheet[f"{col_letter}6"].value if sheet.max_row >= 6 else None
                    parts = cell_value.split("|")

                    if row_6_value:  # If there's a value in row 6
                        if len(parts) > 2:
                            results.add((sheet_name, parts[2].strip()))
                    else:  # If there's no value in row 6
                        if len(parts) > 1:
                            results.add((sheet_name, parts[1].strip()))

    # Convert set back to list for a consistent output format
    return sorted(results)


def map_inventory_items_to_tabs(
    file_handler: OpenPyXLFileHandler,
    codes_list: List[Tuple[str, str]],
) -> List[Tuple[str, str, str]]:
    """
    Maps inventory items to the tabs they are found in an inventory workbook.

    Args:
        file_handler (OpenPyXLFileHandler): inventory Excel workbook.
        codes_list (List[Tuple[str, str]]): A list of tuples with:
            - The original tab where the code was found.
            - The inventory item code.

    Returns:
        List[Tuple[str, str, str]]: A new list with:
            - The original tab where the code was found.
            - The inventory item code.
            - The tab in the inventory workbook where the item was found.
              If the item is not found, "Not Found" is added instead.
    """
    # Load the inventory workbook
    inventory_mapping = []

    # Iterate through the codes list
    for original_tab, code in codes_list:
        item_found = False

        # Check each sheet in the inventory workbook
        for sheet_name in file_handler.workbook.sheetnames:
            sheet = file_handler.workbook[sheet_name]

            # Search for the code in the sheet
            for row in sheet.iter_rows(values_only=True):
                if code in row:
                    inventory_mapping.append((original_tab, code, sheet_name))
                    item_found = True
                    break

            if item_found:
                break

        # If not found in any sheet
        if not item_found:
            inventory_mapping.append((original_tab, code, "Not Found"))

    return inventory_mapping


def filter_inventory_items(items: List[Tuple[str, str, str]]) -> List[Tuple[str, str, str]]:
    """
    Filters out items where the group is simply 'OP' followed by the tab name.

    Args:
        items (List[Tuple[str, str, str]]): The inventory items in the format:
            - Original tab
            - Inventory item code
            - Found in tab (group)

    Returns:
        List[Tuple[str, str, str]]: Filtered inventory items.
    """
    filtered_items = [
        (original_tab, code, group)
        for original_tab, code, group in items
        if group != f"OP{original_tab}"
    ]
    return filtered_items


def extract_duplicate_codes_with_locations(codes_with_tabs: List[Tuple[str, str]]) -> List[Tuple[str, List[str]]]:
    """
    Extracts codes that appear more than once along with the locations where they appear.

    Args:
        codes_with_tabs (List[Tuple[str, str]]): A list of tuples with:
            - The sheet/tab where the code was found.
            - The code itself.

    Returns:
        List[Tuple[str, List[str]]]: A list of tuples where:
            - The first element is the duplicate code.
            - The second element is a list of tabs/sheets where the code appears.
    """
    # Create a dictionary to map codes to the tabs they appear in
    code_locations = defaultdict(list)

    # Populate the dictionary with the code and its location
    for tab, code in codes_with_tabs:
        code_locations[code].append(tab)

    # Filter out codes that appear only once
    return [(code, locations) for code, locations in code_locations.items() if len(locations) > 1]
