import io
from openpyxl import Workbook


def generate_test_row(default_value="", total_columns=55, overrides=None):
    """
    Generate a test array with specified overrides, filling the rest with default values.

    Args:
        default_value (str): The default value to fill in the array.
        total_columns (int): Total number of columns in the array.
        overrides (dict): A dictionary where keys are column indices (1-based) and
                          values are the specific values to set.

    Returns:
        list: A list representing the row of the spreadsheet.
    """
    # Initialize the row with default values
    row = [default_value] * total_columns

    # Apply overrides
    if overrides:
        for col_index, value in overrides.items():
            if 1 <= col_index <= total_columns:  # Ensure the index is valid
                row[col_index - 1] = value  # Convert 1-based to 0-based indexing

    return row


def create_mock_excel(sheets_data):
    """
    Create an in-memory Excel file with specified sheet data.

    Args:
        sheets_data (dict): A dictionary where keys are sheet names and values are lists of rows.
                            Each row should be a list of cell values.

    Returns:
        BytesIO: A file-like object containing the Excel workbook.
    """
    wb = Workbook()

    for i, (sheet_name, rows) in enumerate(sheets_data.items()):
        # Add or select the sheet
        ws = wb.active if i == 0 else wb.create_sheet(title=sheet_name)
        ws.title = sheet_name

        # Add rows to the sheet
        for row in rows:
            ws.append(row)

    # Save to an in-memory file
    excel_file = io.BytesIO()
    wb.save(excel_file)
    excel_file.seek(0)  # Reset file pointer to the beginning
    return excel_file


def get_dummy_inventory_items_header_row():
    return [
        "PkId",
        "Code",
        "Description",
        "DescnPart1 (Material)",
        "DescnPart2 (Material Types)",
        "DescnPart3 (Colour)",
        "Price Grid Code",
        "Cost Grid Code",
        "Discount Group Code",
        "Last Purchase Price",
        "Standard Cost",
        "Tax Rate",
        "Units Purchase",
        "Min Qty",
        "Max Qty",
        "Reorder Multiplier",
        "ForeX Code",
        "Last Purchase ForeX",
        "Purchasing Lead Days",
        "Stocking Multiplier",
        "Units Stock",
        "Selling Multiplier",
        "Units Sell",
        "Cost Method",
        "Product Size",
        "Product Type",
        "Supplier",
        "Supplier Product Code",
        "Supplier Product Description",
        "Length",
        "Maximum Width",
        "Extra Time to Produce",
        "Extra Time to Fit",
        "Custom Var 1 (PackSize)",
        "Custom Var 2 (PackOpt)",
        "Custom Var 3 (PackType)",
        "Warning",
        "RptCat",
        "Active",
        "Last Edit Date",
        "Operation"
    ]


def get_dummy_pricing_header_row():
    return [
        "PkId",
        "Inventory Code",
        "Description",
        "Customer Price Group Code",
        "Date From",
        "Sell Each",
        "SellLMWide",
        "SellLMHeight",
        "SellLMDepth",
        "SellSQM",
        "SellPercentageOnMain",
        "SellMinimum",
        "CostEach",
        "CostLMWide",
        "CostLMHeight",
        "CostLMDepth",
        "CostSQM",
        "CostPercentageOnMain",
        "CostMinimum",
        "InstallCostEach",
        "InstallCostLMWidth",
        "InstallCost Height",
        "InstallCostDepth",
        "InstallCostSQM",
        "InstallCostPercentageOfMain",
        "InstallCostMinimum",
        "InstallSellEach",
        "InstallSellMinimum",
        "InstallSellLMWide",
        "InstallSellSQM",
        "InstallSellHeight",
        "InstallSellDepth",
        "InstallSellPercentageOfMain",
        "Supplier Code",
        "Supplier Descn",
        "IsNotCurrent",
        "Operation",
    ]


def get_dummy_inventory_item_valid(tab_name:str, row_id: int):
    return generate_test_row(
        total_columns=41,
        overrides={
            1: row_id,  # PkId
            2: tab_name + str(row_id),  # Code
            3: "Main Description " + str(row_id),  # Description
            4: "Material " + str(row_id),  # Material
            5: "Material Type " + str(row_id),  # Material Type
            6: "Colour " + str(row_id),  # Colour
            27: "UNLEASHED",  # Supplier
            28: "001",  # Supplier Product Code
        }
    )


def get_dummy_inventory_item_invalid_unleashed_code(tab_name:str, row_id: int):
    return generate_test_row(
        total_columns=41,
        overrides={
            1: row_id,  # PkId
            2: tab_name + str(row_id),  # Code
            3: "Main Description " + str(row_id),  # Description
            4: "Material " + str(row_id),  # Material
            5: "Material Type " + str(row_id),  # Material Type
            6: "Colour " + str(row_id),  # Colour
            27: "UNLEASHED",  # Supplier
            28: "invalidCode",  # Supplier Product Code
        }
    )


def get_dummy_inventory_item_not_unleashed_code(tab_name:str, row_id: int):
    return generate_test_row(
        total_columns=41,
        overrides={
            1: row_id,  # PkId
            2: tab_name + str(row_id),  # Code
            3: "Main Description " + str(row_id),  # Description
            4: "Material " + str(row_id),  # Material
            5: "Material Type " + str(row_id),  # Material Type
            6: "Colour " + str(row_id),  # Colour
            27: "WATSON",  # Supplier
            28: "SomeWatsonCode",  # Supplier Product Code
        }
    )


def get_dummy_inventory_item_backorder_message_current(tab_name:str, row_id: int):
    return generate_test_row(
        total_columns=41,
        overrides={
            1: row_id,  # PkId
            2: tab_name + str(row_id),  # Code
            3: "Main Description " + str(row_id),  # Description
            4: "Material " + str(row_id),  # Material
            5: "Material Type " + str(row_id),  # Material Type
            6: "Colour " + str(row_id),  # Colour
            27: "WATSON",  # Supplier
            28: "SomeWatsonCode",  # Supplier Product Code
            37: "Fabric on backorder until 3 Jan 2050",
        }
    )


def get_dummy_inventory_items_blank_row():
    return [""] * 41


def get_dummy_inventory_items():
    """
    Generate dummy data for the 'Inventory Items' sheet.

    Returns:
        list: A list of rows for the 'Inventory Items' sheet.
    """
    return create_mock_excel({
        "ROLL": [
            get_dummy_inventory_items_blank_row(),
            get_dummy_inventory_items_header_row(),
            get_dummy_inventory_item_valid("ROLL", 1),
            get_dummy_inventory_item_invalid_unleashed_code("ROLL", 2),
            get_dummy_inventory_item_not_unleashed_code("ROLL", 3),
        ],
        "ROLC": [
            get_dummy_inventory_items_blank_row(),
            get_dummy_inventory_items_header_row(),
            get_dummy_inventory_item_valid("ROLC", 1),
            get_dummy_inventory_item_invalid_unleashed_code("ROLC", 2),
            get_dummy_inventory_item_not_unleashed_code("ROLC", 3),
        ]
    })


def get_dummy_inventory_pricing():
    """
    Generate dummy data for the 'Inventory Pricing' sheet.

    Returns:
        list: A list of rows for the 'Inventory Pricing' sheet.
    """
    return [
        ["Code", "Tier1", "Tier2", "Tier3"],  # Header row
        ["001", "5.0", "4.5", "4.0"],        # Data row 1
        ["002", "15.0", "14.0", "13.5"],     # Data row 2
    ]


def get_dummy_unleashed_data():
    return [
        generate_test_row(
            total_columns=55,
            overrides={
                1: "001",  # Product Code
                2: "Docri Acrylic Binding - White - 001 AWN Fab Category 5",  # Product Description
                5: "SQM",  # Unit of Measure
                16: 50,  # Default Purchase Price
                19: "",  # Default Sell Price
                29: 34,  # Sell Price Tier 9
                33: 3,  # Width (m)
                50: "Yes",  # IsObsoleted
                51: "Yes",  # IsSellable
            }
        ),
        generate_test_row(
            total_columns=55,
            overrides={
                1: "1013155D",  # Product Code
                2: "Hampton Beech Cove",  # Product Description
                5: "LM",  # Unit of Measure
                16: 2.2776,  # Default Purchase Price
                19: 0,  # Default Sell Price
                29: "",  # Sell Price Tier 9
                33: 0.089,  # Width (m)
                50: "No",  # IsObsoleted
                51: "Yes",  # IsSellable
            }
        ),
    ]