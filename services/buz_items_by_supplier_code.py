import json


def process_buz_items_by_supplier_codes(uploaded_file, supplier_codes):
    """
    Process all sheets in the uploaded Excel file to filter rows based on supplier codes,
    while ensuring row 2 contains the expected headers or is blank.

    :param uploaded_file: FileStorage object of the uploaded Excel file.
    :param supplier_codes: List of supplier codes to filter by.
    :return: BytesIO object containing the filtered Excel file, or None if no valid sheets.
    """
    import pandas as pd
    from io import BytesIO
    import json

    # Check if the file is provided and not empty
    if not uploaded_file:
        raise ValueError("No file uploaded.")

    # Check if the uploaded file is empty
    if not isinstance(uploaded_file, BytesIO):
        raise TypeError("Unsupported file type. Expected a BytesIO object.")

    # Load expected headers from config.json
    with open("config.json", "r") as file:
        config = json.load(file)
    expected_headers = config["expected_buz_inventory_item_file_headers"]

    # Load all sheets into a dictionary of DataFrames
    sheets = pd.read_excel(uploaded_file, sheet_name=None, engine='openpyxl', header=None)

    filtered_sheets = {}

    for sheet_name, df in sheets.items():
        print(f"Processing sheet: {sheet_name}")  # Debugging: Log sheet name

        # Check if the sheet has at least two rows
        if df.shape[0] > 1:
            row_2 = df.iloc[1].fillna("")  # Row 2 (index 1), fill NaN with empty strings

            # Validate headers: either empty or matches expected headers
            if row_2.str.strip().tolist() == [""] * len(row_2) or row_2.tolist()[
                                                                  :len(expected_headers)] == expected_headers:
                print(f"Valid headers in sheet '{sheet_name}'.")

                # Remove trailing asterisks from titles in row 2
                df.iloc[1] = df.iloc[1].astype(str).str.rstrip('*')

                # Access the 28th column (column AB in Excel terms)
                ab_column = df.iloc[2:, 27]  # Start from row 3 (index 2) to ignore header rows

                # Filter rows based on supplier codes
                filtered_df = df.iloc[2:]  # Ignore rows 0 and 1
                filtered_df = filtered_df[ab_column.isin(supplier_codes)]

                if not filtered_df.empty:
                    # Add 'E' in the 41st column (column AO) for the filtered rows
                    max_columns = max(41, filtered_df.shape[1])  # Ensure at least 41 columns exist
                    filtered_df = filtered_df.reindex(columns=range(max_columns),
                                                      fill_value="")  # Expand columns if needed
                    filtered_df.iloc[:, 40] = 'E'  # Set 'E' in column AO for all rows in filtered_df

                    # Add back the header rows (0 and 1)
                    result_df = pd.concat([df.iloc[:2], filtered_df])

                    # Store the filtered DataFrame
                    filtered_sheets[sheet_name] = result_df
                else:
                    print(f"Skipping sheet '{sheet_name}' as no rows matched the supplier codes.")
            else:
                print(f"Skipping sheet '{sheet_name}' due to invalid headers.")
        else:
            print(f"Skipping sheet '{sheet_name}' due to insufficient rows.")

    if not filtered_sheets:
        print("No sheets met the criteria for processing or contained matching rows.")
        return None  # or {} if you prefer returning an empty dict

    # Save all filtered sheets to a new Excel file
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, filtered_df in filtered_sheets.items():
            filtered_df.to_excel(writer, index=False, header=False, sheet_name=sheet_name)
    output.seek(0)

    return output
