def process_buz_items_by_supplier_codes(uploaded_file, supplier_codes):
    """
    Process all sheets in the uploaded Excel file to filter rows based on supplier codes.

    :param uploaded_file: FileStorage object of the uploaded Excel file.
    :param supplier_codes: List of supplier codes to filter by.
    :return: BytesIO object containing the filtered Excel file.
    """
    import pandas as pd
    from io import BytesIO

    # Load all sheets into a dictionary of DataFrames
    sheets = pd.read_excel(uploaded_file, sheet_name=None, engine='openpyxl', header=None)

    filtered_sheets = {}

    for sheet_name, df in sheets.items():
        print(f"Processing sheet: {sheet_name}")  # Debugging: Log sheet name

        # Check if the sheet has at least 28 non-blank columns in row 2 (index 1)
        if df.shape[0] > 1:  # Ensure there are at least two rows
            non_blank_columns = df.iloc[1].notna().sum()
            print(f"Non-blank columns in row 2 of '{sheet_name}': {non_blank_columns}")  # Debugging

            if non_blank_columns >= 28:
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
                    filtered_df = filtered_df.reindex(columns=range(max_columns), fill_value="")  # Expand columns if needed
                    filtered_df.iloc[:, 40] = 'E'  # Set 'E' in column AO for all rows in filtered_df

                    # Add back the header rows (0 and 1)
                    result_df = pd.concat([df.iloc[:2], filtered_df])

                    # Store the filtered DataFrame
                    filtered_sheets[sheet_name] = result_df
                else:
                    print(f"Skipping sheet '{sheet_name}' as no rows matched the supplier codes.")
            else:
                print(f"Skipping sheet '{sheet_name}' due to insufficient columns.")
        else:
            print(f"Skipping sheet '{sheet_name}' due to insufficient rows.")

    if not filtered_sheets:
        raise ValueError("No sheets met the criteria for processing or contained matching rows.")

    # Save all filtered sheets to a new Excel file
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, filtered_df in filtered_sheets.items():
            filtered_df.to_excel(writer, index=False, header=False, sheet_name=sheet_name)
    output.seek(0)

    return output
