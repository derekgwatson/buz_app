import pandas as pd


def process_matching_buz_items(first_file, second_file, output_file):
    # Load the first and second spreadsheets
    first_sheets = pd.ExcelFile(first_file)
    second_sheets = pd.ExcelFile(second_file)

    # Track collected DataFrames for sheets with matches
    sheets_to_write = {}

    print("Starting processing of matching Buz items...")
    print(f"Sheets in first file: {first_sheets.sheet_names}")
    print(f"Sheets in second file: {second_sheets.sheet_names}")

    for sheet_name in first_sheets.sheet_names:
        print(f"Processing sheet: {sheet_name}")

        # Skip if the sheet is not in the second file
        if sheet_name not in second_sheets.sheet_names:
            print(f"Skipping sheet {sheet_name}: Not found in second file.")
            continue

        # Read the sheets, ensuring the header is on row 2 (index 1)
        first_df = first_sheets.parse(sheet_name, header=1)  # Header on row 2
        second_df = second_sheets.parse(sheet_name, header=1)  # Header on row 2

        # Skip if there is no data from row 3 onwards
        if first_df.empty:
            print(f"Skipping sheet {sheet_name}: No data from row 3 onwards in first file.")
            continue

        print(f"First file (sheet: {sheet_name}) - Rows: {len(first_df)}, Columns: {len(first_df.columns)}")
        print(f"Second file (sheet: {sheet_name}) - Rows: {len(second_df)}, Columns: {len(second_df.columns)}")

        filtered_rows = []
        for index, row in first_df.iterrows():
            try:
                # Access the second column (column B) value in the current row
                code = row.iloc[1]  # Index 1 corresponds to column B
                print(f"Row {index} - Code: {code}")

                # Access the second column (column B) in the second DataFrame
                match_by_code = second_df[second_df.iloc[:, 1] == code]
                if not match_by_code.empty:
                    print(f"Match found by code for row {index} in sheet {sheet_name}.")
                    filtered_rows.append(match_by_code)
                else:
                    print(f"No match by code for row {index} in sheet {sheet_name}. Checking columns D, E, F...")
                    # Check columns D, E, F (indexes 3, 4, 5 respectively)
                    if second_df.shape[1] > 5:  # Ensure there are enough columns to check
                        match_by_def = second_df[
                            (second_df.iloc[:, 3] == row.iloc[3]) &
                            (second_df.iloc[:, 4] == row.iloc[4]) &
                            (second_df.iloc[:, 5] == row.iloc[5])
                        ]
                        if not match_by_def.empty:
                            print(f"Match found by D, E, F for row {index} in sheet {sheet_name}.")
                            filtered_rows.append(match_by_def)
                        else:
                            print(f"No match by D, E, F for row {index} in sheet {sheet_name}.")
            except IndexError as e:
                print(f"IndexError for row {index} in sheet {sheet_name}: {e}")
            except Exception as e:
                print(f"Error for row {index} in sheet {sheet_name}: {e}")

        if filtered_rows:
            # Combine filtered rows into a single DataFrame
            combined_df = pd.concat(filtered_rows)

            # Remove trailing asterisks (*) from column headers
            combined_df.columns = [col.rstrip('*') for col in combined_df.columns]

            # Add 'E' in column AO
            combined_df.insert(40, 'AO', 'E')  # Column AO corresponds to index 40

            sheets_to_write[sheet_name] = combined_df
            print(f"Matches found in sheet {sheet_name}: {len(sheets_to_write[sheet_name])} rows.")
        else:
            print(f"No matches found in sheet {sheet_name}.")

    # Only create the ExcelWriter if there are sheets to write
    if sheets_to_write:
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            for sheet_name, df in sheets_to_write.items():
                # Write a blank row first
                blank_row = pd.DataFrame([[""] * len(df.columns)], columns=df.columns)
                blank_row.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=0)

                # Write the column headers and data starting from row 2
                df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
        print(f"Output file written to: {output_file}")
        return True  # Matches found

    print("No matches found in any sheets.")
    return False  # No matches found
