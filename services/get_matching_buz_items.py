import pandas as pd


def process_matching_buz_items(first_file, second_file, output_file):
    # Load the first and second spreadsheets
    first_sheets = pd.ExcelFile(first_file)
    second_sheets = pd.ExcelFile(second_file)

    # Track collected DataFrames for sheets with matches
    sheets_to_write = {}

    for sheet_name in first_sheets.sheet_names:
        # Skip if the sheet is not in the second file
        if sheet_name not in second_sheets.sheet_names:
            continue

        # Read the sheets, ensuring the header is on row 2 (index 1)
        first_df = first_sheets.parse(sheet_name, header=1)  # Header on row 2
        second_df = second_sheets.parse(sheet_name, header=1)  # Header on row 2

        # Skip if there is no data from row 3 onwards
        if first_df.empty:
            continue

        filtered_rows = []
        for _, row in first_df.iterrows():
            try:
                # Access the second column (column B) value in the current row
                code = row.iloc[1]  # Index 1 corresponds to column B

                # Access the second column (column B) in the second DataFrame
                match_by_code = second_df[second_df.iloc[:, 1] == code]
                if not match_by_code.empty:
                    filtered_rows.append(match_by_code)
                else:
                    # Check columns D, E, F (indexes 3, 4, 5 respectively)
                    if second_df.shape[1] > 5:  # Ensure there are enough columns to check
                        match_by_def = second_df[
                            (second_df.iloc[:, 3] == row.iloc[3]) &
                            (second_df.iloc[:, 4] == row.iloc[4]) &
                            (second_df.iloc[:, 5] == row.iloc[5])
                        ]
                        if not match_by_def.empty:
                            filtered_rows.append(match_by_def)
            except IndexError:
                pass  # Safely ignore errors for missing columns
            except Exception:
                pass  # Safely ignore any unexpected errors

        if filtered_rows:
            # Combine filtered rows into a single DataFrame
            combined_df = pd.concat(filtered_rows)

            # Remove trailing asterisks (*) from column headers
            combined_df.columns = [col.rstrip('*') for col in combined_df.columns]

            # Add 'E' in column AO
            combined_df.insert(40, 'AO', 'E')  # Column AO corresponds to index 40

            sheets_to_write[sheet_name] = combined_df

    # Only create the ExcelWriter if there are sheets to write
    if sheets_to_write:
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            for sheet_name, df in sheets_to_write.items():
                # Write a blank row first
                blank_row = pd.DataFrame([[""] * len(df.columns)], columns=df.columns)
                blank_row.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=0)

                # Write the column headers and data starting from row 2
                df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
        return True  # Matches found

    return False  # No matches found
