import re


def load_friendly_descriptions_from_google_sheet(
    sheets_service,
    spreadsheet_id: str,
    range_name: str,
) -> dict[str, list[str]]:
    """
    Load friendly description overrides from a Google Sheet.

    :param sheets_service: An instance of GoogleSheetsService.
    :param spreadsheet_id: The ID of the spreadsheet.
    :param range_name: Range to read (default A:D for ProductCode + 3 descs)
    :return: Dict mapping product code to [desc1, desc2, desc3]
    """
    rows = sheets_service.fetch_sheet_data(spreadsheet_id, range_name)
    overrides = {}

    # Skip header row, start from second row
    for row in rows[1:]:
        if len(row) >= 4:
            product_code = row[0].strip()
            descs = [col.strip() for col in row[1:4]]
            if product_code:
                overrides[product_code] = descs
    return overrides


def extract_friendly_descriptions(product_code: str, description: str, override: dict[str, list[str]]) -> list[str]:
    if product_code in override:
        return override[product_code]

    match = re.search(r"\|fd\s*(.*?)\s*fd\|", description)
    if match:
        parts = [part.strip() for part in match.group(1).split(",")]
        while len(parts) < 3:
            parts.append("")
        return parts[:3]

    return ["", "", ""]
