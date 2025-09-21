import json
import logging
import time
import os
import re
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
from pathlib import Path

logger = logging.getLogger(__name__)


class GoogleSheetsService:
    """
    A class to handle Google Sheets operations.
    Automatically uses the service account JSON from the app's main folder (NOT inside /app).
    """

    @staticmethod
    def _get_service_account_path():
        env = os.environ.get("GSHEETS_CREDENTIALS") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if env and os.path.isfile(env):
            return env

        candidates = [
            Path(__file__).resolve().parents[1] / "credentials" / "service_account.json",
            Path(__file__).resolve().parents[2] / "credentials" / "service_account.json",
            Path.cwd() / "credentials" / "service_account.json",
            Path.cwd() / "service_account.json",
        ]
        for p in candidates:
            if p.is_file():
                return str(p)

        raise FileNotFoundError(
            "service_account.json not found. Set GSHEETS_CREDENTIALS or GOOGLE_APPLICATION_CREDENTIALS.")

    @staticmethod
    def _authenticate_google_sheets(json_file: str) -> Credentials:
        """
        Authenticate with the Google Sheets API.

        :param json_file: JSON configuration file containing service account credentials.
        :type json_file: str
        :return: Credentials object for use in authorization.
        :rtype: google.oauth2.service_account.Credentials
        """
        with open(json_file) as f:
            creds = json.load(f)
        scope = [
            "https://www.googleapis.com/auth/spreadsheets"
        ]
        return Credentials.from_service_account_info(creds, scopes=scope)

    def __init__(self, json_file: str = None):
        """
        Initialize the GoogleSheetsService with an authorized gspread client.
        If no json_file is provided, uses the app's default credentials path.

        :param json_file: Path to the JSON configuration file.
        :type json_file: str
        """
        if json_file is None:
            json_file = self._get_service_account_path()
        creds = self._authenticate_google_sheets(json_file)
        self._client = gspread.authorize(creds)

        # in __init__
        self._cache: dict[tuple[str, str, str | None], tuple[float, list[list[str]]]] = {}
        self._cache_ttl = 15  # seconds
        self._ss_cache: dict[str, tuple[float, gspread.Spreadsheet]] = {}
        self._ws_cache: dict[tuple[str, str], tuple[float, gspread.Worksheet]] = {}
        self._ss_ttl = 60  # seconds

    def _open(self, spreadsheet_id: str) -> gspread.Spreadsheet:
        now = time.time()
        hit = self._ss_cache.get(spreadsheet_id)
        if hit and (now - hit[0] < self._ss_ttl):
            return hit[1]
        sh = self._client.open_by_key(spreadsheet_id)
        self._ss_cache[spreadsheet_id] = (now, sh)
        return sh

    def _worksheet(self, spreadsheet_id: str, sheet_name: str) -> gspread.Worksheet:
        now = time.time()
        key = (spreadsheet_id, sheet_name)
        hit = self._ws_cache.get(key)
        if hit and (now - hit[0] < self._ss_ttl):
            return hit[1]
        ws = self._open(spreadsheet_id).worksheet(sheet_name)
        self._ws_cache[key] = (now, ws)
        return ws

    def batch_get_cached(self, spreadsheet_id: str, ranges: list[str]):
        now = time.time()
        hits, misses = {}, []
        for r in ranges:
            k = (spreadsheet_id, r)
            if k in self._cache and now - self._cache[k][0] < self._cache_ttl:
                hits[r] = self._cache[k][1]
            else:
                misses.append(r)
        if misses:
            fetched = self.batch_get(spreadsheet_id, misses)
            for r, vals in zip(misses, fetched, strict=False):
                k = (spreadsheet_id, r)
                self._cache[k] = (now, vals)
                hits[r] = vals
        return [hits[r] for r in ranges]

    def fetch_sheet_data(self, spreadsheet_id: str, range_name: str) -> list[list[str]]:
        """Return only the requested A1 range (e.g. 'Sheet1!A:B' or 'Sheet1!A1:D10')."""
        try:
            # If caller passed just a sheet name, return the whole used range for that sheet.
            if "!" not in range_name:
                ws = self._worksheet(spreadsheet_id, range_name)
                return self._with_backoff(lambda: ws.get_all_values())

            # Proper A1 range → use batch values API (faster, cheaper).
            vals = self.batch_get_cached(spreadsheet_id, [range_name])[0]
            return vals or []
        except Exception as e:
            logger.error(f"Error fetching data from {spreadsheet_id} range '{range_name}': {e}")
            return []

    def insert_row(self, spreadsheet_id: str, row_data: list[str], worksheet_name: str = 'Sheet1'):
        """
        Insert a row into a specific worksheet.
        """
        try:
            worksheet = self._worksheet(spreadsheet_id, worksheet_name)
            worksheet.append_row(row_data)
        except Exception as e:
            logger.error(f"Error inserting row into spreadsheet {spreadsheet_id}: {e}")

    # services/google_sheets_service.py

    def get_sheet_names(self, spreadsheet_id: str, *, include_hidden: bool = True) -> list[str]:
        """
        Return worksheet (tab) names. Set include_hidden=False to ignore hidden tabs.
        """
        try:
            sh = self._open(spreadsheet_id)
            names: list[str] = []
            for ws in sh.worksheets():
                # gspread >=5: Worksheet.hidden; fallback to properties/_properties
                hidden = getattr(ws, "hidden", None)
                if hidden is None:
                    props = getattr(ws, "properties", None) or getattr(ws, "_properties", {}) or {}
                    hidden = bool(props.get("hidden", False))
                if include_hidden or not hidden:
                    names.append(ws.title)
            return names
        except Exception as e:
            logger.error(f"Error listing sheets for {spreadsheet_id}: {e}")
            return []

    def read_tab_as_records(
            self,
            spreadsheet_id: str,
            sheet_name: str,
            header_row: int = 1,
    ) -> list[dict[str, str]]:
        """Read rows starting at header_row using the Values API (no full-sheet read)."""
        try:
            a1 = f"'{sheet_name}'!A{header_row}:ZZZ"  # generous width but bounded
            rows = self.fetch_sheet_data(spreadsheet_id, a1)
            if not rows:
                return []

            headers = [h.strip() for h in rows[0]]
            out: list[dict[str, str]] = []
            for row in rows[1:]:
                padded = row + [""] * (len(headers) - len(row))
                item = {headers[i]: padded[i] for i in range(len(headers))}
                if any(x.strip() for x in item.values()):
                    out.append(item)
            return out
        except Exception as e:
            logger.error(f"Error reading tab '{sheet_name}' in {spreadsheet_id}: {e}")
            return []

    @staticmethod
    def _norm(s: str | None) -> str:
        if not s:
            return ""
        s = str(s).strip().lower()
        return re.sub(r"[^a-z0-9]+", "", s)

    @staticmethod
    def _col_letter(n: int) -> str:
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    def find_header_row_by_marker(
            self,
            spreadsheet_id: str,
            sheet_name: str,
            marker_text: str,
            search_col: int = 1,
            scan_limit: int = 200,
    ) -> int | None:
        col = self._col_letter(search_col)
        a1 = f"'{sheet_name}'!{col}1:{col}{scan_limit}"
        col_2d = self.batch_get_cached(spreadsheet_id, [a1])[0] or []
        col_vals: list[str] = col_2d[0] if col_2d and isinstance(col_2d[0], list) else col_2d  # flatten

        needle = self._norm(marker_text)
        for i, v in enumerate(col_vals, start=1):
            if self._norm(v) == needle:
                return i
        return None

    def read_tab_as_records_by_marker(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        header_marker_text: str,
        default_header_row: int = 4,
    ) -> list[dict[str, str]]:
        """
        Find the header row by locating `header_marker_text` in column A,
        then read the tab using that header row. Falls back to `default_header_row`.
        """
        hdr = self.find_header_row_by_marker(spreadsheet_id, sheet_name, header_marker_text) \
            or default_header_row
        return self.read_tab_as_records(spreadsheet_id, sheet_name, header_row=hdr)

    @staticmethod
    def _with_backoff(fn, *, tries: int = 5, base: float = 0.6, factor: float = 2.0):
        """Generic retry for Sheets 429 rate limits."""
        delay = base
        for attempt in range(tries):
            try:
                return fn()
            except APIError as e:
                code = getattr(getattr(e, "response", None), "status_code", None)
                if code == 429 or "quota" in str(e).lower() or "rate" in str(e).lower():
                    if attempt == tries - 1:
                        raise
                    time.sleep(delay)
                    delay *= factor
                    continue
                raise

    def col_values(
            self,
            spreadsheet_id: str,
            sheet_name: str,
            col_index: int,
            max_rows: int | None = None,
    ) -> list[str]:
        """Return up to `max_rows` cells from a column using values.batchGet."""
        col = self._col_letter(col_index)
        if max_rows is None:
            max_rows = 5000
        a1 = f"'{sheet_name}'!{col}1:{col}{max_rows}"
        col_2d = self.batch_get_cached(spreadsheet_id, [a1])[0] or []
        col_vals: list[str] = col_2d[0] if col_2d and isinstance(col_2d[0], list) else col_2d
        return [str(v) for v in col_vals]

    def values(self, spreadsheet_id: str, sheet_name: str, range_a1: str) -> list[list[str]]:
        a1 = f"'{sheet_name}'!{range_a1}" if "!" not in range_a1 else range_a1
        return self.batch_get_cached(spreadsheet_id, [a1])[0] or []

    def read_range(self, spreadsheet_id: str, sheet_name: str, a1: str) -> list[list[str]]:
        return self.values(spreadsheet_id, sheet_name, a1)

    def batch_get(
            self,
            spreadsheet_id: str,
            ranges: list[str],
    ) -> list[list[list[str]]]:
        sh = self._open(spreadsheet_id)
        resp = sh.values_batch_get(ranges)  # ← no major_dimension kwarg
        return [vr.get("values", []) for vr in resp.get("valueRanges", [])]

    def fetch_many(
        self,
        spreadsheet_id: str,
        sheet_names: list[str],
        a1_tail: str = "A1:B20",
    ) -> dict[str, list[list[str]]]:
        """Fetch the same A1 tail from many tabs in one API call."""
        ranges = [f"'{name}'!{a1_tail}" for name in sheet_names]
        blocks = self.batch_get_cached(spreadsheet_id, ranges)
        return {name: (block or []) for name, block in zip(sheet_names, blocks, strict=False)}


def filter_google_sheet_second_column_numeric(
        sheets_service: GoogleSheetsService, spreadsheet_id: str, range_name: str = 'Sheet1!A:B'
) -> list[tuple[str, str]]:

    """
    Get data from a Google Sheet and filter rows based on numeric values in the second column.

    :param sheets_service: An instance of GoogleSheetsService.
    :type sheets_service: GoogleSheetsService
    :param spreadsheet_id: The ID of the Google Sheet to fetch data from.
    :type spreadsheet_id: str
    :param range_name: The range of data to fetch (default is 'Sheet1!A:B').
    :type range_name: str
    :return: A list of tuples (column A, column B) where column B contains numeric values.
    :rtype: list[tuple[str, str]]
    """

    # Fetch data from the specified range
    data = sheets_service.fetch_sheet_data(spreadsheet_id, range_name)
    if not data:
        logger.warning("No data fetched from Google Sheets.")
        return []

    def is_numeric(value: str) -> bool:
        if value is None:
            return False
        v = value.strip().replace(",", "")
        if v.endswith("%"):
            v = v[:-1].strip()
        try:
            float(v)
            return True
        except ValueError:
            return False

    # Filter rows where column B has a numeric value (including percentages)
    return [
        (row[0], row[1])
        for row in data
        if len(row) > 1 and row[0] and row[1] and is_numeric(row[1])
    ]


