
# Pre-Christmas Cutoff Publisher (for Buz) — v2

Generates:
- HTML snippets (Canberra & Regional) for Buz homepage
- Four Excel files (Detailed/Summary × Canberra/Regional) by injecting lead time + cutoff into your macro templates
- Validates that Lead Times and Cutoff sheets resolve to the same set of Buz inventory codes (per store)

## Install
```bash
python -m venv .venv
. .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Configure
Edit `xmas/config.json`:
- Sheet IDs and tab names are prefilled from your links
- Column letters match your current layout
- Set `templates.detailed_path` and `templates.summary_path` to your `.xlsm` files (absolute paths recommended)
- Optionally set output dir

## Run
```bash
python -m xmas.runner --config xmas/config.json
# or only Canberra:
python -m xmas.runner --config xmas/config.json --canberra
# or only Regional:
python -m xmas.runner --config xmas/config.json --regional
```

Outputs (default `out/`):
- `prexmas_canberra.html`, `prexmas_regional.html`
- `Watson_PreXmas_Canberra_Detailed_{YYYYMMDD}.xlsx`
- `Watson_PreXmas_Canberra_Summary_{YYYYMMDD}.xlsx`
- `Watson_PreXmas_Regional_Detailed_{YYYYMMDD}.xlsx`
- `Watson_PreXmas_Regional_Summary_{YYYYMMDD}.xlsx`
- `warnings.txt` if any tab row mismatches were detected

## Notes
- **Hard fail** if the set of resolved Buz codes from Lead Times ≠ Cutoff (per store).
- Lead-time duplicate resolution: parse **all ranges** in the text and pick the **largest upper bound** (weeks). The display text remains the original string.
- **Summary**: append to Column **C**; **Detailed**: append to Column **B** at the first `FALSE` below 'Do Not Show?'.
- We don't insert rows; macros are preserved (`keep_vba=True`). Hidden sheets are left as-is unless pruned by control list.
- Uses your `services.google_sheets_service.GoogleSheetsService.fetch_sheet_data(...)` API.
