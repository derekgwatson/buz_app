# Quote History Scraper

Tool to scrape complete quote history from Buz Manager, including all notes, changes, and timeline events.

## Features

- ✅ Scrapes complete quote history including all change types (Created, Modified, Finalized, UpdateMassItems, etc.)
- ✅ Automatically expands truncated details ("Read more" content)
- ✅ Handles multi-page results with automatic pagination
- ✅ Available as both command-line tool and web interface
- ✅ Exports data to JSON format for analysis
- ✅ Supports multiple Buz accounts (Watson Blinds, Designer Drapes, etc.)

## Setup

### 1. Install Dependencies

```bash
pip install playwright
playwright install chromium
```

### 2. Configure Authentication

Before using the scraper, you need to authenticate with Buz Manager. Run the authentication bootstrap tool:

```bash
# For Watson Blinds account
python tools/buz_auth_bootstrap.py watsonblinds

# For Designer Drapes account
python tools/buz_auth_bootstrap.py designerdrapes
```

This will:
1. Open a browser window
2. Prompt you to log in to Buz Manager
3. Save your authentication credentials to `.secrets/buz_storage_state_{account}.json`

You only need to do this once per account (unless credentials expire).

## Usage

### Command-Line Tool

#### Basic Usage

```bash
python tools/scrape_quote_history.py <order_id>
```

**Example:**

```bash
python tools/scrape_quote_history.py 39e27815-b2c5-469f-9cea-63f6dcc32976
```

#### Options

```bash
# Use different account
python tools/scrape_quote_history.py <order_id> --account designerdrapes

# Save to specific output file
python tools/scrape_quote_history.py <order_id> --output my_quote.json

# Run with visible browser (for debugging)
python tools/scrape_quote_history.py <order_id> --visible
```

#### Getting the Order ID

The order ID is the GUID in the Buz quote URL:

```
https://go.buzmanager.com/Sales/Summary?orderId=39e27815-b2c5-469f-9cea-63f6dcc32976
                                                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                                This is the order ID
```

### Web Interface

1. Start the Flask app:

```bash
python run.py
```

2. Navigate to: `http://localhost:5000/tools/quote-scraper`

3. Enter the order ID and click "Start Scraping"

4. Monitor progress in real-time

5. Download results as JSON when complete

## Output Format

The scraper produces a JSON file with the following structure:

```json
{
  "order_id": "39e27815-b2c5-469f-9cea-63f6dcc32976",
  "total_entries": 55,
  "entries": [
    {
      "changes_title": "UpdateMassItems",
      "date": "15/11/2025 1:03 PM",
      "user": "procurementbuz@watsonblinds.com.au",
      "details": "6 Order Items (LineNumbers: 1, 2, 3, 4, 5, 6) from inventory group code 'CRTWT' were updated via Mass Update. || Brand - Filigree | Design - Simplicity Blockout Silicone 300 | Colour - Specified Below || COLOUR - TBA | HASLINING - Yes | LINING - Designer Drapes Pokolbin | LININGCOLOUR - Raw"
    },
    {
      "changes_title": "Modified",
      "date": "15/11/2025 12:45 PM",
      "user": "procurementbuz@watsonblinds.com.au",
      "details": "Discount 10.00% Applied $1,660.38"
    },
    ...
  ],
  "errors": []
}
```

## What Gets Scraped

The scraper collects all history entries from the quote, including:

- **Created** - Initial quote creation
- **Modified** - General modifications (discounts, pricing, etc.)
- **Finalized** - Quote finalization with installation details
- **UpdateMassItems** - Bulk item updates with fabric/product details
- **Dispatch** - Dispatch-related changes (if enabled)
- **Job Tracking** - Job tracking events (if enabled)
- **Custom events** - Any other change types in the system

## Troubleshooting

### Timeout Errors

If you get "Timeout 30000ms exceeded" or similar errors:

```
Page.goto: Timeout 30000ms exceeded
```

**This usually means:**

1. **Invalid or inaccessible quote ID** - Double-check the order ID from the URL
2. **Permission issues** - You may not have access to view this quote
3. **Buz Manager is slow** - The scraper now uses 60-second timeouts to handle this
4. **Expired authentication** - Re-run the auth bootstrap:

```bash
python tools/buz_auth_bootstrap.py watsonblinds
```

5. **Network issues** - Check your internet connection

**Try this:**
- Run with `--visible` to see what's happening in the browser
- Verify you can access the quote manually in a browser
- Make sure you're logged in to the correct Buz account

### Authentication Errors

If you get errors about missing authentication:

```
ERROR: Auth storage state not found at .secrets/buz_storage_state_watsonblinds.json
```

**Solution:** Run the auth bootstrap tool:

```bash
python tools/buz_auth_bootstrap.py watsonblinds
```

### Empty Results

If scraping completes but returns no entries:

1. Verify the order ID is correct (copy from the Buz URL)
2. Ensure you have permission to view the quote
3. Try running with `--visible` to see what's happening in the browser

### Pagination Issues

If not all entries are scraped:

1. Check the logs for "Total pages" count
2. Look for errors in the pagination section
3. Verify the page structure matches expectations (Buz may have updated their UI)

## API Integration

You can also use the scraper programmatically in Python:

```python
from pathlib import Path
import asyncio
from services.buz_quote_scraper import scrape_quote_history

async def main():
    result = await scrape_quote_history(
        order_id="39e27815-b2c5-469f-9cea-63f6dcc32976",
        storage_state_path=Path(".secrets/buz_storage_state_watsonblinds.json"),
        headless=True
    )

    print(f"Scraped {result.total_entries} entries")
    for entry in result.entries:
        print(f"{entry.date} - {entry.changes_title}")

asyncio.run(main())
```

## Data Analysis Examples

### Export to CSV

```python
import json
import csv

with open('quote_history.json') as f:
    data = json.load(f)

with open('quote_history.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['changes_title', 'date', 'user', 'details'])
    writer.writeheader()
    writer.writerows(data['entries'])
```

### Find Specific Changes

```python
import json

with open('quote_history.json') as f:
    data = json.load(f)

# Find all discount changes
discounts = [e for e in data['entries'] if 'Discount' in e['details']]
print(f"Found {len(discounts)} discount changes")

# Find all fabric updates
fabric_updates = [e for e in data['entries'] if e['changes_title'] == 'UpdateMassItems']
print(f"Found {len(fabric_updates)} fabric updates")
```

## Limitations

- Requires valid Buz Manager credentials
- Can only scrape quotes you have permission to view
- Relies on Buz Manager's HTML structure (may break if Buz updates their UI)
- Rate limited by Buz Manager's page load times

## Support

For issues or questions:
1. Check the logs in the web interface or console output
2. Try running with `--visible` to debug browser interactions
3. Verify authentication is still valid
4. Check if Buz Manager UI has changed
