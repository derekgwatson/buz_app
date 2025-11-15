#!/usr/bin/env python3
# tools/scrape_quote_history.py
"""
Command-line tool to scrape quote history from Buz Manager.

Usage:
    python tools/scrape_quote_history.py <order_id> [--account watsonblinds] [--output history.json] [--visible]

Examples:
    # Scrape quote history
    python tools/scrape_quote_history.py 39e27815-b2c5-469f-9cea-63f6dcc32976

    # Use different account
    python tools/scrape_quote_history.py 39e27815-b2c5-469f-9cea-63f6dcc32976 --account designerdrapes

    # Save to specific output file
    python tools/scrape_quote_history.py 39e27815-b2c5-469f-9cea-63f6dcc32976 --output quote_data.json

    # Run with visible browser (for debugging)
    python tools/scrape_quote_history.py 39e27815-b2c5-469f-9cea-63f6dcc32976 --visible
"""

import asyncio
import json
import sys
import argparse
from pathlib import Path

# Add parent directory to path so we can import from services
sys.path.insert(0, str(Path(__file__).parent.parent))

from services.buz_quote_scraper import scrape_quote_history


def print_progress(pct: int, msg: str):
    """Print progress updates"""
    print(f"[{pct:3d}%] {msg}")


async def main():
    parser = argparse.ArgumentParser(
        description="Scrape quote history from Buz Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "order_id",
        help="Order/quote ID (GUID) to scrape"
    )
    parser.add_argument(
        "--account",
        default="watsonblinds",
        help="Buz account name (default: watsonblinds)"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output JSON file path (default: quote_history_<order_id>.json)"
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Run browser in visible mode (not headless)"
    )

    args = parser.parse_args()

    # Determine storage state path
    storage_state_path = Path(f".secrets/buz_storage_state_{args.account}.json")

    if not storage_state_path.exists():
        print(f"ERROR: Auth storage state not found at {storage_state_path}")
        print(f"\nPlease run authentication bootstrap first:")
        print(f"    python tools/buz_auth_bootstrap.py {args.account}")
        sys.exit(1)

    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        # Truncate order_id for filename
        order_id_short = args.order_id[:8]
        output_path = Path(f"quote_history_{order_id_short}.json")

    print(f"=== Buz Quote History Scraper ===")
    print(f"Order ID: {args.order_id}")
    print(f"Account: {args.account}")
    print(f"Output: {output_path}")
    print(f"Headless: {not args.visible}")
    print()

    # Run the scraper
    result = await scrape_quote_history(
        order_id=args.order_id,
        storage_state_path=storage_state_path,
        headless=not args.visible,
        progress_callback=print_progress
    )

    # Save results to JSON
    output_path.write_text(json.dumps(result.to_dict(), indent=2))

    # Print summary
    print()
    print("=== Results ===")
    print(f"Total entries: {result.total_entries}")
    print(f"Errors: {len(result.errors)}")

    if result.errors:
        print("\nErrors encountered:")
        for error in result.errors:
            print(f"  - {error}")

    print(f"\nResults saved to: {output_path.resolve()}")

    # Print first few entries as preview
    if result.entries:
        print("\n=== Sample Entries (first 3) ===")
        for i, entry in enumerate(result.entries[:3], 1):
            print(f"\n{i}. {entry.changes_title} - {entry.date}")
            print(f"   User: {entry.user}")
            details_preview = entry.details[:100] + "..." if len(entry.details) > 100 else entry.details
            print(f"   Details: {details_preview}")

    return 0 if not result.errors else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
