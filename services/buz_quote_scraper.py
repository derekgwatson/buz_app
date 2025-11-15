# services/buz_quote_scraper.py
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

logger = logging.getLogger(__name__)


@dataclass
class HistoryEntry:
    """Single entry from the quote history table"""
    changes_title: str
    date: str
    user: str
    details: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class QuoteHistoryResult:
    """Result of quote history scraping"""
    order_id: str
    total_entries: int
    entries: List[HistoryEntry]
    errors: List[str]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'order_id': self.order_id,
            'total_entries': self.total_entries,
            'entries': [e.to_dict() for e in self.entries],
            'errors': self.errors
        }


class BuzQuoteScraper:
    """Scrape quote history from Buz Manager"""

    def __init__(self, storage_state_path: Path, headless: bool = True):
        """
        Initialize quote scraper.

        Args:
            storage_state_path: Path to Playwright storage state JSON file for authentication
            headless: Run browser in headless mode
        """
        self.storage_state_path = storage_state_path
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.playwright = None

    async def __aenter__(self):
        """Context manager entry - launch browser"""
        if not self.storage_state_path.exists():
            raise FileNotFoundError(
                f"Auth storage state not found at {self.storage_state_path}. "
                f"Run tools/buz_auth_bootstrap.py <account_name> first."
            )

        self.playwright = await async_playwright().__aenter__()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            storage_state=str(self.storage_state_path)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close browser"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()

    async def handle_org_selector_if_present(self, page: Page, intended_url: str):
        """
        Check if we're on the org selector page and automatically click through.

        Args:
            page: The page object
            intended_url: The URL we were trying to reach
        """
        if "mybuz/organizations" not in page.url:
            return  # Not on org selector, all good

        logger.info("Landed on org selector, clicking through...")

        # Click the first org in the table
        org_link = page.locator('td a').first
        if await org_link.count() > 0:
            await org_link.click()
            await page.wait_for_load_state('networkidle')
            logger.info("Clicked through org selector")

            # Re-navigate to intended destination
            logger.info(f"Re-navigating to intended page...")
            await page.goto(intended_url, wait_until='networkidle')
        else:
            raise Exception("On org selector page but couldn't find org link to click")

    async def expand_all_details(self, page: Page):
        """
        Click all 'Read more' buttons to expand truncated details.

        Args:
            page: The page object
        """
        # Find all "Read more" buttons
        read_more_buttons = page.locator('button.readmoreButton')
        count = await read_more_buttons.count()

        if count > 0:
            logger.info(f"Expanding {count} truncated entries...")
            # Click all buttons
            for i in range(count):
                try:
                    await read_more_buttons.nth(i).click()
                    await page.wait_for_timeout(100)  # Small delay between clicks
                except Exception as e:
                    logger.warning(f"Could not click 'Read more' button {i}: {e}")

    async def scrape_table_page(self, page: Page) -> List[HistoryEntry]:
        """
        Scrape history entries from the current page of the table.

        Args:
            page: The page object

        Returns:
            List of HistoryEntry objects
        """
        entries = []

        # First, expand all truncated details
        await self.expand_all_details(page)

        # Wait a bit for expansions to complete
        await page.wait_for_timeout(500)

        # Get all data rows (excluding header row)
        rows = page.locator('table#_grdDevEx_DXMainTable tbody tr.dxgvDataRow_Bootstrap')
        row_count = await rows.count()

        logger.info(f"Found {row_count} history entries on this page")

        for i in range(row_count):
            row = rows.nth(i)
            cells = row.locator('td.dxgv')

            # Extract text from each cell
            changes_title = await cells.nth(0).text_content()
            date = await cells.nth(1).text_content()
            user = await cells.nth(2).text_content()

            # For details cell, get the full text (including expanded content)
            details_cell = cells.nth(3)

            # After clicking "Read more", the full text is in the paragraph
            # Get text content which will include both visible parts
            para = details_cell.locator('p.summary')
            if await para.count() > 0:
                # Get the full text - after expansion, dots are hidden and more is visible
                details = await para.text_content()
                details = details.strip() if details else ""
            else:
                # Fallback: just get cell text
                details = await details_cell.text_content()
                details = details.strip() if details else ""

            entry = HistoryEntry(
                changes_title=changes_title.strip() if changes_title else "",
                date=date.strip() if date else "",
                user=user.strip() if user else "",
                details=details
            )
            entries.append(entry)

        return entries

    async def get_total_pages(self, page: Page) -> int:
        """
        Get the total number of pages from the pager.

        Args:
            page: The page object

        Returns:
            Total number of pages
        """
        # Look for pager summary text like "Page 1 of 3 (55 items)"
        pager_summary = page.locator('b.dxp-summary')

        if await pager_summary.count() == 0:
            return 1  # No pager means single page

        summary_text = await pager_summary.text_content()
        # Parse "Page 1 of 3 (55 items)" to get total pages
        if " of " in summary_text:
            parts = summary_text.split(" of ")
            if len(parts) >= 2:
                # Extract the number after "of" and before any parenthesis
                total_pages_str = parts[1].split()[0]
                try:
                    return int(total_pages_str)
                except ValueError:
                    logger.warning(f"Could not parse total pages from: {summary_text}")
                    return 1

        return 1

    async def click_next_page(self, page: Page) -> bool:
        """
        Click the "Next" button to go to the next page.

        Args:
            page: The page object

        Returns:
            True if successfully navigated to next page, False if no next page available
        """
        # Find the "Next" button (it's an <a> tag with specific onclick)
        next_button = page.locator('a.dxp-button.dxp-bi[aria-label="Next"]')

        if await next_button.count() == 0:
            return False

        # Check if it's disabled (would be a <b> tag instead of <a>)
        disabled_next = page.locator('b.dxp-button.dxp-bi.dxp-disabledButton[aria-label="Next"]')
        if await disabled_next.count() > 0:
            logger.info("Next button is disabled (last page)")
            return False

        # Click the next button
        await next_button.click()

        # Wait for the table to update
        await page.wait_for_load_state('networkidle')
        await page.wait_for_timeout(1000)  # Additional wait for table refresh

        return True

    async def scrape_quote_history(
        self,
        order_id: str,
        progress_callback=None
    ) -> QuoteHistoryResult:
        """
        Scrape complete history for a quote.

        Args:
            order_id: The order/quote ID (GUID)
            progress_callback: Optional callback(pct, message) for progress updates

        Returns:
            QuoteHistoryResult with all history entries
        """
        def update_progress(pct: int, msg: str):
            if progress_callback:
                progress_callback(pct, msg)
            logger.info(f"[{pct}%] {msg}")

        errors = []
        all_entries = []

        update_progress(0, f"Navigating to quote {order_id}")

        page = await self.context.new_page()
        try:
            # Navigate to quote summary page
            quote_url = f"https://go.buzmanager.com/Sales/Summary?orderId={order_id}"
            await page.goto(quote_url, wait_until='networkidle')

            # Handle org selector if present
            await self.handle_org_selector_if_present(page, quote_url)

            update_progress(20, "Enabling history options")

            # Check the two checkboxes
            include_job_tracking = page.locator('input#includeJobTracking')
            include_dispatch = page.locator('input#includeDispatch')

            # Check both checkboxes if they're not already checked
            if not await include_job_tracking.is_checked():
                await include_job_tracking.check()
                logger.info("Checked 'includeJobTracking'")

            if not await include_dispatch.is_checked():
                await include_dispatch.check()
                logger.info("Checked 'includeDispatch'")

            update_progress(40, "Loading history")

            # Click the "Show History" button
            show_history_btn = page.locator('a#btnHistory')
            await show_history_btn.click()

            # Wait for the table to load
            await page.wait_for_load_state('networkidle')
            await page.wait_for_timeout(1500)  # Additional wait for table to populate

            # Check if table exists
            table = page.locator('table#_grdDevEx_DXMainTable')
            if await table.count() == 0:
                raise Exception("History table did not load")

            update_progress(50, "Scraping history entries")

            # Get total pages
            total_pages = await self.get_total_pages(page)
            logger.info(f"Total pages: {total_pages}")

            # Scrape first page
            entries = await self.scrape_table_page(page)
            all_entries.extend(entries)

            # Navigate through remaining pages
            current_page = 1
            while current_page < total_pages:
                progress_pct = 50 + int((current_page / total_pages) * 40)
                update_progress(progress_pct, f"Scraping page {current_page + 1} of {total_pages}")

                # Click next page
                has_next = await self.click_next_page(page)
                if not has_next:
                    logger.info("No more pages available")
                    break

                current_page += 1

                # Scrape this page
                entries = await self.scrape_table_page(page)
                all_entries.extend(entries)

            update_progress(100, f"Complete! Scraped {len(all_entries)} history entries")

            return QuoteHistoryResult(
                order_id=order_id,
                total_entries=len(all_entries),
                entries=all_entries,
                errors=errors
            )

        except Exception as e:
            logger.error(f"Error scraping quote history: {e}", exc_info=True)
            errors.append(str(e))
            return QuoteHistoryResult(
                order_id=order_id,
                total_entries=len(all_entries),
                entries=all_entries,
                errors=errors
            )
        finally:
            await page.close()


async def scrape_quote_history(
    order_id: str,
    storage_state_path: Optional[Path] = None,
    headless: bool = True,
    progress_callback=None
) -> QuoteHistoryResult:
    """
    High-level function to scrape quote history.

    Args:
        order_id: The order/quote ID (GUID)
        storage_state_path: Path to auth storage state (defaults to Watson Blinds)
        headless: Run browser in headless mode
        progress_callback: Optional callback(pct, message) for progress updates

    Returns:
        QuoteHistoryResult
    """
    if storage_state_path is None:
        # Default to Watson Blinds auth
        storage_state_path = Path(".secrets/buz_storage_state_watsonblinds.json")

    async with BuzQuoteScraper(storage_state_path=storage_state_path, headless=headless) as scraper:
        return await scraper.scrape_quote_history(order_id, progress_callback)
