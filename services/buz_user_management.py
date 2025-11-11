# services/buz_user_management.py
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

logger = logging.getLogger(__name__)


@dataclass
class User:
    """Buz user data"""
    full_name: str
    email: str
    mfa_enabled: bool
    group: str
    last_session: str
    is_active: bool
    user_type: str  # "employee" or "customer"

    def to_dict(self) -> Dict[str, Any]:
        return {
            'full_name': self.full_name,
            'email': self.email,
            'mfa_enabled': self.mfa_enabled,
            'group': self.group,
            'last_session': self.last_session,
            'is_active': self.is_active,
            'user_type': self.user_type
        }


@dataclass
class OrgUsers:
    """Users for one org"""
    org_name: str
    users: List[User]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'org_name': self.org_name,
            'users': [user.to_dict() for user in self.users]
        }


class UserManagementResult:
    """Result of user management review"""

    def __init__(self):
        self.orgs: List[OrgUsers] = []
        self.steps: list[str] = []

    def add_step(self, message: str):
        """Add a step to the result log"""
        self.steps.append(message)
        logger.info(message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary"""
        return {
            'orgs': [org.to_dict() for org in self.orgs],
            'steps': self.steps
        }


class BuzUserManagement:
    """Scrape user data from Buz orgs"""

    USER_MANAGEMENT_URL = "https://console1.buzmanager.com/myorg/user-management/users"

    # Org configurations
    ORGS = {
        'canberra': {
            'display_name': 'Canberra',
            'storage_state': '.secrets/buz_storage_state_canberra.json'
        },
        'tweed': {
            'display_name': 'Tweed',
            'storage_state': '.secrets/buz_storage_state_tweed.json'
        },
        'bay': {
            'display_name': 'Batemans Bay',
            'storage_state': '.secrets/buz_storage_state_bay.json'
        },
        'shoalhaven': {
            'display_name': 'Shoalhaven',
            'storage_state': '.secrets/buz_storage_state_shoalhaven.json'
        },
        'wagga': {
            'display_name': 'Wagga Wagga',
            'storage_state': '.secrets/buz_storage_state_wagga.json'
        }
    }

    def __init__(self, headless: bool = True):
        """
        Initialize user management scraper.

        Args:
            headless: Run browser in headless mode
        """
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.playwright = None
        self.result = UserManagementResult()

    async def __aenter__(self):
        """Context manager entry - launch browser"""
        self.playwright = await async_playwright().__aenter__()
        self.browser = await self.playwright.chromium.launch(headless=self.headless)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close browser"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()

    async def switch_to_org(self, org_key: str):
        """
        Switch to a different Buz org by creating a new browser context.

        Args:
            org_key: Key from ORGS dict (e.g., 'canberra', 'tweed')
        """
        org_config = self.ORGS[org_key]
        storage_state_path = Path(org_config['storage_state'])

        if not storage_state_path.exists():
            raise FileNotFoundError(
                f"Auth storage state not found at {storage_state_path}. "
                f"Run tools/buz_auth_bootstrap.py {org_key} first."
            )

        self.result.add_step(f"Switching to: {org_config['display_name']}")

        # Close current context if exists
        if self.context:
            await self.context.close()

        # Create new context with org's authentication
        self.context = await self.browser.new_context(
            storage_state=str(storage_state_path)
        )

        self.result.add_step(f"✓ Switched to {org_config['display_name']}")

    async def handle_org_selector_if_present(self, page: Page, intended_url: str):
        """
        Check if we're on the org selector page and automatically click through.

        Args:
            page: The page object
            intended_url: The URL we were trying to reach
        """
        if "mybuz/organizations" not in page.url:
            return

        self.result.add_step("⚠️  Landed on org selector, clicking through...")

        org_link = page.locator('td a').first
        if await org_link.count() > 0:
            await org_link.click()
            await page.wait_for_load_state('networkidle')
            self.result.add_step("✓ Clicked through org selector")

            # Re-navigate to intended destination
            self.result.add_step(f"Re-navigating to intended page...")
            await page.goto(intended_url, wait_until='networkidle')
        else:
            raise Exception("On org selector page but couldn't find org link to click")

    async def scrape_users_from_page(self, page: Page, is_active: bool, user_type: str) -> List[User]:
        """
        Scrape users from the current page.

        Args:
            page: The page object
            is_active: Whether these are active or inactive users
            user_type: "employee" or "customer"

        Returns:
            List of User objects
        """
        users = []

        # Wait for the table to be present
        await page.wait_for_selector('table#userListTable tbody', timeout=10000)

        # Get all rows
        rows = await page.locator('table#userListTable tbody tr').all()

        for row in rows:
            try:
                # Full Name (in an <a> tag within first td)
                full_name_elem = row.locator('td:nth-child(1) a')
                full_name = await full_name_elem.text_content() if await full_name_elem.count() > 0 else ""
                full_name = full_name.strip()

                # Email (second td)
                email_elem = row.locator('td:nth-child(2)')
                email = await email_elem.text_content() if await email_elem.count() > 0 else ""
                email = email.strip()

                # MFA (third td - check for checkmark icon)
                mfa_elem = row.locator('td:nth-child(3) i.fa-check')
                mfa_enabled = await mfa_elem.count() > 0

                # Group (fourth td - text inside badge span)
                group_elem = row.locator('td:nth-child(4) span.badge')
                group = await group_elem.text_content() if await group_elem.count() > 0 else ""
                group = group.strip()

                # Last Session (fifth td)
                last_session_elem = row.locator('td:nth-child(5)')
                last_session = await last_session_elem.text_content() if await last_session_elem.count() > 0 else ""
                last_session = last_session.strip()

                # Skip empty rows
                if not email:
                    continue

                users.append(User(
                    full_name=full_name,
                    email=email,
                    mfa_enabled=mfa_enabled,
                    group=group,
                    last_session=last_session,
                    is_active=is_active,
                    user_type=user_type
                ))

            except Exception as e:
                logger.warning(f"Error parsing user row: {e}")
                continue

        return users

    async def scrape_org_users(self, org_key: str) -> List[User]:
        """
        Scrape all users for an org (active/inactive × employees/customers).

        Args:
            org_key: Key from ORGS dict

        Returns:
            List of User objects
        """
        org_config = self.ORGS[org_key]
        org_name = org_config['display_name']

        self.result.add_step(f"Scraping users for {org_name}")

        page = await self.context.new_page()
        all_users = []

        try:
            # First navigate to home screen to establish org context
            # This prevents being pushed to the org switcher
            self.result.add_step(f"Navigating to home screen...")
            await page.goto("https://go.buzmanager.com", wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(1000)  # Give it a moment to settle

            # Now navigate to user management page
            self.result.add_step(f"Navigating to user management page...")
            await page.goto(self.USER_MANAGEMENT_URL, wait_until='domcontentloaded', timeout=30000)

            # Wait for the page to load
            await page.wait_for_selector('table#userListTable', timeout=10000)

            # Set page size to 500 (maximum)
            self.result.add_step(f"Setting page size to 500...")
            await page.select_option('select.ng-pristine.ng-valid.ng-touched', value='6: 500')
            await page.wait_for_timeout(1000)  # Wait for page to update

            # Combinations to scrape: active/inactive × employee/customer
            combinations = [
                (True, "employee", "0: true", "0: 0"),    # Active employees
                (False, "employee", "1: false", "0: 0"),  # Inactive employees
                (True, "customer", "0: true", "1: 5"),    # Active customers
                (False, "customer", "1: false", "1: 5"),  # Inactive customers
            ]

            for is_active, user_type, active_value, type_value in combinations:
                status_text = "Active" if is_active else "Inactive"
                type_text = "Employees" if user_type == "employee" else "Customers"
                self.result.add_step(f"Fetching {status_text} {type_text}...")

                # Select active/inactive (second select in the list-inline ul)
                active_select = page.locator('ul.list-inline li:nth-child(2) select')
                await active_select.select_option(value=active_value)
                await page.wait_for_timeout(500)

                # Select employee/customer (third select in the list-inline ul)
                type_select = page.locator('ul.list-inline li:nth-child(3) select')
                await type_select.select_option(value=type_value)
                await page.wait_for_timeout(1000)  # Wait for table to update

                # Scrape users from this combination
                users = await self.scrape_users_from_page(page, is_active, user_type)
                all_users.extend(users)
                self.result.add_step(f"  Found {len(users)} {status_text.lower()} {type_text.lower()}")

            self.result.add_step(f"✓ Total users scraped: {len(all_users)}")
            return all_users

        finally:
            await page.close()

    async def review_users(self, selected_orgs: Optional[List[str]] = None) -> UserManagementResult:
        """
        Scrape users from selected orgs.

        Args:
            selected_orgs: List of org keys to process (e.g., ['canberra', 'tweed']).
                          If None, processes all orgs.

        Returns:
            UserManagementResult with data from selected orgs
        """
        self.result.add_step("=== Starting User Management Review ===")

        # Filter orgs if selection provided
        orgs_to_process = self.ORGS.items()
        if selected_orgs:
            orgs_to_process = [(k, v) for k, v in self.ORGS.items() if k in selected_orgs]

        # Process each org
        for idx, (org_key, org_config) in enumerate(orgs_to_process):
            try:
                # Switch to org
                await self.switch_to_org(org_key)

                # Scrape users
                users = await self.scrape_org_users(org_key)

                # Store in result
                org_users = OrgUsers(
                    org_name=org_config['display_name'],
                    users=users
                )
                self.result.orgs.append(org_users)

            except Exception as e:
                self.result.add_step(f"❌ Error processing {org_config['display_name']}: {str(e)}")
                logger.exception(f"Error processing {org_key}")
                # Continue with other orgs even if one fails

        self.result.add_step("=== Review Complete ===")
        return self.result


async def review_users_all_orgs(
    headless: bool = True,
    selected_orgs: Optional[List[str]] = None,
    job_update_callback=None
) -> UserManagementResult:
    """
    High-level function to review users across selected orgs.

    Args:
        headless: Run browser in headless mode
        selected_orgs: List of org keys to process (e.g., ['canberra', 'tweed']).
                      If None, processes all orgs.
        job_update_callback: Optional callback(pct, message) for job progress

    Returns:
        UserManagementResult
    """
    def update(pct: int, msg: str):
        if job_update_callback:
            job_update_callback(pct, msg)
        logger.info(f"[{pct}%] {msg}")

    update(0, "Starting user management review")

    async with BuzUserManagement(headless=headless) as review:
        # Wrap add_step to provide progress updates
        original_add_step = review.result.add_step

        def wrapped_add_step(message: str):
            original_add_step(message)
            # Estimate progress
            step_count = len(review.result.steps)
            pct = min(5 + (step_count * 3), 95)
            update(pct, message)

        review.result.add_step = wrapped_add_step

        # Run the review
        result = await review.review_users(selected_orgs=selected_orgs)

    update(100, "Complete")
    return result
