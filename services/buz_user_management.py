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

    USER_MANAGEMENT_URL = "https://go.buzmanager.com/Settings/Users"  # Redirects to console URL

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
        'dd': {
            'display_name': 'Designer Drapes',
            'storage_state': '.secrets/buz_storage_state_dd.json'
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
            # Navigate directly to user management page
            # Since we now have console domain auth in storage state, this should work
            self.result.add_step(f"Navigating to user management page...")
            await page.goto(self.USER_MANAGEMENT_URL, wait_until='networkidle', timeout=30000)
            self.result.add_step(f"User page loaded at: {page.url}")

            # Check if we ended up on the org selector page
            if "mybuz/organizations" in page.url:
                self.result.add_step(f"⚠️  On org selector page, clicking through...")
                org_link = page.locator('td a').first
                if await org_link.count() > 0:
                    await org_link.click()
                    await page.wait_for_load_state('networkidle', timeout=30000)
                    self.result.add_step(f"After org selector: {page.url}")
                else:
                    raise Exception("On org selector but couldn't find org link")

            # Wait for the page to load
            await page.wait_for_selector('table#userListTable', timeout=15000)
            self.result.add_step(f"✓ User table found")

            # Set page size to 500 (maximum)
            # The selector is in a div.select-editable within col-sm-1
            self.result.add_step(f"Setting page size to 500...")
            page_size_select = page.locator('div.select-editable select')
            await page_size_select.select_option(value='6: 500')
            await page.wait_for_timeout(1000)  # Wait for page to update
            self.result.add_step(f"✓ Page size set to 500")

            # Combinations to scrape: active/inactive × employee/customer
            # Note: Only Canberra and Designer Drapes have customers, other orgs only have employees
            if org_key in ['canberra', 'dd']:
                combinations = [
                    (True, "employee", "0: true", "0: 0"),    # Active employees
                    (False, "employee", "1: false", "0: 0"),  # Inactive employees
                    (True, "customer", "0: true", "1: 5"),    # Active customers
                    (False, "customer", "1: false", "1: 5"),  # Inactive customers
                ]
            else:
                # Other orgs only have employees
                combinations = [
                    (True, "employee", "0: true", "0: 0"),    # Active employees
                    (False, "employee", "1: false", "0: 0"),  # Inactive employees
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


async def toggle_user_active_status(
    org_key: str,
    user_email: str,
    is_active: bool,
    user_type: str,
    headless: bool = True
) -> Dict[str, Any]:
    """
    Toggle a user's active/inactive status in Buz.

    Args:
        org_key: Key from ORGS dict (e.g., 'canberra', 'tweed')
        user_email: Email address of the user
        is_active: Current active status of the user
        user_type: 'employee' or 'customer'
        headless: Run browser in headless mode

    Returns:
        Dict with success status and new active state
    """
    result = {
        'success': False,
        'new_state': None,
        'message': ''
    }

    async with BuzUserManagement(headless=headless) as manager:
        try:
            # Switch to the org
            await manager.switch_to_org(org_key)

            # Navigate to user management page
            page = await manager.context.new_page()

            await page.goto(manager.USER_MANAGEMENT_URL, wait_until='networkidle', timeout=30000)

            # Wait for the table to load
            await page.wait_for_selector('table#userListTable', timeout=15000)

            # Set the active/inactive filter (second li in the list)
            # The active dropdown has values: "0: true" for active, "1: false" for inactive
            active_select = page.locator('ul.list-inline li:nth-child(2) select')
            active_value = "0: true" if is_active else "1: false"
            await active_select.select_option(value=active_value)
            await page.wait_for_timeout(300)

            # Set the employee/customer filter (third li in the list)
            # The user type dropdown has values: "0: 0" for employee, "1: 5" for customer
            user_type_select = page.locator('ul.list-inline li:nth-child(3) select')
            user_type_value = "1: 5" if user_type == "customer" else "0: 0"
            await user_type_select.select_option(value=user_type_value)
            await page.wait_for_timeout(300)

            # Use the search field to filter by email
            # Type character-by-character to trigger Angular change detection
            search_input = page.locator('input#search-text')
            await search_input.click()  # Focus the input
            await search_input.press_sequentially(user_email, delay=20)  # Type like a real user
            await page.wait_for_timeout(500)  # Wait for Angular to filter

            # Find the toggle switch for this user by email
            # The checkbox ID is the email address - use attribute selector to handle @ and . characters
            toggle_checkbox = page.locator(f'input.onoffswitch-checkbox[id="{user_email}"]')

            # Check if the checkbox exists
            if await toggle_checkbox.count() == 0:
                # User not found in expected state - cache might be stale
                # Try the opposite state to see if they're already in the desired final state
                logger.info(f"User {user_email} not found in {is_active} state, checking opposite state...")

                # Switch to opposite active/inactive filter
                opposite_active_value = "1: false" if is_active else "0: true"
                await active_select.select_option(value=opposite_active_value)
                await page.wait_for_timeout(300)

                # Clear and re-enter search to trigger filter
                await search_input.clear()
                await search_input.click()
                await search_input.press_sequentially(user_email, delay=20)
                await page.wait_for_timeout(500)

                # Check again
                if await toggle_checkbox.count() == 0:
                    result['message'] = f"User {user_email} not found in either active or inactive state (type={user_type})"
                    await page.close()
                    return result

                # Found in opposite state! Cache was stale, but they're already in desired final state
                result['success'] = True
                result['new_state'] = not is_active
                result['message'] = f"User {user_email} was already {'active' if result['new_state'] else 'inactive'} (cache was stale)"
                await page.close()
                return result

            # User found in expected state - verify and toggle
            actual_is_active = await toggle_checkbox.is_checked()
            if actual_is_active != is_active:
                result['message'] = f"User state mismatch: expected active={is_active}, got active={actual_is_active}"
                await page.close()
                return result

            # Click the label (the checkbox itself is hidden by CSS)
            # The label has a 'for' attribute matching the checkbox ID
            toggle_label = page.locator(f'label.onoffswitch-label[for="{user_email}"]')
            await toggle_label.click()

            # Wait for the toggle to process
            await page.wait_for_timeout(1000)

            # Verify by checking if user now appears in the OPPOSITE filter
            # This confirms the backend save worked, not just UI change
            opposite_active_value = "1: false" if is_active else "0: true"
            await active_select.select_option(value=opposite_active_value)
            await page.wait_for_timeout(300)

            # Clear and re-search
            await search_input.clear()
            await search_input.click()
            await search_input.press_sequentially(user_email, delay=20)
            await page.wait_for_timeout(500)

            # Check if user appears in the opposite state
            if await toggle_checkbox.count() > 0:
                # User found in opposite state - toggle succeeded!
                result['success'] = True
                result['new_state'] = not is_active
                result['message'] = f"User {user_email} is now {'active' if result['new_state'] else 'inactive'}"
            else:
                # User not found in opposite state - toggle failed
                result['message'] = f"Toggle failed - user did not move to opposite state"

        except Exception as e:
            result['message'] = f"Error toggling user status: {str(e)}"
            logger.exception(f"Error toggling user {user_email} in {org_key}")
        finally:
            # Always close the page, even on error
            try:
                await page.close()
            except Exception as close_error:
                logger.warning(f"Error closing page: {close_error}")

    return result


async def batch_toggle_users_for_org(
    org_key: str,
    user_changes: List[Dict[str, Any]],
    headless: bool = True
) -> List[Dict[str, Any]]:
    """
    Toggle multiple users' active/inactive status for a single org efficiently.
    Reuses the browser context and page for all toggles in the same org.

    Args:
        org_key: Key from ORGS dict (e.g., 'canberra', 'tweed')
        user_changes: List of dicts with {user_email, is_active, user_type}
        headless: Run browser in headless mode

    Returns:
        List of result dicts for each toggle
    """
    results = []

    async with BuzUserManagement(headless=headless) as manager:
        try:
            # Switch to the org once
            await manager.switch_to_org(org_key)

            # Navigate to user management page once
            page = await manager.context.new_page()
            await page.goto(manager.USER_MANAGEMENT_URL, wait_until='networkidle', timeout=30000)
            await page.wait_for_selector('table#userListTable', timeout=15000)

            # Get locators once
            active_select = page.locator('ul.list-inline li:nth-child(2) select')
            user_type_select = page.locator('ul.list-inline li:nth-child(3) select')
            search_input = page.locator('input#search-text')

            # Process each user toggle
            for change in user_changes:
                user_email = change['user_email']
                is_active = change['is_active']
                user_type = change['user_type']

                result = {
                    'org_key': org_key,
                    'user_email': user_email,
                    'success': False,
                    'new_state': None,
                    'message': ''
                }

                try:
                    logger.info(f"Toggling {user_email}: cache says is_active={is_active}, user_type={user_type}")

                    # Set filters for this user
                    active_value = "0: true" if is_active else "1: false"
                    await active_select.select_option(value=active_value)
                    await page.wait_for_timeout(500)

                    user_type_value = "1: 5" if user_type == "customer" else "0: 0"
                    await user_type_select.select_option(value=user_type_value)
                    await page.wait_for_timeout(500)

                    # Clear search and type email
                    await search_input.clear()
                    await page.wait_for_timeout(300)
                    await search_input.click()
                    await page.wait_for_timeout(100)
                    await search_input.press_sequentially(user_email, delay=100)
                    # Explicitly trigger input event for Angular
                    await search_input.dispatch_event('input')
                    # Wait for Angular to filter - wait for table to stabilize
                    await page.wait_for_timeout(1000)

                    # Find toggle
                    toggle_checkbox = page.locator(f'input.onoffswitch-checkbox[id="{user_email}"]')
                    checkbox_count = await toggle_checkbox.count()
                    logger.info(f"User {user_email}: found {checkbox_count} checkbox(es) in expected state (active={is_active})")

                    # Debug: log all emails currently visible in table
                    if checkbox_count == 0:
                        try:
                            all_emails = await page.locator('table#userListTable td:nth-child(2)').all_text_contents()
                            logger.info(f"Emails visible in table: {all_emails[:10]}")  # First 10
                        except Exception as e:
                            logger.warning(f"Could not get table emails: {e}")

                    if checkbox_count == 0:
                        # Try opposite state (stale cache)
                        logger.info(f"User {user_email} not found in expected state, checking opposite...")
                        opposite_active_value = "1: false" if is_active else "0: true"
                        await active_select.select_option(value=opposite_active_value)
                        await page.wait_for_timeout(500)

                        await search_input.clear()
                        await page.wait_for_timeout(300)
                        await search_input.click()
                        await page.wait_for_timeout(100)
                        await search_input.press_sequentially(user_email, delay=100)
                        # Explicitly trigger input event for Angular
                        await search_input.dispatch_event('input')
                        await page.wait_for_timeout(1000)

                        checkbox_count = await toggle_checkbox.count()
                        logger.info(f"User {user_email}: found {checkbox_count} checkbox(es) in opposite state (active={not is_active})")

                        if checkbox_count == 0:
                            result['message'] = f"User not found in either state (tried both active/inactive with user_type={user_type})"
                            logger.error(f"User {user_email} not found in either filter!")
                            results.append(result)
                            continue

                        # Found in opposite state - already done
                        result['success'] = True
                        result['new_state'] = not is_active
                        result['message'] = f"Already {'active' if result['new_state'] else 'inactive'} (cache was stale)"
                        logger.info(f"User {user_email} already in desired state, no toggle needed")
                        results.append(result)
                        continue

                    # Verify state and toggle
                    actual_is_active = await toggle_checkbox.is_checked()
                    if actual_is_active != is_active:
                        result['message'] = f"State mismatch"
                        results.append(result)
                        continue

                    # Click toggle
                    toggle_label = page.locator(f'label.onoffswitch-label[for="{user_email}"]')
                    await toggle_label.click()

                    # Wait for the toggle to process
                    await page.wait_for_timeout(1000)

                    # Verify by checking if user now appears in the OPPOSITE filter
                    # This confirms the backend save worked, not just UI change
                    opposite_active_value = "1: false" if is_active else "0: true"
                    await active_select.select_option(value=opposite_active_value)
                    await page.wait_for_timeout(200)

                    # Clear and re-search
                    await search_input.clear()
                    await search_input.click()
                    await search_input.press_sequentially(user_email, delay=20)
                    await page.wait_for_timeout(400)

                    # Check if user appears in the opposite state
                    if await toggle_checkbox.count() > 0:
                        # User found in opposite state - toggle succeeded!
                        result['success'] = True
                        result['new_state'] = not is_active
                        result['message'] = f"User is now {'active' if result['new_state'] else 'inactive'}"
                    else:
                        # User not found in opposite state - toggle failed
                        result['message'] = f"Toggle failed - user did not move to opposite state"

                    results.append(result)

                except Exception as e:
                    result['message'] = f"Error: {str(e)}"
                    logger.exception(f"Error toggling {user_email} in batch")
                    results.append(result)

            await page.close()

        except Exception as e:
            logger.exception(f"Error in batch toggle for org {org_key}")
            # Return errors for any remaining users
            for change in user_changes:
                if not any(r['user_email'] == change['user_email'] for r in results):
                    results.append({
                        'org_key': org_key,
                        'user_email': change['user_email'],
                        'success': False,
                        'new_state': None,
                        'message': f"Org-level error: {str(e)}"
                    })

    return results
