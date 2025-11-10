# services/buz_customer_automation.py
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

from services.zendesk_service import CustomerData

logger = logging.getLogger(__name__)


class CustomerAutomationResult:
    """Result of customer automation workflow"""

    def __init__(self):
        self.user_existed = False
        self.user_reactivated = False
        self.customer_existed = False
        self.customer_created = False
        self.user_created = False
        self.customer_name: Optional[str] = None
        self.user_email: Optional[str] = None
        self.steps: list[str] = []

    def add_step(self, message: str):
        """Add a step to the result log"""
        self.steps.append(message)
        logger.info(message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary"""
        return {
            'user_existed': self.user_existed,
            'user_reactivated': self.user_reactivated,
            'customer_existed': self.customer_existed,
            'customer_created': self.customer_created,
            'user_created': self.user_created,
            'customer_name': self.customer_name,
            'user_email': self.user_email,
            'steps': self.steps
        }


class BuzCustomerAutomation:
    """Automate customer and user creation in Buz Manager"""

    STORAGE_STATE_PATH = Path(".secrets/buz_storage_state.json")
    USER_MANAGEMENT_URL = "https://console1.buzmanager.com/myorg/user-management/users"
    CUSTOMERS_URL = "https://go.buzmanager.com/Contacts/Customers"

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.result = CustomerAutomationResult()

    async def __aenter__(self):
        """Context manager entry - launch browser"""
        if not self.STORAGE_STATE_PATH.exists():
            raise FileNotFoundError(
                f"Auth storage state not found at {self.STORAGE_STATE_PATH}. "
                f"Run tools/buz_auth_bootstrap.py first."
            )

        playwright = await async_playwright().__aenter__()
        self.browser = await playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            storage_state=str(self.STORAGE_STATE_PATH)
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close browser"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()

    async def check_user_exists(self, email: str) -> tuple[bool, bool, Optional[str]]:
        """
        Check if user already exists by email (checks both active and inactive)
        If found in inactive users, reactivates them.

        Returns:
            (exists: bool, was_reactivated: bool, customer_name: Optional[str])
        """
        self.result.add_step(f"Checking if user exists with email: {email}")

        page = await self.context.new_page()
        try:
            await page.goto(self.USER_MANAGEMENT_URL, wait_until='networkidle')

            # Select 'customers' from the dropdown (Angular select with special value binding)
            # There are 2 selects, we want the one with Employees/Customers (not Active/Deactivated)
            user_type_select = page.locator('select.form-control').filter(has_text='Employees')
            await user_type_select.select_option(label='Customers')
            self.result.add_step("Selected 'Customers' user type")

            # Get the active/deactivated dropdown
            status_select = page.locator('select.form-control').filter(has_text='Active users')

            # First check active users
            await status_select.select_option(label='Active users')
            self.result.add_step("Checking active users")

            # Type email into search field
            search_input = page.locator('input#search-text, input[placeholder*="Name, user name or email"]')
            await search_input.fill(email)
            await page.wait_for_timeout(1000)

            # Check if any results exist in the table
            results_table = page.locator('table tbody tr')
            count = await results_table.count()

            if count > 0:
                self.result.add_step(f"User already exists (active) with email: {email}")
                try:
                    first_row = results_table.first
                    # Customer name is in the first column inside an anchor tag
                    customer_name_link = first_row.locator('td:first-child a')
                    customer_name = await customer_name_link.text_content()
                    return True, False, customer_name.strip() if customer_name else None
                except:
                    return True, False, None

            # Not found in active users, check deactivated users
            self.result.add_step("Not found in active users, checking deactivated users")
            await status_select.select_option(label='Deactivated users')
            await page.wait_for_timeout(1000)

            count = await results_table.count()
            if count > 0:
                self.result.add_step(f"User found in deactivated users: {email}")

                # Get customer name before reactivating
                customer_name = None
                try:
                    first_row = results_table.first
                    # Customer name is in the first column inside an anchor tag
                    customer_name_link = first_row.locator('td:first-child a')
                    customer_name = await customer_name_link.text_content()
                    customer_name = customer_name.strip() if customer_name else None
                except:
                    pass

                # Reactivate the user by clicking the toggle
                # The checkbox has id equal to the email
                toggle_label = page.locator(f'label[for="{email}"]')
                await toggle_label.click()
                await page.wait_for_timeout(500)
                self.result.add_step(f"Reactivated user: {email}")

                return True, True, customer_name

            self.result.add_step("User does not exist")
            return False, False, None

        finally:
            await page.close()

    async def search_customer(self, page: Page, company_name: str, email: str) -> Optional[str]:
        """
        Search for customer by company name and email

        Returns:
            Customer name if found, None otherwise
        """
        self.result.add_step(f"Searching for customer: {company_name}")

        # Click advanced search
        await page.click('a:has-text("Advanced Search")')
        await page.wait_for_timeout(500)

        # Enter company name
        company_input = page.locator('input[name="CompanyName"], input#CompanyName')
        await company_input.fill(company_name)
        self.result.add_step(f"Entered company name: {company_name}")

        # Click Display button (with search icon)
        await page.click('button:has-text("Display"), input[value="Display"]')
        await page.wait_for_load_state('networkidle')

        # Check for results
        results = page.locator('table tbody tr')
        count = await results.count()

        if count > 0:
            self.result.add_step(f"Found {count} customer(s) by company name")

            # If multiple results, try to match by email
            if count > 1:
                for i in range(count):
                    row = results.nth(i)
                    row_text = await row.text_content()
                    if email.lower() in row_text.lower():
                        # Get customer name from 2nd column
                        customer_name = await row.locator('td').nth(1).text_content()
                        self.result.add_step(f"Matched customer by email: {customer_name.strip()}")
                        return customer_name.strip()

            # Single result or no email match - use first result
            first_row = results.first
            customer_name = await first_row.locator('td').nth(1).text_content()
            self.result.add_step(f"Using customer: {customer_name.strip()}")
            return customer_name.strip()

        # No results by company name - try email search
        self.result.add_step("No results by company name, trying email search")

        # Clear and search by email
        await company_input.clear()
        email_input = page.locator('input[name="Email"], input#Email')
        await email_input.fill(email)
        await page.click('button:has-text("Display"), input[value="Display"]')
        await page.wait_for_load_state('networkidle')

        count = await results.count()
        if count > 0:
            first_row = results.first
            customer_name = await first_row.locator('td').nth(1).text_content()
            self.result.add_step(f"Found customer by email: {customer_name.strip()}")
            return customer_name.strip()

        self.result.add_step("Customer not found")
        return None

    async def create_customer(self, page: Page, customer_data: CustomerData) -> str:
        """
        Create a new customer

        Returns:
            Customer name (company name)
        """
        self.result.add_step(f"Creating customer: {customer_data.company_name}")

        # Click Add Customer
        await page.click('a:has-text("Add Customer"), button:has-text("Add Customer")')
        await page.wait_for_load_state('networkidle')

        # Select Wholesale customer group
        group_select = page.locator('select#CustomerGroupId, select[name="CustomerGroupId"]')
        await group_select.select_option(label='Wholesale')
        self.result.add_step("Selected 'Wholesale' customer group")

        # Fill in company name
        await page.fill('input#CompanyName, input[name="CompanyName"]', customer_data.company_name)

        # Fill in first and last name
        await page.fill('input#FirstName, input[name="FirstName"]', customer_data.first_name)
        await page.fill('input#LastName, input[name="LastName"]', customer_data.last_name)
        self.result.add_step(f"Filled in name: {customer_data.first_name} {customer_data.last_name}")

        # Fill in phone number (mobile or landline)
        if customer_data.phone:
            if customer_data.is_mobile:
                await page.fill('input#MobilePhone, input[name="MobilePhone"]', customer_data.phone)
                self.result.add_step(f"Added mobile phone: {customer_data.phone}")
            else:
                await page.fill('input#Phone, input[name="Phone"]', customer_data.phone)
                self.result.add_step(f"Added phone: {customer_data.phone}")

        # Handle async address autocomplete
        self.result.add_step(f"Entering address: {customer_data.address}")
        address_input = page.locator('input#Address, input[name="Address"]')
        await address_input.fill(customer_data.address)

        # Wait for autocomplete dropdown to appear
        await page.wait_for_timeout(1500)

        # Look for autocomplete dropdown and select first result
        # Common autocomplete patterns: ul.ui-autocomplete, .autocomplete-suggestions, etc.
        dropdown = page.locator('ul.ui-autocomplete li:first-child, .autocomplete-suggestions div:first-child, [role="option"]:first-child')
        if await dropdown.count() > 0:
            await dropdown.first.click()
            self.result.add_step("Selected address from autocomplete")
        else:
            self.result.add_step("No autocomplete dropdown appeared, using typed address")

        # Click Save
        await page.click('button:has-text("Save"), input[value="Save"]')
        await page.wait_for_load_state('networkidle')

        self.result.add_step(f"Customer created: {customer_data.company_name}")
        return customer_data.company_name

    async def create_user(self, customer_name: str, customer_data: CustomerData) -> bool:
        """
        Create a new user linked to the customer

        Returns:
            True if successful
        """
        self.result.add_step(f"Creating user for: {customer_data.email}")

        page = await self.context.new_page()
        try:
            await page.goto(self.USER_MANAGEMENT_URL, wait_until='networkidle')

            # Select 'customers' from dropdown (Angular select with special value binding)
            # There are 2 selects, we want the one with Employees/Customers (not Active/Deactivated)
            select_element = page.locator('select.form-control').filter(has_text='Employees')
            await select_element.select_option(label='Customers')

            # Click Invite User
            await page.click('button:has-text("Invite User")')
            await page.wait_for_load_state('networkidle')

            # Fill in user details
            await page.fill('input#FirstName, input[name="FirstName"]', customer_data.first_name)
            await page.fill('input#LastName, input[name="LastName"]', customer_data.last_name)
            await page.fill('input#Email, input[name="Email"]', customer_data.email)
            self.result.add_step(f"Filled in user: {customer_data.first_name} {customer_data.last_name} ({customer_data.email})")

            # Select Customers group
            group_select = page.locator('select#GroupId, select[name="GroupId"]')
            await group_select.select_option(label='Customers')
            self.result.add_step("Selected 'Customers' group")

            # Handle finicky customer name autocomplete
            self.result.add_step(f"Entering customer name with slow typing: {customer_name}")
            customer_input = page.locator('input[placeholder*="start typing customer name"]')

            # Type slowly, character by character
            # First, type first word quickly
            words = customer_name.split()
            first_word = words[0] if words else customer_name

            for char in first_word:
                await customer_input.type(char, delay=100)

            # If there are more words, add a space slowly
            if len(words) > 1:
                await page.wait_for_timeout(300)
                await customer_input.type(' ', delay=200)
                await page.wait_for_timeout(500)

                # Type a bit of the second word
                for char in words[1][:2]:
                    await customer_input.type(char, delay=150)
                    await page.wait_for_timeout(200)

            # Wait for dropdown to appear
            await page.wait_for_timeout(1000)

            # Look for dropdown with customer name and address
            # The dropdown should show customer name and address
            dropdown_item = page.locator(f'[role="option"]:has-text("{customer_name}"), li:has-text("{customer_name}")')

            if await dropdown_item.count() > 0:
                await dropdown_item.first.click()
                self.result.add_step(f"Selected customer from autocomplete: {customer_name}")
            else:
                self.result.add_step("Warning: Could not find customer in autocomplete dropdown")

            # Click Save User
            await page.click('button:has-text("Save User")')
            await page.wait_for_load_state('networkidle')

            self.result.add_step(f"User created successfully: {customer_data.email}")
            return True

        finally:
            await page.close()

    async def add_customer_from_ticket(self, customer_data: CustomerData) -> CustomerAutomationResult:
        """
        Complete workflow to add customer and user from Zendesk ticket data

        Steps:
        1. Check if user exists (optimization)
        2. Search for customer
        3. Create customer if needed
        4. Create user

        Returns:
            CustomerAutomationResult with details
        """
        self.result.add_step("=== Starting Customer Addition Workflow ===")
        self.result.user_email = customer_data.email

        # Step 1: Check if user exists first (optimization)
        user_exists, was_reactivated, existing_customer = await self.check_user_exists(customer_data.email)

        if user_exists:
            self.result.user_existed = True
            self.result.user_reactivated = was_reactivated
            self.result.customer_name = existing_customer or customer_data.company_name
            if was_reactivated:
                self.result.add_step(f"✓ User existed (inactive) and was reactivated. Done.")
            else:
                self.result.add_step(f"✓ User already exists (active). Nothing to do.")
            return self.result

        # Step 2 & 3: Search for customer, create if needed
        page = await self.context.new_page()
        try:
            await page.goto(self.CUSTOMERS_URL, wait_until='networkidle')

            customer_name = await self.search_customer(page, customer_data.company_name, customer_data.email)

            if customer_name:
                self.result.customer_existed = True
                self.result.customer_name = customer_name
                self.result.add_step(f"✓ Customer exists: {customer_name}")
            else:
                # Create customer
                customer_name = await self.create_customer(page, customer_data)
                self.result.customer_created = True
                self.result.customer_name = customer_name
                self.result.add_step(f"✓ Customer created: {customer_name}")

        finally:
            await page.close()

        # Step 4: Create user
        success = await self.create_user(customer_name, customer_data)
        if success:
            self.result.user_created = True
            self.result.add_step(f"✓ User created: {customer_data.email}")

        self.result.add_step("=== Workflow Complete ===")
        return self.result


async def add_customer_from_zendesk_ticket(
    ticket_id: int,
    headless: bool = True,
    job_update_callback=None
) -> CustomerAutomationResult:
    """
    High-level function to add customer from Zendesk ticket

    Args:
        ticket_id: Zendesk ticket ID
        headless: Run browser in headless mode
        job_update_callback: Optional callback(pct, message) for job progress

    Returns:
        CustomerAutomationResult
    """
    def update(pct: int, msg: str):
        if job_update_callback:
            job_update_callback(pct, msg)
        logger.info(f"[{pct}%] {msg}")

    update(0, f"Fetching Zendesk ticket #{ticket_id}")

    # Import here to avoid circular imports
    from services.zendesk_service import ZendeskService

    zd_service = ZendeskService()
    customer_data = zd_service.get_customer_data(ticket_id)

    update(20, f"Ticket parsed. Customer: {customer_data.company_name}")

    async with BuzCustomerAutomation(headless=headless) as automation:
        # Wrap the automation to provide progress updates
        original_add_step = automation.result.add_step

        def wrapped_add_step(message: str):
            original_add_step(message)
            # Estimate progress based on steps
            step_count = len(automation.result.steps)
            pct = min(20 + (step_count * 5), 95)
            update(pct, message)

        automation.result.add_step = wrapped_add_step

        result = await automation.add_customer_from_ticket(customer_data)

    update(100, "Complete")
    return result
