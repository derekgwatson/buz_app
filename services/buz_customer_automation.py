# services/buz_customer_automation.py
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from playwright.async_api import async_playwright, Page, Browser, BrowserContext

from services.zendesk_service import CustomerData

logger = logging.getLogger(__name__)


class CustomerAutomationError(Exception):
    """Exception raised during customer automation that includes the result object with steps"""

    def __init__(self, message: str, result: 'CustomerAutomationResult'):
        super().__init__(message)
        self.result = result


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
    INVITE_USER_URL = "https://console1.buzmanager.com/myorg/user-management/inviteuser/new"
    CUSTOMERS_URL = "https://go.buzmanager.com/Contacts/Customers"
    ORG_SELECTOR_URL = "https://console.buzmanager.com/mybuz/organizations"

    def __init__(self, headless: bool = True, keep_open: bool = False):
        self.headless = headless
        self.keep_open = keep_open
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
        if not self.keep_open:
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        else:
            # Keep browser open for debugging - don't close it
            # Browser subprocess will stay alive until manually closed or Flask app restarts
            self.result.add_step("Browser left open for debugging - manually close when done")
            # Don't sleep - let the function return so results can be shown immediately

    async def get_current_organization(self, page: Page) -> Optional[str]:
        """
        Check which organization we're currently in by reading the brand link.

        Returns:
            Organization name if in an org, None if on org selector page
        """
        try:
            # Check if we're on the org selector page
            my_buz_link = page.locator('a.navbar-brand.buz-brand:has-text("My BUZ")')
            if await my_buz_link.count() > 0:
                return None  # On org selector page

            # Check for org brand link (Watson Blinds, Designer Drapes, etc.)
            brand_link = page.locator('a.brand[style*="border-right"]')
            if await brand_link.count() > 0:
                org_name = await brand_link.text_content()
                return org_name.strip()

            # Fallback: check URL
            if "mybuz/organizations" in page.url:
                return None  # On org selector page

            return None  # Unknown state
        except:
            return None

    async def switch_organization(self, org_name: str):
        """
        Switch to a specific Buz organization

        Args:
            org_name: Name of the organization (e.g., "Watson Blinds", "Designer Drapes")
        """
        self.result.add_step(f"Switching to Buz instance: {org_name}")

        page = await self.context.new_page()
        try:
            await page.goto(self.ORG_SELECTOR_URL, wait_until='networkidle')

            # Click the organization link in the table (not the heading)
            # The table cell contains the org name, so target the link within a table cell
            org_link = page.locator(f'td a:has-text("{org_name}")')
            await org_link.click()
            await page.wait_for_load_state('networkidle')

            self.result.add_step(f"✓ Switched to: {org_name}")

        finally:
            await page.close()

    async def ensure_correct_organization(self, org_name: str):
        """
        Check if we're in the correct org, and switch only if needed

        Args:
            org_name: Name of the organization to ensure we're in
        """
        page = await self.context.new_page()
        try:
            # Navigate to a page to check which org we're in
            await page.goto("https://console1.buzmanager.com/myorg/user-management/users", wait_until='networkidle')

            # Check which org we're currently in
            current_org = await self.get_current_organization(page)

            if current_org is None:
                # On org selector page, need to select the org
                self.result.add_step(f"On org selector page, selecting {org_name}")
                await page.close()
                await self.switch_organization(org_name)
            elif current_org == org_name:
                # Already in the correct org
                self.result.add_step(f"✓ Already in {org_name} instance")
                await page.close()
            else:
                # In wrong org, need to switch
                self.result.add_step(f"Currently in '{current_org}', switching to {org_name}")
                await page.close()
                await self.switch_organization(org_name)

        except:
            await page.close()

    async def verify_not_on_org_selector(self, page: Page, intended_url: str, org_name: str, max_retries: int = 5):
        """
        After navigating, verify we didn't land on the org selector page.
        If we did, switch to correct org and navigate back - keep trying until it sticks.

        Args:
            page: The page we just navigated
            intended_url: Where we intended to go
            org_name: Organization we should be in
            max_retries: Maximum number of times to retry switching (default 5)
        """
        for attempt in range(max_retries):
            current_org = await self.get_current_organization(page)

            if current_org == org_name:
                # We're in the correct org - success!
                if attempt > 0:
                    self.result.add_step(f"✓ Successfully in {org_name} (after {attempt} attempts)")
                return  # All good, exit

            # Not in correct org - need to switch
            if current_org is None:
                self.result.add_step(f"⚠️ Attempt {attempt + 1}: On org selector, switching to {org_name}")
            else:
                self.result.add_step(f"⚠️ Attempt {attempt + 1}: In wrong org '{current_org}', switching to {org_name}")

            # Switch to correct org
            switch_page = await self.context.new_page()
            try:
                await switch_page.goto(self.ORG_SELECTOR_URL, wait_until='networkidle')
                org_link = switch_page.locator(f'td a:has-text("{org_name}")')
                await org_link.click()
                await switch_page.wait_for_load_state('networkidle')
            finally:
                await switch_page.close()

            # Navigate back to intended destination
            await page.goto(intended_url, wait_until='networkidle')

            # Wait a moment for page to settle
            await page.wait_for_timeout(500)

            # Loop will check again if we're in the right org

        # If we get here, we've exceeded max retries
        current_org = await self.get_current_organization(page)
        raise Exception(f"Failed to switch to {org_name} after {max_retries} attempts. Currently in: {current_org or 'org selector'}")

    async def check_user_exists(self, email: str) -> tuple[bool, bool, Optional[str], Optional[str]]:
        """
        Check if user already exists and what group they're in.
        Uses hybrid approach:
        1. Check existence and group via inviteuser/{email} URL (simple, reliable)
        2. If exists in Customers group, check if active/inactive and reactivate if needed

        Returns:
            (exists: bool, was_reactivated: bool, customer_name: Optional[str], error_group: Optional[str])
            - exists: True if user exists
            - was_reactivated: True if user was inactive and got reactivated
            - customer_name: Name of linked customer (if available)
            - error_group: Non-None if user exists in wrong group (name of that group)
        """
        self.result.add_step(f"Checking if user exists: {email}")

        page = await self.context.new_page()
        try:
            # STEP 1: Check existence and group using invite URL (simple and reliable)
            invite_url = f"https://console1.buzmanager.com/myorg/user-management/inviteuser/{email}"
            await page.goto(invite_url, wait_until='networkidle')

            # Check if the email field is filled (indicates user exists)
            email_input = page.locator('input#text-email')
            email_value = await email_input.input_value()

            if not email_value or not email_value.strip():
                # User doesn't exist - email field is empty
                self.result.add_step("User does not exist")
                return False, False, None, None

            # User exists - check their group
            self.result.add_step(f"✓ User exists: {email}")

            # Extract selected group from dropdown
            # Angular apps often don't set 'selected' attribute, so use JavaScript directly
            group_select = page.locator('select.form-control').first
            group_name = await group_select.evaluate('(select) => select.options[select.selectedIndex].text')
            group_name = group_name.strip() if group_name else "Unknown"
            self.result.add_step(f"User is in group: {group_name}")

            # Check if they're in a non-Customers group
            if "Customers" not in group_name:
                error_msg = f"⚠️ User '{email}' already exists in group '{group_name}'. Cannot create as Customer user."
                self.result.add_step(error_msg)
                self.result.add_step("This must be handled manually - user is not in Customers group.")
                # Return with error_group set to indicate this error condition
                return True, False, None, group_name

            # STEP 2: User is in Customers group - check if active or inactive
            self.result.add_step("User is a Customer - checking if active or inactive")

            await page.goto(self.USER_MANAGEMENT_URL, wait_until='networkidle')

            # Select Customers group
            user_type_select = page.locator('select.form-control').filter(has_text='Employees')
            await user_type_select.select_option(label='Customers')

            # Get the active/deactivated dropdown
            status_select = page.locator('select.form-control').filter(has_text='Active users')

            # Check active Customer users first
            await status_select.select_option(label='Active users')
            search_input = page.locator('input#search-text, input[placeholder*="Name, user name or email"]')
            await search_input.click()
            await search_input.fill('')
            await search_input.type(email, delay=50)
            await page.wait_for_timeout(1500)

            # NOTE: User table uses plain tr elements (no special classes)
            results_table = page.locator('table tbody tr')
            count = await results_table.count()

            if count > 0:
                # Found in active users
                self.result.add_step("✓ User is active")
                try:
                    first_row = results_table.first
                    customer_name_link = first_row.locator('td:first-child a')
                    customer_name = await customer_name_link.text_content()
                    return True, False, customer_name.strip() if customer_name else None, None
                except:
                    return True, False, None, None

            # Not found in active - check deactivated users
            self.result.add_step("Not found in active Customers, checking inactive")
            await status_select.select_option(label='Deactivated users')
            await page.wait_for_timeout(1500)

            results_table = page.locator('table tbody tr')
            count = await results_table.count()

            if count > 0:
                # Found in deactivated users - reactivate them
                self.result.add_step(f"Found as inactive Customer user - reactivating")

                # Get customer name before reactivating
                customer_name = None
                try:
                    first_row = results_table.first
                    customer_name_link = first_row.locator('td:first-child a')
                    customer_name = await customer_name_link.text_content()
                    customer_name = customer_name.strip() if customer_name else None
                except:
                    pass

                # Reactivate the user by clicking the toggle
                toggle_label = page.locator(f'label[for="{email}"]')
                await toggle_label.click()
                await page.wait_for_timeout(500)
                self.result.add_step(f"✓ Reactivated Customer user: {email}")

                return True, True, customer_name, None

            # User exists in Customers group but not found in list (shouldn't happen, but handle it)
            self.result.add_step("User exists as Customer but not found in user list (unusual)")
            return True, False, None, None

        finally:
            await page.close()

    async def get_customer_pkid(self, page: Page, customer_code: str) -> str:
        """
        Navigate to customer details page and extract PKID from New Sale button

        Args:
            page: Playwright page object
            customer_code: Customer code (e.g., "MYCO2000.1")

        Returns:
            Customer PKID (GUID)
        """
        # Navigate to customer details page using the code
        details_url = f"https://go.buzmanager.com/Contacts/Customers/Details?Code={customer_code}"
        await page.goto(details_url, wait_until='networkidle')

        # Find the "New Sale" button and extract PKID from its href
        new_sale_link = page.locator('a:has-text("New Sale")')
        href = await new_sale_link.get_attribute('href')

        # Extract PKID from URL parameter: /Sales/NewSale?customerPkId={pkid}
        if href and 'customerPkId=' in href:
            customer_pkid = href.split('customerPkId=')[-1]
            return customer_pkid
        else:
            raise Exception(f"Could not find customer PKID for code {customer_code}")

    async def search_customer(self, page: Page, company_name: str, email: str) -> Optional[tuple[str, str]]:
        """
        Search for customer by company name and email

        Returns:
            (customer_name, customer_pkid) if found, None otherwise
        """
        self.result.add_step(f"Searching for customer: {company_name}")

        # Click advanced search
        await page.click('a:has-text("Advanced Search")')
        await page.wait_for_timeout(500)

        # Enter company name
        company_input = page.locator('input[name="CompanyName"], input#CompanyName')
        await company_input.fill(company_name)
        self.result.add_step(f"Entered company name: {company_name}")

        # Click Display button (with search icon) - target by ID to avoid invisible duplicate
        await page.click('button#AdvancedDisplay')
        await page.wait_for_load_state('networkidle')
        await page.wait_for_timeout(2000)  # Let table update

        # Check if empty data row is present (indicates no results)
        empty_row = page.locator('tr.dxgvEmptyDataRow_Bootstrap, tr#_grdDevEx_DXEmptyRow')
        has_empty_row = await empty_row.count() > 0

        if not has_empty_row:
            # Get actual data rows (rows with class dxgvDataRow_Bootstrap)
            results = page.locator('table tbody tr.dxgvDataRow_Bootstrap')
            count = await results.count()
            self.result.add_step(f"Found {count} customer(s) by company name")

            # If multiple results, try to match by email
            if count > 1:
                for i in range(count):
                    row = results.nth(i)
                    row_text = await row.text_content()
                    if email.lower() in row_text.lower():
                        # Get customer code from 2nd column (index 1)
                        customer_code = await row.locator('td').nth(1).text_content()
                        customer_code = customer_code.strip()
                        # Get customer name from 3rd column (index 2) - it's inside an <a> tag
                        customer_name_link = row.locator('td').nth(2).locator('a')
                        customer_name = await customer_name_link.text_content()
                        # Navigate to customer details to get PKID
                        customer_pkid = await self.get_customer_pkid(page, customer_code)
                        self.result.add_step(f"Matched customer by email: {customer_name.strip()} (Code: {customer_code}, ID: {customer_pkid})")
                        return (customer_name.strip(), customer_pkid)

            # Single result or no email match - use first result
            first_row = results.first
            # Get customer code from 2nd column (index 1)
            customer_code = await first_row.locator('td').nth(1).text_content()
            customer_code = customer_code.strip()
            # Get customer name from 3rd column (index 2) inside an <a> tag
            customer_name_link = first_row.locator('td').nth(2).locator('a')
            customer_name = await customer_name_link.text_content()
            # Navigate to customer details to get PKID
            customer_pkid = await self.get_customer_pkid(page, customer_code)
            self.result.add_step(f"Using customer: {customer_name.strip()} (Code: {customer_code}, ID: {customer_pkid})")
            return (customer_name.strip(), customer_pkid)

        # No results by company name - try email search
        self.result.add_step("No results by company name, trying email search")

        # Clear and search by email
        await company_input.clear()
        email_input = page.locator('input[name="Email"], input#Email')
        await email_input.fill(email)
        await page.click('button#AdvancedDisplay')
        await page.wait_for_load_state('networkidle')
        await page.wait_for_timeout(2000)  # Let table update

        # Check if empty data row is present (indicates no results)
        empty_row = page.locator('tr.dxgvEmptyDataRow_Bootstrap, tr#_grdDevEx_DXEmptyRow')
        has_empty_row = await empty_row.count() > 0

        if not has_empty_row:
            # Get actual data rows (rows with class dxgvDataRow_Bootstrap)
            results = page.locator('table tbody tr.dxgvDataRow_Bootstrap')
            first_row = results.first
            # Get customer code from 2nd column (index 1)
            customer_code = await first_row.locator('td').nth(1).text_content()
            customer_code = customer_code.strip()
            # Get customer name from 3rd column (index 2) inside an <a> tag
            customer_name_link = first_row.locator('td').nth(2).locator('a')
            customer_name = await customer_name_link.text_content()
            # Navigate to customer details to get PKID
            customer_pkid = await self.get_customer_pkid(page, customer_code)
            self.result.add_step(f"Found customer by email: {customer_name.strip()} (Code: {customer_code}, ID: {customer_pkid})")
            return (customer_name.strip(), customer_pkid)

        self.result.add_step("Customer not found")
        return None

    async def create_customer(self, page: Page, customer_data: CustomerData) -> tuple[str, str]:
        """
        Create a new customer

        Returns:
            (customer_name, customer_pkid)
        """
        self.result.add_step(f"Creating customer: {customer_data.company_name}")

        # Click Add Customer
        await page.click('a:has-text("Add Customer"), button:has-text("Add Customer")')
        await page.wait_for_load_state('networkidle')

        # Select Wholesale customer group
        group_select = page.locator('select#CustomerGroupPkId')
        await group_select.select_option(label='Wholesale')
        self.result.add_step("Selected 'Wholesale' customer group")

        # Fill in company name (field is called "Descn" - description)
        await page.fill('input#Descn', customer_data.company_name)
        self.result.add_step(f"Filled in company: {customer_data.company_name}")

        # Fill in billing details (first and last name)
        await page.fill('input#BillingDetails_FirstName', customer_data.first_name)
        await page.fill('input#BillingDetails_LastName', customer_data.last_name)
        self.result.add_step(f"Filled in name: {customer_data.first_name} {customer_data.last_name}")

        # Fill in email
        await page.fill('input#emailAddress', customer_data.email)
        self.result.add_step(f"Added email: {customer_data.email}")

        # Fill in phone number (mobile or landline)
        if customer_data.phone:
            if customer_data.is_mobile:
                await page.fill('input#BillingDetails_Mobile', customer_data.phone)
                self.result.add_step(f"Added mobile: {customer_data.phone}")
            else:
                await page.fill('input#BillingDetails_DirectPhone', customer_data.phone)
                self.result.add_step(f"Added phone: {customer_data.phone}")

        # Handle Google Places async address autocomplete
        self.result.add_step(f"Entering address: {customer_data.address}")
        address_input = page.locator('input#BillingDetails_FullAddress')
        await address_input.fill(customer_data.address)

        # Wait for Google Places autocomplete dropdown to appear
        # Google Places uses .pac-container for dropdown and .pac-item for suggestions
        self.result.add_step("Waiting for Google Places suggestions...")
        await page.wait_for_timeout(1500)

        # Wait for and click the first Google Places suggestion
        first_suggestion = page.locator('.pac-container .pac-item').first
        try:
            await first_suggestion.wait_for(state='visible', timeout=5000)
            await first_suggestion.click()
            # Wait for Google Places place_changed event to populate hidden fields (lat/lng, etc)
            await page.wait_for_timeout(1000)
            self.result.add_step("✓ Selected address from Google Places autocomplete")
        except Exception as e:
            self.result.add_step(f"⚠️  Could not select from Google Places dropdown: {str(e)}")
            self.result.add_step("Continuing with typed address (lat/lng may be missing)...")

        # Pause for manual inspection if debugging
        if self.keep_open:
            self.result.add_step("⏸️  PAUSED for inspection - check the form, then click 'Resume' in Playwright Inspector")
            await page.pause()

        # Click Save
        await page.click('button:has-text("Save"), input[value="Save"]')
        await page.wait_for_load_state('networkidle')

        # After creating, we need to find the customer code and get the PKID
        # Navigate back to customer list to search for the customer we just created
        await page.goto(self.CUSTOMERS_URL, wait_until='networkidle')

        # Search for the customer we just created to get the code and PKID
        result = await self.search_customer(page, customer_data.company_name, customer_data.email)
        if not result:
            raise Exception(f"Failed to find customer after creation: {customer_data.company_name}")

        customer_name, customer_pkid = result
        self.result.add_step(f"Customer created: {customer_name} (ID: {customer_pkid})")
        return (customer_name, customer_pkid)

    async def create_user(self, customer_name: str, customer_pkid: str, customer_data: CustomerData) -> bool:
        """
        Create a new user linked to the customer

        Args:
            customer_name: Customer company name
            customer_pkid: Customer primary key ID (GUID)
            customer_data: Customer data from Zendesk

        Returns:
            True if successful
        """
        self.result.add_step(f"Creating user for: {customer_data.email}")

        page = await self.context.new_page()
        try:
            # Navigate directly to the Invite User page
            await page.goto(self.INVITE_USER_URL, wait_until='networkidle')

            # Fill in user details
            await page.fill('input#text-firstName', customer_data.first_name)
            await page.fill('input#text-lastName', customer_data.last_name)
            await page.fill('input#text-email', customer_data.email)
            self.result.add_step(f"Filled in user: {customer_data.first_name} {customer_data.last_name} ({customer_data.email})")

            # Fill in phone number if available
            if customer_data.phone:
                if customer_data.is_mobile:
                    await page.fill('input#text-mobile', customer_data.phone)
                    self.result.add_step(f"Filled mobile: {customer_data.phone}")
                else:
                    await page.fill('input#text-phone', customer_data.phone)
                    self.result.add_step(f"Filled phone: {customer_data.phone}")

            # Select Customers group - there's only one select in this section
            group_select = page.locator('select.form-control').first
            await group_select.select_option(label='Customers')
            self.result.add_step("Selected 'Customers' group")

            # Set the customer PKID directly using JavaScript (it's a hidden field)
            # Also fill the visible customer name field in case it's required for validation
            self.result.add_step(f"Setting customer: {customer_name} (ID: {customer_pkid})")

            # Set the hidden PKID field using JavaScript
            await page.evaluate(f'document.getElementById("customerPkId").value = "{customer_pkid}"')

            # Also fill the visible customer name field (may be needed for form validation)
            customer_input = page.locator('input#customers')
            await customer_input.fill(customer_name)

            self.result.add_step(f"✓ Customer linked successfully")

            # Click Save User button
            await page.click('button#save-button')
            await page.wait_for_load_state('networkidle')

            # User saved - trust the redirect and continue
            current_url = page.url
            self.result.add_step(f"✓ User saved: {customer_data.email}")
            self.result.add_step(f"Redirected to: {current_url}")
            return True

        finally:
            if not self.keep_open:
                await page.close()

    async def add_customer_from_ticket(self, customer_data: CustomerData, org_name: str) -> CustomerAutomationResult:
        """
        Complete workflow to add customer and user from Zendesk ticket data

        Args:
            customer_data: Customer information from Zendesk
            org_name: Buz organization name (e.g., "Watson Blinds", "Designer Drapes")

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
        user_exists, was_reactivated, existing_customer, error_group = await self.check_user_exists(customer_data.email)

        # Handle case where user exists in wrong group
        if error_group:
            self.result.add_step("=== CANNOT PROCEED - Manual intervention required ===")
            raise CustomerAutomationError(
                f"User '{customer_data.email}' already exists in group '{error_group}'. Cannot create as Customer user. Please handle manually.",
                self.result
            )

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

            # Verify we didn't land on org selector (Buz sometimes redirects there)
            await self.verify_not_on_org_selector(page, self.CUSTOMERS_URL, org_name)

            result = await self.search_customer(page, customer_data.company_name, customer_data.email)

            if result:
                customer_name, customer_pkid = result
                self.result.customer_existed = True
                self.result.customer_name = customer_name
                self.result.add_step(f"✓ Customer exists: {customer_name}")
            else:
                # Create customer
                customer_name, customer_pkid = await self.create_customer(page, customer_data)
                self.result.customer_created = True
                self.result.customer_name = customer_name
                self.result.add_step(f"✓ Customer created: {customer_name}")

        finally:
            await page.close()

        # Step 4: Create user
        success = await self.create_user(customer_name, customer_pkid, customer_data)
        if success:
            self.result.user_created = True
            self.result.add_step(f"✓ User created: {customer_data.email}")

        self.result.add_step("=== Workflow Complete ===")
        return self.result


async def add_customer_from_zendesk_ticket(
    ticket_id: int,
    headless: bool = True,
    keep_open: bool = False,
    job_update_callback=None
) -> CustomerAutomationResult:
    """
    High-level function to add customer from Zendesk ticket

    Args:
        ticket_id: Zendesk ticket ID
        headless: Run browser in headless mode
        keep_open: Keep browser open after completion (for debugging)
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

    update(20, f"Ticket parsed. Customer: {customer_data.company_name}, Instances: {', '.join(customer_data.buz_instances)}")

    # Process each Buz instance
    async with BuzCustomerAutomation(headless=headless, keep_open=keep_open) as automation:
        # Wrap the automation to provide progress updates
        original_add_step = automation.result.add_step

        def wrapped_add_step(message: str):
            original_add_step(message)
            # Estimate progress based on steps
            step_count = len(automation.result.steps)
            pct = min(20 + (step_count * 5), 95)
            update(pct, message)

        automation.result.add_step = wrapped_add_step

        # Loop through each instance
        for idx, instance in enumerate(customer_data.buz_instances):
            if idx > 0:
                # Reset some flags for subsequent instances
                automation.result.user_existed = False
                automation.result.user_reactivated = False
                automation.result.customer_existed = False
                automation.result.customer_created = False
                automation.result.user_created = False

            # Switch to the instance
            if idx == 0:
                # First instance: ensure we're in the right org (handles landing on org selector)
                await automation.ensure_correct_organization(instance)
            else:
                # Subsequent instances: just switch normally
                await automation.switch_organization(instance)

            # Run the workflow for this instance
            result = await automation.add_customer_from_ticket(customer_data, instance)

            # If processing multiple instances, continue to the next one
            if idx < len(customer_data.buz_instances) - 1:
                automation.result.add_step(f"--- Moving to next instance ---")

    update(100, "Complete")
    return result
