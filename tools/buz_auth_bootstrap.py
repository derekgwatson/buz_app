# tools/buz_auth_bootstrap.py
from __future__ import annotations
import asyncio
import sys
from pathlib import Path
from playwright.async_api import async_playwright


START_URL = "https://go.buzmanager.com/Settings/Inventory"  # lands you in the app after login


async def main(account_name: str = "default") -> None:
    """
    Bootstrap Buz authentication for a specific account.

    Usage:
        python tools/buz_auth_bootstrap.py watsonblinds
        python tools/buz_auth_bootstrap.py designerdrapes

    This will save separate storage state files for each account:
        .secrets/buz_storage_state_watsonblinds.json
        .secrets/buz_storage_state_designerdrapes.json
    """
    state_path = Path(f".secrets/buz_storage_state_{account_name}.json")
    state_path.parent.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # show the window for MFA etc.
        ctx = await browser.new_context(accept_downloads=True)
        page = await ctx.new_page()
        # Go to app; it will bounce you to the identity login automatically
        await page.goto(START_URL)
        print(f">>> Log in as the {account_name} user in the opened window.")
        print(">>> Approve MFA if prompted.")
        print(f">>> Make sure you're logged in to the correct account for: {account_name}")

        # Wait for login to complete - could land on org selector OR directly in app
        print(">>> Waiting for login to complete...")
        await page.wait_for_url(
            lambda url: url.startswith("https://go.buzmanager.com/") or "mybuz/organizations" in url,
            timeout=120_000
        )

        # Check if we landed on org selector
        if "mybuz/organizations" in page.url:
            print("\n>>> You're on the organization selector page.")
            print(f">>> CLICK THE ORGANIZATION for {account_name} in the browser window")
            print(">>> (This selects which org this account will always use)")
            # Wait until they've selected an org and are in the app
            await page.wait_for_url(lambda url: url.startswith("https://go.buzmanager.com/"), timeout=120_000)
            print(">>> Org selected! Continuing...")

        # Ensure org cookie set by visiting a page inside the app
        await page.goto(START_URL)
        await page.wait_for_load_state('networkidle')

        # Also visit the console domain to capture those auth cookies
        # The user management page is on console1.buzmanager.com which requires separate auth
        print(">>> Visiting console domain to capture authentication...")

        # Try going to the console URL via the go.buzmanager redirect
        await page.goto("https://go.buzmanager.com/Settings/Users", wait_until='networkidle', timeout=60000)
        print(f">>> After redirect, landed at: {page.url}")

        # Check if we're on the console domain
        if "console" not in page.url:
            print(f">>> ⚠️  Warning: Not on console domain yet, at: {page.url}")

        # Wait for the page to fully load and user table to appear
        print(">>> Waiting for user table to confirm authentication...")
        try:
            await page.wait_for_selector('table#userListTable', state='visible', timeout=15000)
            print(">>> ✓ Console authentication successful - user table found!")
        except Exception as e:
            print(f">>> ❌ ERROR: Could not find user table!")
            print(f">>> Current URL: {page.url}")
            print(f">>> This means console authentication likely FAILED")
            print(f">>> You may need to manually visit the console page in the browser window")
            raise Exception(f"Console authentication failed - user table not found at {page.url}")

        # Save storage state (now includes both go.buzmanager.com and console auth)
        await ctx.storage_state(path=str(state_path))
        print(f"\n✓ Saved auth state to: {state_path.resolve()}")
        print(f"✓ This account is now configured for: {account_name}")
        print(f"✓ Includes authentication for both go.buzmanager.com and console1.buzmanager.com")
        await browser.close()


if __name__ == "__main__":
    account_name = sys.argv[1] if len(sys.argv) > 1 else "default"
    if account_name == "default":
        print("WARNING: No account name specified. Use: python tools/buz_auth_bootstrap.py <account_name>")
        print("Example: python tools/buz_auth_bootstrap.py watsonblinds")
        print("Falling back to 'default' account name...")
    asyncio.run(main(account_name))
