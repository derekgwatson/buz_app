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

        # Console authentication needs manual intervention
        print("\n" + "="*80)
        print(">>> MANUAL STEP REQUIRED TO CAPTURE CONSOLE AUTHENTICATION")
        print("="*80)
        print(">>> In the browser window, please:")
        print(">>>   1. Navigate to Settings > Users (in the Buz menu)")
        print(">>>   2. If prompted with another login, complete the authentication")
        print(">>>   3. Wait for the user management page to fully load")
        print(">>>")
        print(">>> Once you see the user table on the screen, return here.")
        print("="*80)

        # Wait for user confirmation
        input("\n>>> Press ENTER when you're ready to continue (after navigating to Users page)... ")

        print(">>> Checking for user table...")
        try:
            # Give a generous timeout since user just confirmed
            await page.wait_for_selector('table#userListTable', state='visible', timeout=10000)
            print(f">>> ✓ Console authentication successful!")
            print(f">>> Current URL: {page.url}")
        except Exception as e:
            print(f">>> ❌ ERROR: Could not find user table!")
            print(f">>> Current URL: {page.url}")
            print(f">>> Make sure you've navigated to the Users page and can see the user list.")
            raise Exception(f"Console authentication verification failed - user table not found at {page.url}")

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
