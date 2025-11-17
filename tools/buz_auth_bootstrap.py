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

        # IMPORTANT: Also visit console1 to ensure cookies are saved for that domain too
        # This is needed for user management operations
        print("\n" + "="*80)
        print(">>> NOW YOU NEED TO NAVIGATE TO THE USER MANAGEMENT PAGE")
        print(">>> In the open browser window, navigate to:")
        print(">>>   https://console1.buzmanager.com/myorg/user-management/users")
        print(">>> (You may need to authenticate again for this domain)")
        print(">>> Once you're on the user list page, come back here and press ENTER")
        print("="*80)
        input("Press ENTER when you're on the console1 user management page... ")

        # Verify they're on the console1 domain
        current_url = page.url
        if "console1.buzmanager.com" in current_url:
            print("✓ Console1 page confirmed!")
        else:
            print(f"⚠️  Warning: Current URL is {current_url}")
            print("⚠️  Make sure you're on console1.buzmanager.com before continuing")
            confirm = input("Continue anyway? (y/n): ")
            if confirm.lower() != 'y':
                print("Aborting. Please navigate to console1 and try again.")
                await browser.close()
                return

        # Save storage state (now includes both go.buzmanager.com and console1.buzmanager.com cookies)
        await ctx.storage_state(path=str(state_path))
        print(f"\n✓ Saved auth state to: {state_path.resolve()}")
        print(f"✓ This account is now configured for: {account_name}")
        print(f"✓ Cookies saved for both go.buzmanager.com and console1.buzmanager.com")
        await browser.close()


if __name__ == "__main__":
    account_name = sys.argv[1] if len(sys.argv) > 1 else "default"
    if account_name == "default":
        print("WARNING: No account name specified. Use: python tools/buz_auth_bootstrap.py <account_name>")
        print("Example: python tools/buz_auth_bootstrap.py watsonblinds")
        print("Falling back to 'default' account name...")
    asyncio.run(main(account_name))
