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
        # Wait until we're **back** on the app (not the identity host)
        await page.wait_for_url(lambda url: url.startswith("https://go.buzmanager.com/"), timeout=120_000)
        # Ensure org cookie set by visiting a page inside the app
        await page.goto(START_URL)
        # Save storage state
        await ctx.storage_state(path=str(state_path))
        print(f"\n✓ Saved auth state to: {state_path.resolve()}")
        print(f"✓ This account is now configured for: {account_name}")
        await browser.close()


if __name__ == "__main__":
    account_name = sys.argv[1] if len(sys.argv) > 1 else "default"
    if account_name == "default":
        print("WARNING: No account name specified. Use: python tools/buz_auth_bootstrap.py <account_name>")
        print("Example: python tools/buz_auth_bootstrap.py watsonblinds")
        print("Falling back to 'default' account name...")
    asyncio.run(main(account_name))
