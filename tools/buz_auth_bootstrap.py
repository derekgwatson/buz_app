# tools/buz_auth_bootstrap.py
from __future__ import annotations
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright


STATE_PATH = Path(".secrets/buz_storage_state.json")  # will contain cookies for both domains
START_URL = "https://go.buzmanager.com/Settings/Inventory"  # lands you in the app after login


async def main() -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # show the window for MFA etc.
        ctx = await browser.new_context(accept_downloads=True)
        page = await ctx.new_page()
        # Go to app; it will bounce you to the identity login automatically
        await page.goto(START_URL)
        print(">>> Log in as normal in the opened window. Approve MFA if prompted.")
        # Wait until we're **back** on the app (not the identity host)
        await page.wait_for_url(lambda url: url.startswith("https://go.buzmanager.com/"), timeout=120_000)
        # Ensure org cookie set by visiting a page inside the app
        await page.goto(START_URL)
        # Save storage state
        await ctx.storage_state(path=str(STATE_PATH))
        print(f"Saved auth state to: {STATE_PATH.resolve()}")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
