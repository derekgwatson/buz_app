#!/usr/bin/env python
"""
Interactive test harness for user toggle functionality.
Runs scenarios and prompts for manual verification.

Usage:
    python tools/test_user_toggle.py
"""
import asyncio
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.buz_user_management import batch_toggle_users_for_org

# Test scenarios covering all combinations
# is_active = what the CACHE/UI shows (not Buz reality)
# stale_cache = whether this scenario tests stale cache detection
TEST_SCENARIOS = [
    # NORMAL OPERATIONS (cache matches Buz)
    {"name": "Activate inactive employee", "user_type": "employee", "is_active": False, "action": "activate", "stale_cache": False},
    {"name": "Deactivate active employee", "user_type": "employee", "is_active": True, "action": "deactivate", "stale_cache": False},
    {"name": "Activate inactive customer", "user_type": "customer", "is_active": False, "action": "activate", "stale_cache": False},
    {"name": "Deactivate active customer", "user_type": "customer", "is_active": True, "action": "deactivate", "stale_cache": False},

    # STALE CACHE DETECTION (cache doesn't match Buz)
    {"name": "Activate active employee (stale cache)", "user_type": "employee", "is_active": False, "action": "activate", "stale_cache": True},
    {"name": "Deactivate inactive employee (stale cache)", "user_type": "employee", "is_active": True, "action": "deactivate", "stale_cache": True},
    {"name": "Activate active customer (stale cache)", "user_type": "customer", "is_active": False, "action": "activate", "stale_cache": True},
    {"name": "Deactivate inactive customer (stale cache)", "user_type": "customer", "is_active": True, "action": "deactivate", "stale_cache": True},
]


async def run_test_scenario(scenario, test_user, org_key):
    """
    Run a single test scenario with manual verification prompts.

    Returns:
        dict with test results and verification status
    """
    print(f"\n{'='*80}")
    print(f"TEST: {scenario['name']}")
    print(f"{'='*80}")
    print(f"User: {test_user}")
    print(f"Type: {scenario['user_type']}")
    print()

    # Determine expected states based on scenario type
    if scenario['stale_cache']:
        # Stale cache: Buz reality differs from cache
        cache_state = scenario['is_active']
        buz_state = not cache_state  # Opposite of cache
        expected_result = "Detect stale cache, report already in desired state, update cache"
        should_toggle_in_buz = False
    else:
        # Normal: Buz and cache match
        cache_state = scenario['is_active']
        buz_state = cache_state  # Same as cache
        expected_result = f"Toggle user to {'Active' if scenario['action'] == 'activate' else 'Inactive'}"
        should_toggle_in_buz = True

    # Print setup instructions
    print("SETUP INSTRUCTIONS:")
    print(f"{'='*80}")
    if scenario['stale_cache']:
        print(f"1. In Buz: Manually set user to {'ACTIVE ✓' if buz_state else 'INACTIVE ✗'}")
        print(f"2. In UI: DON'T refresh cache - leave it showing {'Active ✓' if cache_state else 'Inactive ✗'} (WRONG)")
        print(f"   (This creates a stale cache scenario)")
    else:
        print(f"1. In Buz: Manually set user to {'ACTIVE ✓' if buz_state else 'INACTIVE ✗'}")
        print(f"2. In UI: Refresh the page to update cache (should now show {'Active ✓' if cache_state else 'Inactive ✗'})")
        print(f"   (This ensures cache matches Buz)")

    print()
    print("EXPECTED BEHAVIOR:")
    print(f"  Cache shows: {'Active ✓' if cache_state else 'Inactive ✗'}")
    print(f"  Buz reality: {'Active ✓' if buz_state else 'Inactive ✗'}")
    print(f"  Action: {scenario['action'].upper()}")
    print(f"  Result: {expected_result}")
    print(f"{'='*80}")
    print()

    input("Press ENTER when setup is complete and you're ready to run the test...")

    # Run the toggle (headed mode so user can watch)
    print("\n>>> Running toggle operation (watch the browser window)...")
    try:
        result = await batch_toggle_users_for_org(
            org_key=org_key,
            user_changes=[{
                'user_email': test_user,
                'is_active': scenario['is_active'],
                'user_type': scenario['user_type']
            }],
            headless=False  # Always show browser for testing
        )

        print(f"\n>>> Toggle completed!")
        print(f"    Success: {result[0]['success']}")
        print(f"    Message: {result[0]['message']}")
        if result[0]['success']:
            print(f"    New state: {'Active' if result[0]['new_state'] else 'Inactive'}")

    except Exception as e:
        print(f"\n>>> ✗ ERROR during toggle: {e}")
        return {
            "scenario": scenario['name'],
            "error": str(e),
            "success": False,
            "buz_correct": False,
            "ui_correct": False,
            "cache_correct": False,
            "all_pass": False
        }

    # Manual verification prompts
    print(f"\n{'='*80}")
    print("MANUAL VERIFICATION")
    print(f"{'='*80}")

    # Calculate expected final state
    if scenario['stale_cache']:
        # Stale cache: Buz shouldn't change, cache should update
        expected_buz_final = buz_state  # No change in Buz
        expected_cache_final = buz_state  # Cache updates to match Buz
    else:
        # Normal: Buz should toggle, cache should match
        expected_buz_final = scenario['action'] == 'activate'
        expected_cache_final = expected_buz_final

    print(f"Expected final Buz state: {'Active ✓' if expected_buz_final else 'Inactive ✗'}")
    print(f"Expected final Cache state: {'Active ✓' if expected_cache_final else 'Inactive ✗'}")
    print()

    # Verify Buz
    buz_correct = input(f"1. Check Buz - is user {'Active ✓' if expected_buz_final else 'Inactive ✗'}? (y/n): ").lower() == 'y'

    # Verify UI (without refresh)
    ui_correct = input(f"2. Check UI (DON'T refresh) - does badge show {'✓' if expected_cache_final else '✗'}? (y/n): ").lower() == 'y'

    # Verify Cache (after refresh)
    print(f"\n3. Now refresh the page to reload from cache...")
    input("   Press ENTER after page has refreshed...")
    cache_correct = input(f"   Does cache show {'Active ✓' if expected_cache_final else 'Inactive ✗'}? (y/n): ").lower() == 'y'

    all_pass = result[0]['success'] and buz_correct and ui_correct and cache_correct

    if all_pass:
        print("\n✓ TEST PASSED")
    else:
        print("\n✗ TEST FAILED")
        if not result[0]['success']:
            print("  - Toggle operation failed")
        if not buz_correct:
            print("  - Buz state incorrect")
        if not ui_correct:
            print("  - UI badge incorrect")
        if not cache_correct:
            print("  - Cache state incorrect")

    return {
        "scenario": scenario['name'],
        "success": result[0]['success'],
        "message": result[0]['message'],
        "stale_cache": scenario['stale_cache'],
        "expected_buz_final": expected_buz_final,
        "expected_cache_final": expected_cache_final,
        "should_toggle_in_buz": should_toggle_in_buz,
        "buz_correct": buz_correct,
        "ui_correct": ui_correct,
        "cache_correct": cache_correct,
        "all_pass": all_pass
    }


async def main():
    """Run all test scenarios"""
    print("="*80)
    print("USER TOGGLE TEST HARNESS")
    print("="*80)
    print()
    print("This tool will guide you through testing all user toggle scenarios.")
    print("You'll manually verify each result in Buz, the UI, and the cache.")
    print()

    # Get test configuration
    test_user = input("Enter test user email (e.g., test.user@watsonblinds.com.au): ").strip()
    if not test_user:
        print("Error: User email is required")
        return

    org_key = input("Enter org key (canberra/tweed/dd/bay/shoalhaven/wagga) [canberra]: ").strip() or "canberra"
    valid_orgs = ['canberra', 'tweed', 'dd', 'bay', 'shoalhaven', 'wagga']
    if org_key not in valid_orgs:
        print(f"Error: Invalid org. Must be one of: {', '.join(valid_orgs)}")
        return

    print(f"\nRunning tests for: {test_user} in {org_key}")
    print(f"Number of scenarios: {len(TEST_SCENARIOS)}")
    print()

    run_all = input("Run all scenarios automatically? (y/n) [y]: ").lower()
    run_all = run_all != 'n'

    results = []

    for i, scenario in enumerate(TEST_SCENARIOS, 1):
        if not run_all:
            cont = input(f"\nRun scenario {i}/{len(TEST_SCENARIOS)}: {scenario['name']}? (y/n/q) [y]: ").lower()
            if cont == 'q':
                print("Quitting...")
                break
            if cont == 'n':
                print("Skipping...")
                continue

        result = await run_test_scenario(scenario, test_user, org_key)
        results.append(result)

    # Print summary
    print(f"\n\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    print(f"Total tests run: {len(results)}")
    print(f"Passed: {sum(1 for r in results if r['all_pass'])}")
    print(f"Failed: {sum(1 for r in results if not r['all_pass'])}")
    print()

    for r in results:
        status = "✓ PASS" if r['all_pass'] else "✗ FAIL"
        print(f"{status}: {r['scenario']}")
        if not r['all_pass']:
            issues = []
            if not r.get('success', False):
                issues.append(f"Toggle failed: {r.get('message', 'Unknown error')}")
            if not r['buz_correct']:
                issues.append("Buz state incorrect")
            if not r['ui_correct']:
                issues.append("UI badge incorrect")
            if not r['cache_correct']:
                issues.append("Cache state incorrect")
            for issue in issues:
                print(f"      - {issue}")

    print(f"\n{'='*80}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nTest harness interrupted by user")
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
