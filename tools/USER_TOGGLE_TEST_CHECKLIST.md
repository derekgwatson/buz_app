# User Toggle Testing Checklist

Test all scenarios systematically using the web UI at `/tools/user-management`

**Test User:** `a.piggott@live.com` (or choose your own)
**Test Org:** Canberra

---

## Normal Operations (4 tests)
Cache matches Buz - testing actual toggle functionality

### ☐ 1. Activate Inactive Employee
**Setup:**
- In Buz: Set user to INACTIVE ✗
- In UI: Refresh page (cache should show Inactive ✗)

**Action:** Click badge → Apply Changes (uncheck Headless to watch)

**Verify:**
- ✓ Buz: Now Active ✓
- ✓ UI: Badge changes to ✓ (without refresh)
- ✓ Cache: After refresh, shows Active ✓

---

### ☐ 2. Deactivate Active Employee
**Setup:**
- In Buz: Set user to ACTIVE ✓
- In UI: Refresh page (cache should show Active ✓)

**Action:** Click badge → Apply Changes

**Verify:**
- ✓ Buz: Now Inactive ✗
- ✓ UI: Badge changes to ✗ (without refresh)
- ✓ Cache: After refresh, shows Inactive ✗

---

### ☐ 3. Activate Inactive Customer
**Setup:**
- In Buz: Set user to INACTIVE ✗ (ensure they're a customer!)
- In UI: Refresh page (cache should show Inactive ✗)

**Action:** Click badge → Apply Changes

**Verify:**
- ✓ Buz: Now Active ✓
- ✓ UI: Badge changes to ✓ (without refresh)
- ✓ Cache: After refresh, shows Active ✓

---

### ☐ 4. Deactivate Active Customer
**Setup:**
- In Buz: Set user to ACTIVE ✓ (ensure they're a customer!)
- In UI: Refresh page (cache should show Active ✓)

**Action:** Click badge → Apply Changes

**Verify:**
- ✓ Buz: Now Inactive ✗
- ✓ UI: Badge changes to ✗ (without refresh)
- ✓ Cache: After refresh, shows Inactive ✗

---

## Stale Cache Detection (4 tests)
Cache doesn't match Buz - testing stale cache handling

### ☐ 5. Activate Active Employee (Stale Cache)
**Setup:**
- In Buz: Manually set user to ACTIVE ✓
- In UI: **DON'T refresh** - leave cache showing Inactive ✗ (WRONG)

**Action:** Click badge (showing ✗) → Apply Changes

**Expected Result:**
- Operation reports "Already active (cache stale)"
- Buz: UNCHANGED (still Active ✓)
- UI: Badge changes to ✓ (cache updated)
- Cache: After refresh, shows Active ✓

**Verify:**
- ✓ Buz: Still Active ✓ (no toggle occurred)
- ✓ UI: Badge updated to ✓
- ✓ Cache: After refresh, shows Active ✓
- ✓ Message: Indicated stale cache detected

---

### ☐ 6. Deactivate Inactive Employee (Stale Cache)
**Setup:**
- In Buz: Manually set user to INACTIVE ✗
- In UI: **DON'T refresh** - leave cache showing Active ✓ (WRONG)

**Action:** Click badge (showing ✓) → Apply Changes

**Expected Result:**
- Operation reports "Already inactive (cache stale)"
- Buz: UNCHANGED (still Inactive ✗)
- UI: Badge changes to ✗ (cache updated)
- Cache: After refresh, shows Inactive ✗

**Verify:**
- ✓ Buz: Still Inactive ✗ (no toggle occurred)
- ✓ UI: Badge updated to ✗
- ✓ Cache: After refresh, shows Inactive ✗
- ✓ Message: Indicated stale cache detected

---

### ☐ 7. Activate Active Customer (Stale Cache)
**Setup:**
- In Buz: Manually set user to ACTIVE ✓
- In UI: **DON'T refresh** - leave cache showing Inactive ✗ (WRONG)

**Action:** Click badge (showing ✗) → Apply Changes

**Expected Result:**
- Operation reports "Already active (cache stale)"
- Buz: UNCHANGED (still Active ✓)
- UI: Badge changes to ✓ (cache updated)
- Cache: After refresh, shows Active ✓

**Verify:**
- ✓ Buz: Still Active ✓ (no toggle occurred)
- ✓ UI: Badge updated to ✓
- ✓ Cache: After refresh, shows Active ✓
- ✓ Message: Indicated stale cache detected

---

### ☐ 8. Deactivate Inactive Customer (Stale Cache)
**Setup:**
- In Buz: Manually set user to INACTIVE ✗
- In UI: **DON'T refresh** - leave cache showing Active ✓ (WRONG)

**Action:** Click badge (showing ✓) → Apply Changes

**Expected Result:**
- Operation reports "Already inactive (cache stale)"
- Buz: UNCHANGED (still Inactive ✗)
- UI: Badge changes to ✗ (cache updated)
- Cache: After refresh, shows Inactive ✗

**Verify:**
- ✓ Buz: Still Inactive ✗ (no toggle occurred)
- ✓ UI: Badge updated to ✗
- ✓ Cache: After refresh, shows Inactive ✗
- ✓ Message: Indicated stale cache detected

---

## Testing Tips

1. **Keep browser console open (F12)** - watch for errors or debug logs
2. **Uncheck "Headless" checkbox** before Apply Changes - so you can watch the Playwright browser
3. **Take notes** on any failures - which verifications failed?
4. **Test employees first** - they're simpler (no org-specific customer filtering)
5. **Use same test user** throughout - easier to track state

## Common Issues to Watch For

- ❌ UI badge doesn't update after toggle (should update immediately)
- ❌ Cache shows wrong state after refresh (cache not being updated)
- ❌ Buz toggle fails (Playwright can't find/click toggle)
- ❌ "User not found" errors (stale cache detection failing)
- ❌ Database lock errors (connection management issue)
